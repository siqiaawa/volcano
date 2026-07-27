/*
Copyright 2025 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package hypernode

import (
	"fmt"
	"sort"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	topologyv1alpha1 "volcano.sh/apis/pkg/apis/topology/v1alpha1"
	vcclientset "volcano.sh/apis/pkg/client/clientset/versioned"
	vcinformer "volcano.sh/apis/pkg/client/informers/externalversions"
	topologyinformerv1alpha1 "volcano.sh/apis/pkg/client/informers/externalversions/topology/v1alpha1"
	topologylisterv1alpha1 "volcano.sh/apis/pkg/client/listers/topology/v1alpha1"
	"volcano.sh/volcano/pkg/controllers/framework"
	"volcano.sh/volcano/pkg/controllers/hypernode/api"
	"volcano.sh/volcano/pkg/controllers/hypernode/config"
	"volcano.sh/volcano/pkg/controllers/hypernode/discovery"
	"volcano.sh/volcano/pkg/controllers/hypernode/utils"
)

func init() {
	framework.RegisterController(&hyperNodeController{})
}

const (
	name = "hyperNode-controller"
)

type hyperNodeController struct {
	vcClient          vcclientset.Interface
	kubeClient        kubernetes.Interface
	vcInformerFactory vcinformer.SharedInformerFactory
	informerFactory   informers.SharedInformerFactory

	hyperNodeInformer topologyinformerv1alpha1.HyperNodeInformer
	hyperNodeLister   topologylisterv1alpha1.HyperNodeLister
	hyperNodeQueue    workqueue.TypedRateLimitingInterface[string]
	nodeLister        listersv1.NodeLister

	configMapInformer coreinformers.ConfigMapInformer
	configMapLister   listersv1.ConfigMapLister
	configMapQueue    workqueue.TypedRateLimitingInterface[string]

	discoveryManager     discovery.Manager
	discoveryResultQueue workqueue.TypedRateLimitingInterface[*discovery.Result]
	discoveryWatchWG     sync.WaitGroup
	discoveryWorkerWG    sync.WaitGroup
	configMapNamespace   string
	configMapName        string
}

// Run starts the hyperNode controller
func (hn *hyperNodeController) Run(stopCh <-chan struct{}) {
	hn.vcInformerFactory.Start(stopCh)
	hn.informerFactory.Start(stopCh)
	for informerType, ok := range hn.informerFactory.WaitForCacheSync(stopCh) {
		if !ok {
			klog.ErrorS(nil, "Failed to sync informer cache: %v", informerType)
			return
		}
	}
	for informerType, ok := range hn.vcInformerFactory.WaitForCacheSync(stopCh) {
		if !ok {
			klog.ErrorS(nil, "Failed to sync informer cache", "informerType", informerType)
			return
		}
	}

	if err := hn.discoveryManager.Start(); err != nil {
		klog.ErrorS(err, "Failed to start network topology discovery manager")
		return
	}
	if hn.discoveryResultQueue == nil {
		hn.discoveryResultQueue = workqueue.NewTypedRateLimitingQueue(
			workqueue.DefaultTypedControllerRateLimiter[*discovery.Result]())
	}
	hn.discoveryWorkerWG.Add(1)
	go func() {
		defer hn.discoveryWorkerWG.Done()
		hn.processDiscoveryResults()
	}()
	hn.discoveryWatchWG.Add(1)
	go func() {
		defer hn.discoveryWatchWG.Done()
		hn.watchDiscoveryResults()
	}()

	// Start HyperNode queue processor
	go hn.processHyperNodeQueue()

	klog.InfoS("HyperNode controller started")
	<-stopCh
	hn.discoveryManager.Stop()
	hn.discoveryWatchWG.Wait()
	hn.discoveryResultQueue.ShutDownWithDrain()
	hn.discoveryWorkerWG.Wait()
	hn.hyperNodeQueue.ShutDown()
	klog.InfoS("HyperNode controller stopped")
}

// Name returns the name of the controller
func (hn *hyperNodeController) Name() string {
	return name
}

// Initialize initializes the hyperNode controller
func (hn *hyperNodeController) Initialize(opt *framework.ControllerOption) error {
	hn.vcClient = opt.VolcanoClient
	hn.kubeClient = opt.KubeClient
	hn.vcInformerFactory = opt.VCSharedInformerFactory
	hn.informerFactory = opt.SharedInformerFactory

	hn.hyperNodeInformer = hn.vcInformerFactory.Topology().V1alpha1().HyperNodes()
	hn.hyperNodeLister = hn.hyperNodeInformer.Lister()
	hn.hyperNodeQueue = workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	hn.nodeLister = hn.informerFactory.Core().V1().Nodes().Lister()

	hn.setConfigMapNamespaceAndName()
	hn.setupConfigMapInformer()
	hn.configMapLister = hn.configMapInformer.Lister()
	hn.configMapQueue = workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	hn.discoveryResultQueue = workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[*discovery.Result]())

	configLoader := config.NewConfigLoader(
		hn.configMapLister,
		hn.configMapNamespace,
		hn.configMapName,
	)

	hn.discoveryManager = discovery.NewManager(configLoader, hn.configMapQueue, hn.kubeClient, hn.vcClient)

	// Add event handlers for HyperNode
	hn.hyperNodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    hn.addHyperNode,
		UpdateFunc: hn.updateHyperNode,
	})

	return nil
}

func (hn *hyperNodeController) watchDiscoveryResults() {
	resultCh := hn.discoveryManager.ResultChannel()
	klog.InfoS("Starting to watch discovery results")
	for result := range resultCh {
		resultCopy := result
		hn.discoveryResultQueue.Add(&resultCopy)
	}
	klog.InfoS("Discovery result channel closed")
}

func (hn *hyperNodeController) processDiscoveryResults() {
	for hn.processNextDiscoveryResult() {
	}
}

func (hn *hyperNodeController) processNextDiscoveryResult() bool {
	result, shutdown := hn.discoveryResultQueue.Get()
	if shutdown {
		return false
	}
	defer hn.discoveryResultQueue.Done(result)

	if result.HyperNodes == nil || !result.IsCurrent() {
		result.Ack()
		hn.discoveryResultQueue.Forget(result)
		return true
	}

	if err := hn.reconcileTopology(result.Source, result.HyperNodes); err != nil {
		hn.discoveryResultQueue.AddRateLimited(result)
		klog.ErrorS(err, "Failed to reconcile discovered HyperNodes; will retry",
			"source", result.Source, "generation", result.Generation)
		return true
	}

	result.Ack()
	hn.discoveryResultQueue.Forget(result)
	return true
}

// reconcileTopology reconciles the discovered topology with existing HyperNode resources
func (hn *hyperNodeController) reconcileTopology(source string, discoveredNodes []*topologyv1alpha1.HyperNode) error {
	klog.InfoS("Starting topology reconciliation", "source", source, "discoveredNodeCount", len(discoveredNodes))

	existingNodes, err := hn.hyperNodeLister.List(labels.SelectorFromSet(labels.Set{
		api.NetworkTopologySourceLabelKey: source,
	}))
	if err != nil {
		return fmt.Errorf("list existing HyperNodes for source %s: %w", source, err)
	}

	existingNodeMap := make(map[string]*topologyv1alpha1.HyperNode)
	for _, node := range existingNodes {
		existingNodeMap[node.Name] = node
	}

	discoveredNodeMap := make(map[string]*topologyv1alpha1.HyperNode, len(discoveredNodes))
	for _, node := range discoveredNodes {
		if node == nil {
			return fmt.Errorf("discovered HyperNode for source %s is nil", source)
		}
		node = node.DeepCopy()
		if node.Labels == nil {
			node.Labels = make(map[string]string)
		}
		node.Labels[api.NetworkTopologySourceLabelKey] = source
		if _, exists := discoveredNodeMap[node.Name]; exists {
			return fmt.Errorf("discovered duplicate HyperNode %q for source %s", node.Name, source)
		}
		discoveredNodeMap[node.Name] = node
	}

	orderedDiscovered := make([]*topologyv1alpha1.HyperNode, 0, len(discoveredNodeMap))
	for _, node := range discoveredNodeMap {
		orderedDiscovered = append(orderedDiscovered, node)
	}
	sort.Slice(orderedDiscovered, func(i, j int) bool {
		if orderedDiscovered[i].Spec.Tier != orderedDiscovered[j].Spec.Tier {
			return orderedDiscovered[i].Spec.Tier < orderedDiscovered[j].Spec.Tier
		}
		return orderedDiscovered[i].Name < orderedDiscovered[j].Name
	})

	for _, node := range orderedDiscovered {
		name := node.Name
		_, exists := existingNodeMap[name]
		if !exists {
			klog.InfoS("Creating new HyperNode", "name", name, "source", source)
			if err := utils.CreateHyperNode(hn.vcClient, node); err != nil {
				if !apierrors.IsAlreadyExists(err) {
					return fmt.Errorf("create HyperNode %s: %w", name, err)
				}
				if err := utils.UpdateHyperNode(hn.vcClient, node); err != nil {
					return fmt.Errorf("update concurrently created HyperNode %s: %w", name, err)
				}
			}
		} else {
			klog.InfoS("Updating HyperNode", "name", name, "source", source)
			if err := utils.UpdateHyperNode(hn.vcClient, node); err != nil {
				if !apierrors.IsNotFound(err) {
					return fmt.Errorf("update HyperNode %s: %w", name, err)
				}
				if err := utils.CreateHyperNode(hn.vcClient, node); err != nil {
					return fmt.Errorf("recreate concurrently deleted HyperNode %s: %w", name, err)
				}
			}
		}

		delete(existingNodeMap, name)
	}

	orderedDeletes := make([]*topologyv1alpha1.HyperNode, 0, len(existingNodeMap))
	for _, node := range existingNodeMap {
		orderedDeletes = append(orderedDeletes, node)
	}
	sort.Slice(orderedDeletes, func(i, j int) bool {
		if orderedDeletes[i].Spec.Tier != orderedDeletes[j].Spec.Tier {
			return orderedDeletes[i].Spec.Tier > orderedDeletes[j].Spec.Tier
		}
		return orderedDeletes[i].Name < orderedDeletes[j].Name
	})
	for _, node := range orderedDeletes {
		klog.InfoS("Deleting HyperNode", "name", node.Name, "source", source)
		if err := utils.DeleteHyperNode(hn.vcClient, node.Name); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete HyperNode %s: %w", node.Name, err)
		}
	}

	klog.InfoS("Topology reconciliation completed",
		"source", source,
		"discovered", len(discoveredNodes),
		"created/updated", len(discoveredNodeMap),
		"deleted", len(existingNodeMap))
	return nil
}
