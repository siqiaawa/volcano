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
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/util/workqueue"

	topologyv1alpha1 "volcano.sh/apis/pkg/apis/topology/v1alpha1"
	vcclientset "volcano.sh/apis/pkg/client/clientset/versioned/fake"
	vcinformer "volcano.sh/apis/pkg/client/informers/externalversions"
	"volcano.sh/volcano/pkg/controllers/framework"
	"volcano.sh/volcano/pkg/controllers/hypernode/api"
	"volcano.sh/volcano/pkg/controllers/hypernode/config"
	"volcano.sh/volcano/pkg/controllers/hypernode/discovery"
)

type mockDiscoveryManager struct {
	startCalled bool
	stopCalled  bool
	resultCh    chan discovery.Result

	mu sync.Mutex
}

func (m *mockDiscoveryManager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.startCalled = true
	return nil
}

func (m *mockDiscoveryManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.stopCalled = true
	close(m.resultCh)
}

func (m *mockDiscoveryManager) ResultChannel() <-chan discovery.Result {
	return m.resultCh
}

func TestHyperNodeController_Run(t *testing.T) {
	stopCh := make(chan struct{})

	fakeVcClient := vcclientset.NewSimpleClientset()
	fakeKubeClient := k8sfake.NewSimpleClientset()

	existingHyperNodes := []*topologyv1alpha1.HyperNode{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "existing-node-1",
				Labels: map[string]string{
					api.NetworkTopologySourceLabelKey: "ufm",
				},
			},
			Spec: topologyv1alpha1.HyperNodeSpec{
				Members: []topologyv1alpha1.MemberSpec{
					{
						Type: topologyv1alpha1.MemberTypeNode,
						Selector: topologyv1alpha1.MemberSelector{
							ExactMatch: &topologyv1alpha1.ExactMatch{Name: "existing-node-1"},
						},
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "existing-node-2",
				Labels: map[string]string{
					api.NetworkTopologySourceLabelKey: "ufm",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "existing-node-3",
				Labels: map[string]string{
					api.NetworkTopologySourceLabelKey: "roce",
				},
			},
		},
	}

	for _, node := range existingHyperNodes {
		_, err := fakeVcClient.TopologyV1alpha1().HyperNodes().Create(context.TODO(), node, metav1.CreateOptions{})
		assert.NoError(t, err, "Should be able to create the existing HyperNode")
	}

	vcInformerFactory := vcinformer.NewSharedInformerFactory(fakeVcClient, 0)
	kubeInformerFactory := informers.NewSharedInformerFactory(fakeKubeClient, 0)

	mockManager := &mockDiscoveryManager{
		resultCh: make(chan discovery.Result),
	}

	controller := &hyperNodeController{
		vcClient:           fakeVcClient,
		kubeClient:         fakeKubeClient,
		vcInformerFactory:  vcInformerFactory,
		informerFactory:    kubeInformerFactory,
		hyperNodeInformer:  vcInformerFactory.Topology().V1alpha1().HyperNodes(),
		hyperNodeLister:    vcInformerFactory.Topology().V1alpha1().HyperNodes().Lister(),
		configMapInformer:  kubeInformerFactory.Core().V1().ConfigMaps(),
		configMapLister:    kubeInformerFactory.Core().V1().ConfigMaps().Lister(),
		discoveryManager:   mockManager,
		configMapNamespace: "test-namespace",
		configMapName:      "test-release-controller-configmap",
		hyperNodeQueue:     workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
	}

	go controller.Run(stopCh)

	time.Sleep(time.Second)
	assert.True(t, func() bool { mockManager.mu.Lock(); defer mockManager.mu.Unlock(); return mockManager.startCalled }(), "Discovery manager should be started")

	// phase1: update and create hypernode
	go func() {
		updatedHyperNode := &topologyv1alpha1.HyperNode{
			ObjectMeta: metav1.ObjectMeta{
				Name: "existing-node-1",
			},
			Spec: topologyv1alpha1.HyperNodeSpec{
				Members: []topologyv1alpha1.MemberSpec{
					{
						Type: topologyv1alpha1.MemberTypeNode,
						Selector: topologyv1alpha1.MemberSelector{
							ExactMatch: &topologyv1alpha1.ExactMatch{Name: "updated-node-1"},
						},
					},
				},
			},
		}

		newHyperNode := &topologyv1alpha1.HyperNode{
			ObjectMeta: metav1.ObjectMeta{
				Name: "new-hypernode",
			},
			Spec: topologyv1alpha1.HyperNodeSpec{},
		}

		mockManager.resultCh <- discovery.Result{
			Source:     "ufm",
			HyperNodes: []*topologyv1alpha1.HyperNode{updatedHyperNode, newHyperNode},
		}
	}()

	time.Sleep(300 * time.Millisecond)

	// verify if the existing HyperNode is updated
	updatedNode, err := fakeVcClient.TopologyV1alpha1().HyperNodes().Get(context.TODO(), "existing-node-1", metav1.GetOptions{})
	assert.NoError(t, err, "Should be able to get the updated HyperNode")
	assert.Equal(t, "updated-node-1", updatedNode.Spec.Members[0].Selector.ExactMatch.Name)

	// verify if the new HyperNode is created
	_, err = fakeVcClient.TopologyV1alpha1().HyperNodes().Get(context.TODO(), "new-hypernode", metav1.GetOptions{})
	assert.NoError(t, err, "Should be able to get the created HyperNode")

	// phase2: delete hypernode
	go func() {
		mockManager.resultCh <- discovery.Result{
			Source:     "ufm",
			HyperNodes: []*topologyv1alpha1.HyperNode{},
		}
	}()

	time.Sleep(300 * time.Millisecond)

	// verify if the existing HyperNode with source match is deleted
	nodeList, err := fakeVcClient.TopologyV1alpha1().HyperNodes().List(context.TODO(), metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{
			api.NetworkTopologySourceLabelKey: "ufm",
		}).String(),
	})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(nodeList.Items), "All HyperNodes should have been deleted")

	// verify if the existing HyperNode with source match is deleted
	nodeList, err = fakeVcClient.TopologyV1alpha1().HyperNodes().List(context.TODO(), metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{
			api.NetworkTopologySourceLabelKey: "roce",
		}).String(),
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(nodeList.Items), "HyperNodes from different discovery sources should not be deleted")

	close(stopCh)
	time.Sleep(100 * time.Millisecond)
	assert.True(t, func() bool { mockManager.mu.Lock(); defer mockManager.mu.Unlock(); return mockManager.stopCalled }(), "Discovery manager should be stopped")
}

func TestInvalidLabelDiscoveryConfigPreservesLastValidTopology(t *testing.T) {
	const (
		configKey   = "test-namespace/test-config"
		profileKey  = "example.com/topology-profile"
		domainKey   = "example.com/hypernode-domain"
		domainValue = "hn-0"
	)

	validConfig := func() *api.NetworkTopologyConfig {
		return &api.NetworkTopologyConfig{NetworkTopologyDiscovery: []api.DiscoveryConfig{
			{
				Source:  "label",
				Enabled: true,
				Config: map[string]interface{}{
					"networkTopologyTypes": map[string]interface{}{
						"topologyA3": map[string]interface{}{
							"nodeSelector": map[string]interface{}{
								"matchLabels": map[string]interface{}{profileKey: "a3"},
							},
							"levels": []interface{}{
								map[string]interface{}{"nodeLabel": domainKey, "tierName": "volcano.sh/hypernode"},
								map[string]interface{}{"nodeLabel": corev1.LabelHostname},
							},
						},
					},
				},
			},
		}}
	}
	invalidConfig := &api.NetworkTopologyConfig{NetworkTopologyDiscovery: []api.DiscoveryConfig{
		{
			Source:  "label",
			Enabled: true,
			Config: map[string]interface{}{
				"networkTopologyTypes": map[string]interface{}{
					"topologyA3": map[string]interface{}{
						"levels": []interface{}{
							map[string]interface{}{"nodeLabel": domainKey, "tierNmae": "volcano.sh/hypernode"},
							map[string]interface{}{"nodeLabel": corev1.LabelHostname},
						},
					},
				},
			},
		},
	}}

	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "a3-node",
		Labels: map[string]string{
			profileKey:           "a3",
			domainKey:            domainValue,
			corev1.LabelHostname: "a3-node",
		},
	}}
	kubeClient := k8sfake.NewSimpleClientset(node)
	vcClient := vcclientset.NewSimpleClientset()
	vcInformerFactory := vcinformer.NewSharedInformerFactory(vcClient, 0)
	hyperNodeInformer := vcInformerFactory.Topology().V1alpha1().HyperNodes()
	_ = hyperNodeInformer.Informer()
	stopInformers := make(chan struct{})
	defer close(stopInformers)
	vcInformerFactory.Start(stopInformers)
	for informerType, synced := range vcInformerFactory.WaitForCacheSync(stopInformers) {
		require.True(t, synced, "failed to sync informer %v", informerType)
	}

	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	loader := config.NewFakeLoader(validConfig())
	manager := discovery.NewManager(loader, queue, kubeClient, vcClient)
	require.NoError(t, manager.Start())
	defer func() {
		manager.Stop()
		queue.ShutDown()
	}()

	controller := &hyperNodeController{
		vcClient:          vcClient,
		hyperNodeInformer: hyperNodeInformer,
		hyperNodeLister:   hyperNodeInformer.Lister(),
	}
	reconcileNextResult := func() string {
		t.Helper()
		select {
		case result := <-manager.ResultChannel():
			require.Equal(t, "label", result.Source)
			require.Len(t, result.HyperNodes, 1)
			require.NoError(t, controller.reconcileTopology(&result))
			result.Ack()
			return result.HyperNodes[0].Name
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for label discovery result")
			return ""
		}
	}
	listHyperNodes := func() []topologyv1alpha1.HyperNode {
		t.Helper()
		list, err := vcClient.TopologyV1alpha1().HyperNodes().List(context.Background(), metav1.ListOptions{})
		require.NoError(t, err)
		return list.Items
	}
	deleteActionCount := func() int {
		count := 0
		for _, action := range vcClient.Actions() {
			if action.GetVerb() == "delete" && action.GetResource().Resource == "hypernodes" {
				count++
			}
		}
		return count
	}

	queue.Add(configKey)
	expectedName := reconcileNextResult()
	require.Eventually(t, func() bool {
		hyperNodes := listHyperNodes()
		return len(hyperNodes) == 1 && hyperNodes[0].Name == expectedName
	}, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		hyperNodes, err := controller.hyperNodeLister.List(labels.Everything())
		return err == nil && len(hyperNodes) == 1 && hyperNodes[0].Name == expectedName
	}, 5*time.Second, 10*time.Millisecond, "the informer must observe the initial topology before testing its replacement")
	deleteCountBeforeInvalidConfig := deleteActionCount()

	loader.SetConfig(invalidConfig)
	queue.Add(configKey)
	require.Eventually(t, func() bool {
		return queue.NumRequeues(configKey) > 0
	}, 5*time.Second, 10*time.Millisecond, "invalid config should fail and be retried")
	select {
	case result := <-manager.ResultChannel():
		t.Fatalf("invalid config unexpectedly published %d HyperNodes", len(result.HyperNodes))
	case <-time.After(200 * time.Millisecond):
	}
	hyperNodesDuringError := listHyperNodes()
	require.Len(t, hyperNodesDuringError, 1)
	assert.Equal(t, expectedName, hyperNodesDuringError[0].Name)
	assert.Equal(t, deleteCountBeforeInvalidConfig, deleteActionCount(), "invalid config must not delete the last valid topology")

	updatedNode, err := kubeClient.CoreV1().Nodes().Get(context.Background(), node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	updatedNode.Labels[domainKey] = "hn-1"
	_, err = kubeClient.CoreV1().Nodes().Update(context.Background(), updatedNode, metav1.UpdateOptions{})
	require.NoError(t, err)
	nameUpdatedByOldDiscoverer := reconcileNextResult()
	assert.NotEqual(t, expectedName, nameUpdatedByOldDiscoverer,
		"the old discoverer must continue processing Node updates while the replacement config is invalid")
	require.Eventually(t, func() bool {
		hyperNodes := listHyperNodes()
		return len(hyperNodes) == 1 && hyperNodes[0].Name == nameUpdatedByOldDiscoverer
	}, 5*time.Second, 10*time.Millisecond)
	deleteCountAfterNodeUpdate := deleteActionCount()

	loader.SetConfig(validConfig())
	queue.Add(configKey)
	recoveredName := reconcileNextResult()
	assert.Equal(t, nameUpdatedByOldDiscoverer, recoveredName)
	require.Eventually(t, func() bool {
		hyperNodes := listHyperNodes()
		return len(hyperNodes) == 1 && hyperNodes[0].Name == nameUpdatedByOldDiscoverer
	}, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, deleteCountAfterNodeUpdate, deleteActionCount(), "recovery must not replace an already current topology")
}

func TestDiscoveryResultRetriesBeforeAcknowledgement(t *testing.T) {
	fakeVcClient := vcclientset.NewSimpleClientset()
	vcInformerFactory := vcinformer.NewSharedInformerFactory(fakeVcClient, 0)
	var createAttempts atomic.Int32
	fakeVcClient.PrependReactor("create", "hypernodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		if createAttempts.Add(1) == 1 {
			return true, nil, errors.New("injected create failure")
		}
		return false, nil, nil
	})

	controller := &hyperNodeController{
		vcClient:        fakeVcClient,
		hyperNodeLister: vcInformerFactory.Topology().V1alpha1().HyperNodes().Lister(),
		discoveryResultQueue: workqueue.NewTypedRateLimitingQueue(
			workqueue.DefaultTypedControllerRateLimiter[*discovery.Result]()),
	}
	var acknowledgements atomic.Int32
	result := &discovery.Result{
		Source: "label",
		HyperNodes: []*topologyv1alpha1.HyperNode{{
			ObjectMeta: metav1.ObjectMeta{Name: "retry-tier-1"},
			Spec:       topologyv1alpha1.HyperNodeSpec{Tier: 1},
		}},
		Acknowledge: func() {
			acknowledgements.Add(1)
		},
	}

	workerDone := make(chan struct{})
	go func() {
		controller.processDiscoveryResults()
		close(workerDone)
	}()
	controller.discoveryResultQueue.Add(result)
	require.Eventually(t, func() bool {
		return createAttempts.Load() >= 2 && acknowledgements.Load() == 1
	}, 3*time.Second, 10*time.Millisecond)
	assert.Equal(t, int32(1), acknowledgements.Load(), "a failed reconcile must not acknowledge before its retry succeeds")
	_, err := fakeVcClient.TopologyV1alpha1().HyperNodes().Get(
		context.Background(), "retry-tier-1", metav1.GetOptions{})
	require.NoError(t, err)

	controller.discoveryResultQueue.ShutDownWithDrain()
	select {
	case <-workerDone:
	case <-time.After(time.Second):
		t.Fatal("discovery result worker did not stop")
	}
}

func TestDiscoveryResultRetriesUpdateAndDeleteBeforeAcknowledgement(t *testing.T) {
	const source = "label"
	existingHyperNode := func() *topologyv1alpha1.HyperNode {
		return &topologyv1alpha1.HyperNode{
			ObjectMeta: metav1.ObjectMeta{Name: "existing", Labels: map[string]string{
				api.NetworkTopologySourceLabelKey: source,
			}},
			Spec: topologyv1alpha1.HyperNodeSpec{Tier: 1},
		}
	}
	runWorker := func(t *testing.T, controller *hyperNodeController, result *discovery.Result, completed func() bool) {
		t.Helper()
		workerDone := make(chan struct{})
		go func() {
			controller.processDiscoveryResults()
			close(workerDone)
		}()
		controller.discoveryResultQueue.Add(result)
		require.Eventually(t, completed, 3*time.Second, 10*time.Millisecond)
		controller.discoveryResultQueue.ShutDownWithDrain()
		select {
		case <-workerDone:
		case <-time.After(time.Second):
			t.Fatal("discovery result worker did not stop")
		}
	}

	t.Run("update", func(t *testing.T) {
		existing := existingHyperNode()
		fakeVcClient := vcclientset.NewSimpleClientset(existing.DeepCopy())
		vcInformerFactory := vcinformer.NewSharedInformerFactory(fakeVcClient, 0)
		hyperNodeInformer := vcInformerFactory.Topology().V1alpha1().HyperNodes()
		require.NoError(t, hyperNodeInformer.Informer().GetIndexer().Add(existing.DeepCopy()))
		var updateAttempts atomic.Int32
		fakeVcClient.PrependReactor("update", "hypernodes", func(action k8stesting.Action) (bool, runtime.Object, error) {
			if action.GetSubresource() == "" && updateAttempts.Add(1) == 1 {
				return true, nil, errors.New("injected update failure")
			}
			return false, nil, nil
		})
		controller := &hyperNodeController{
			vcClient:        fakeVcClient,
			hyperNodeLister: hyperNodeInformer.Lister(),
			discoveryResultQueue: workqueue.NewTypedRateLimitingQueue(
				workqueue.DefaultTypedControllerRateLimiter[*discovery.Result]()),
		}
		var acknowledgements atomic.Int32
		result := &discovery.Result{
			Source: source, HyperNodes: []*topologyv1alpha1.HyperNode{existing.DeepCopy()},
			Acknowledge: func() { acknowledgements.Add(1) },
		}
		runWorker(t, controller, result, func() bool {
			return updateAttempts.Load() >= 2 && acknowledgements.Load() == 1
		})
		assert.Equal(t, int32(1), acknowledgements.Load())
	})

	t.Run("delete", func(t *testing.T) {
		existing := existingHyperNode()
		fakeVcClient := vcclientset.NewSimpleClientset(existing.DeepCopy())
		vcInformerFactory := vcinformer.NewSharedInformerFactory(fakeVcClient, 0)
		hyperNodeInformer := vcInformerFactory.Topology().V1alpha1().HyperNodes()
		require.NoError(t, hyperNodeInformer.Informer().GetIndexer().Add(existing.DeepCopy()))
		var deleteAttempts atomic.Int32
		fakeVcClient.PrependReactor("delete", "hypernodes", func(k8stesting.Action) (bool, runtime.Object, error) {
			if deleteAttempts.Add(1) == 1 {
				return true, nil, errors.New("injected delete failure")
			}
			return false, nil, nil
		})
		controller := &hyperNodeController{
			vcClient:        fakeVcClient,
			hyperNodeLister: hyperNodeInformer.Lister(),
			discoveryResultQueue: workqueue.NewTypedRateLimitingQueue(
				workqueue.DefaultTypedControllerRateLimiter[*discovery.Result]()),
		}
		var acknowledgements atomic.Int32
		result := &discovery.Result{
			Source: source, HyperNodes: []*topologyv1alpha1.HyperNode{},
			Acknowledge: func() { acknowledgements.Add(1) },
		}
		runWorker(t, controller, result, func() bool {
			return deleteAttempts.Load() >= 2 && acknowledgements.Load() == 1
		})
		assert.Equal(t, int32(1), acknowledgements.Load())
	})
}

func TestStaleDiscoveryResultIsAcknowledgedWithoutReconcile(t *testing.T) {
	fakeVcClient := vcclientset.NewSimpleClientset()
	vcInformerFactory := vcinformer.NewSharedInformerFactory(fakeVcClient, 0)
	controller := &hyperNodeController{
		vcClient:        fakeVcClient,
		hyperNodeLister: vcInformerFactory.Topology().V1alpha1().HyperNodes().Lister(),
		discoveryResultQueue: workqueue.NewTypedRateLimitingQueue(
			workqueue.DefaultTypedControllerRateLimiter[*discovery.Result]()),
	}
	var acknowledgements atomic.Int32
	result := &discovery.Result{
		Source:     "label",
		HyperNodes: []*topologyv1alpha1.HyperNode{{ObjectMeta: metav1.ObjectMeta{Name: "stale"}}},
		Current:    func() bool { return false },
		Acknowledge: func() {
			acknowledgements.Add(1)
		},
	}

	workerDone := make(chan struct{})
	go func() {
		controller.processDiscoveryResults()
		close(workerDone)
	}()
	controller.discoveryResultQueue.Add(result)
	require.Eventually(t, func() bool { return acknowledgements.Load() == 1 }, time.Second, 10*time.Millisecond)
	assert.Empty(t, fakeVcClient.Actions())
	controller.discoveryResultQueue.ShutDownWithDrain()
	<-workerDone
}

func TestDiscoveryResultBecomingStaleSkipsDeletesAndRetry(t *testing.T) {
	const source = "label"
	newHyperNode := func(name string) *topologyv1alpha1.HyperNode {
		return &topologyv1alpha1.HyperNode{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
				Labels: map[string]string{
					api.NetworkTopologySourceLabelKey: source,
				},
			},
			Spec: topologyv1alpha1.HyperNodeSpec{Tier: 1},
		}
	}

	desired := newHyperNode("desired")
	obsolete := newHyperNode("obsolete")
	fakeVcClient := vcclientset.NewSimpleClientset(desired.DeepCopy(), obsolete.DeepCopy())
	vcInformerFactory := vcinformer.NewSharedInformerFactory(fakeVcClient, 0)
	hyperNodeInformer := vcInformerFactory.Topology().V1alpha1().HyperNodes()
	require.NoError(t, hyperNodeInformer.Informer().GetIndexer().Add(desired.DeepCopy()))
	require.NoError(t, hyperNodeInformer.Informer().GetIndexer().Add(obsolete.DeepCopy()))

	queue := workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[*discovery.Result]())
	defer queue.ShutDown()
	controller := &hyperNodeController{
		vcClient:             fakeVcClient,
		hyperNodeLister:      hyperNodeInformer.Lister(),
		discoveryResultQueue: queue,
	}

	var currentChecks atomic.Int32
	var acknowledgements atomic.Int32
	result := &discovery.Result{
		Source:     source,
		HyperNodes: []*topologyv1alpha1.HyperNode{desired.DeepCopy()},
		Current: func() bool {
			return currentChecks.Add(1) == 1
		},
		Acknowledge: func() {
			acknowledgements.Add(1)
		},
	}
	queue.Add(result)
	require.True(t, controller.processNextDiscoveryResult())

	assert.Equal(t, int32(1), acknowledgements.Load())
	assert.GreaterOrEqual(t, currentChecks.Load(), int32(2))
	assert.Zero(t, queue.NumRequeues(result), "stale results must not be rate-limit retried")
	for _, action := range fakeVcClient.Actions() {
		assert.NotEqual(t, "delete", action.GetVerb(), "stale reconciliation must stop before deleting HyperNodes")
	}
	_, err := fakeVcClient.TopologyV1alpha1().HyperNodes().Get(context.Background(), obsolete.Name, metav1.GetOptions{})
	assert.NoError(t, err, "obsolete HyperNode must remain when the result becomes stale before deletion")
}

func TestReconcileTopologyUsesDependencyOrder(t *testing.T) {
	const source = "label"
	newHyperNode := func(name string, tier int) *topologyv1alpha1.HyperNode {
		return &topologyv1alpha1.HyperNode{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{
				api.NetworkTopologySourceLabelKey: source,
			}},
			Spec: topologyv1alpha1.HyperNodeSpec{Tier: tier},
		}
	}
	desired := []*topologyv1alpha1.HyperNode{
		newHyperNode("tier-3", 3),
		newHyperNode("tier-1", 1),
		newHyperNode("tier-2", 2),
	}

	t.Run("create and update children before parents", func(t *testing.T) {
		fakeVcClient := vcclientset.NewSimpleClientset()
		vcInformerFactory := vcinformer.NewSharedInformerFactory(fakeVcClient, 0)
		controller := &hyperNodeController{
			vcClient:        fakeVcClient,
			hyperNodeLister: vcInformerFactory.Topology().V1alpha1().HyperNodes().Lister(),
		}
		require.NoError(t, controller.reconcileTopology(&discovery.Result{Source: source, HyperNodes: desired}))

		var created []string
		for _, action := range fakeVcClient.Actions() {
			if action.GetVerb() != "create" || action.GetResource().Resource != "hypernodes" {
				continue
			}
			created = append(created, action.(k8stesting.CreateAction).GetObject().(*topologyv1alpha1.HyperNode).Name)
		}
		assert.Equal(t, []string{"tier-1", "tier-2", "tier-3"}, created)
	})

	t.Run("update children before parents", func(t *testing.T) {
		existing := []runtime.Object{desired[0].DeepCopy(), desired[1].DeepCopy(), desired[2].DeepCopy()}
		fakeVcClient := vcclientset.NewSimpleClientset(existing...)
		vcInformerFactory := vcinformer.NewSharedInformerFactory(fakeVcClient, 0)
		hyperNodeInformer := vcInformerFactory.Topology().V1alpha1().HyperNodes()
		for _, object := range existing {
			require.NoError(t, hyperNodeInformer.Informer().GetIndexer().Add(object))
		}
		controller := &hyperNodeController{
			vcClient:        fakeVcClient,
			hyperNodeLister: hyperNodeInformer.Lister(),
		}
		require.NoError(t, controller.reconcileTopology(&discovery.Result{Source: source, HyperNodes: desired}))

		var updated []string
		for _, action := range fakeVcClient.Actions() {
			if action.GetVerb() != "update" || action.GetResource().Resource != "hypernodes" || action.GetSubresource() != "" {
				continue
			}
			updated = append(updated, action.(k8stesting.UpdateAction).GetObject().(*topologyv1alpha1.HyperNode).Name)
		}
		assert.Equal(t, []string{"tier-1", "tier-2", "tier-3"}, updated)
	})

	t.Run("delete parents before children", func(t *testing.T) {
		existing := []runtime.Object{desired[0].DeepCopy(), desired[1].DeepCopy(), desired[2].DeepCopy()}
		fakeVcClient := vcclientset.NewSimpleClientset(existing...)
		vcInformerFactory := vcinformer.NewSharedInformerFactory(fakeVcClient, 0)
		hyperNodeInformer := vcInformerFactory.Topology().V1alpha1().HyperNodes()
		for _, object := range existing {
			require.NoError(t, hyperNodeInformer.Informer().GetIndexer().Add(object))
		}
		controller := &hyperNodeController{
			vcClient:        fakeVcClient,
			hyperNodeLister: hyperNodeInformer.Lister(),
		}
		require.NoError(t, controller.reconcileTopology(&discovery.Result{Source: source, HyperNodes: []*topologyv1alpha1.HyperNode{}}))

		var deleted []string
		for _, action := range fakeVcClient.Actions() {
			if action.GetVerb() == "delete" && action.GetResource().Resource == "hypernodes" {
				deleted = append(deleted, action.(k8stesting.DeleteAction).GetName())
			}
		}
		assert.Equal(t, []string{"tier-3", "tier-2", "tier-1"}, deleted)
	})
}

func TestHyperNodeController_Initialize(t *testing.T) {
	os.Setenv(config.NamespaceEnvKey, "test-namespace")
	os.Setenv(config.ReleaseNameEnvKey, "test-release")
	defer func() {
		os.Unsetenv(config.NamespaceEnvKey)
		os.Unsetenv(config.ReleaseNameEnvKey)
	}()

	fakeVcClient := vcclientset.NewSimpleClientset()
	fakeKubeClient := k8sfake.NewSimpleClientset()
	vcInformerFactory := vcinformer.NewSharedInformerFactory(fakeVcClient, 0)
	kubeInformerFactory := informers.NewSharedInformerFactory(fakeKubeClient, 0)

	controller := &hyperNodeController{
		informerFactory: kubeInformerFactory,
	}

	err := controller.Initialize(&framework.ControllerOption{
		VolcanoClient:           fakeVcClient,
		KubeClient:              fakeKubeClient,
		VCSharedInformerFactory: vcInformerFactory,
		SharedInformerFactory:   kubeInformerFactory,
	})

	assert.NoError(t, err)
	assert.Equal(t, fakeVcClient, controller.vcClient)
	assert.Equal(t, fakeKubeClient, controller.kubeClient)
	assert.Equal(t, vcInformerFactory, controller.vcInformerFactory)
	assert.NotNil(t, controller.hyperNodeInformer)
	assert.NotNil(t, controller.hyperNodeLister)
	assert.NotNil(t, controller.discoveryManager)
	assert.NotNil(t, controller.configMapQueue)
	assert.Equal(t, "test-namespace", controller.configMapNamespace)
	assert.Equal(t, "test-release-controller-configmap", controller.configMapName)
}
