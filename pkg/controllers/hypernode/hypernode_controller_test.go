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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	k8sfake "k8s.io/client-go/kubernetes/fake"
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

func (m *mockDiscoveryManager) ResultSynced(source string) {
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
		hyperNodeInformer: vcInformerFactory.Topology().V1alpha1().HyperNodes(),
		hyperNodeLister:   vcInformerFactory.Topology().V1alpha1().HyperNodes().Lister(),
	}
	reconcileNextResult := func() string {
		t.Helper()
		select {
		case result := <-manager.ResultChannel():
			require.Equal(t, "label", result.Source)
			require.Len(t, result.HyperNodes, 1)
			controller.reconcileTopology(result.Source, result.HyperNodes)
			manager.ResultSynced(result.Source)
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

	loader.SetConfig(validConfig())
	queue.Add(configKey)
	recoveredName := reconcileNextResult()
	assert.Equal(t, expectedName, recoveredName)
	require.Eventually(t, func() bool {
		hyperNodes := listHyperNodes()
		return len(hyperNodes) == 1 && hyperNodes[0].Name == expectedName
	}, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, deleteCountBeforeInvalidConfig, deleteActionCount())
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
