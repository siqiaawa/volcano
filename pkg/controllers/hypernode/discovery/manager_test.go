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

package discovery

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/workqueue"

	topologyv1alpha1 "volcano.sh/apis/pkg/apis/topology/v1alpha1"
	vcclientset "volcano.sh/apis/pkg/client/clientset/versioned"
	fakevcclientset "volcano.sh/apis/pkg/client/clientset/versioned/fake"
	"volcano.sh/volcano/pkg/controllers/hypernode/api"
	"volcano.sh/volcano/pkg/controllers/hypernode/config"
	fakedisc "volcano.sh/volcano/pkg/controllers/hypernode/discovery/fake"
)

type startFailingDiscoverer struct {
	stopCalled bool
}

func (f *startFailingDiscoverer) Start() (chan []*topologyv1alpha1.HyperNode, error) {
	return nil, errors.New("invalid discovery config")
}

func (f *startFailingDiscoverer) Stop() error {
	f.stopCalled = true
	return nil
}

func (f *startFailingDiscoverer) Name() string {
	return "start-failing"
}

func (f *startFailingDiscoverer) ResultSynced() {
}

type countingLoader struct {
	loader *config.FakeLoader
	loads  atomic.Int32
}

func (l *countingLoader) LoadConfig() (*api.NetworkTopologyConfig, error) {
	l.loads.Add(1)
	return l.loader.LoadConfig()
}

type controllableDiscoverer struct {
	outputCh     chan []*topologyv1alpha1.HyperNode
	startErr     error
	stopOnce     sync.Once
	stopped      atomic.Bool
	acknowledged atomic.Int32
}

func newControllableDiscoverer(startErr error) *controllableDiscoverer {
	return &controllableDiscoverer{
		outputCh: make(chan []*topologyv1alpha1.HyperNode),
		startErr: startErr,
	}
}

func (d *controllableDiscoverer) Start() (chan []*topologyv1alpha1.HyperNode, error) {
	if d.startErr != nil {
		return nil, d.startErr
	}
	return d.outputCh, nil
}

func (d *controllableDiscoverer) Stop() error {
	d.stopOnce.Do(func() {
		d.stopped.Store(true)
	})
	return nil
}

func (d *controllableDiscoverer) Name() string {
	return "controllable"
}

func (d *controllableDiscoverer) ResultSynced() {
	d.acknowledged.Add(1)
}

func TestManager_StartMultipleDiscoverers(t *testing.T) {
	// Prepare test data
	hyperNodesA := []*topologyv1alpha1.HyperNode{
		{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "ha1"}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "ha2"}},
	}

	hyperNodesB := []*topologyv1alpha1.HyperNode{
		{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "hb"}},
	}

	constructorA := api.DiscovererConstructor(func(cfg api.DiscoveryConfig, kubeClient clientset.Interface, vcClient vcclientset.Interface) api.Discoverer {
		return fakedisc.NewFakeDiscoverer(hyperNodesA, cfg)
	})
	constructorB := api.DiscovererConstructor(func(cfg api.DiscoveryConfig, kubeClient clientset.Interface, vcClient vcclientset.Interface) api.Discoverer {
		return fakedisc.NewFakeDiscoverer(hyperNodesB, cfg)
	})

	api.RegisterDiscoverer("sourceA", constructorA)
	api.RegisterDiscoverer("sourceB", constructorB)

	discoveryConfig := &api.NetworkTopologyConfig{
		NetworkTopologyDiscovery: []api.DiscoveryConfig{
			{
				Source:  "sourceA",
				Enabled: true,
			},
			{
				Source:  "sourceB",
				Enabled: true,
			},
		},
	}
	loader := config.NewFakeLoader(discoveryConfig)

	// Create manager
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	queue.Add("test-namespace/test-config")
	fakeClient := fake.NewSimpleClientset()
	fakeVcClient := fakevcclientset.NewSimpleClientset()
	m := NewManager(loader, queue, fakeClient, fakeVcClient)
	err := m.Start()
	assert.NoError(t, err)

	timeout := time.After(time.Second)

	for i := 0; i < 2; i++ {
		select {
		case result := <-m.ResultChannel():
			if result.Source == "sourceA" {
				assert.Equal(t, 2, len(result.HyperNodes))
				assert.Equal(t, "ha1", result.HyperNodes[0].Name)
				assert.Equal(t, "ha2", result.HyperNodes[1].Name)
			} else if result.Source == "sourceB" {
				assert.Equal(t, 1, len(result.HyperNodes))
				assert.Equal(t, "hb", result.HyperNodes[0].Name)
			}
		case <-timeout:
			t.Fatal("Test timed out waiting for results")
		}
	}
	mgr := m.(*manager)
	mgr.mutex.Lock()
	assert.Equal(t, discoveryConfig, mgr.config)
	mgr.mutex.Unlock()
	// Stop manager
	m.Stop()
}

func TestManager_syncHandler(t *testing.T) {
	// Prepare test data
	hyperNodes := []*topologyv1alpha1.HyperNode{
		{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "ha1"}},
	}

	constructor := api.DiscovererConstructor(func(cfg api.DiscoveryConfig, kubeClient clientset.Interface, vcClient vcclientset.Interface) api.Discoverer {
		return fakedisc.NewFakeDiscoverer(hyperNodes, cfg)
	})

	api.RegisterDiscoverer("testSource", constructor)
	discoveryConfigV1 := &api.NetworkTopologyConfig{
		NetworkTopologyDiscovery: []api.DiscoveryConfig{
			{
				Source:  "testSource",
				Enabled: true,
				Config: map[string]interface{}{
					"key": "value",
				},
			},
		},
	}
	discoveryConfigV2 := &api.NetworkTopologyConfig{
		NetworkTopologyDiscovery: []api.DiscoveryConfig{
			{
				Source:  "testSource",
				Enabled: false,
				Config: map[string]interface{}{
					"key": "value",
				},
			},
		},
	}

	loader := config.NewFakeLoader(discoveryConfigV1)
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	fakeClient := fake.NewSimpleClientset()
	fakeVcClient := fakevcclientset.NewSimpleClientset()
	m := NewManager(loader, queue, fakeClient, fakeVcClient)

	// Start the manager
	err := m.Start()
	assert.NoError(t, err)

	// Enqueue a dummy key to trigger the sync handler
	queue.Add("test-namespace/test-config")

	// Give the manager some time to process the initial config
	time.Sleep(100 * time.Millisecond)

	//// Update the config with V2 version that disables the discoverer
	loader.SetConfig(discoveryConfigV2)
	// Enqueue the key again to trigger the sync handler with the updated config
	queue.Add("test-namespace/test-config")

	// Give the manager some time to process the updated config
	time.Sleep(100 * time.Millisecond)

	// Assert that the discoverer has been stopped
	mgr := m.(*manager)
	mgr.mutex.Lock()
	_, exists := mgr.discoverers["testSource"]
	mgr.mutex.Unlock()
	assert.False(t, exists, "Discoverer should be stopped")

	// Stop the manager
	m.Stop()
}

func TestManagerDoesNotRegisterDiscovererThatFailsToStart(t *testing.T) {
	const source = "startFailingSource"
	failingDiscoverer := &startFailingDiscoverer{}
	api.RegisterDiscoverer(source, func(api.DiscoveryConfig, clientset.Interface, vcclientset.Interface) api.Discoverer {
		return failingDiscoverer
	})

	discoveryConfig := &api.NetworkTopologyConfig{
		NetworkTopologyDiscovery: []api.DiscoveryConfig{{
			Source:  source,
			Enabled: true,
		}},
	}
	loader := config.NewFakeLoader(discoveryConfig)
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	m := NewManager(loader, queue, fake.NewSimpleClientset(), fakevcclientset.NewSimpleClientset())
	require.NoError(t, m.Start())
	defer func() {
		m.Stop()
		queue.ShutDown()
	}()

	mgr := m.(*manager)
	err := mgr.syncHandler("test-namespace/test-config")
	require.ErrorContains(t, err, "invalid discovery config")
	assert.True(t, failingDiscoverer.stopCalled, "failed discoverer should be cleaned up")
	_, exists := mgr.discoverers[source]
	assert.False(t, exists, "failed discoverer must not receive future ResultSynced notifications")
}

func TestManagerReplacesDiscoverersTransactionally(t *testing.T) {
	const source = "transactionalSource"
	var (
		instancesMu sync.Mutex
		instances   []*controllableDiscoverer
	)
	api.RegisterDiscoverer(source, func(cfg api.DiscoveryConfig, _ clientset.Interface, _ vcclientset.Interface) api.Discoverer {
		var startErr error
		if cfg.Config["version"] == "invalid" {
			startErr = errors.New("invalid replacement")
		}
		instance := newControllableDiscoverer(startErr)
		instancesMu.Lock()
		instances = append(instances, instance)
		instancesMu.Unlock()
		return instance
	})

	configForVersion := func(version string) *api.NetworkTopologyConfig {
		return &api.NetworkTopologyConfig{NetworkTopologyDiscovery: []api.DiscoveryConfig{{
			Source: source, Enabled: true, Config: map[string]interface{}{"version": version},
		}}}
	}
	fakeLoader := config.NewFakeLoader(configForVersion("v1"))
	loader := &countingLoader{loader: fakeLoader}
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	m := NewManager(loader, queue, fake.NewSimpleClientset(), fakevcclientset.NewSimpleClientset())
	require.NoError(t, m.Start())
	mgr := m.(*manager)
	require.NoError(t, mgr.syncHandler("test-namespace/test-config"))
	assert.Equal(t, int32(2), loader.loads.Load(), "Start and the first sync should each load once")

	instancesMu.Lock()
	require.Len(t, instances, 1)
	oldDiscoverer := instances[0]
	instancesMu.Unlock()
	mgr.mutex.RLock()
	oldInstance := mgr.discoverers[source]
	mgr.mutex.RUnlock()
	require.NotNil(t, oldInstance)

	fakeLoader.SetConfig(configForVersion("invalid"))
	loadsBeforeInvalid := loader.loads.Load()
	require.ErrorContains(t, mgr.syncHandler("test-namespace/test-config"), "invalid replacement")
	assert.Equal(t, loadsBeforeInvalid+1, loader.loads.Load(), "a sync must use one config snapshot")
	assert.False(t, oldDiscoverer.stopped.Load(), "an invalid replacement must not stop the active discoverer")
	mgr.mutex.RLock()
	assert.Same(t, oldInstance, mgr.discoverers[source])
	mgr.mutex.RUnlock()

	nodes := []*topologyv1alpha1.HyperNode{{ObjectMeta: metav1.ObjectMeta{Name: "still-current"}}}
	select {
	case oldDiscoverer.outputCh <- nodes:
	case <-time.After(time.Second):
		t.Fatal("old discoverer stopped forwarding after an invalid update")
	}
	var oldResult Result
	select {
	case oldResult = <-m.ResultChannel():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the active discoverer result")
	}
	assert.True(t, oldResult.IsCurrent())

	fakeLoader.SetConfig(configForVersion("v3"))
	require.NoError(t, mgr.syncHandler("test-namespace/test-config"))
	assert.True(t, oldDiscoverer.stopped.Load(), "old discoverer must stop after the replacement commits")
	assert.False(t, oldResult.IsCurrent(), "queued results from the replaced instance must become stale")

	instancesMu.Lock()
	require.Len(t, instances, 3, "v1, invalid v2, and valid v3 instances should be constructed")
	newDiscoverer := instances[2]
	instancesMu.Unlock()
	oldResult.Ack()
	oldResult.Ack()
	assert.Equal(t, int32(1), oldDiscoverer.acknowledged.Load(), "a result must acknowledge its producing instance once")
	assert.Zero(t, newDiscoverer.acknowledged.Load(), "an old result must not acknowledge the new instance")

	m.Stop()
	m.Stop()
	_, open := <-m.ResultChannel()
	assert.False(t, open, "Stop must close the result channel")
}
