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
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	topologyv1alpha1 "volcano.sh/apis/pkg/apis/topology/v1alpha1"
	vcclientset "volcano.sh/apis/pkg/client/clientset/versioned"
	"volcano.sh/volcano/pkg/controllers/hypernode/api"
	"volcano.sh/volcano/pkg/controllers/hypernode/config"

	_ "volcano.sh/volcano/pkg/controllers/hypernode/discovery/label"
	_ "volcano.sh/volcano/pkg/controllers/hypernode/discovery/ufm"
)

type Result struct {
	// HyperNodes contains the discovered hypernodes
	HyperNodes []*topologyv1alpha1.HyperNode
	// Source indicates the source of the discovery
	Source string
	// Generation identifies the discoverer instance that produced this result.
	Generation uint64
	// Acknowledge notifies the producing discoverer after reconciliation succeeds.
	Acknowledge func()
	// Current reports whether this result still belongs to the active discoverer.
	Current func() bool
}

// Ack acknowledges this result at most once when the manager supplied a callback.
func (r Result) Ack() {
	if r.Acknowledge != nil {
		r.Acknowledge()
	}
}

// IsCurrent reports whether the result is still current. Results constructed by
// tests or external callers without a callback are treated as current.
func (r Result) IsCurrent() bool {
	return r.Current == nil || r.Current()
}

// Manager is the interface for managing network topology discovery
type Manager interface {
	// Start initializes and starts the topology discovery manager
	Start() error
	// Stop halts all discovery processes
	Stop()
	// ResultChannel returns a channel for receiving discovery results
	ResultChannel() <-chan Result
}

type discovererInstance struct {
	source            string
	generation        uint64
	discoverer        api.Discoverer
	outputCh          <-chan []*topologyv1alpha1.HyperNode
	processorStopCh   chan struct{}
	processorStopOnce sync.Once
}

func (d *discovererInstance) stopProcessor() {
	d.processorStopOnce.Do(func() {
		close(d.processorStopCh)
	})
}

// manager manages network topology discovery processes
type manager struct {
	mutex sync.RWMutex

	configLoader config.Loader
	config       *api.NetworkTopologyConfig

	discoverers map[string]*discovererInstance
	workQueue   workqueue.TypedRateLimitingInterface[string]
	stopCh      chan struct{}
	stopOnce    sync.Once
	workerWG    sync.WaitGroup
	processorWG sync.WaitGroup
	generation  atomic.Uint64

	kubeClient clientset.Interface
	vcClient   vcclientset.Interface

	resultCh chan Result
}

// NewManager create a new network topology discovery manager
func NewManager(configLoader config.Loader, queue workqueue.TypedRateLimitingInterface[string], kubeClient clientset.Interface, vcClient vcclientset.Interface) Manager {
	return &manager{
		configLoader: configLoader,
		discoverers:  make(map[string]*discovererInstance),
		resultCh:     make(chan Result),
		stopCh:       make(chan struct{}),
		workQueue:    queue,
		kubeClient:   kubeClient,
		vcClient:     vcClient,
	}
}

// Start initializes and starts the topology discovery manager
func (m *manager) Start() error {
	var err error
	cfg, err := m.configLoader.LoadConfig()
	if err != nil {
		klog.ErrorS(err, "Failed to load config")
		// Initialize with an empty config to avoid nil pointer dereference.
		m.mutex.Lock()
		m.config = &api.NetworkTopologyConfig{}
		m.mutex.Unlock()
		// Do not return an error here, in case of configMap is updated correctly later.
	} else {
		m.mutex.Lock()
		m.config = cfg
		m.mutex.Unlock()
	}

	m.workerWG.Add(1)
	go func() {
		defer m.workerWG.Done()
		m.worker()
	}()

	klog.InfoS("Network topology discovery manager started")
	return nil
}

// Stop halts all discovery processes
func (m *manager) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
		m.workQueue.ShutDown()
		m.workerWG.Wait()

		m.mutex.Lock()
		instances := m.discoverers
		m.discoverers = make(map[string]*discovererInstance)
		m.mutex.Unlock()

		m.stopInstances(instances)
		m.processorWG.Wait()
		close(m.resultCh)
		klog.InfoS("Network topology discovery manager stopped")
	})
}

func (m *manager) ResultChannel() <-chan Result {
	return m.resultCh
}

func (m *manager) startDiscoverer(discoveryCfg api.DiscoveryConfig) (*discovererInstance, error) {
	discoverer, err := api.NewDiscoverer(discoveryCfg, m.kubeClient, m.vcClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create discoverer: %v", err)
	}

	outputCh, err := discoverer.Start()
	if err != nil {
		if stopErr := discoverer.Stop(); stopErr != nil {
			klog.ErrorS(stopErr, "Failed to clean up discoverer after start failure", "source", discoveryCfg.Source)
		}
		return nil, fmt.Errorf("failed to start discoverer: %v", err)
	}

	return &discovererInstance{
		source:          discoveryCfg.Source,
		generation:      m.generation.Add(1),
		discoverer:      discoverer,
		outputCh:        outputCh,
		processorStopCh: make(chan struct{}),
	}, nil
}

func (m *manager) stopInstances(instances map[string]*discovererInstance) {
	sources := make([]string, 0, len(instances))
	for source := range instances {
		sources = append(sources, source)
	}
	sort.Strings(sources)

	for _, source := range sources {
		instance := instances[source]
		instance.stopProcessor()
		if err := instance.discoverer.Stop(); err != nil {
			klog.ErrorS(err, "Failed to stop discoverer", "source", source, "generation", instance.generation)
		}
	}
}

func (m *manager) worker() {
	for m.processNext() {
	}
}

// processNext handles a single workQueue item
func (m *manager) processNext() bool {
	key, shutdown := m.workQueue.Get()
	if shutdown {
		return false
	}
	defer m.workQueue.Done(key)

	if err := m.syncHandler(key); err != nil {
		m.workQueue.AddRateLimited(key)
		klog.ErrorS(err, "Failed to process network topology discoverer", "key", key)
		return true
	}
	m.workQueue.Forget(key)
	return true
}

// parseConfig loads and parses the configuration from ConfigMap
func (m *manager) parseConfig(key string) (*api.NetworkTopologyConfig, error) {
	_, _, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return nil, err
	}
	newConfig, err := m.configLoader.LoadConfig()
	if err != nil {
		if !errors.IsNotFound(err) {
			return nil, err
		}
		// set an empty config and should not return err because we should handle configMap deletion event.
		newConfig = &api.NetworkTopologyConfig{}
	}
	return newConfig, nil
}

// syncHandler handles the configuration update event.
func (m *manager) syncHandler(key string) error {
	klog.InfoS("Received configuration update")
	newConfig, err := m.parseConfig(key)
	if err != nil {
		return err
	}

	return m.replaceDiscoverers(newConfig)
}

func (m *manager) replaceDiscoverers(newConfig *api.NetworkTopologyConfig) error {
	sources := newConfig.GetEnabledDiscoverySources()
	sort.Strings(sources)

	newInstances := make(map[string]*discovererInstance, len(sources))
	for _, source := range sources {
		if _, exists := newInstances[source]; exists {
			m.stopInstances(newInstances)
			return fmt.Errorf("duplicate enabled discovery source: %s", source)
		}
		discoveryCfg := newConfig.GetDiscoveryConfig(source)
		if discoveryCfg == nil {
			m.stopInstances(newInstances)
			return fmt.Errorf("configuration not found for network topology discovery source: %s", source)
		}

		instance, err := m.startDiscoverer(*discoveryCfg)
		if err != nil {
			m.stopInstances(newInstances)
			return err
		}
		newInstances[source] = instance
	}

	select {
	case <-m.stopCh:
		m.stopInstances(newInstances)
		return nil
	default:
	}

	m.mutex.Lock()
	oldInstances := m.discoverers
	m.discoverers = newInstances
	m.config = newConfig
	m.mutex.Unlock()

	for _, source := range sources {
		instance := newInstances[source]
		m.processorWG.Add(1)
		go func() {
			defer m.processorWG.Done()
			m.processTopology(instance)
		}()
		klog.InfoS("Started network topology discoverer", "source", source, "generation", instance.generation)
	}
	m.stopInstances(oldInstances)
	return nil
}

func (m *manager) instanceIsCurrent(instance *discovererInstance) bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.discoverers[instance.source] == instance
}

// processTopology processes the topology data received from one discoverer instance.
func (m *manager) processTopology(instance *discovererInstance) {
	for {
		select {
		case hyperNodes, ok := <-instance.outputCh:
			if !ok {
				klog.InfoS("Topology channel closed, stopping processor", "source", instance.source, "generation", instance.generation)
				return
			}

			result := Result{
				HyperNodes: hyperNodes,
				Source:     instance.source,
				Generation: instance.generation,
				Acknowledge: sync.OnceFunc(func() {
					instance.discoverer.ResultSynced()
				}),
				Current: func() bool {
					return m.instanceIsCurrent(instance)
				},
			}
			select {
			case m.resultCh <- result:
				klog.V(3).InfoS("Forwarded discovery results to unified channel",
					"source", instance.source,
					"generation", instance.generation,
					"nodeCount", len(hyperNodes))
			case <-instance.processorStopCh:
				return
			case <-m.stopCh:
				return
			}

		case <-instance.processorStopCh:
			return
		case <-m.stopCh:
			return
		}
	}
}
