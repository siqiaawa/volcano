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

package label

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/klog/v2"

	topologyv1alpha1 "volcano.sh/apis/pkg/apis/topology/v1alpha1"
	vcclient "volcano.sh/apis/pkg/client/clientset/versioned"
	vcclientset "volcano.sh/apis/pkg/client/clientset/versioned/fake"
	"volcano.sh/volcano/pkg/controllers/hypernode/api"
	"volcano.sh/volcano/pkg/controllers/hypernode/utils"
)

func TestNewLabelDiscoverer_start(t *testing.T) {
	tests := []struct {
		name                 string
		config               api.DiscoveryConfig
		nodes                map[string]*corev1.Node
		existHyperNode       map[string]*topologyv1alpha1.HyperNode
		expectedHyperNodeMap map[string]*topologyv1alpha1.HyperNode
	}{
		{
			name:                 "test1",
			config:               getCfg(),
			nodes:                expectedNodeForTest1(),
			expectedHyperNodeMap: expectedHyperNodesForTest1(),
		},
		// Some nodes only have tier1 labels and do not have tier2 labels.
		{
			name:                 "test2",
			config:               getCfg(),
			nodes:                expectedNodeForTest2(),
			expectedHyperNodeMap: expectedHyperNodesForTest2(),
		},
		// Some nodes only have tier2 labels and do not have tier1 labels.
		{
			name:                 "test3",
			config:               getCfg(),
			nodes:                expectedNodeForTest3(),
			expectedHyperNodeMap: expectedHyperNodesForTest3(),
		},
		{
			name:                 "test4",
			config:               getCfg(),
			nodes:                expectedNodeForTest4(),
			existHyperNode:       getExistHyperNodesForTest4(),
			expectedHyperNodeMap: expectedHyperNodesForTest4(),
		},
		{
			name:                 "test5",
			config:               getCfg(),
			nodes:                expectedNodeForTest5(),
			existHyperNode:       getExistHyperNodesForTest5(),
			expectedHyperNodeMap: expectedHyperNodesForTest5(),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.config
			kubeClient := fake.NewSimpleClientset()
			fakeVcClient := vcclientset.NewSimpleClientset()
			createNode(kubeClient, tc.nodes)
			createHyperNode(fakeVcClient, tc.existHyperNode)
			time.Sleep(300 * time.Millisecond)
			d := NewLabelDiscoverer(cfg, kubeClient, fakeVcClient)
			outputCh, _ := d.Start()
			var hyperNodes []*topologyv1alpha1.HyperNode
			select {
			case hyperNodes = <-outputCh:
			case <-time.After(time.Second):
				t.Fatal("Timeout waiting for output")
			}
			expectedHyperNodeMap := tc.expectedHyperNodeMap
			assert.Equal(t, len(expectedHyperNodeMap), len(hyperNodes), "Hypernode count should match")
			for _, hn := range hyperNodes {
				klog.Infof("target hyperNode name is %s\n", hn.Name)
			}
			d.Stop()
		})
	}
}

func getCfg() api.DiscoveryConfig {
	return api.DiscoveryConfig{
		Source: "label",
		Config: map[string]interface{}{
			"networkTopologyTypes": map[interface{}]interface{}{
				"topologyA2": []interface{}{
					map[interface{}]interface{}{
						"nodeLabel": "volcano.sh/torA2-2",
					},
					map[interface{}]interface{}{
						"nodeLabel": "volcano.sh_torA2-1",
					},
					map[interface{}]interface{}{
						"nodeLabel": "kubernetes.io/hostname",
					},
				},
				"topologyA3": []interface{}{
					map[interface{}]interface{}{
						"nodeLabel": "volcano.sh/torA3-2",
					},
					map[interface{}]interface{}{
						"nodeLabel": "volcano.sh/torA3-1",
					},
					map[interface{}]interface{}{
						"nodeLabel": "kubernetes.io/hostname",
					},
				},
				"topologyA5": []interface{}{
					map[interface{}]interface{}{
						"nodeLabel": "volcano.sh/torA5-2",
					},
					map[interface{}]interface{}{
						"nodeLabel": "volcano.sh/torA5-1",
					},
					map[interface{}]interface{}{
						"nodeLabel": "kubernetes.io/hostname",
					},
				},
			},
		},
	}
}

func createNode(kubeClient clientset.Interface, nodes map[string]*corev1.Node) {
	for _, node := range nodes {
		kubeClient.CoreV1().Nodes().Create(context.TODO(), node, metav1.CreateOptions{})
	}
}

func expectedHyperNodesForTest1() map[string]*topologyv1alpha1.HyperNode {
	return map[string]*topologyv1alpha1.HyperNode{
		"hypernode-topologya2-tier1-jcdfg": utils.BuildHyperNodeWithTierName("hypernode-topologya2-tier1-jcdfg", 1, "volcano.sh_torA2-1",
			[]topologyv1alpha1.MemberSpec{
				{
					Type:     topologyv1alpha1.MemberTypeNode,
					Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: "node0"}},
				},
				{
					Type:     topologyv1alpha1.MemberTypeNode,
					Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: "node1"}},
				},
			}, map[string]string{api.NetworkTopologySourceLabelKey: "label",
				"volcano.sh_torA2-1": "s0"}),
		"hypernode-topologya2-tier1-jxcdr": utils.BuildHyperNodeWithTierName("hypernode-topologya2-tier1-jxcdr", 1, "volcano.sh_torA2-1",
			[]topologyv1alpha1.MemberSpec{
				{
					Type:     topologyv1alpha1.MemberTypeNode,
					Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: "node2"}},
				},
				{
					Type:     topologyv1alpha1.MemberTypeNode,
					Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: "node3"}},
				},
			}, map[string]string{api.NetworkTopologySourceLabelKey: "label",
				"volcano.sh_torA2-1": "s1"}),
		"hypernode-topologya2-tier1-quksd": utils.BuildHyperNodeWithTierName("hypernode-topologya2-tier1-quksd", 1, "volcano.sh_torA2-1",
			[]topologyv1alpha1.MemberSpec{
				{
					Type:     topologyv1alpha1.MemberTypeNode,
					Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: "node4"}},
				},
				{
					Type:     topologyv1alpha1.MemberTypeNode,
					Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: "node5"}},
				},
			}, map[string]string{api.NetworkTopologySourceLabelKey: "label",
				"volcano.sh_torA2-1": "s2"}),
		"hypernode-topologya2-tier1-akdhg": utils.BuildHyperNodeWithTierName("hypernode-topologya2-tier1-akdhg", 1, "volcano.sh_torA2-1",
			[]topologyv1alpha1.MemberSpec{
				{
					Type:     topologyv1alpha1.MemberTypeNode,
					Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: "node6"}},
				},
				{
					Type:     topologyv1alpha1.MemberTypeNode,
					Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: "node7"}},
				},
			}, map[string]string{api.NetworkTopologySourceLabelKey: "label",
				"volcano.sh_torA2-1": "s3"}),
		"hypernode-topologya2-tier2-7hslk": utils.BuildHyperNodeWithTierName("hypernode-topologya2-tier2-7hslk", 2, "volcano.sh_torA2-1",
			[]topologyv1alpha1.MemberSpec{
				{
					Type:     topologyv1alpha1.MemberTypeHyperNode,
					Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: "hypernode-topologya2-tier1-jcdfg"}},
				},
				{
					Type:     topologyv1alpha1.MemberTypeHyperNode,
					Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: "hypernode-topologya2-tier1-jxcdr"}},
				},
			}, map[string]string{api.NetworkTopologySourceLabelKey: "label",
				"volcano.sh/torA2-2": "s4"}),
		"hypernode-topologya2-tier2-zmonf": utils.BuildHyperNodeWithTierName("hypernode-topologya2-tier2-zmonf", 2, "volcano.sh_torA2-1",
			[]topologyv1alpha1.MemberSpec{
				{
					Type:     topologyv1alpha1.MemberTypeHyperNode,
					Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: "hypernode-topologya2-tier1-quksd"}},
				},
				{
					Type:     topologyv1alpha1.MemberTypeHyperNode,
					Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: "hypernode-topologya2-tier1-akdhg"}},
				},
			}, map[string]string{api.NetworkTopologySourceLabelKey: "label",
				"volcano.sh/torA2-2": "s5"}),
	}
}

func expectedNodeForTest1() map[string]*corev1.Node {
	return map[string]*corev1.Node{
		"node0": {
			ObjectMeta: metav1.ObjectMeta{
				Name: "node0",
				Labels: map[string]string{
					"volcano.sh_torA2-1": "s0",
					"volcano.sh/torA2-2": "s4",
				},
			},
		},
		"node1": {
			ObjectMeta: metav1.ObjectMeta{
				Name: "node1",
				Labels: map[string]string{
					"volcano.sh_torA2-1": "s0",
					"volcano.sh/torA2-2": "s4",
				},
			},
		},
		"node2": {
			ObjectMeta: metav1.ObjectMeta{
				Name: "node2",
				Labels: map[string]string{
					"volcano.sh_torA2-1": "s1",
					"volcano.sh/torA2-2": "s4",
				},
			},
		},
		"node3": {
			ObjectMeta: metav1.ObjectMeta{
				Name: "node3",
				Labels: map[string]string{
					"volcano.sh_torA2-1": "s1",
					"volcano.sh/torA2-2": "s4",
				},
			},
		},
		"node4": {
			ObjectMeta: metav1.ObjectMeta{
				Name: "node4",
				Labels: map[string]string{
					"volcano.sh_torA2-1": "s2",
					"volcano.sh/torA2-2": "s5",
				},
			},
		},
		"node5": {
			ObjectMeta: metav1.ObjectMeta{
				Name: "node5",
				Labels: map[string]string{
					"volcano.sh_torA2-1": "s2",
					"volcano.sh/torA2-2": "s5",
				},
			},
		},
		"node6": {
			ObjectMeta: metav1.ObjectMeta{
				Name: "node6",
				Labels: map[string]string{
					"volcano.sh_torA2-1": "s3",
					"volcano.sh/torA2-2": "s5",
				},
			},
		},
		"node7": {
			ObjectMeta: metav1.ObjectMeta{
				Name: "node7",
				Labels: map[string]string{
					"volcano.sh_torA2-1": "s3",
					"volcano.sh/torA2-2": "s5",
				},
			},
		},
	}
}

func expectedHyperNodesForTest2() map[string]*topologyv1alpha1.HyperNode {
	return map[string]*topologyv1alpha1.HyperNode{
		"hypernode-topologya2-tier1-jcdfg": utils.BuildHyperNodeWithTierName("hypernode-topologya2-tier1-jcdfg", 1, "volcano.sh_torA2-1",
			[]topologyv1alpha1.MemberSpec{
				{
					Type:     topologyv1alpha1.MemberTypeNode,
					Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: "node0"}},
				},
				{
					Type:     topologyv1alpha1.MemberTypeNode,
					Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: "node1"}},
				},
			}, map[string]string{api.NetworkTopologySourceLabelKey: "label",
				"volcano.sh_torA2-1": "s0"}),
		"hypernode-topologya2-tier1-cjain": utils.BuildHyperNodeWithTierName("hypernode-topologya2-tier1-cjain", 1, "volcano.sh_torA2-1",
			[]topologyv1alpha1.MemberSpec{
				{
					Type:     topologyv1alpha1.MemberTypeNode,
					Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: "node2"}},
				},
				{
					Type:     topologyv1alpha1.MemberTypeNode,
					Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: "node3"}},
				},
			}, map[string]string{api.NetworkTopologySourceLabelKey: "label",
				"volcano.sh_torA2-1": "s1"}),
		"hypernode-topologya2-tier2-fanfn": utils.BuildHyperNodeWithTierName("hypernode-topologya2-tier2-fanfn", 2, "volcano.sh_torA2-1",
			[]topologyv1alpha1.MemberSpec{
				{
					Type:     topologyv1alpha1.MemberTypeHyperNode,
					Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: "hypernode-topologya2-tier1-cjain"}},
				}}, map[string]string{api.NetworkTopologySourceLabelKey: "label",
				"volcano.sh_torA2-2": "s2"}),
	}
}

func expectedNodeForTest2() map[string]*corev1.Node {
	return map[string]*corev1.Node{
		"node0": {
			ObjectMeta: metav1.ObjectMeta{
				Name: "node0",
				Labels: map[string]string{
					"volcano.sh_torA2-1": "s0",
				},
			},
		},
		"node1": {
			ObjectMeta: metav1.ObjectMeta{
				Name: "node1",
				Labels: map[string]string{
					"volcano.sh_torA2-1": "s0",
					"volcano.sh/torA2-2": "s2",
				},
			},
		},
		"node2": {
			ObjectMeta: metav1.ObjectMeta{
				Name: "node2",
				Labels: map[string]string{
					"volcano.sh_torA2-1": "s1",
				},
			},
		},
		"node3": {
			ObjectMeta: metav1.ObjectMeta{
				Name: "node3",
				Labels: map[string]string{
					"volcano.sh_torA2-1": "s1",
				},
			},
		},
	}
}

func expectedHyperNodesForTest3() map[string]*topologyv1alpha1.HyperNode {
	return map[string]*topologyv1alpha1.HyperNode{
		"hypernode-topologya2-tier1-fanfn": utils.BuildHyperNodeWithTierName("hypernode-topologya2-tier1-fanfn", 1, "volcano.sh_torA2-1",
			[]topologyv1alpha1.MemberSpec{
				{
					Type:     topologyv1alpha1.MemberTypeNode,
					Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: "node1"}},
				},
			}, map[string]string{api.NetworkTopologySourceLabelKey: "label",
				"volcano.sh_torA2-1": "s0"}),
	}
}

func expectedNodeForTest3() map[string]*corev1.Node {
	return map[string]*corev1.Node{
		"node0": {
			ObjectMeta: metav1.ObjectMeta{
				Name: "node0",
				Labels: map[string]string{
					"volcano.sh/torA2-2": "s2",
				},
			},
		},
		"node1": {
			ObjectMeta: metav1.ObjectMeta{
				Name: "node1",
				Labels: map[string]string{
					"volcano.sh_torA2-1": "s0",
				},
			},
		},
		"node2": {
			ObjectMeta: metav1.ObjectMeta{
				Name: "node2",
				Labels: map[string]string{
					"volcano.sh/torA2-2": "s2",
				},
			},
		},
		"node3": {
			ObjectMeta: metav1.ObjectMeta{
				Name: "node3",
				Labels: map[string]string{
					"volcano.sh/torA2-2": "s2",
				},
			},
		},
	}
}

func getExistHyperNodesForTest4() map[string]*topologyv1alpha1.HyperNode {
	return map[string]*topologyv1alpha1.HyperNode{
		"hypernode-topologya2-tier1-mnsx6": utils.BuildHyperNode("hypernode-topologya2-tier1-mnsx6", 1,
			[]topologyv1alpha1.MemberSpec{
				{
					Type:     topologyv1alpha1.MemberTypeNode,
					Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: "node0"}},
				},
			}, map[string]string{api.NetworkTopologySourceLabelKey: "label",
				"volcano.sh/torA3-1": "s0"}),
	}
}

func expectedHyperNodesForTest4() map[string]*topologyv1alpha1.HyperNode {
	return map[string]*topologyv1alpha1.HyperNode{
		"hn-topologya3-s0": utils.BuildHyperNodeWithTierName("hn-topologya3-s0", 1, "volcano.sh/torA3-1",
			[]topologyv1alpha1.MemberSpec{
				{
					Type:     topologyv1alpha1.MemberTypeNode,
					Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: "node0"}},
				},
			}, map[string]string{api.NetworkTopologySourceLabelKey: "label",
				"volcano.sh/torA3-1": "s0"}),
	}
}

func expectedNodeForTest4() map[string]*corev1.Node {
	return map[string]*corev1.Node{
		"node0": {
			ObjectMeta: metav1.ObjectMeta{
				Name: "node0",
				Labels: map[string]string{
					"volcano.sh/torA3-1": "s0",
				},
			},
		},
	}
}

func getExistHyperNodesForTest5() map[string]*topologyv1alpha1.HyperNode {
	return map[string]*topologyv1alpha1.HyperNode{
		"hypernode-topologya2-tier1-mnsx6": utils.BuildHyperNodeWithTierName("hypernode-topologya2-tier1-mnsx6", 1, "volcano.sh/torA3-1",
			[]topologyv1alpha1.MemberSpec{
				{
					Type:     topologyv1alpha1.MemberTypeNode,
					Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: "node0"}},
				},
			}, map[string]string{api.NetworkTopologySourceLabelKey: "label",
				"volcano.sh/torA3-1": "s0"}),
	}
}

func expectedHyperNodesForTest5() map[string]*topologyv1alpha1.HyperNode {
	return map[string]*topologyv1alpha1.HyperNode{
		"hn-topologya3-s0": utils.BuildHyperNodeWithTierName("hn-topologya3-s0", 1, "volcano.sh/torA3-1",
			[]topologyv1alpha1.MemberSpec{
				{
					Type:     topologyv1alpha1.MemberTypeNode,
					Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: "node0"}},
				},
			}, map[string]string{api.NetworkTopologySourceLabelKey: "label",
				"volcano.sh/torA3-1": "s0"}),
	}
}

func expectedNodeForTest5() map[string]*corev1.Node {
	return map[string]*corev1.Node{
		"node0": {
			ObjectMeta: metav1.ObjectMeta{
				Name: "node0",
				Labels: map[string]string{
					"volcano.sh/torA3-1": "s0",
				},
			},
		},
	}
}

func TestGenerateHyperNodesWithMixedTopologyProfiles(t *testing.T) {
	const (
		profileLabel    = "example.com/topology-profile"
		hyperCluster    = "volcano.sh/hypercluster"
		hyperNode       = "volcano.sh/hypernode"
		superPod        = "volcano.sh/superpod"
		hyperNodeKey    = "example.com/hypernode-domain"
		hyperClusterKey = "example.com/hypercluster-domain"
	)

	cfg := api.DiscoveryConfig{
		Source: "label",
		Config: map[string]interface{}{
			"networkTopologyTypes": map[string]interface{}{
				"topologyA3": map[string]interface{}{
					"nodeSelector": map[string]interface{}{
						"matchLabels": map[string]interface{}{profileLabel: "a3"},
					},
					"levels": []interface{}{
						map[string]interface{}{"nodeLabel": hyperClusterKey, "tierName": hyperCluster},
						map[string]interface{}{"nodeLabel": hyperNodeKey, "tierName": hyperNode},
						map[string]interface{}{"nodeLabel": "kubernetes.io/hostname"},
					},
				},
				"topologyA5": map[string]interface{}{
					"nodeSelector": map[string]interface{}{
						"matchLabels": map[string]interface{}{profileLabel: "a5"},
					},
					"levels": []interface{}{
						map[string]interface{}{"nodeLabel": hyperClusterKey, "tierName": hyperCluster},
						map[string]interface{}{"nodeLabel": hyperNodeKey, "tierName": hyperNode},
						map[string]interface{}{"nodeLabel": "example.com/superpod-domain", "tierName": superPod},
						map[string]interface{}{"nodeLabel": "kubernetes.io/hostname"},
					},
				},
			},
		},
	}
	nodes := []*corev1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "a3-node", Labels: map[string]string{
				profileLabel: "a3", hyperNodeKey: "hn-0", hyperClusterKey: "hc-0",
			}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "a5-node", Labels: map[string]string{
				profileLabel: "a5", hyperNodeKey: "hn-0", hyperClusterKey: "hc-0",
				"example.com/superpod-domain": "sp-0",
			}},
		},
	}

	discoverer := newDiscovererForGenerationTest(t, cfg, nodes)
	infoMap, err := discoverer.generateHyperNodeInfo()
	require.NoError(t, err)
	require.Len(t, infoMap, 5)

	// Rebuilding from the same desired state must produce stable names and
	// graph content even though networkTopologyTypes is backed by a Go map.
	secondInfoMap, err := discoverer.generateHyperNodeInfo()
	require.NoError(t, err)
	assert.Equal(t, infoMap, secondInfoMap)

	byProfileTier := make(map[string]map[int]*topologyv1alpha1.HyperNode)
	for _, hn := range discoverer.buildHyperNodes(infoMap) {
		profile := hn.Labels[api.NetworkTopologyProfileLabelKey]
		if byProfileTier[profile] == nil {
			byProfileTier[profile] = make(map[int]*topologyv1alpha1.HyperNode)
		}
		require.Nil(t, byProfileTier[profile][hn.Spec.Tier], "fixture expects one domain per profile tier")
		byProfileTier[profile][hn.Spec.Tier] = hn
	}

	require.Len(t, byProfileTier["topologya3"], 2)
	require.Len(t, byProfileTier["topologya5"], 3)
	assert.Equal(t, hyperNode, byProfileTier["topologya3"][1].Spec.TierName)
	assert.Equal(t, hyperCluster, byProfileTier["topologya3"][2].Spec.TierName)
	assert.Equal(t, superPod, byProfileTier["topologya5"][1].Spec.TierName)
	assert.Equal(t, hyperNode, byProfileTier["topologya5"][2].Spec.TierName)
	assert.Equal(t, hyperCluster, byProfileTier["topologya5"][3].Spec.TierName)

	// A3 and A5 intentionally reuse the same domain key/value. Profile-scoped
	// identity must still create independent HyperNodes at different tiers.
	a3HyperNode := byProfileTier["topologya3"][1]
	a5HyperNode := byProfileTier["topologya5"][2]
	assert.Equal(t, "hn-0", a3HyperNode.Labels[hyperNodeKey])
	assert.Equal(t, "hn-0", a5HyperNode.Labels[hyperNodeKey])
	assert.NotEqual(t, a3HyperNode.Name, a5HyperNode.Name)
	assert.Equal(t, []string{"a3-node"}, exactMemberNames(a3HyperNode))
	assert.Equal(t, []string{"a5-node"}, exactMemberNames(byProfileTier["topologya5"][1]))
	assert.Equal(t, []string{a5HyperNode.Name}, exactMemberNames(byProfileTier["topologya5"][3]))
}

func TestGenerateHyperNodesRejectsMultipleProfileMatches(t *testing.T) {
	profile := func(levelLabel string) map[string]interface{} {
		return map[string]interface{}{
			"nodeSelector": map[string]interface{}{
				"matchLabels": map[string]interface{}{"example.com/role": "compute"},
			},
			"levels": []interface{}{
				map[string]interface{}{"nodeLabel": levelLabel, "tierName": "volcano.sh/hypernode"},
				map[string]interface{}{"nodeLabel": "kubernetes.io/hostname"},
			},
		}
	}
	cfg := api.DiscoveryConfig{
		Source: "label",
		Config: map[string]interface{}{
			"networkTopologyTypes": map[string]interface{}{
				"topologyA3": profile("example.com/a3-hypernode"),
				"topologyA5": profile("example.com/a5-hypernode"),
			},
		},
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "ambiguous-node", Labels: map[string]string{
		"example.com/role":         "compute",
		"example.com/a3-hypernode": "a3-hn",
		"example.com/a5-hypernode": "a5-hn",
	}}}

	discoverer := newDiscovererForGenerationTest(t, cfg, []*corev1.Node{node})
	infoMap, err := discoverer.generateHyperNodeInfo()
	require.ErrorContains(t, err, "matches multiple topology profiles")
	assert.Empty(t, infoMap, "an ambiguous node must not publish a partial topology")
}

func TestGenerateHyperNodesRejectsMultipleParents(t *testing.T) {
	cfg := api.DiscoveryConfig{Config: map[string]interface{}{
		"networkTopologyTypes": map[string]interface{}{
			"topologyA3": map[string]interface{}{
				"nodeSelector": map[string]interface{}{
					"matchLabels": map[string]interface{}{"example.com/profile": "a3"},
				},
				"levels": []interface{}{
					map[string]interface{}{"nodeLabel": "example.com/hypercluster", "tierName": "volcano.sh/hypercluster"},
					map[string]interface{}{"nodeLabel": "example.com/hypernode", "tierName": "volcano.sh/hypernode"},
					map[string]interface{}{"nodeLabel": "kubernetes.io/hostname"},
				},
			},
		},
	}}
	nodes := []*corev1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{
			"example.com/profile": "a3", "example.com/hypernode": "hn-0", "example.com/hypercluster": "hc-0",
		}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{
			"example.com/profile": "a3", "example.com/hypernode": "hn-0", "example.com/hypercluster": "hc-1",
		}}},
	}

	discoverer := newDiscovererForGenerationTest(t, cfg, nodes)
	_, err := discoverer.generateHyperNodeInfo()
	require.ErrorContains(t, err, "belongs to multiple parents")
}

func TestParseCfgRejectsDuplicateSemanticTierName(t *testing.T) {
	cfg := api.DiscoveryConfig{Config: map[string]interface{}{
		"networkTopologyTypes": map[string]interface{}{
			"topologyA5": map[string]interface{}{
				"levels": []interface{}{
					map[string]interface{}{"nodeLabel": "example.com/cluster", "tierName": "volcano.sh/hypernode"},
					map[string]interface{}{"nodeLabel": "example.com/hypernode", "tierName": "volcano.sh/hypernode"},
					map[string]interface{}{"nodeLabel": "kubernetes.io/hostname"},
				},
			},
		},
	}}

	_, _, err := parseCfg(cfg)
	require.ErrorContains(t, err, "duplicate tierName")
}

func TestParseMixedTopologyProfilesFromYAML(t *testing.T) {
	const configYAML = `
networkTopologyDiscovery:
  - source: label
    enabled: true
    config:
      networkTopologyTypes:
        topologyA3:
          nodeSelector:
            matchLabels:
              volcano.sh/network-topology-profile: a3
          levels:
            - nodeLabel: volcano.sh/a3-hypercluster
              tierName: volcano.sh/hypercluster
            - nodeLabel: volcano.sh/a3-hypernode
              tierName: volcano.sh/hypernode
            - nodeLabel: kubernetes.io/hostname
        topologyA5:
          nodeSelector:
            matchLabels:
              volcano.sh/network-topology-profile: a5
          levels:
            - nodeLabel: volcano.sh/a5-hypercluster
              tierName: volcano.sh/hypercluster
            - nodeLabel: volcano.sh/a5-hypernode
              tierName: volcano.sh/hypernode
            - nodeLabel: volcano.sh/a5-superpod
              tierName: volcano.sh/superpod
            - nodeLabel: kubernetes.io/hostname
`
	config := &api.NetworkTopologyConfig{}
	require.NoError(t, yaml.Unmarshal([]byte(configYAML), config))
	require.Len(t, config.NetworkTopologyDiscovery, 1)

	profiles, watchedKeys, err := parseCfg(config.NetworkTopologyDiscovery[0])
	require.NoError(t, err)
	require.Len(t, profiles, 2)
	assert.Equal(t, "topologyA3", profiles[0].name)
	assert.Equal(t, []NodeLabel{
		{NodeLabel: "volcano.sh/a3-hypernode", TierName: "volcano.sh/hypernode"},
		{NodeLabel: "volcano.sh/a3-hypercluster", TierName: "volcano.sh/hypercluster"},
	}, profiles[0].levels)
	assert.Contains(t, watchedKeys, "volcano.sh/network-topology-profile")
	assert.Contains(t, watchedKeys, "volcano.sh/a5-superpod")
}

func newDiscovererForGenerationTest(t *testing.T, cfg api.DiscoveryConfig, nodes []*corev1.Node) *labelDiscoverer {
	t.Helper()
	discoverer, ok := NewLabelDiscoverer(cfg, fake.NewSimpleClientset(), vcclientset.NewSimpleClientset()).(*labelDiscoverer)
	require.True(t, ok)
	require.NoError(t, discoverer.configErr)
	for _, node := range nodes {
		require.NoError(t, discoverer.nodeInformer.Informer().GetIndexer().Add(node))
	}
	return discoverer
}

func exactMemberNames(hyperNode *topologyv1alpha1.HyperNode) []string {
	names := make([]string, 0, len(hyperNode.Spec.Members))
	for _, member := range hyperNode.Spec.Members {
		if member.Selector.ExactMatch != nil {
			names = append(names, member.Selector.ExactMatch.Name)
		}
	}
	sort.Strings(names)
	return names
}

func createHyperNode(vcClient vcclient.Interface, nodeMap map[string]*topologyv1alpha1.HyperNode) {
	for _, node := range nodeMap {
		vcClient.TopologyV1alpha1().HyperNodes().Create(
			context.Background(),
			node,
			metav1.CreateOptions{},
		)
	}
}
