package framework

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/ptr"

	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	"volcano.sh/apis/pkg/apis/scheduling"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	topologyv1alpha1 "volcano.sh/apis/pkg/apis/topology/v1alpha1"
	"volcano.sh/volcano/pkg/scheduler/api"
)

func TestSessionEnsureTopologyTrees(t *testing.T) {
	newHyperNode := func(name string, tier int, children ...string) *api.HyperNodeInfo {
		info := api.NewHyperNodeInfo(api.BuildHyperNode(name, tier, nil))
		info.Children.Insert(children...)
		return info
	}

	hyperNodes := api.HyperNodeInfoMap{
		"a3-leaf":           newHyperNode("a3-leaf", 1),
		"a3-root":           newHyperNode("a3-root", 2, "a3-leaf"),
		"a5-leaf":           newHyperNode("a5-leaf", 1),
		"a5-middle":         newHyperNode("a5-middle", 2, "a5-leaf"),
		"a5-root":           newHyperNode("a5-root", 3, "a5-middle"),
		ClusterTopHyperNode: newHyperNode(ClusterTopHyperNode, 4, "a3-root", "a5-root"),
	}
	for _, parent := range hyperNodes {
		for child := range parent.Children {
			hyperNodes[child].Parent = parent.Name
		}
	}

	ssn := &Session{
		HyperNodes: hyperNodes,
		RealNodesSet: map[string]sets.Set[string]{
			"a3-root": sets.New("a3-node"),
			"a5-root": sets.New("a5-node"),
		},
	}
	ssn.EnsureTopologyTrees()

	assert.Equal(t, sets.New("a3-root", "a5-root"), sets.KeySet(ssn.TopologyTrees))
	assert.Equal(t, []int{1, 2}, ssn.TopologyTrees["a3-root"].Tiers)
	assert.Equal(t, []int{1, 2, 3}, ssn.TopologyTrees["a5-root"].Tiers)
	assert.Equal(t, sets.New("a3-root", "a3-leaf"), ssn.TopologyTrees["a3-root"].HyperNodes)
	assert.Equal(t, sets.New("a5-root", "a5-middle", "a5-leaf"), ssn.TopologyTrees["a5-root"].HyperNodes)
	assert.Equal(t, sets.New("a3-node"), ssn.TopologyTrees["a3-root"].RealNodes)
	assert.Equal(t, sets.New("a5-node"), ssn.TopologyTrees["a5-root"].RealNodes)
	assert.Equal(t, "a3-root", ssn.HyperNodeToTopologyTree["a3-leaf"])
	assert.Equal(t, "a5-root", ssn.HyperNodeToTopologyTree["a5-middle"])
	_, clusterRootIndexed := ssn.HyperNodeToTopologyTree[ClusterTopHyperNode]
	assert.False(t, clusterRootIndexed)
}

func TestSessionRecoverAllocatedHyperNodeAcrossMixedTopology(t *testing.T) {
	newHyperNode := func(name string, tier int, children ...string) *api.HyperNodeInfo {
		info := api.NewHyperNodeInfo(api.BuildHyperNode(name, tier, nil))
		info.Children.Insert(children...)
		return info
	}

	hyperNodes := api.HyperNodeInfoMap{
		"a3-hypernode-0": newHyperNode("a3-hypernode-0", 1),
		"a3-hypernode-1": newHyperNode("a3-hypernode-1", 1),
		"a3-hypercluster": newHyperNode(
			"a3-hypercluster", 2, "a3-hypernode-0", "a3-hypernode-1"),
		"a5-superpod-0": newHyperNode("a5-superpod-0", 1),
		"a5-superpod-1": newHyperNode("a5-superpod-1", 1),
		"a5-hypernode": newHyperNode(
			"a5-hypernode", 2, "a5-superpod-0", "a5-superpod-1"),
		"a5-hypercluster": newHyperNode("a5-hypercluster", 3, "a5-hypernode"),
		ClusterTopHyperNode: newHyperNode(
			ClusterTopHyperNode, 4, "a3-hypercluster", "a5-hypercluster"),
	}
	for _, parent := range hyperNodes {
		for child := range parent.Children {
			hyperNodes[child].Parent = parent.Name
		}
	}

	realNodes := map[string]sets.Set[string]{
		"a3-hypernode-0": sets.New("a3-node-0", "a3-node-1"),
		"a3-hypernode-1": sets.New("a3-node-2", "a3-node-3"),
		"a3-hypercluster": sets.New(
			"a3-node-0", "a3-node-1", "a3-node-2", "a3-node-3"),
		"a5-superpod-0": sets.New("a5-node-0", "a5-node-1"),
		"a5-superpod-1": sets.New("a5-node-2", "a5-node-3"),
		"a5-hypernode": sets.New(
			"a5-node-0", "a5-node-1", "a5-node-2", "a5-node-3"),
		"a5-hypercluster": sets.New(
			"a5-node-0", "a5-node-1", "a5-node-2", "a5-node-3"),
		ClusterTopHyperNode: sets.New(
			"a3-node-0", "a3-node-1", "a3-node-2", "a3-node-3",
			"a5-node-0", "a5-node-1", "a5-node-2", "a5-node-3"),
	}

	const (
		namespace    = "test"
		podGroupName = "mixed-recovery"
		taskName     = "worker"
	)
	jobID := api.JobID(namespace + "/" + podGroupName)
	subGroupSize := int32(2)
	minSubGroups := int32(2)
	job := api.NewJobInfo(jobID)
	job.SetPodGroup(&api.PodGroup{PodGroup: scheduling.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: podGroupName, Namespace: namespace},
		Spec: scheduling.PodGroupSpec{
			MinMember: 4,
			NetworkTopology: &scheduling.NetworkTopologySpec{
				Mode:            scheduling.HardNetworkTopologyMode,
				HighestTierName: "volcano.sh/hypercluster",
			},
			SubGroupPolicy: []scheduling.SubGroupPolicySpec{{
				Name:         taskName,
				SubGroupSize: &subGroupSize,
				MinSubGroups: &minSubGroups,
				LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
					batchv1alpha1.TaskSpecKey: taskName,
				}},
				MatchLabelKeys: []string{batchv1alpha1.TaskPartitionID},
				NetworkTopology: &scheduling.NetworkTopologySpec{
					Mode:            scheduling.HardNetworkTopologyMode,
					HighestTierName: "volcano.sh/superpod",
				},
			}},
		},
	}})

	for partition, nodes := range [][]string{
		{"a5-node-0", "a5-node-1"},
		{"a5-node-2", "a5-node-3"},
	} {
		for index, nodeName := range nodes {
			podName := fmt.Sprintf("worker-%d-%d", partition, index)
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      podName,
					Namespace: namespace,
					UID:       types.UID(podName),
					Labels: map[string]string{
						batchv1alpha1.TaskSpecKey:     taskName,
						batchv1alpha1.TaskPartitionID: fmt.Sprint(partition),
					},
					Annotations: map[string]string{
						schedulingv1beta1.KubeGroupNameAnnotationKey: podGroupName,
					},
				},
				Spec:   v1.PodSpec{NodeName: nodeName},
				Status: v1.PodStatus{Phase: v1.PodRunning},
			}
			job.AddTaskInfo(api.NewTaskInfo(pod))
		}
	}

	ssn := &Session{DirtyJobs: sets.New[api.JobID]()}
	ssn.recoverAllocatedHyperNode(job, sets.KeySet(hyperNodes), hyperNodes, realNodes)

	assert.Equal(t, "a5-hypernode", job.AllocatedHyperNode)
	assert.Len(t, job.SubJobs, 2)
	expectedSubGroupHyperNodes := map[int]string{
		0: "a5-superpod-0",
		1: "a5-superpod-1",
	}
	for _, subJob := range job.SubJobs {
		assert.Equal(t, expectedSubGroupHyperNodes[subJob.MatchIndex], subJob.AllocatedHyperNode)
	}
	assert.True(t, ssn.DirtyJobs.Has(jobID))
}

func TestSession_adjustNetworkTopologySpec(t *testing.T) {
	tests := []struct {
		name         string
		jobs         map[api.JobID]*api.JobInfo
		nameMap      api.HyperNodeTierNameMap
		expectedJobs map[api.JobID]*api.JobInfo
	}{
		{
			name: "job with highestTierAllowed, no translation",
			jobs: map[api.JobID]*api.JobInfo{
				"test-uid": {
					PodGroup: &api.PodGroup{
						PodGroup: scheduling.PodGroup{
							Spec: scheduling.PodGroupSpec{
								NetworkTopology: &scheduling.NetworkTopologySpec{
									HighestTierName:    "",
									HighestTierAllowed: ptr.To(2),
								},
								SubGroupPolicy: []scheduling.SubGroupPolicySpec{
									{
										NetworkTopology: &scheduling.NetworkTopologySpec{
											HighestTierName:    "",
											HighestTierAllowed: ptr.To(1),
										},
									},
								},
							},
						},
					},
					SubJobs: map[api.SubJobID]*api.SubJobInfo{
						"test-uid": {
							NetworkTopology: &scheduling.NetworkTopologySpec{
								HighestTierName:    "",
								HighestTierAllowed: ptr.To(1),
							},
						},
					},
				},
			},
			nameMap: api.HyperNodeTierNameMap{
				"volcano.sh/hypernode":    1,
				"volcano.sh/hypercluster": 2,
			},
			expectedJobs: map[api.JobID]*api.JobInfo{
				"test-uid": {
					PodGroup: &api.PodGroup{
						PodGroup: scheduling.PodGroup{
							Spec: scheduling.PodGroupSpec{
								NetworkTopology: &scheduling.NetworkTopologySpec{
									HighestTierName:    "",
									HighestTierAllowed: ptr.To(2),
								},
								SubGroupPolicy: []scheduling.SubGroupPolicySpec{
									{
										NetworkTopology: &scheduling.NetworkTopologySpec{
											HighestTierName:    "",
											HighestTierAllowed: ptr.To(1),
										},
									},
								},
							},
						},
					},
					SubJobs: map[api.SubJobID]*api.SubJobInfo{
						"test-uid": {
							NetworkTopology: &scheduling.NetworkTopologySpec{
								HighestTierName:    "",
								HighestTierAllowed: ptr.To(1),
							},
						},
					},
				},
			},
		},
		{
			name: "job with highestTierName is preserved for branch resolution",
			jobs: map[api.JobID]*api.JobInfo{
				"test-uid": {
					PodGroup: &api.PodGroup{
						PodGroup: scheduling.PodGroup{
							Spec: scheduling.PodGroupSpec{
								NetworkTopology: &scheduling.NetworkTopologySpec{
									HighestTierName:    "volcano.sh/hypercluster",
									HighestTierAllowed: nil,
								},
								SubGroupPolicy: []scheduling.SubGroupPolicySpec{
									{
										NetworkTopology: &scheduling.NetworkTopologySpec{
											HighestTierName:    "volcano.sh/hypernode",
											HighestTierAllowed: nil,
										},
									},
								},
							},
						},
					},
					SubJobs: map[api.SubJobID]*api.SubJobInfo{
						"test-uid": {
							NetworkTopology: &scheduling.NetworkTopologySpec{
								HighestTierName:    "volcano.sh/hypernode",
								HighestTierAllowed: nil,
							},
						},
					},
				},
			},
			nameMap: api.HyperNodeTierNameMap{
				"volcano.sh/hypernode":    1,
				"volcano.sh/hypercluster": 2,
			},
			expectedJobs: map[api.JobID]*api.JobInfo{
				"test-uid": {
					PodGroup: &api.PodGroup{
						PodGroup: scheduling.PodGroup{
							Spec: scheduling.PodGroupSpec{
								NetworkTopology: &scheduling.NetworkTopologySpec{
									HighestTierName:    "volcano.sh/hypercluster",
									HighestTierAllowed: nil,
								},
								SubGroupPolicy: []scheduling.SubGroupPolicySpec{
									{
										NetworkTopology: &scheduling.NetworkTopologySpec{
											HighestTierName:    "volcano.sh/hypernode",
											HighestTierAllowed: nil,
										},
									},
								},
							},
						},
					},
					SubJobs: map[api.SubJobID]*api.SubJobInfo{
						"test-uid": {
							NetworkTopology: &scheduling.NetworkTopologySpec{
								HighestTierName:    "volcano.sh/hypernode",
								HighestTierAllowed: nil,
							},
						},
					},
				},
			},
		},
		{
			name: "job with highestTierName, failed to translate",
			jobs: map[api.JobID]*api.JobInfo{
				"test-uid": {
					PodGroup: &api.PodGroup{
						PodGroup: scheduling.PodGroup{
							Spec: scheduling.PodGroupSpec{
								NetworkTopology: &scheduling.NetworkTopologySpec{
									HighestTierName:    "volcano.sh/hypercluster-test",
									HighestTierAllowed: nil,
								},
								SubGroupPolicy: []scheduling.SubGroupPolicySpec{
									{
										NetworkTopology: &scheduling.NetworkTopologySpec{
											HighestTierName:    "volcano.sh/hypernode-test",
											HighestTierAllowed: nil,
										},
									},
								},
							},
						},
					},
					SubJobs: map[api.SubJobID]*api.SubJobInfo{
						"test-uid": {
							NetworkTopology: &scheduling.NetworkTopologySpec{
								HighestTierName:    "volcano.sh/hypernode",
								HighestTierAllowed: ptr.To(1),
							},
						},
					},
				},
			},
			nameMap: api.HyperNodeTierNameMap{
				"volcano.sh/hypernode":    1,
				"volcano.sh/hypercluster": 2,
			},
			expectedJobs: map[api.JobID]*api.JobInfo{
				"test-uid": {
					PodGroup: &api.PodGroup{
						PodGroup: scheduling.PodGroup{
							Spec: scheduling.PodGroupSpec{
								NetworkTopology: &scheduling.NetworkTopologySpec{
									HighestTierName:    "volcano.sh/hypercluster-test",
									HighestTierAllowed: nil,
								},
								SubGroupPolicy: []scheduling.SubGroupPolicySpec{
									{
										NetworkTopology: &scheduling.NetworkTopologySpec{
											HighestTierName:    "volcano.sh/hypernode-test",
											HighestTierAllowed: nil,
										},
									},
								},
							},
						},
					},
					SubJobs: map[api.SubJobID]*api.SubJobInfo{
						"test-uid": {
							NetworkTopology: &scheduling.NetworkTopologySpec{
								HighestTierName:    "volcano.sh/hypernode",
								HighestTierAllowed: ptr.To(1),
							},
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, job := range test.jobs {
				if job.PodGroup != nil && job.NetworkTopology == nil {
					job.NetworkTopology = job.PodGroup.Spec.NetworkTopology.DeepCopy()
				}
			}
			for _, job := range test.expectedJobs {
				if job.PodGroup != nil && job.NetworkTopology == nil {
					job.NetworkTopology = job.PodGroup.Spec.NetworkTopology.DeepCopy()
				}
			}
			ssn := &Session{
				Jobs:                 test.jobs,
				HyperNodeTierNameMap: test.nameMap,
			}
			ssn.adjustNetworkTopologySpec()
			for jobID, expectedJob := range test.expectedJobs {
				gotJob := ssn.Jobs[jobID]
				assert.Equal(t, expectedJob.NetworkTopology.HighestTierName,
					gotJob.NetworkTopology.HighestTierName, "job highestTierName should be equal")
				assert.Equal(t, expectedJob.NetworkTopology.HighestTierAllowed,
					gotJob.NetworkTopology.HighestTierAllowed, "job highestTierAllowed should be equal")
				for subJobID := range expectedJob.SubJobs {
					assert.Equal(t, expectedJob.SubJobs[subJobID].NetworkTopology.HighestTierName,
						gotJob.SubJobs[subJobID].NetworkTopology.HighestTierName, "subJob highestTierName should be equal")
					assert.Equal(t, expectedJob.SubJobs[subJobID].NetworkTopology.HighestTierAllowed,
						gotJob.SubJobs[subJobID].NetworkTopology.HighestTierAllowed, "subJob highestTierAllowed should be equal")
				}
			}
		})
	}
}

func TestAdjustNetworkTopologySpec_DoesNotMutatePodGroupSpec(t *testing.T) {
	maxTier := 4
	topHn := &topologyv1alpha1.HyperNode{}
	topHn.Name = ClusterTopHyperNode
	topHn.Spec.Tier = maxTier

	job := api.NewJobInfo("test-job")
	pg := &api.PodGroup{
		PodGroup: scheduling.PodGroup{
			Spec: scheduling.PodGroupSpec{
				MinMember: 4,
				NetworkTopology: &scheduling.NetworkTopologySpec{
					Mode:            scheduling.SoftNetworkTopologyMode,
					HighestTierName: "volcano.sh/hypercluster",
				},
				SubGroupPolicy: []scheduling.SubGroupPolicySpec{
					{
						Name:         "worker",
						SubGroupSize: ptr.To(int32(4)),
						NetworkTopology: &scheduling.NetworkTopologySpec{
							Mode:            scheduling.SoftNetworkTopologyMode,
							HighestTierName: "volcano.sh/hypernode",
						},
					},
				},
			},
		},
	}
	job.SetPodGroup(pg)
	job.SubJobs["test-job/worker/0"] = api.NewSubJobInfo("test-job/worker", "test-job/worker/0", job.UID, &pg.Spec.SubGroupPolicy[0], []string{"0"})

	originalJobTopology := job.PodGroup.Spec.NetworkTopology.DeepCopy()
	originalSubGroupTopology := job.PodGroup.Spec.SubGroupPolicy[0].NetworkTopology.DeepCopy()

	ssn := &Session{
		Jobs: map[api.JobID]*api.JobInfo{
			job.UID: job,
		},
		HyperNodeTierNameMap: api.HyperNodeTierNameMap{
			"volcano.sh/hypernode":    1,
			"volcano.sh/hypercluster": 2,
		},
		HyperNodes: api.HyperNodeInfoMap{
			ClusterTopHyperNode: api.NewHyperNodeInfo(topHn),
		},
	}

	ssn.adjustNetworkTopologySpec()

	assert.Equal(t, originalJobTopology, job.PodGroup.Spec.NetworkTopology)
	assert.Equal(t, originalSubGroupTopology, job.PodGroup.Spec.SubGroupPolicy[0].NetworkTopology)
}

func TestConvertSoftToHardTopology(t *testing.T) {
	maxTier := 4

	tests := []struct {
		name                     string
		jobNetworkTopology       *scheduling.NetworkTopologySpec
		subGroupPolicies         []scheduling.SubGroupPolicySpec
		wantJobMode              scheduling.NetworkTopologyMode
		wantJobTier              *int
		wantSubGroupPolicyModes  []scheduling.NetworkTopologyMode
		wantSubGroupPolicyTiers  []*int
		wantContainsHardTopology bool
	}{
		{
			name: "job-level soft topology is converted to hard",
			jobNetworkTopology: &scheduling.NetworkTopologySpec{
				Mode: scheduling.SoftNetworkTopologyMode,
			},
			wantJobMode:              scheduling.HardNetworkTopologyMode,
			wantJobTier:              ptr.To(maxTier),
			wantContainsHardTopology: true,
		},
		{
			name: "job-level hard topology is unchanged",
			jobNetworkTopology: &scheduling.NetworkTopologySpec{
				Mode:               scheduling.HardNetworkTopologyMode,
				HighestTierAllowed: ptr.To(2),
			},
			wantJobMode:              scheduling.HardNetworkTopologyMode,
			wantJobTier:              ptr.To(2),
			wantContainsHardTopology: true,
		},
		{
			name:                     "nil job topology remains nil",
			jobNetworkTopology:       nil,
			wantContainsHardTopology: false,
		},
		{
			name: "subGroupPolicy-level soft topology is converted to hard",
			subGroupPolicies: []scheduling.SubGroupPolicySpec{
				{
					Name:         "worker",
					SubGroupSize: ptr.To(int32(4)),
					NetworkTopology: &scheduling.NetworkTopologySpec{
						Mode: scheduling.SoftNetworkTopologyMode,
					},
				},
			},
			wantSubGroupPolicyModes:  []scheduling.NetworkTopologyMode{scheduling.HardNetworkTopologyMode},
			wantSubGroupPolicyTiers:  []*int{ptr.To(maxTier)},
			wantContainsHardTopology: true,
		},
		{
			name: "subGroupPolicy-level hard topology is unchanged",
			subGroupPolicies: []scheduling.SubGroupPolicySpec{
				{
					Name:         "worker",
					SubGroupSize: ptr.To(int32(4)),
					NetworkTopology: &scheduling.NetworkTopologySpec{
						Mode:               scheduling.HardNetworkTopologyMode,
						HighestTierAllowed: ptr.To(2),
					},
				},
			},
			wantSubGroupPolicyModes:  []scheduling.NetworkTopologyMode{scheduling.HardNetworkTopologyMode},
			wantSubGroupPolicyTiers:  []*int{ptr.To(2)},
			wantContainsHardTopology: true,
		},
		{
			name: "mixed: job soft + subGroupPolicy soft both converted",
			jobNetworkTopology: &scheduling.NetworkTopologySpec{
				Mode: scheduling.SoftNetworkTopologyMode,
			},
			subGroupPolicies: []scheduling.SubGroupPolicySpec{
				{
					Name:         "worker",
					SubGroupSize: ptr.To(int32(4)),
					NetworkTopology: &scheduling.NetworkTopologySpec{
						Mode: scheduling.SoftNetworkTopologyMode,
					},
				},
			},
			wantJobMode:              scheduling.HardNetworkTopologyMode,
			wantJobTier:              ptr.To(maxTier),
			wantSubGroupPolicyModes:  []scheduling.NetworkTopologyMode{scheduling.HardNetworkTopologyMode},
			wantSubGroupPolicyTiers:  []*int{ptr.To(maxTier)},
			wantContainsHardTopology: true,
		},
		{
			name: "mixed: job hard + subGroupPolicy soft (subgroup bounded by job tier)",
			jobNetworkTopology: &scheduling.NetworkTopologySpec{
				Mode:               scheduling.HardNetworkTopologyMode,
				HighestTierAllowed: ptr.To(2),
			},
			subGroupPolicies: []scheduling.SubGroupPolicySpec{
				{
					Name:         "worker",
					SubGroupSize: ptr.To(int32(4)),
					NetworkTopology: &scheduling.NetworkTopologySpec{
						Mode: scheduling.SoftNetworkTopologyMode,
					},
				},
			},
			wantJobMode:              scheduling.HardNetworkTopologyMode,
			wantJobTier:              ptr.To(2),
			wantSubGroupPolicyModes:  []scheduling.NetworkTopologyMode{scheduling.HardNetworkTopologyMode},
			wantSubGroupPolicyTiers:  []*int{ptr.To(2)}, // bounded by job's HighestTierAllowed=2
			wantContainsHardTopology: true,
		},
		{
			name: "mixed: job hard tier=3 + multiple subGroupPolicies soft (all bounded by job tier)",
			jobNetworkTopology: &scheduling.NetworkTopologySpec{
				Mode:               scheduling.HardNetworkTopologyMode,
				HighestTierAllowed: ptr.To(3),
			},
			subGroupPolicies: []scheduling.SubGroupPolicySpec{
				{
					Name:         "worker",
					SubGroupSize: ptr.To(int32(4)),
					NetworkTopology: &scheduling.NetworkTopologySpec{
						Mode: scheduling.SoftNetworkTopologyMode,
					},
				},
				{
					Name:         "ps",
					SubGroupSize: ptr.To(int32(2)),
					NetworkTopology: &scheduling.NetworkTopologySpec{
						Mode: scheduling.SoftNetworkTopologyMode,
					},
				},
			},
			wantJobMode:              scheduling.HardNetworkTopologyMode,
			wantJobTier:              ptr.To(3),
			wantSubGroupPolicyModes:  []scheduling.NetworkTopologyMode{scheduling.HardNetworkTopologyMode, scheduling.HardNetworkTopologyMode},
			wantSubGroupPolicyTiers:  []*int{ptr.To(3), ptr.To(3)}, // both bounded by job's HighestTierAllowed=3
			wantContainsHardTopology: true,
		},
		{
			name: "multiple subGroupPolicies: some soft some hard",
			subGroupPolicies: []scheduling.SubGroupPolicySpec{
				{
					Name:         "worker",
					SubGroupSize: ptr.To(int32(4)),
					NetworkTopology: &scheduling.NetworkTopologySpec{
						Mode: scheduling.SoftNetworkTopologyMode,
					},
				},
				{
					Name:         "ps",
					SubGroupSize: ptr.To(int32(2)),
					NetworkTopology: &scheduling.NetworkTopologySpec{
						Mode:               scheduling.HardNetworkTopologyMode,
						HighestTierAllowed: ptr.To(1),
					},
				},
			},
			wantSubGroupPolicyModes:  []scheduling.NetworkTopologyMode{scheduling.HardNetworkTopologyMode, scheduling.HardNetworkTopologyMode},
			wantSubGroupPolicyTiers:  []*int{ptr.To(maxTier), ptr.To(1)},
			wantContainsHardTopology: true,
		},
		{
			name: "subGroupPolicy with nil NetworkTopology is unchanged",
			subGroupPolicies: []scheduling.SubGroupPolicySpec{
				{
					Name:            "worker",
					SubGroupSize:    ptr.To(int32(4)),
					NetworkTopology: nil,
				},
			},
			wantContainsHardTopology: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build JobInfo with PodGroup
			job := api.NewJobInfo("test-job")
			pg := &api.PodGroup{
				PodGroup: scheduling.PodGroup{
					Spec: scheduling.PodGroupSpec{
						MinMember:       4,
						NetworkTopology: tt.jobNetworkTopology,
						SubGroupPolicy:  tt.subGroupPolicies,
					},
				},
			}
			job.SetPodGroup(pg)

			// Create SubJobs based on SubGroupPolicy
			for _, policy := range tt.subGroupPolicies {
				policyCopy := policy
				subJobID := api.SubJobID(fmt.Sprintf("test-job/%s/0", policy.Name))
				gid := api.SubJobGID(fmt.Sprintf("test-job/%s", policy.Name))
				job.SubJobs[subJobID] = api.NewSubJobInfo(gid, subJobID, "test-job", &policyCopy, []string{"0"})
			}
			// Create default SubJob if no SubGroupPolicy
			if len(tt.subGroupPolicies) == 0 {
				defaultSubJobID := job.DefaultSubJobID()
				defaultPolicy := &scheduling.SubGroupPolicySpec{
					SubGroupSize: ptr.To(int32(4)),
				}
				if tt.jobNetworkTopology != nil {
					defaultPolicy.NetworkTopology = tt.jobNetworkTopology.DeepCopy()
				}
				gid := api.SubJobGID(string(job.UID))
				job.SubJobs[defaultSubJobID] = api.NewSubJobInfo(gid, defaultSubJobID, job.UID, defaultPolicy, nil)
			}

			// Call the function under test
			convertSoftToHardTopology(job, maxTier)

			// Verify job-level NetworkTopology
			if tt.jobNetworkTopology != nil {
				assert.NotNil(t, job.NetworkTopology)
				assert.Equal(t, tt.wantJobMode, job.NetworkTopology.Mode,
					"job-level mode mismatch")
				if tt.wantJobTier != nil {
					assert.NotNil(t, job.NetworkTopology.HighestTierAllowed)
					assert.Equal(t, *tt.wantJobTier, *job.NetworkTopology.HighestTierAllowed,
						"job-level tier mismatch")
				}
			} else {
				assert.Nil(t, job.NetworkTopology,
					"job-level topology should remain nil")
			}

			// Verify SubJob-level NetworkTopology derived from SubGroupPolicy.
			for i, policy := range tt.subGroupPolicies {
				if i < len(tt.wantSubGroupPolicyModes) && policy.NetworkTopology != nil {
					subJobID := api.SubJobID(fmt.Sprintf("test-job/%s/0", policy.Name))
					subJob := job.SubJobs[subJobID]
					assert.NotNil(t, subJob)
					assert.Equal(t, tt.wantSubGroupPolicyModes[i], subJob.NetworkTopology.Mode,
						"SubJob derived from SubGroupPolicy[%d] mode mismatch", i)
					if tt.wantSubGroupPolicyTiers[i] != nil {
						assert.NotNil(t, subJob.NetworkTopology.HighestTierAllowed)
						assert.Equal(t, *tt.wantSubGroupPolicyTiers[i], *subJob.NetworkTopology.HighestTierAllowed,
							"SubJob derived from SubGroupPolicy[%d] tier mismatch", i)
					}
				}
			}

			// Verify ContainsHardTopology
			assert.Equal(t, tt.wantContainsHardTopology, job.ContainsHardTopology(),
				"ContainsHardTopology mismatch")

			// Verify SubJob-level topology conversion
			for _, subJob := range job.SubJobs {
				if subJob.WithNetworkTopology() {
					isHard, tier := subJob.IsHardTopologyMode()
					assert.True(t, isHard,
						"SubJob %s should be hard mode after conversion", subJob.UID)
					assert.True(t, tier > 0,
						"SubJob %s should have a valid tier", subJob.UID)
					assert.False(t, subJob.IsSoftTopologyMode(),
						"SubJob %s should not be soft mode after conversion", subJob.UID)
				}
			}
		})
	}
}

func TestConvertSoftToHardTopology_NilPodGroup(t *testing.T) {
	job := api.NewJobInfo("test-job")
	// PodGroup is nil, should not panic
	convertSoftToHardTopology(job, 4)
	assert.Nil(t, job.PodGroup, "PodGroup should remain nil")
}

func TestAdjustNetworkTopologySpec_SoftToHardConversion(t *testing.T) {
	// This test verifies that adjustNetworkTopologySpec converts soft mode while
	// preserving hard tier names for branch-local resolution.
	maxTier := 4 // ClusterTopHyperNode tier will be max(existing tiers) + 1 = 3 + 1 = 4

	topHn := &topologyv1alpha1.HyperNode{}
	topHn.Name = ClusterTopHyperNode
	topHn.Spec.Tier = maxTier

	tests := []struct {
		name        string
		jobs        map[api.JobID]*api.JobInfo
		nameMap     api.HyperNodeTierNameMap
		hyperNodes  api.HyperNodeInfoMap
		wantJobMode scheduling.NetworkTopologyMode
		wantJobTier *int
		wantJobName string
	}{
		{
			name: "soft topology with tierName is converted to an unrestricted numeric boundary",
			jobs: map[api.JobID]*api.JobInfo{
				"test-uid": {
					PodGroup: &api.PodGroup{
						PodGroup: scheduling.PodGroup{
							Spec: scheduling.PodGroupSpec{
								NetworkTopology: &scheduling.NetworkTopologySpec{
									Mode:            scheduling.SoftNetworkTopologyMode,
									HighestTierName: "volcano.sh/hypercluster",
								},
							},
						},
					},
					SubJobs: map[api.SubJobID]*api.SubJobInfo{},
				},
			},
			nameMap: api.HyperNodeTierNameMap{
				"volcano.sh/hypernode":    1,
				"volcano.sh/hypercluster": 2,
			},
			hyperNodes: api.HyperNodeInfoMap{
				ClusterTopHyperNode: api.NewHyperNodeInfo(topHn),
			},
			wantJobMode: scheduling.HardNetworkTopologyMode,
			wantJobTier: ptr.To(maxTier),
		},
		{
			name: "pure soft topology without tierName: converted with maxTier",
			jobs: map[api.JobID]*api.JobInfo{
				"test-uid": {
					PodGroup: &api.PodGroup{
						PodGroup: scheduling.PodGroup{
							Spec: scheduling.PodGroupSpec{
								NetworkTopology: &scheduling.NetworkTopologySpec{
									Mode: scheduling.SoftNetworkTopologyMode,
								},
							},
						},
					},
					SubJobs: map[api.SubJobID]*api.SubJobInfo{},
				},
			},
			nameMap: api.HyperNodeTierNameMap{},
			hyperNodes: api.HyperNodeInfoMap{
				ClusterTopHyperNode: api.NewHyperNodeInfo(topHn),
			},
			wantJobMode: scheduling.HardNetworkTopologyMode,
			wantJobTier: ptr.To(maxTier),
		},
		{
			name: "hard topology with tierName is preserved",
			jobs: map[api.JobID]*api.JobInfo{
				"test-uid": {
					PodGroup: &api.PodGroup{
						PodGroup: scheduling.PodGroup{
							Spec: scheduling.PodGroupSpec{
								NetworkTopology: &scheduling.NetworkTopologySpec{
									Mode:            scheduling.HardNetworkTopologyMode,
									HighestTierName: "volcano.sh/hypernode",
								},
							},
						},
					},
					SubJobs: map[api.SubJobID]*api.SubJobInfo{},
				},
			},
			nameMap: api.HyperNodeTierNameMap{
				"volcano.sh/hypernode":    1,
				"volcano.sh/hypercluster": 2,
			},
			hyperNodes: api.HyperNodeInfoMap{
				ClusterTopHyperNode: api.NewHyperNodeInfo(topHn),
			},
			wantJobMode: scheduling.HardNetworkTopologyMode,
			wantJobTier: nil,
			wantJobName: "volcano.sh/hypernode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, job := range tt.jobs {
				if job.PodGroup != nil && job.NetworkTopology == nil {
					job.NetworkTopology = job.PodGroup.Spec.NetworkTopology.DeepCopy()
				}
			}
			ssn := &Session{
				Jobs:                 tt.jobs,
				HyperNodeTierNameMap: tt.nameMap,
				HyperNodes:           tt.hyperNodes,
			}
			ssn.adjustNetworkTopologySpec()

			gotJob := ssn.Jobs["test-uid"]
			assert.Equal(t, tt.wantJobMode, gotJob.NetworkTopology.Mode, "job mode mismatch")
			assert.Equal(t, tt.wantJobTier, gotJob.NetworkTopology.HighestTierAllowed, "job tier mismatch")
			assert.Equal(t, tt.wantJobName, gotJob.NetworkTopology.HighestTierName, "job tier name mismatch")
		})
	}
}
