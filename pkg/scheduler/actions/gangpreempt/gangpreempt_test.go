/*
Copyright 2026 The Volcano Authors.

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

package gangpreempt

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	schedulingapi "volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"volcano.sh/volcano/cmd/scheduler/app/options"
	"volcano.sh/volcano/pkg/scheduler/actions/utils"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/cache"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const mixedDomainPreemptPluginName = "mixed-domain-gangpreempt-test"

type mixedDomainPreemptPlugin struct {
	domains        [2]string
	rejectedDomain string
	visitedDomains []string
	jobPurposes    []api.SearchPurpose
	subJobPurposes []api.SearchPurpose
}

func (p *mixedDomainPreemptPlugin) Name() string { return mixedDomainPreemptPluginName }

func (p *mixedDomainPreemptPlugin) OnSessionOpen(ssn *framework.Session) {
	ssn.AddHyperNodeGradientForJobFn(p.Name(), func(_ *api.JobInfo, _ *api.HyperNodeInfo, purpose api.SearchPurpose) [][]*api.HyperNodeInfo {
		p.jobPurposes = append(p.jobPurposes, purpose)
		return [][]*api.HyperNodeInfo{{ssn.HyperNodes[p.domains[0]]}, {ssn.HyperNodes[p.domains[1]]}}
	})
	ssn.AddHyperNodeGradientForSubJobFn(p.Name(), func(_ *api.SubJobInfo, domain *api.HyperNodeInfo, purpose api.SearchPurpose) [][]*api.HyperNodeInfo {
		p.subJobPurposes = append(p.subJobPurposes, purpose)
		return [][]*api.HyperNodeInfo{{domain}}
	})
	ssn.AddUnifiedEvictableFn(p.Name(), func(ctx *api.EvictionContext, candidates []*api.TaskInfo) ([]*api.TaskInfo, int) {
		p.visitedDomains = append(p.visitedDomains, ctx.HyperNode)
		if ctx.HyperNode == p.rejectedDomain {
			return nil, 1
		}
		return candidates, 1
	})
}

func (p *mixedDomainPreemptPlugin) OnSessionClose(*framework.Session) {}

func TestPickDomainsFromGradients_MaxDomainsAndDedup(t *testing.T) {
	gradients := [][]*api.HyperNodeInfo{
		{
			{Name: "d1"},
			{Name: "d2"},
		},
		{
			{Name: "d2"},
			{Name: "d3"},
		},
	}

	domains := utils.PickDomainsFromGradients(gradients, 2, "")
	assert.Equal(t, []string{"d1", "d2"}, domains)
}

func TestPickDomainsFromGradients_Fallback(t *testing.T) {
	domains := utils.PickDomainsFromGradients(nil, 8, "<cluster-top-hypernode>")
	assert.Equal(t, []string{"<cluster-top-hypernode>"}, domains)
}

func TestParseArguments(t *testing.T) {
	ssn := &framework.Session{
		Configurations: []conf.Configuration{
			{
				Name: "gangpreempt",
				Arguments: map[string]interface{}{
					MaxDomainsKey:       3,
					AllowWholeBundleKey: false,
				},
			},
		},
	}
	action := New()
	action.parseArguments(ssn)
	assert.Equal(t, 3, action.maxDomains)
	assert.False(t, action.allowWholeBundle)
}

func TestParseArguments_InvalidMaxDomainsFallsBackToDefault(t *testing.T) {
	cases := []struct {
		name  string
		value int
	}{
		{name: "zero", value: 0},
		{name: "negative", value: -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ssn := &framework.Session{
				Configurations: []conf.Configuration{
					{
						Name: "gangpreempt",
						Arguments: map[string]interface{}{
							MaxDomainsKey: tc.value,
						},
					},
				},
			}
			action := New()
			action.parseArguments(ssn)
			assert.Equal(t, defaultMaxDomains, action.maxDomains)
		})
	}
}

func TestSelectDomainVictims_RespectAllowWholeBundle(t *testing.T) {
	preemptorJobID := api.JobID("ns/preemptor")
	victimJobID := api.JobID("ns/victim")
	nodeName := "n1"

	preemptor := testTask(preemptorJobID, "preemptor", nodeName, api.Pending, 100, 1000)
	victim := testTask(victimJobID, "victim", nodeName, api.Running, 10, 1000)

	preemptorJob := api.NewJobInfo(preemptorJobID, preemptor)
	preemptorJob.Queue = "q1"
	preemptorJob.Priority = 100
	victimJob := api.NewJobInfo(victimJobID, victim)
	victimJob.Queue = "q1"
	victimJob.Priority = 10
	victimJob.Preemptable = true
	victimJob.MinAvailable = 1 // one running task => whole bundle only

	ssn := &framework.Session{
		Jobs: map[api.JobID]*api.JobInfo{
			preemptorJobID: preemptorJob,
			victimJobID:    victimJob,
		},
		Queues: map[api.QueueID]*api.QueueInfo{
			"q1": {UID: "q1", Name: "q1"},
		},
		Tiers: []conf.Tier{
			{
				Plugins: []conf.PluginOption{
					{Name: "test-preempt"},
				},
			},
		},
	}
	ssn.AddUnifiedEvictableFn("test-preempt", func(_ *api.EvictionContext, candidates []*api.TaskInfo) ([]*api.TaskInfo, int) {
		return candidates, 1
	})

	node := &api.NodeInfo{Name: nodeName, Tasks: map[api.TaskID]*api.TaskInfo{api.PodKey(victim.Pod): victim}}
	ssn.RealNodesList = map[string][]*api.NodeInfo{
		"d1": {node},
	}
	action := New()

	pending := []*api.TaskInfo{preemptor}
	jobNeed := utils.SumInitResreq(pending)

	action.allowWholeBundle = false
	victimsNoWhole := utils.FlattenBundles(action.selectDomainBundles(ssn, preemptorJob, pending, jobNeed, "d1"))
	assert.Len(t, victimsNoWhole, 0)

	action.allowWholeBundle = true
	victimsWhole := utils.FlattenBundles(action.selectDomainBundles(ssn, preemptorJob, pending, jobNeed, "d1"))
	assert.Len(t, victimsWhole, 1)
	assert.Equal(t, api.TaskID(victim.UID), victimsWhole[0].UID)
}

func TestSelectDomainVictims_RespectVictimJobPreemptable(t *testing.T) {
	preemptorJobID := api.JobID("ns/preemptor")
	victimJobID := api.JobID("ns/victim")
	nodeName := "n1"

	preemptor := testTask(preemptorJobID, "preemptor", nodeName, api.Pending, 100, 1000)
	victim := testTask(victimJobID, "victim", nodeName, api.Running, 10, 1000)

	preemptorJob := api.NewJobInfo(preemptorJobID, preemptor)
	preemptorJob.Queue = "q1"
	preemptorJob.Priority = 100
	victimJob := api.NewJobInfo(victimJobID, victim)
	victimJob.Queue = "q1"
	victimJob.Priority = 10
	victimJob.Preemptable = false
	victimJob.PodGroup = &api.PodGroup{
		PodGroup: schedulingapi.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					v1beta1.PodPreemptable: "false",
				},
			},
		},
	}

	ssn := &framework.Session{
		Jobs: map[api.JobID]*api.JobInfo{
			preemptorJobID: preemptorJob,
			victimJobID:    victimJob,
		},
		Queues: map[api.QueueID]*api.QueueInfo{
			"q1": {UID: "q1", Name: "q1"},
		},
		Tiers: []conf.Tier{
			{
				Plugins: []conf.PluginOption{
					{Name: "test-preempt"},
				},
			},
		},
	}
	ssn.AddUnifiedEvictableFn("test-preempt", func(_ *api.EvictionContext, candidates []*api.TaskInfo) ([]*api.TaskInfo, int) {
		return candidates, 1
	})

	node := &api.NodeInfo{Name: nodeName, Tasks: map[api.TaskID]*api.TaskInfo{api.PodKey(victim.Pod): victim}}
	ssn.RealNodesList = map[string][]*api.NodeInfo{
		"d1": {node},
	}
	action := New()
	action.allowWholeBundle = true

	pending := []*api.TaskInfo{preemptor}
	jobNeed := utils.SumInitResreq(pending)
	victims := utils.FlattenBundles(action.selectDomainBundles(ssn, preemptorJob, pending, jobNeed, "d1"))
	assert.Len(t, victims, 0)
}

func TestSelectDomainVictims_AllowVictimJobWhenPreemptableUnset(t *testing.T) {
	preemptorJobID := api.JobID("ns/preemptor")
	victimJobID := api.JobID("ns/victim")
	nodeName := "n1"

	preemptor := testTask(preemptorJobID, "preemptor", nodeName, api.Pending, 100, 1000)
	victim := testTask(victimJobID, "victim", nodeName, api.Running, 10, 1000)

	preemptorJob := api.NewJobInfo(preemptorJobID, preemptor)
	preemptorJob.Queue = "q1"
	preemptorJob.Priority = 100
	victimJob := api.NewJobInfo(victimJobID, victim)
	victimJob.Queue = "q1"
	victimJob.Priority = 10
	// Unset podgroup-level preemptability defaults to allowed for gang actions.
	victimJob.Preemptable = false
	victimJob.PodGroup = &api.PodGroup{
		PodGroup: schedulingapi.PodGroup{},
	}

	ssn := &framework.Session{
		Jobs: map[api.JobID]*api.JobInfo{
			preemptorJobID: preemptorJob,
			victimJobID:    victimJob,
		},
		Queues: map[api.QueueID]*api.QueueInfo{
			"q1": {UID: "q1", Name: "q1"},
		},
		Tiers: []conf.Tier{
			{
				Plugins: []conf.PluginOption{
					{Name: "test-preempt"},
				},
			},
		},
	}
	ssn.AddUnifiedEvictableFn("test-preempt", func(_ *api.EvictionContext, candidates []*api.TaskInfo) ([]*api.TaskInfo, int) {
		return candidates, 1
	})

	node := &api.NodeInfo{Name: nodeName, Tasks: map[api.TaskID]*api.TaskInfo{api.PodKey(victim.Pod): victim}}
	ssn.RealNodesList = map[string][]*api.NodeInfo{
		"d1": {node},
	}
	action := New()
	action.allowWholeBundle = true

	pending := []*api.TaskInfo{preemptor}
	jobNeed := utils.SumInitResreq(pending)
	victims := utils.FlattenBundles(action.selectDomainBundles(ssn, preemptorJob, pending, jobNeed, "d1"))
	assert.Len(t, victims, 1)
	assert.Equal(t, api.TaskID(victim.UID), victims[0].UID)
}

func TestPreemptJobInDomains_MixedTopologyContinuesToSecondTree(t *testing.T) {
	if options.ServerOpts == nil {
		options.ServerOpts = options.NewServerOption()
	}
	if options.ServerOpts.MinNodesToFind <= 0 {
		options.ServerOpts.MinNodesToFind = 1
	}

	const (
		a3Domain = "a3-hypercluster"
		a5Domain = "a5-hypercluster"
		a3Node   = "a3-node"
		a5Node   = "a5-node"
	)
	plugin := &mixedDomainPreemptPlugin{
		domains:        [2]string{a3Domain, a5Domain},
		rejectedDomain: a3Domain,
	}
	framework.RegisterPluginBuilder(plugin.Name(), func(framework.Arguments) framework.Plugin { return plugin })
	enabled := true
	schedulerCache := &cache.SchedulerCache{
		Nodes:             map[string]*api.NodeInfo{},
		Jobs:              map[api.JobID]*api.JobInfo{},
		Queues:            map[api.QueueID]*api.QueueInfo{},
		HyperNodesInfo:    api.NewHyperNodesInfo(nil),
		InUseNodesInShard: sets.Set[string]{},
	}
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{{Plugins: []conf.PluginOption{{
		Name:                     plugin.Name(),
		EnabledHyperNodeGradient: &enabled,
	}}}}, nil)

	preemptorJobID := api.JobID("ns/preemptor-mixed")
	preemptor := testTask(preemptorJobID, "preemptor-mixed", "", api.Pending, 100, 1000)
	highestTier := 3
	preemptorJob := api.NewJobInfo(preemptorJobID)
	preemptorJob.Queue = "q1"
	preemptorJob.Priority = 100
	preemptorJob.MinAvailable = 1
	preemptorJob.NetworkTopology = &schedulingapi.NetworkTopologySpec{
		Mode:               schedulingapi.HardNetworkTopologyMode,
		HighestTierAllowed: &highestTier,
	}
	preemptorJob.AddTaskInfo(preemptor)

	victimJobID := api.JobID("ns/victim-mixed")
	a3Victim := testTask(victimJobID, "a3-victim", a3Node, api.Running, 10, 1000)
	a5Victim := testTask(victimJobID, "a5-victim", a5Node, api.Running, 10, 1000)
	victimJob := api.NewJobInfo(victimJobID, a3Victim, a5Victim)
	victimJob.Queue = "q1"
	victimJob.Priority = 10
	victimJob.Preemptable = true

	newOccupiedNode := func(name string, victim *api.TaskInfo) *api.NodeInfo {
		node := api.NewNodeInfo(&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}})
		node.Idle = (&api.Resource{MilliCPU: 1000}).Clone()
		node.Allocatable = (&api.Resource{MilliCPU: 1000}).Clone()
		node.Capacity = (&api.Resource{MilliCPU: 1000}).Clone()
		assert.NoError(t, node.AddTask(victim))
		return node
	}
	a3 := newOccupiedNode(a3Node, a3Victim)
	a5 := newOccupiedNode(a5Node, a5Victim)
	root := framework.ClusterTopHyperNode
	ssn.Jobs = map[api.JobID]*api.JobInfo{preemptorJobID: preemptorJob, victimJobID: victimJob}
	ssn.Queues = map[api.QueueID]*api.QueueInfo{"q1": {UID: "q1", Name: "q1"}}
	ssn.Nodes = map[string]*api.NodeInfo{a3Node: a3, a5Node: a5}
	ssn.NodeList = []*api.NodeInfo{a3, a5}
	ssn.NodesInShard = sets.New[string](a3Node, a5Node)
	ssn.HyperNodes = api.HyperNodeInfoMap{
		a3Domain: {Name: a3Domain},
		a5Domain: {Name: a5Domain},
		root:     {Name: root},
	}
	ssn.RealNodesList = map[string][]*api.NodeInfo{
		a3Domain: {a3},
		a5Domain: {a5},
		root:     {a3, a5},
	}

	action := New()
	action.maxDomains = 16
	stmt := framework.NewStatement(ssn)
	nominations := action.preemptJobInDomains(ssn, stmt, ssn.Queues["q1"], preemptorJob)

	assert.Equal(t, []api.SearchPurpose{api.PurposeEvict}, plugin.jobPurposes)
	assert.Equal(t, []api.SearchPurpose{api.PurposeEvict}, plugin.subJobPurposes)
	assert.Equal(t, []string{a3Domain, a5Domain}, plugin.visitedDomains)
	assert.Len(t, nominations, 1)
	for _, domain := range nominations {
		assert.Equal(t, a5Domain, domain)
	}
	assert.Equal(t, api.Running, victimJob.Tasks[a3Victim.UID].Status)
	assert.Equal(t, api.Releasing, victimJob.Tasks[a5Victim.UID].Status)
	assert.Equal(t, api.Pipelined, preemptorJob.Tasks[preemptor.UID].Status)
	assert.Equal(t, a5Node, preemptorJob.Tasks[preemptor.UID].NodeName)
	_, placedOnA3 := a3.Tasks[api.PodKey(preemptor.Pod)]
	_, placedOnA5 := a5.Tasks[api.PodKey(preemptor.Pod)]
	assert.False(t, placedOnA3)
	assert.True(t, placedOnA5)
}

func testTask(jobID api.JobID, name, node string, status api.TaskStatus, priority int32, milliCPU float64) *api.TaskInfo {
	res := (&api.Resource{MilliCPU: milliCPU}).Clone()
	return &api.TaskInfo{
		UID:         api.TaskID(name),
		Job:         jobID,
		Name:        name,
		Namespace:   "ns",
		Priority:    priority,
		Preemptable: true,
		Resreq:      res.Clone(),
		InitResreq:  res.Clone(),
		Pod: &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "ns",
				UID:       types.UID(name),
			},
		},
		NumaInfo: &api.TopologyInfo{
			ResMap: map[int]v1.ResourceList{},
		},
		TransactionContext: api.TransactionContext{
			NodeName: node,
			Status:   status,
		},
	}
}

func boolPtr(v bool) *bool { return &v }
