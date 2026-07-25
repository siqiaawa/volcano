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

package hypernode

import (
	"context"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"

	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	topologyv1alpha1 "volcano.sh/apis/pkg/apis/topology/v1alpha1"
	e2eutil "volcano.sh/volcano/test/e2e/util"
)

const (
	mixedProfileLabel       = "volcano.sh/e2e-topology-profile"
	mixedA3HyperNodeLabel   = "volcano.sh/e2e-a3-hypernode"
	mixedA5SuperPodLabel    = "volcano.sh/e2e-a5-superpod"
	mixedA5HyperNodeLabel   = "volcano.sh/e2e-a5-hypernode"
	mixedClusterLabel       = "volcano.sh/e2e-hypercluster"
	mixedTopologySourceKey  = "volcano.sh/network-topology-source"
	mixedTopologyProfileKey = "volcano.sh/network-topology-profile"
)

type mixedTopologyHyperNodeSnapshot struct {
	UID  string
	Spec topologyv1alpha1.HyperNodeSpec
}

var mixedTopologyTolerations = []v1.Toleration{{
	Key:      "kwok.x-k8s.io/node",
	Operator: v1.TolerationOpEqual,
	Value:    "fake",
	Effect:   v1.TaintEffectNoSchedule,
}}

var _ = Describe("Mixed A3 and A5 topology", Serial, func() {
	var testCtx *e2eutil.TestContext

	BeforeEach(func() {
		testCtx = e2eutil.InitTestContext(e2eutil.Options{NodesNumLimit: 8})
	})

	AfterEach(func() {
		e2eutil.CleanupTestContext(testCtx)
	})

	It("discovers mixed profiles, schedules by tier name, and survives an invalid config update", func() {
		controllerConfigMap, err := findControllerConfigMap(testCtx.Kubeclient)
		Expect(err).NotTo(HaveOccurred())
		originalControllerConfig := controllerConfigMap.Data["volcano-controller.conf"]

		originalNodes, err := labelMixedTopologyNodes(testCtx.Kubeclient)
		Expect(err).NotTo(HaveOccurred())
		defer func() {
			Expect(setControllerConfig(testCtx.Kubeclient, controllerConfigMap, originalControllerConfig)).To(Succeed())
			Expect(restoreNodeLabels(testCtx.Kubeclient, originalNodes)).To(Succeed())
		}()

		validConfig := mixedTopologyDiscoveryConfig()
		By("enabling A3 and A5 label discovery profiles")
		Expect(setControllerConfig(testCtx.Kubeclient, controllerConfigMap, validConfig)).To(Succeed())
		Eventually(func() (bool, error) {
			return mixedTopologyHasExpectedTiers(testCtx, 7, "")
		}, 60*time.Second, time.Second).Should(BeTrue())
		Eventually(func() (bool, error) {
			return mixedTopologyHasExpectedGraph(testCtx)
		}, 60*time.Second, time.Second).Should(BeTrue(),
			"A3 and A5 should remain separate complete trees even when their roots share one label key/value")
		a5Snapshot, err := mixedTopologyProfileSnapshot(testCtx, "topologya5")
		Expect(err).NotTo(HaveOccurred())
		Expect(a5Snapshot).To(HaveLen(4))

		By("scheduling a job whose semantic hypernode tier is tier 1 in A3 and tier 2 in A5")
		job := e2eutil.CreateJob(testCtx, &e2eutil.JobSpec{
			Name: "mixed-tier-name-job",
			NetworkTopology: &batchv1alpha1.NetworkTopologySpec{
				Mode:            batchv1alpha1.HardNetworkTopologyMode,
				HighestTierName: "volcano.sh/hypernode",
			},
			Tasks: []e2eutil.TaskSpec{{
				Name:        "worker",
				Img:         e2eutil.DefaultNginxImage,
				Req:         e2eutil.CPU5Mem5,
				Min:         4,
				Rep:         4,
				Tolerations: mixedTopologyTolerations,
			}},
		})
		defer e2eutil.DeleteJob(testCtx, job)
		Expect(e2eutil.WaitJobReady(testCtx, job)).NotTo(HaveOccurred())
		Expect(e2eutil.VerifyPodScheduling(testCtx, job,
			[]string{"kwok-node-4", "kwok-node-5", "kwok-node-6", "kwok-node-7"})).NotTo(HaveOccurred())

		By("publishing an invalid replacement config without removing the active topology")
		invalidConfig := strings.Replace(validConfig,
			"tierName: volcano.sh/hypernode", "tierNmae: volcano.sh/hypernode", 1)
		Expect(setControllerConfig(testCtx.Kubeclient, controllerConfigMap, invalidConfig)).To(Succeed())
		Consistently(func() (bool, error) {
			return mixedTopologyHasExpectedTiers(testCtx, 7, "")
		}, 2*time.Second, 200*time.Millisecond).Should(BeTrue())

		By("changing a Node while the replacement config is invalid")
		Expect(setNodeLabel(testCtx.Kubeclient, "kwok-node-0", mixedA3HyperNodeLabel, "hn-a3-new")).To(Succeed())
		Eventually(func() (bool, error) {
			return mixedTopologyHasExpectedTiers(testCtx, 8, "hn-a3-new")
		}, 60*time.Second, time.Second).Should(BeTrue(), "the old discoverer should remain active")
		currentA5, err := mixedTopologyProfileSnapshot(testCtx, "topologya5")
		Expect(err).NotTo(HaveOccurred())
		Expect(currentA5).To(Equal(a5Snapshot), "an A3-only domain change must not recreate or mutate A5")

		By("restoring the valid config and verifying the replacement discoverer converges")
		Expect(setControllerConfig(testCtx.Kubeclient, controllerConfigMap, validConfig)).To(Succeed())
		Expect(setNodeLabel(testCtx.Kubeclient, "kwok-node-0", mixedA3HyperNodeLabel, "hn-a3-0")).To(Succeed())
		Eventually(func() (bool, error) {
			return mixedTopologyHasExpectedTiers(testCtx, 7, "")
		}, 60*time.Second, time.Second).Should(BeTrue())
		Eventually(func() (bool, error) {
			return mixedTopologyHasExpectedGraph(testCtx)
		}, 60*time.Second, time.Second).Should(BeTrue())
		currentA5, err = mixedTopologyProfileSnapshot(testCtx, "topologya5")
		Expect(err).NotTo(HaveOccurred())
		Expect(currentA5).To(Equal(a5Snapshot), "restoring A3 must preserve the independent A5 tree")
	})

	It("selects feasible hard topology trees without crossing semantic boundaries", func() {
		controllerConfigMap, err := findControllerConfigMap(testCtx.Kubeclient)
		Expect(err).NotTo(HaveOccurred())
		originalControllerConfig := controllerConfigMap.Data["volcano-controller.conf"]

		originalNodes, err := labelMixedTopologyNodes(testCtx.Kubeclient)
		Expect(err).NotTo(HaveOccurred())
		defer func() {
			Expect(setControllerConfig(testCtx.Kubeclient, controllerConfigMap, originalControllerConfig)).To(Succeed())
			Expect(restoreNodeLabels(testCtx.Kubeclient, originalNodes)).To(Succeed())
		}()

		By("enabling A3 and A5 label discovery profiles")
		Expect(setControllerConfig(testCtx.Kubeclient, controllerConfigMap, mixedTopologyDiscoveryConfig())).To(Succeed())
		Eventually(func() (bool, error) {
			return mixedTopologyHasExpectedTiers(testCtx, 7, "")
		}, 60*time.Second, time.Second).Should(BeTrue())

		By("selecting A3 when A5 is the only resource-infeasible tree")
		a5Blockers := createMixedTopologyBlockers(testCtx, "mixed-a5-blocker", []int{4, 5, 6, 7})
		a3OnlyJob := createMixedTopologyHardJob(testCtx, "mixed-hard-a3-only", "volcano.sh/hypercluster", 4)
		Expect(e2eutil.WaitJobReady(testCtx, a3OnlyJob)).NotTo(HaveOccurred())
		Expect(e2eutil.VerifyPodScheduling(testCtx, a3OnlyJob,
			[]string{"kwok-node-0", "kwok-node-1", "kwok-node-2", "kwok-node-3"})).NotTo(HaveOccurred())
		e2eutil.DeleteJob(testCtx, a3OnlyJob)
		Expect(e2eutil.WaitJobCleanedUp(testCtx, a3OnlyJob)).NotTo(HaveOccurred())
		deleteMixedTopologyBlockers(testCtx, a5Blockers)

		By("selecting A5 when only its semantic hypernode domain can contain the gang")
		a5OnlyJob := createMixedTopologyHardJob(testCtx, "mixed-hard-a5-only", "volcano.sh/hypernode", 4)
		Expect(e2eutil.WaitJobReady(testCtx, a5OnlyJob)).NotTo(HaveOccurred())
		Expect(e2eutil.VerifyPodScheduling(testCtx, a5OnlyJob,
			[]string{"kwok-node-4", "kwok-node-5", "kwok-node-6", "kwok-node-7"})).NotTo(HaveOccurred())
		e2eutil.DeleteJob(testCtx, a5OnlyJob)
		Expect(e2eutil.WaitJobCleanedUp(testCtx, a5OnlyJob)).NotTo(HaveOccurred())

		By("keeping a hard gang in one real tree when both trees are feasible")
		bothFeasibleJob := createMixedTopologyHardJob(testCtx, "mixed-hard-both-feasible", "volcano.sh/hypercluster", 4)
		Expect(e2eutil.WaitJobReady(testCtx, bothFeasibleJob)).NotTo(HaveOccurred())
		hasA3, hasA5, err := mixedTopologyJobUsesProfiles(testCtx, bothFeasibleJob)
		Expect(err).NotTo(HaveOccurred())
		Expect(hasA3 != hasA5).To(BeTrue(), "a hard gang must select exactly one feasible real tree")
		e2eutil.DeleteJob(testCtx, bothFeasibleJob)
		Expect(e2eutil.WaitJobCleanedUp(testCtx, bothFeasibleJob)).NotTo(HaveOccurred())

		By("excluding A3 when the requested semantic tier exists only in A5")
		a5TierOnlyJob := createMixedTopologyHardJob(testCtx, "mixed-hard-a5-tier-only", "volcano.sh/superpod", 2)
		Expect(e2eutil.WaitJobReady(testCtx, a5TierOnlyJob)).NotTo(HaveOccurred())
		Expect(e2eutil.VerifyPodScheduling(testCtx, a5TierOnlyJob,
			[]string{"kwok-node-4", "kwok-node-5", "kwok-node-6", "kwok-node-7"})).NotTo(HaveOccurred())
		domains, err := mixedTopologyJobDomains(testCtx, a5TierOnlyJob, mixedA5SuperPodLabel)
		Expect(err).NotTo(HaveOccurred())
		Expect(domains).To(HaveLen(1), "the hard gang must fit within one A5 superpod")
		e2eutil.DeleteJob(testCtx, a5TierOnlyJob)
		Expect(e2eutil.WaitJobCleanedUp(testCtx, a5TierOnlyJob)).NotTo(HaveOccurred())

		By("keeping the complete hard gang pending when neither tree is feasible")
		allBlockers := createMixedTopologyBlockers(testCtx, "mixed-all-blocker", []int{0, 1, 2, 3, 4, 5, 6, 7})
		defer deleteMixedTopologyBlockers(testCtx, allBlockers)
		neitherFeasibleJob := createMixedTopologyHardJob(testCtx, "mixed-hard-neither-feasible", "volcano.sh/hypercluster", 4)
		defer e2eutil.DeleteJob(testCtx, neitherFeasibleJob)
		Expect(e2eutil.WaitTaskPhase(testCtx, neitherFeasibleJob, []v1.PodPhase{v1.PodPending}, 4)).NotTo(HaveOccurred())
		Consistently(func() (bool, error) {
			return mixedTopologyJobPodsUnbound(testCtx, neitherFeasibleJob, 4)
		}, 10*time.Second, 250*time.Millisecond).Should(BeTrue(),
			"an infeasible hard gang must not partially bind or cross A3 and A5")
	})

	It("keeps hard subgroups in their semantic domains across pod and scheduler restarts", func() {
		controllerConfigMap, err := findControllerConfigMap(testCtx.Kubeclient)
		Expect(err).NotTo(HaveOccurred())
		originalControllerConfig := controllerConfigMap.Data["volcano-controller.conf"]

		originalNodes, err := labelMixedTopologyNodes(testCtx.Kubeclient)
		Expect(err).NotTo(HaveOccurred())
		defer func() {
			Expect(setControllerConfig(testCtx.Kubeclient, controllerConfigMap, originalControllerConfig)).To(Succeed())
			Expect(restoreNodeLabels(testCtx.Kubeclient, originalNodes)).To(Succeed())
		}()

		By("enabling A3 and A5 label discovery profiles")
		Expect(setControllerConfig(testCtx.Kubeclient, controllerConfigMap, mixedTopologyDiscoveryConfig())).To(Succeed())
		Eventually(func() (bool, error) {
			return mixedTopologyHasExpectedTiers(testCtx, 7, "")
		}, 60*time.Second, time.Second).Should(BeTrue())

		By("scheduling two hard subgroups on the A5-only superpod tier")
		job := e2eutil.CreateJob(testCtx, &e2eutil.JobSpec{
			Name: "mixed-hard-subgroup-job",
			NetworkTopology: &batchv1alpha1.NetworkTopologySpec{
				Mode:            batchv1alpha1.HardNetworkTopologyMode,
				HighestTierName: "volcano.sh/hypercluster",
			},
			Tasks: []e2eutil.TaskSpec{{
				Name:        "worker",
				Img:         e2eutil.DefaultNginxImage,
				Req:         e2eutil.CPU5Mem5,
				Min:         4,
				Rep:         4,
				Tolerations: mixedTopologyTolerations,
				PartitionPolicy: &batchv1alpha1.PartitionPolicySpec{
					TotalPartitions: 2,
					PartitionSize:   2,
					MinPartitions:   2,
					NetworkTopology: &batchv1alpha1.NetworkTopologySpec{
						Mode:            batchv1alpha1.HardNetworkTopologyMode,
						HighestTierName: "volcano.sh/superpod",
					},
				},
			}},
		})
		defer e2eutil.DeleteJob(testCtx, job)
		Expect(e2eutil.WaitJobReady(testCtx, job)).NotTo(HaveOccurred())

		domainsBefore, err := mixedTopologySubGroupDomains(testCtx, job, "a5", mixedA5SuperPodLabel, 2, 2)
		Expect(err).NotTo(HaveOccurred())
		Expect(sets.New(domainsBefore["0"], domainsBefore["1"])).To(HaveLen(2),
			"the two CPU-saturated subgroups must occupy different A5 superpods")

		By("deleting one subgroup pod and waiting for a different pod instance")
		Expect(replaceMixedTopologyJobPod(testCtx, job, "0", 4)).To(Succeed())
		Expect(e2eutil.WaitJobReady(testCtx, job)).NotTo(HaveOccurred())

		By("verifying the replacement remains in the original subgroup superpod")
		domainsAfter, err := mixedTopologySubGroupDomains(testCtx, job, "a5", mixedA5SuperPodLabel, 2, 2)
		Expect(err).NotTo(HaveOccurred())
		Expect(domainsAfter).To(Equal(domainsBefore))

		By("restarting every Volcano Scheduler process and waiting for new ready instances")
		Expect(restartVolcanoScheduler(testCtx.Kubeclient)).To(Succeed())

		By("replacing a pod after the scheduler has lost its in-memory allocation state")
		Expect(replaceMixedTopologyJobPod(testCtx, job, "1", 4)).To(Succeed())
		Expect(e2eutil.WaitJobReady(testCtx, job)).NotTo(HaveOccurred())

		By("verifying scheduler recovery keeps every subgroup in its original superpod")
		domainsAfterRestart, err := mixedTopologySubGroupDomains(testCtx, job, "a5", mixedA5SuperPodLabel, 2, 2)
		Expect(err).NotTo(HaveOccurred())
		Expect(domainsAfterRestart).To(Equal(domainsBefore))
	})

	It("keeps soft jobs in one tree when possible and uses the virtual root as fallback", func() {
		controllerConfigMap, err := findControllerConfigMap(testCtx.Kubeclient)
		Expect(err).NotTo(HaveOccurred())
		originalControllerConfig := controllerConfigMap.Data["volcano-controller.conf"]

		originalNodes, err := labelMixedTopologyNodes(testCtx.Kubeclient)
		Expect(err).NotTo(HaveOccurred())
		defer func() {
			Expect(setControllerConfig(testCtx.Kubeclient, controllerConfigMap, originalControllerConfig)).To(Succeed())
			Expect(restoreNodeLabels(testCtx.Kubeclient, originalNodes)).To(Succeed())
		}()

		By("enabling A3 and A5 label discovery profiles")
		Expect(setControllerConfig(testCtx.Kubeclient, controllerConfigMap, mixedTopologyDiscoveryConfig())).To(Succeed())
		Eventually(func() (bool, error) {
			return mixedTopologyHasExpectedTiers(testCtx, 7, "")
		}, 60*time.Second, time.Second).Should(BeTrue())

		By("scheduling a soft job entirely within one real topology tree")
		singleTreeJob := e2eutil.CreateJob(testCtx, &e2eutil.JobSpec{
			Name: "mixed-soft-single-tree-job",
			NetworkTopology: &batchv1alpha1.NetworkTopologySpec{
				Mode: batchv1alpha1.SoftNetworkTopologyMode,
			},
			Tasks: []e2eutil.TaskSpec{{
				Name:        "worker",
				Img:         e2eutil.DefaultNginxImage,
				Req:         e2eutil.CPU5Mem5,
				Min:         4,
				Rep:         4,
				Tolerations: mixedTopologyTolerations,
			}},
		})
		Expect(e2eutil.WaitJobReady(testCtx, singleTreeJob)).NotTo(HaveOccurred())
		hasA3, hasA5, err := mixedTopologyJobUsesProfiles(testCtx, singleTreeJob)
		Expect(err).NotTo(HaveOccurred())
		Expect(hasA3 != hasA5).To(BeTrue(), "a soft job that fits in one tree must not span A3 and A5")
		e2eutil.DeleteJob(testCtx, singleTreeJob)
		Expect(e2eutil.WaitJobCleanedUp(testCtx, singleTreeJob)).NotTo(HaveOccurred())

		By("scheduling a soft job across trees only when neither real tree is sufficient")
		crossTreeJob := e2eutil.CreateJob(testCtx, &e2eutil.JobSpec{
			Name: "mixed-soft-cross-tree-job",
			NetworkTopology: &batchv1alpha1.NetworkTopologySpec{
				Mode: batchv1alpha1.SoftNetworkTopologyMode,
			},
			Tasks: []e2eutil.TaskSpec{{
				Name:        "worker",
				Img:         e2eutil.DefaultNginxImage,
				Req:         e2eutil.CPU5Mem5,
				Min:         8,
				Rep:         8,
				Tolerations: mixedTopologyTolerations,
			}},
		})
		Expect(e2eutil.WaitJobReady(testCtx, crossTreeJob)).NotTo(HaveOccurred())
		hasA3, hasA5, err = mixedTopologyJobUsesProfiles(testCtx, crossTreeJob)
		Expect(err).NotTo(HaveOccurred())
		Expect(hasA3).To(BeTrue(), "virtual-root fallback should use the A3 tree")
		Expect(hasA5).To(BeTrue(), "virtual-root fallback should use the A5 tree")
		e2eutil.DeleteJob(testCtx, crossTreeJob)
		Expect(e2eutil.WaitJobCleanedUp(testCtx, crossTreeJob)).NotTo(HaveOccurred())

		By("keeping soft subgroups inside the real tree selected by a hard job")
		hardJobSoftSubGroups := e2eutil.CreateJob(testCtx, &e2eutil.JobSpec{
			Name: "mixed-hard-job-soft-subgroups",
			NetworkTopology: &batchv1alpha1.NetworkTopologySpec{
				Mode:            batchv1alpha1.HardNetworkTopologyMode,
				HighestTierName: "volcano.sh/hypercluster",
			},
			Tasks: []e2eutil.TaskSpec{{
				Name:        "worker",
				Img:         e2eutil.DefaultNginxImage,
				Req:         e2eutil.CPU5Mem5,
				Min:         4,
				Rep:         4,
				Tolerations: mixedTopologyTolerations,
				PartitionPolicy: &batchv1alpha1.PartitionPolicySpec{
					TotalPartitions: 2,
					PartitionSize:   2,
					MinPartitions:   2,
					NetworkTopology: &batchv1alpha1.NetworkTopologySpec{
						Mode: batchv1alpha1.SoftNetworkTopologyMode,
					},
				},
			}},
		})
		Expect(e2eutil.WaitJobReady(testCtx, hardJobSoftSubGroups)).NotTo(HaveOccurred())
		hasA3, hasA5, err = mixedTopologyJobUsesProfiles(testCtx, hardJobSoftSubGroups)
		Expect(err).NotTo(HaveOccurred())
		Expect(hasA3 != hasA5).To(BeTrue(), "soft subgroups must not escape the hard job's selected tree")
		subGroupProfiles, err := mixedTopologySubGroupProfiles(testCtx, hardJobSoftSubGroups, 2, 2)
		Expect(err).NotTo(HaveOccurred())
		for partition, profiles := range subGroupProfiles {
			Expect(profiles).To(HaveLen(1), "soft subgroup %s should stay in one real tree", partition)
		}
		e2eutil.DeleteJob(testCtx, hardJobSoftSubGroups)
		Expect(e2eutil.WaitJobCleanedUp(testCtx, hardJobSoftSubGroups)).NotTo(HaveOccurred())

		By("letting a soft subgroup use the virtual root when no real tree can contain it")
		virtualRootSubGroup := e2eutil.CreateJob(testCtx, &e2eutil.JobSpec{
			Name: "mixed-soft-subgroup-virtual-root",
			Tasks: []e2eutil.TaskSpec{{
				Name:        "worker",
				Img:         e2eutil.DefaultNginxImage,
				Req:         e2eutil.CPU5Mem5,
				Min:         8,
				Rep:         8,
				Tolerations: mixedTopologyTolerations,
				PartitionPolicy: &batchv1alpha1.PartitionPolicySpec{
					TotalPartitions: 1,
					PartitionSize:   8,
					MinPartitions:   1,
					NetworkTopology: &batchv1alpha1.NetworkTopologySpec{
						Mode: batchv1alpha1.SoftNetworkTopologyMode,
					},
				},
			}},
		})
		Expect(e2eutil.WaitJobReady(testCtx, virtualRootSubGroup)).NotTo(HaveOccurred())
		subGroupProfiles, err = mixedTopologySubGroupProfiles(testCtx, virtualRootSubGroup, 1, 8)
		Expect(err).NotTo(HaveOccurred())
		Expect(subGroupProfiles["0"]).To(Equal(sets.New("a3", "a5")),
			"a soft subgroup larger than either real tree must use the virtual root")
		e2eutil.DeleteJob(testCtx, virtualRootSubGroup)
		Expect(e2eutil.WaitJobCleanedUp(testCtx, virtualRootSubGroup)).NotTo(HaveOccurred())

		By("keeping the entire gang pending when mixed-tree total capacity is insufficient")
		insufficientJob := e2eutil.CreateJob(testCtx, &e2eutil.JobSpec{
			Name: "mixed-soft-insufficient-job",
			NetworkTopology: &batchv1alpha1.NetworkTopologySpec{
				Mode: batchv1alpha1.SoftNetworkTopologyMode,
			},
			Tasks: []e2eutil.TaskSpec{{
				Name:        "worker",
				Img:         e2eutil.DefaultNginxImage,
				Req:         e2eutil.CPU5Mem5,
				Min:         9,
				Rep:         9,
				Tolerations: mixedTopologyTolerations,
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{{
								MatchExpressions: []v1.NodeSelectorRequirement{{
									Key:      mixedProfileLabel,
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{"a3", "a5"},
								}},
							}},
						},
					},
				},
			}},
		})
		defer e2eutil.DeleteJob(testCtx, insufficientJob)
		Expect(e2eutil.WaitTaskPhase(testCtx, insufficientJob, []v1.PodPhase{v1.PodPending}, 9)).NotTo(HaveOccurred())
		Consistently(func() (bool, error) {
			pods, err := testCtx.Kubeclient.CoreV1().Pods(insufficientJob.Namespace).List(
				context.Background(), metav1.ListOptions{})
			if err != nil {
				return false, err
			}
			controlledPods := 0
			for i := range pods.Items {
				pod := &pods.Items[i]
				if !metav1.IsControlledBy(pod, insufficientJob) {
					continue
				}
				controlledPods++
				if pod.Spec.NodeName != "" || pod.Status.Phase != v1.PodPending {
					return false, nil
				}
			}
			return controlledPods == 9, nil
		}, 10*time.Second, 250*time.Millisecond).Should(BeTrue(),
			"an unsatisfied soft gang must not partially bind across A3 and A5")
	})
})

func findControllerConfigMap(client kubernetes.Interface) (*v1.ConfigMap, error) {
	configMaps, err := client.CoreV1().ConfigMaps(v1.NamespaceAll).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var matched *v1.ConfigMap
	for i := range configMaps.Items {
		configMap := &configMaps.Items[i]
		if !strings.HasSuffix(configMap.Name, "-controller-configmap") {
			continue
		}
		if _, exists := configMap.Data["volcano-controller.conf"]; !exists {
			continue
		}
		if matched != nil {
			return nil, fmt.Errorf("multiple Volcano controller ConfigMaps found: %s/%s and %s/%s",
				matched.Namespace, matched.Name, configMap.Namespace, configMap.Name)
		}
		matched = configMap.DeepCopy()
	}
	if matched == nil {
		return nil, fmt.Errorf("Volcano controller ConfigMap not found")
	}
	return matched, nil
}

func setControllerConfig(client kubernetes.Interface, configMap *v1.ConfigMap, value string) error {
	current, err := client.CoreV1().ConfigMaps(configMap.Namespace).Get(
		context.Background(), configMap.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	current = current.DeepCopy()
	if current.Data == nil {
		current.Data = make(map[string]string)
	}
	current.Data["volcano-controller.conf"] = value
	_, err = client.CoreV1().ConfigMaps(configMap.Namespace).Update(
		context.Background(), current, metav1.UpdateOptions{})
	return err
}

func labelMixedTopologyNodes(client kubernetes.Interface) (map[string]*v1.Node, error) {
	originals := make(map[string]*v1.Node, 8)
	for i := 0; i < 8; i++ {
		name := fmt.Sprintf("kwok-node-%d", i)
		node, err := client.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		originals[name] = node.DeepCopy()
		if err := updateNodeLabels(client, name, func(labels map[string]string) {
			labels[mixedClusterLabel] = "hc-shared"
			if i < 4 {
				labels[mixedProfileLabel] = "a3"
				labels[mixedA3HyperNodeLabel] = fmt.Sprintf("hn-a3-%d", i/2)
			} else {
				labels[mixedProfileLabel] = "a5"
				labels[mixedA5HyperNodeLabel] = "hn-a5"
				labels[mixedA5SuperPodLabel] = fmt.Sprintf("sp-a5-%d", (i-4)/2)
			}
		}); err != nil {
			return nil, err
		}
	}
	return originals, nil
}

func restoreNodeLabels(client kubernetes.Interface, originals map[string]*v1.Node) error {
	for name, original := range originals {
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			current, err := client.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			current = current.DeepCopy()
			current.Labels = original.Labels
			_, err = client.CoreV1().Nodes().Update(context.Background(), current, metav1.UpdateOptions{})
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

func setNodeLabel(client kubernetes.Interface, nodeName, key, value string) error {
	return updateNodeLabels(client, nodeName, func(labels map[string]string) {
		labels[key] = value
	})
}

func updateNodeLabels(client kubernetes.Interface, nodeName string, update func(map[string]string)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		node, err := client.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		node = node.DeepCopy()
		if node.Labels == nil {
			node.Labels = make(map[string]string)
		}
		update(node.Labels)
		_, err = client.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{})
		return err
	})
}

func mixedTopologyHasExpectedTiers(testCtx *e2eutil.TestContext, expectedCount int, expectedDomain string) (bool, error) {
	hyperNodes, err := testCtx.Vcclient.TopologyV1alpha1().HyperNodes().List(context.Background(), metav1.ListOptions{
		LabelSelector: labels.Set{mixedTopologySourceKey: "label"}.AsSelector().String(),
	})
	if err != nil {
		return false, err
	}
	if len(hyperNodes.Items) != expectedCount {
		return false, nil
	}

	tiersByName := map[string]map[int]int{}
	domainFound := expectedDomain == ""
	for i := range hyperNodes.Items {
		hyperNode := &hyperNodes.Items[i]
		if tiersByName[hyperNode.Spec.TierName] == nil {
			tiersByName[hyperNode.Spec.TierName] = make(map[int]int)
		}
		tiersByName[hyperNode.Spec.TierName][hyperNode.Spec.Tier]++
		if hyperNode.Labels[mixedA3HyperNodeLabel] == expectedDomain {
			domainFound = true
		}
	}

	return domainFound &&
		tiersByName["volcano.sh/hypernode"][1] >= 2 &&
		tiersByName["volcano.sh/hypernode"][2] == 1 &&
		tiersByName["volcano.sh/hypercluster"][2] == 1 &&
		tiersByName["volcano.sh/hypercluster"][3] == 1 &&
		tiersByName["volcano.sh/superpod"][1] == 2, nil
}

func mixedTopologyHasExpectedGraph(testCtx *e2eutil.TestContext) (bool, error) {
	hyperNodes, err := testCtx.Vcclient.TopologyV1alpha1().HyperNodes().List(context.Background(), metav1.ListOptions{
		LabelSelector: labels.Set{mixedTopologySourceKey: "label"}.AsSelector().String(),
	})
	if err != nil {
		return false, err
	}
	if len(hyperNodes.Items) != 7 {
		return false, nil
	}

	byProfile := map[string][]*topologyv1alpha1.HyperNode{}
	for i := range hyperNodes.Items {
		hyperNode := &hyperNodes.Items[i]
		profile := hyperNode.Labels[mixedTopologyProfileKey]
		byProfile[profile] = append(byProfile[profile], hyperNode)
	}

	validateProfile := func(profile string, expectedNodes sets.Set[string], expectedTierNames map[int]string) (string, bool) {
		byTier := map[int][]*topologyv1alpha1.HyperNode{}
		for _, hyperNode := range byProfile[profile] {
			if hyperNode.Spec.TierName != expectedTierNames[hyperNode.Spec.Tier] {
				return "", false
			}
			byTier[hyperNode.Spec.Tier] = append(byTier[hyperNode.Spec.Tier], hyperNode)
		}
		if len(byTier[1]) != 2 || len(byProfile[profile]) != len(expectedTierNames)+1 {
			return "", false
		}

		leafNodes := sets.New[string]()
		for _, leaf := range byTier[1] {
			if len(leaf.Spec.Members) != 2 {
				return "", false
			}
			for _, member := range leaf.Spec.Members {
				if member.Type != topologyv1alpha1.MemberTypeNode || member.Selector.ExactMatch == nil {
					return "", false
				}
				leafNodes.Insert(member.Selector.ExactMatch.Name)
			}
		}
		if !leafNodes.Equal(expectedNodes) {
			return "", false
		}

		previousTier := byTier[1]
		var root *topologyv1alpha1.HyperNode
		for tier := 2; tier <= len(expectedTierNames); tier++ {
			if len(byTier[tier]) != 1 {
				return "", false
			}
			expectedMembers := sets.New[string]()
			for _, child := range previousTier {
				expectedMembers.Insert(child.Name)
			}
			actualMembers := sets.New[string]()
			for _, member := range byTier[tier][0].Spec.Members {
				if member.Type != topologyv1alpha1.MemberTypeHyperNode || member.Selector.ExactMatch == nil {
					return "", false
				}
				actualMembers.Insert(member.Selector.ExactMatch.Name)
			}
			if !actualMembers.Equal(expectedMembers) {
				return "", false
			}
			root = byTier[tier][0]
			previousTier = byTier[tier]
		}
		if root == nil || root.Labels[mixedClusterLabel] != "hc-shared" {
			return "", false
		}
		return root.Name, true
	}

	a3Root, a3Valid := validateProfile("topologya3", sets.New[string](
		"kwok-node-0", "kwok-node-1", "kwok-node-2", "kwok-node-3"), map[int]string{
		1: "volcano.sh/hypernode",
		2: "volcano.sh/hypercluster",
	})
	a5Root, a5Valid := validateProfile("topologya5", sets.New[string](
		"kwok-node-4", "kwok-node-5", "kwok-node-6", "kwok-node-7"), map[int]string{
		1: "volcano.sh/superpod",
		2: "volcano.sh/hypernode",
		3: "volcano.sh/hypercluster",
	})
	return a3Valid && a5Valid && a3Root != a5Root, nil
}

func mixedTopologyProfileSnapshot(testCtx *e2eutil.TestContext, profile string) (map[string]mixedTopologyHyperNodeSnapshot, error) {
	hyperNodes, err := testCtx.Vcclient.TopologyV1alpha1().HyperNodes().List(context.Background(), metav1.ListOptions{
		LabelSelector: labels.Set{
			mixedTopologySourceKey:  "label",
			mixedTopologyProfileKey: profile,
		}.AsSelector().String(),
	})
	if err != nil {
		return nil, err
	}
	snapshot := make(map[string]mixedTopologyHyperNodeSnapshot, len(hyperNodes.Items))
	for i := range hyperNodes.Items {
		hyperNode := hyperNodes.Items[i].DeepCopy()
		snapshot[hyperNode.Name] = mixedTopologyHyperNodeSnapshot{
			UID:  string(hyperNode.UID),
			Spec: hyperNode.Spec,
		}
	}
	return snapshot, nil
}

func mixedTopologyJobUsesProfiles(testCtx *e2eutil.TestContext, job *batchv1alpha1.Job) (bool, bool, error) {
	hasA3 := false
	hasA5 := false
	for _, pod := range e2eutil.GetTasksOfJob(testCtx, job) {
		if pod.Spec.NodeName == "" {
			return false, false, fmt.Errorf("pod %s/%s is not scheduled", pod.Namespace, pod.Name)
		}
		node, err := testCtx.Kubeclient.CoreV1().Nodes().Get(context.Background(), pod.Spec.NodeName, metav1.GetOptions{})
		if err != nil {
			return false, false, err
		}
		switch node.Labels[mixedProfileLabel] {
		case "a3":
			hasA3 = true
		case "a5":
			hasA5 = true
		default:
			return false, false, fmt.Errorf("pod %s/%s scheduled to node %s without a mixed topology profile", pod.Namespace, pod.Name, pod.Spec.NodeName)
		}
	}
	return hasA3, hasA5, nil
}

func createMixedTopologyHardJob(
	testCtx *e2eutil.TestContext,
	name, highestTierName string,
	replicas int32,
) *batchv1alpha1.Job {
	return e2eutil.CreateJob(testCtx, &e2eutil.JobSpec{
		Name: name,
		NetworkTopology: &batchv1alpha1.NetworkTopologySpec{
			Mode:            batchv1alpha1.HardNetworkTopologyMode,
			HighestTierName: highestTierName,
		},
		Tasks: []e2eutil.TaskSpec{{
			Name:        "worker",
			Img:         e2eutil.DefaultNginxImage,
			Req:         e2eutil.CPU5Mem5,
			Min:         replicas,
			Rep:         replicas,
			Tolerations: mixedTopologyTolerations,
		}},
	})
}

func createMixedTopologyBlockers(testCtx *e2eutil.TestContext, prefix string, nodeIndexes []int) []*v1.Pod {
	pods := make([]*v1.Pod, 0, len(nodeIndexes))
	for _, nodeIndex := range nodeIndexes {
		pod := e2eutil.CreatePod(testCtx, e2eutil.PodSpec{
			Name:        fmt.Sprintf("%s-%d", prefix, nodeIndex),
			Node:        fmt.Sprintf("kwok-node-%d", nodeIndex),
			Req:         e2eutil.CPU4Mem4,
			Tolerations: mixedTopologyTolerations,
		})
		Expect(e2eutil.WaitPodReady(testCtx, pod)).NotTo(HaveOccurred())
		pods = append(pods, pod)
	}
	return pods
}

func deleteMixedTopologyBlockers(testCtx *e2eutil.TestContext, pods []*v1.Pod) {
	for _, pod := range pods {
		e2eutil.DeletePod(testCtx, pod)
	}
	for _, pod := range pods {
		Expect(e2eutil.WaitPodGone(testCtx, pod.Name, pod.Namespace)).NotTo(HaveOccurred())
	}
}

func mixedTopologyJobDomains(
	testCtx *e2eutil.TestContext,
	job *batchv1alpha1.Job,
	domainLabel string,
) (sets.Set[string], error) {
	domains := sets.New[string]()
	for _, pod := range e2eutil.GetTasksOfJob(testCtx, job) {
		if pod.Spec.NodeName == "" {
			return nil, fmt.Errorf("pod %s/%s is not scheduled", pod.Namespace, pod.Name)
		}
		node, err := testCtx.Kubeclient.CoreV1().Nodes().Get(
			context.Background(), pod.Spec.NodeName, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		domain := node.Labels[domainLabel]
		if domain == "" {
			return nil, fmt.Errorf("node %s has no topology domain label %s", node.Name, domainLabel)
		}
		domains.Insert(domain)
	}
	return domains, nil
}

func mixedTopologyJobPodsUnbound(
	testCtx *e2eutil.TestContext,
	job *batchv1alpha1.Job,
	expectedPods int,
) (bool, error) {
	pods, err := testCtx.Kubeclient.CoreV1().Pods(job.Namespace).List(
		context.Background(), metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	controlledPods := 0
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, job) {
			continue
		}
		controlledPods++
		if pod.Spec.NodeName != "" || pod.Status.Phase != v1.PodPending {
			return false, nil
		}
	}
	return controlledPods == expectedPods, nil
}

func mixedTopologySubGroupProfiles(
	testCtx *e2eutil.TestContext,
	job *batchv1alpha1.Job,
	expectedPartitions, expectedPartitionSize int,
) (map[string]sets.Set[string], error) {
	pods := e2eutil.GetTasksOfJob(testCtx, job)
	expectedPods := expectedPartitions * expectedPartitionSize
	if len(pods) != expectedPods {
		return nil, fmt.Errorf("expected %d pods for job %s, got %d", expectedPods, job.Name, len(pods))
	}

	podCounts := make(map[string]int, expectedPartitions)
	profilesByPartition := make(map[string]sets.Set[string], expectedPartitions)
	for _, pod := range pods {
		partition, found := pod.Labels[batchv1alpha1.TaskPartitionID]
		if !found || partition == "" {
			return nil, fmt.Errorf("pod %s/%s has no partition label", pod.Namespace, pod.Name)
		}
		if pod.Spec.NodeName == "" {
			return nil, fmt.Errorf("pod %s/%s is not scheduled", pod.Namespace, pod.Name)
		}

		node, err := testCtx.Kubeclient.CoreV1().Nodes().Get(
			context.Background(), pod.Spec.NodeName, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		profile := node.Labels[mixedProfileLabel]
		if profile != "a3" && profile != "a5" {
			return nil, fmt.Errorf("pod %s/%s is on node %s with unexpected profile %q",
				pod.Namespace, pod.Name, node.Name, profile)
		}
		if profilesByPartition[partition] == nil {
			profilesByPartition[partition] = sets.New[string]()
		}
		profilesByPartition[partition].Insert(profile)
		podCounts[partition]++
	}

	for partitionIndex := 0; partitionIndex < expectedPartitions; partitionIndex++ {
		partition := fmt.Sprint(partitionIndex)
		if podCounts[partition] != expectedPartitionSize {
			return nil, fmt.Errorf("partition %s has %d pods, expected %d",
				partition, podCounts[partition], expectedPartitionSize)
		}
	}
	if len(profilesByPartition) != expectedPartitions {
		return nil, fmt.Errorf("found unexpected partitions: %v", sets.KeySet(profilesByPartition).UnsortedList())
	}
	return profilesByPartition, nil
}

func mixedTopologySubGroupDomains(
	testCtx *e2eutil.TestContext,
	job *batchv1alpha1.Job,
	expectedProfile, domainLabel string,
	expectedPartitions, expectedPartitionSize int,
) (map[string]string, error) {
	pods := e2eutil.GetTasksOfJob(testCtx, job)
	expectedPods := expectedPartitions * expectedPartitionSize
	if len(pods) != expectedPods {
		return nil, fmt.Errorf("expected %d pods for job %s, got %d", expectedPods, job.Name, len(pods))
	}

	podCounts := make(map[string]int, expectedPartitions)
	domainsByPartition := make(map[string]sets.Set[string], expectedPartitions)
	for _, pod := range pods {
		partition, found := pod.Labels[batchv1alpha1.TaskPartitionID]
		if !found || partition == "" {
			return nil, fmt.Errorf("pod %s/%s has no partition label", pod.Namespace, pod.Name)
		}
		if pod.Spec.NodeName == "" {
			return nil, fmt.Errorf("pod %s/%s is not scheduled", pod.Namespace, pod.Name)
		}

		node, err := testCtx.Kubeclient.CoreV1().Nodes().Get(
			context.Background(), pod.Spec.NodeName, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if profile := node.Labels[mixedProfileLabel]; profile != expectedProfile {
			return nil, fmt.Errorf("pod %s/%s is on profile %q, expected %q",
				pod.Namespace, pod.Name, profile, expectedProfile)
		}
		domain := node.Labels[domainLabel]
		if domain == "" {
			return nil, fmt.Errorf("node %s has no topology domain label %s", node.Name, domainLabel)
		}

		if domainsByPartition[partition] == nil {
			domainsByPartition[partition] = sets.New[string]()
		}
		domainsByPartition[partition].Insert(domain)
		podCounts[partition]++
	}

	result := make(map[string]string, expectedPartitions)
	for partitionIndex := 0; partitionIndex < expectedPartitions; partitionIndex++ {
		partition := fmt.Sprint(partitionIndex)
		if podCounts[partition] != expectedPartitionSize {
			return nil, fmt.Errorf("partition %s has %d pods, expected %d",
				partition, podCounts[partition], expectedPartitionSize)
		}
		domains := domainsByPartition[partition]
		if domains.Len() != 1 {
			return nil, fmt.Errorf("partition %s spans topology domains %v", partition, domains.UnsortedList())
		}
		result[partition] = domains.UnsortedList()[0]
	}
	if len(domainsByPartition) != expectedPartitions {
		return nil, fmt.Errorf("found unexpected partitions: %v", sets.KeySet(domainsByPartition).UnsortedList())
	}
	return result, nil
}

func replaceMixedTopologyJobPod(
	testCtx *e2eutil.TestContext,
	job *batchv1alpha1.Job,
	partition string,
	expectedPods int,
) error {
	pods, err := testCtx.Kubeclient.CoreV1().Pods(job.Namespace).List(
		context.Background(), metav1.ListOptions{})
	if err != nil {
		return err
	}

	initialUIDs := sets.New[string]()
	var victim *v1.Pod
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, job) {
			continue
		}
		initialUIDs.Insert(string(pod.UID))
		if victim == nil && pod.Labels[batchv1alpha1.TaskPartitionID] == partition {
			victim = pod.DeepCopy()
		}
	}
	if victim == nil {
		return fmt.Errorf("job %s has no pod in partition %s", job.Name, partition)
	}
	if len(initialUIDs) != expectedPods {
		return fmt.Errorf("job %s has %d pods before replacement, expected %d",
			job.Name, len(initialUIDs), expectedPods)
	}
	if err := testCtx.Kubeclient.CoreV1().Pods(victim.Namespace).Delete(
		context.Background(), victim.Name, metav1.DeleteOptions{}); err != nil {
		return err
	}

	lastState := "replacement not observed"
	err = wait.PollUntilContextTimeout(context.Background(), 250*time.Millisecond, 2*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			currentPods, err := testCtx.Kubeclient.CoreV1().Pods(job.Namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, err
			}

			controlledPods := 0
			readyPods := 0
			replacementFound := false
			victimStillExists := false
			for i := range currentPods.Items {
				pod := &currentPods.Items[i]
				if !metav1.IsControlledBy(pod, job) {
					continue
				}
				controlledPods++
				if pod.UID == victim.UID {
					victimStillExists = true
				}
				if !initialUIDs.Has(string(pod.UID)) {
					replacementFound = true
				}
				if pod.Status.Phase == v1.PodRunning && mixedTopologyPodReady(pod) {
					readyPods++
				}
			}
			lastState = fmt.Sprintf("controlled=%d ready=%d replacement=%t victimPresent=%t",
				controlledPods, readyPods, replacementFound, victimStillExists)
			return controlledPods == expectedPods && readyPods == expectedPods &&
				replacementFound && !victimStillExists, nil
		})
	if err != nil {
		return fmt.Errorf("wait for replacement of pod %s/%s: %w (%s)",
			victim.Namespace, victim.Name, err, lastState)
	}
	return nil
}

func restartVolcanoScheduler(client kubernetes.Interface) error {
	const schedulerSelector = "app=volcano-scheduler"

	pods, err := client.CoreV1().Pods(v1.NamespaceAll).List(
		context.Background(), metav1.ListOptions{LabelSelector: schedulerSelector})
	if err != nil {
		return err
	}
	if len(pods.Items) == 0 {
		return fmt.Errorf("no Volcano Scheduler pods found")
	}

	oldUIDs := sets.New[string]()
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !mixedTopologyPodReady(pod) {
			return fmt.Errorf("Volcano Scheduler pod %s/%s is not ready before restart", pod.Namespace, pod.Name)
		}
		oldUIDs.Insert(string(pod.UID))
		if err := client.CoreV1().Pods(pod.Namespace).Delete(
			context.Background(), pod.Name, metav1.DeleteOptions{}); err != nil {
			return err
		}
	}

	lastState := "new scheduler pod not observed"
	err = wait.PollUntilContextTimeout(context.Background(), 500*time.Millisecond, 2*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			currentPods, err := client.CoreV1().Pods(v1.NamespaceAll).List(
				ctx, metav1.ListOptions{LabelSelector: schedulerSelector})
			if err != nil {
				return false, err
			}

			readyNewPods := 0
			oldPodPresent := false
			for i := range currentPods.Items {
				pod := &currentPods.Items[i]
				if oldUIDs.Has(string(pod.UID)) {
					oldPodPresent = true
					continue
				}
				if pod.DeletionTimestamp == nil && mixedTopologyPodReady(pod) {
					readyNewPods++
				}
			}
			lastState = fmt.Sprintf("readyNew=%d expected=%d oldPresent=%t",
				readyNewPods, len(oldUIDs), oldPodPresent)
			return readyNewPods >= len(oldUIDs) && !oldPodPresent, nil
		})
	if err != nil {
		return fmt.Errorf("wait for Volcano Scheduler restart: %w (%s)", err, lastState)
	}
	return nil
}

func mixedTopologyPodReady(pod *v1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == v1.PodReady {
			return condition.Status == v1.ConditionTrue
		}
	}
	return false
}

func mixedTopologyDiscoveryConfig() string {
	return `networkTopologyDiscovery:
  - source: label
    enabled: true
    config:
      networkTopologyTypes:
        topologyA3:
          nodeSelector:
            matchLabels:
              volcano.sh/e2e-topology-profile: a3
          levels:
            - nodeLabel: volcano.sh/e2e-hypercluster
              tierName: volcano.sh/hypercluster
            - nodeLabel: volcano.sh/e2e-a3-hypernode
              tierName: volcano.sh/hypernode
            - nodeLabel: kubernetes.io/hostname
        topologyA5:
          nodeSelector:
            matchLabels:
              volcano.sh/e2e-topology-profile: a5
          levels:
            - nodeLabel: volcano.sh/e2e-hypercluster
              tierName: volcano.sh/hypercluster
            - nodeLabel: volcano.sh/e2e-a5-hypernode
              tierName: volcano.sh/hypernode
            - nodeLabel: volcano.sh/e2e-a5-superpod
              tierName: volcano.sh/superpod
            - nodeLabel: kubernetes.io/hostname
`
}
