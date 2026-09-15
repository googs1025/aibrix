/*
Copyright 2026 The Aibrix Team.

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

package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"

	orchestrationv1alpha1 "github.com/vllm-project/aibrix/api/orchestration/v1alpha1"
	aibrixclientset "github.com/vllm-project/aibrix/pkg/client/clientset/versioned"
	orchestrationclient "github.com/vllm-project/aibrix/pkg/client/clientset/versioned/typed/orchestration/v1alpha1"
	controllerconstants "github.com/vllm-project/aibrix/pkg/controller/constants"
)

const (
	stormServiceVolcanoE2EEnv       = "AIBRIX_STORMSERVICE_VOLCANO_E2E"
	stormServiceE2EKeepOnFailureEnv = "AIBRIX_STORMSERVICE_E2E_KEEP_ON_FAILURE"
	stormServiceE2ENamespaceEnv     = "AIBRIX_E2E_NAMESPACE"
	stormServiceE2EDefaultNamespace = "default"
	stormServiceWorkerRoleName      = "worker"
	stormServiceWorkerContainerName = "worker"
	stormServiceInPlaceImageV1      = "aibrix/inplace-e2e:v1"
	stormServiceInPlaceImageV2      = "aibrix/inplace-e2e:v2"
	stormServiceMissingImage        = "aibrix/inplace-e2e:missing"
	stormServicePollInterval        = time.Second
	stormServicePollTimeout         = 3 * time.Minute
	stormServiceCleanupTimeout      = 3 * time.Minute
	stormServiceVolcanoDefaultQueue = "default"
)

var (
	roleSetGVR = schema.GroupVersionResource{
		Group: "orchestration.aibrix.ai", Version: "v1alpha1", Resource: "rolesets",
	}
	podSetGVR = schema.GroupVersionResource{
		Group: "orchestration.aibrix.ai", Version: "v1alpha1", Resource: "podsets",
	}
	volcanoPodGroupGVR = schema.GroupVersionResource{
		Group: "scheduling.volcano.sh", Version: "v1beta1", Resource: "podgroups",
	}
)

// stormServiceHarness keeps the clients and namespace needed for cleanup together.
// Cleanup cannot derive the original StormService UID after foreground deletion, and
// the UID is needed to distinguish generated ControllerRevisions from same-name data.
type stormServiceHarness struct {
	namespace     string
	kubeClient    kubernetes.Interface
	stormServices orchestrationclient.StormServiceInterface
	dynamicClient dynamic.Interface
}

func stormServiceNamespace() string {
	if namespace := strings.TrimSpace(os.Getenv(stormServiceE2ENamespaceEnv)); namespace != "" {
		return namespace
	}
	return stormServiceE2EDefaultNamespace
}

func newStormServiceHarness(t *testing.T, namespace string) *stormServiceHarness {
	t.Helper()
	kubeClient, stormServices, dynamicClient := stormServiceClients(t, namespace)
	return &stormServiceHarness{
		namespace:     namespace,
		kubeClient:    kubeClient,
		stormServices: stormServices,
		dynamicClient: dynamicClient,
	}
}

// stormServiceClients creates direct clients without starting shared informers.
// Live-controller tests only poll API state, so framework.InitializeClient's
// informer setup is unnecessary here.
func stormServiceClients(
	t *testing.T,
	namespace string,
) (kubernetes.Interface, orchestrationclient.StormServiceInterface, dynamic.Interface) {
	t.Helper()

	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeConfig := os.Getenv("KUBECONFIG"); kubeConfig != "" {
		loadingRules.ExplicitPath = kubeConfig
	}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules,
		&clientcmd.ConfigOverrides{},
	).ClientConfig()
	if err != nil {
		t.Fatalf("build Kubernetes client configuration: %v", err)
	}

	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("create Kubernetes client: %v", err)
	}
	aibrixClient, err := aibrixclientset.NewForConfig(config)
	if err != nil {
		t.Fatalf("create AIBrix client: %v", err)
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatalf("create dynamic client: %v", err)
	}

	return kubeClient, aibrixClient.OrchestrationV1alpha1().StormServices(namespace), dynamicClient
}

func newStormService(namespace, name, image string) *orchestrationv1alpha1.StormService {
	labels := map[string]string{"app": name}
	return &orchestrationv1alpha1.StormService{
		TypeMeta: metav1.TypeMeta{
			APIVersion: orchestrationv1alpha1.GroupVersion.String(),
			Kind:       orchestrationv1alpha1.StormServiceKind,
		},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: orchestrationv1alpha1.StormServiceSpec{
			Replicas: ptr.To(int32(1)),
			Mode:     orchestrationv1alpha1.StormServicePooledMode,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: orchestrationv1alpha1.RoleSetTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: &orchestrationv1alpha1.RoleSetSpec{
					UpdateStrategy: orchestrationv1alpha1.ParallelRoleSetUpdateStrategyType,
					Roles: []orchestrationv1alpha1.RoleSpec{{
						Name:     stormServiceWorkerRoleName,
						Replicas: ptr.To(int32(1)),
						UpdateStrategy: orchestrationv1alpha1.RoleUpdateStrategy{
							Type: orchestrationv1alpha1.InPlaceIfPossibleRoleUpdateStrategyType,
						},
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{Containers: []corev1.Container{{
								Name:            stormServiceWorkerContainerName,
								Image:           image,
								ImagePullPolicy: corev1.PullIfNotPresent,
							}}},
						},
					}},
				},
			},
			UpdateStrategy: orchestrationv1alpha1.StormServiceUpdateStrategy{
				Type: orchestrationv1alpha1.InPlaceUpdateStormServiceStrategyType,
			},
		},
	}
}

func newReplicaLifecycleStormService(namespace, name string, replicas int32) *orchestrationv1alpha1.StormService {
	stormService := newStormService(namespace, name, stormServiceInPlaceImageV1)
	maxSurge := intstr.FromInt32(0)
	maxUnavailable := intstr.FromInt32(1)
	stormService.Spec.Replicas = ptr.To(replicas)
	stormService.Spec.Mode = orchestrationv1alpha1.StormServiceReplicaMode
	stormService.Spec.UpdateStrategy = orchestrationv1alpha1.StormServiceUpdateStrategy{
		Type:           orchestrationv1alpha1.RollingUpdateStormServiceStrategyType,
		MaxSurge:       &maxSurge,
		MaxUnavailable: &maxUnavailable,
	}
	stormService.Spec.Template.Spec.Roles[0].UpdateStrategy.Type = orchestrationv1alpha1.RecreateRoleUpdateStrategyType
	return stormService
}

func newUpdateStormService(namespace, name string) *orchestrationv1alpha1.StormService {
	return newReplicaLifecycleStormService(namespace, name, 2)
}

func newDeadlineStormService(namespace, name string, deadlineSeconds int32) *orchestrationv1alpha1.StormService {
	stormService := newReplicaLifecycleStormService(namespace, name, 2)
	stormService.Spec.ProgressDeadlineSeconds = ptr.To(deadlineSeconds)
	return stormService
}

func newVolcanoStormService(
	namespace, name string,
	impossible bool,
	eligibleNodes int,
) *orchestrationv1alpha1.StormService {
	stormService := newStormService(namespace, name, stormServiceInPlaceImageV1)
	minMember := int32(1)
	if impossible {
		// This makes the single-worker base fixture unschedulable as a gang without
		// relying on cluster-specific node labels. Task 6 may replace it with its
		// two-role anti-affinity shape.
		minMember = 2
	}
	if eligibleNodes < 1 {
		eligibleNodes = 1
	}
	stormService.Spec.Template.Spec.SchedulingStrategy = &orchestrationv1alpha1.SchedulingStrategy{
		VolcanoSchedulingStrategy: &orchestrationv1alpha1.VolcanoSchedulingStrategySpec{
			MinMember:     minMember,
			MinTaskMember: map[string]int32{stormServiceWorkerRoleName: minMember},
			Queue:         stormServiceVolcanoDefaultQueue,
		},
	}
	// Keep the shape deterministic until the Volcano-specific tests add their
	// node-affinity topology. The parameter remains part of the fixture contract.
	_ = eligibleNodes
	return stormService
}

func waitForStormServiceState(
	ctx context.Context,
	stormServices orchestrationclient.StormServiceInterface,
	name string,
	predicate func(*orchestrationv1alpha1.StormService) bool,
) (*orchestrationv1alpha1.StormService, error) {
	var latest string
	var observed *orchestrationv1alpha1.StormService
	err := wait.PollUntilContextTimeout(ctx, stormServicePollInterval, stormServicePollTimeout, true, func(ctx context.Context) (bool, error) {
		stormService, err := stormServices.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			latest = fmt.Sprintf("get error: %v", err)
			return false, nil
		}
		observed = stormService
		latest = describeStormService(stormService)
		return predicate(stormService), nil
	})
	if err != nil {
		return observed, fmt.Errorf("wait for StormService %q state: %w; latest observation: %s", name, err, latest)
	}
	return observed, nil
}

func waitForRoleSets(
	ctx context.Context,
	dynamicClient dynamic.Interface,
	namespace, stormServiceName string,
	count int,
) ([]unstructured.Unstructured, error) {
	selector := stormServiceSelector(stormServiceName)
	var latest string
	var observed []unstructured.Unstructured
	err := wait.PollUntilContextTimeout(ctx, stormServicePollInterval, stormServicePollTimeout, true, func(ctx context.Context) (bool, error) {
		roleSets, err := dynamicClient.Resource(roleSetGVR).Namespace(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			latest = fmt.Sprintf("list error: %v", err)
			return false, nil
		}
		observed = roleSets.Items
		latest = describeUnstructuredList(roleSets.Items)
		return len(roleSets.Items) == count, nil
	})
	if err != nil {
		return observed, fmt.Errorf("wait for %d RoleSets for StormService %q: %w; latest observation: %s", count, stormServiceName, err, latest)
	}
	return observed, nil
}

func waitForPods(
	ctx context.Context,
	kubeClient kubernetes.Interface,
	namespace, stormServiceName string,
	count int,
	ready bool,
) ([]corev1.Pod, error) {
	selector := stormServiceSelector(stormServiceName)
	var latest string
	var observed []corev1.Pod
	err := wait.PollUntilContextTimeout(ctx, stormServicePollInterval, stormServicePollTimeout, true, func(ctx context.Context) (bool, error) {
		pods, err := kubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			latest = fmt.Sprintf("list error: %v", err)
			return false, nil
		}
		observed = pods.Items
		latest = describePods(pods.Items)
		if len(pods.Items) != count {
			return false, nil
		}
		if !ready {
			return true, nil
		}
		for i := range pods.Items {
			if !podReady(&pods.Items[i]) {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		return observed, fmt.Errorf("wait for %d pods for StormService %q (ready=%t): %w; latest observation: %s", count, stormServiceName, ready, err, latest)
	}
	return observed, nil
}

func waitForOwnedService(
	ctx context.Context,
	kubeClient kubernetes.Interface,
	namespace, name string,
	ownerUID types.UID,
) (*corev1.Service, error) {
	var latest string
	var observed *corev1.Service
	err := wait.PollUntilContextTimeout(ctx, stormServicePollInterval, stormServicePollTimeout, true, func(ctx context.Context) (bool, error) {
		service, err := kubeClient.CoreV1().Services(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			latest = fmt.Sprintf("get error: %v", err)
			return false, nil
		}
		observed = service
		latest = describeService(service)
		return hasOwnerUID(service.OwnerReferences, ownerUID), nil
	})
	if err != nil {
		return observed, fmt.Errorf("wait for Service %s/%s owned by %q: %w; latest observation: %s", namespace, name, ownerUID, err, latest)
	}
	return observed, nil
}

func updateStormService(
	ctx context.Context,
	stormServices orchestrationclient.StormServiceInterface,
	name string,
	mutate func(*orchestrationv1alpha1.StormService),
) (*orchestrationv1alpha1.StormService, error) {
	var updated *orchestrationv1alpha1.StormService
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		stormService, err := stormServices.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		mutate(stormService)
		updated, err = stormServices.Update(ctx, stormService, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return updated, fmt.Errorf("update StormService %q: %w", name, err)
	}
	return updated, nil
}

func condition(conditions orchestrationv1alpha1.Conditions, conditionType orchestrationv1alpha1.ConditionType) *orchestrationv1alpha1.Condition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}

func (h *stormServiceHarness) cleanupStormService(t *testing.T, name string, expectPodGroup bool) {
	t.Helper()
	if t.Failed() && strings.EqualFold(strings.TrimSpace(os.Getenv(stormServiceE2EKeepOnFailureEnv)), "true") {
		t.Logf("preserving StormService e2e resources for %s/%s", h.namespace, name)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), stormServiceCleanupTimeout)
	defer cancel()

	stormService, err := h.stormServices.Get(ctx, name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("get StormService %s/%s before cleanup: %v", h.namespace, name, err)
	}
	if apierrors.IsNotFound(err) {
		return
	}

	roleSets, err := h.dynamicClient.Resource(roleSetGVR).Namespace(h.namespace).List(ctx, metav1.ListOptions{LabelSelector: stormServiceSelector(name)})
	if err != nil {
		t.Fatalf("list RoleSets for StormService %s/%s before cleanup: %v", h.namespace, name, err)
	}
	roleSetNames := make([]string, 0, len(roleSets.Items))
	for i := range roleSets.Items {
		roleSetNames = append(roleSetNames, roleSets.Items[i].GetName())
	}

	foreground := metav1.DeletePropagationForeground
	if err := h.stormServices.Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &foreground}); err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("foreground delete StormService %s/%s: %v", h.namespace, name, err)
	}

	if err := waitForStormServiceDeleted(ctx, h.stormServices, name); err != nil {
		t.Fatal(err)
	}
	if _, err := waitForRoleSets(ctx, h.dynamicClient, h.namespace, name, 0); err != nil {
		t.Fatal(err)
	}
	if err := waitForNoPodSets(ctx, h.dynamicClient, h.namespace, name); err != nil {
		t.Fatal(err)
	}
	if _, err := waitForPods(ctx, h.kubeClient, h.namespace, name, 0, false); err != nil {
		t.Fatal(err)
	}
	if err := waitForControllerRevisionsDeleted(ctx, h.kubeClient, h.namespace, name, stormService.UID); err != nil {
		t.Fatal(err)
	}
	if err := waitForServiceDeleted(ctx, h.kubeClient, h.namespace, name, stormService.UID); err != nil {
		t.Fatal(err)
	}
	if expectPodGroup {
		if err := waitForPodGroupsDeleted(ctx, h.dynamicClient, h.namespace, roleSetNames); err != nil {
			t.Fatal(err)
		}
	}
}

func stormServiceSelector(name string) string {
	return fmt.Sprintf("%s=%s", controllerconstants.StormServiceNameLabelKey, name)
}

func waitForStormServiceDeleted(ctx context.Context, stormServices orchestrationclient.StormServiceInterface, name string) error {
	var latest string
	err := wait.PollUntilContextTimeout(ctx, stormServicePollInterval, stormServiceCleanupTimeout, true, func(ctx context.Context) (bool, error) {
		stormService, err := stormServices.Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			latest = fmt.Sprintf("get error: %v", err)
			return false, nil
		}
		latest = describeStormService(stormService)
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("wait for StormService %q deletion: %w; latest observation: %s", name, err, latest)
	}
	return nil
}

func waitForNoPodSets(ctx context.Context, dynamicClient dynamic.Interface, namespace, stormServiceName string) error {
	selector := stormServiceSelector(stormServiceName)
	var latest string
	err := wait.PollUntilContextTimeout(ctx, stormServicePollInterval, stormServiceCleanupTimeout, true, func(ctx context.Context) (bool, error) {
		podSets, err := dynamicClient.Resource(podSetGVR).Namespace(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			latest = fmt.Sprintf("list error: %v", err)
			return false, nil
		}
		latest = describeUnstructuredList(podSets.Items)
		return len(podSets.Items) == 0, nil
	})
	if err != nil {
		return fmt.Errorf("wait for PodSets for StormService %q deletion: %w; latest observation: %s", stormServiceName, err, latest)
	}
	return nil
}

func waitForControllerRevisionsDeleted(ctx context.Context, kubeClient kubernetes.Interface, namespace, name string, ownerUID types.UID) error {
	var latest string
	err := wait.PollUntilContextTimeout(ctx, stormServicePollInterval, stormServiceCleanupTimeout, true, func(ctx context.Context) (bool, error) {
		revisions, err := kubeClient.AppsV1().ControllerRevisions(namespace).List(ctx, metav1.ListOptions{LabelSelector: fmt.Sprintf("name=%s", name)})
		if err != nil {
			latest = fmt.Sprintf("list error: %v", err)
			return false, nil
		}
		owned := controllerRevisionsWithOwner(revisions.Items, ownerUID)
		latest = describeControllerRevisions(owned)
		return len(owned) == 0, nil
	})
	if err != nil {
		return fmt.Errorf("wait for ControllerRevisions for StormService %q deletion: %w; latest observation: %s", name, err, latest)
	}
	return nil
}

func waitForServiceDeleted(ctx context.Context, kubeClient kubernetes.Interface, namespace, name string, ownerUID types.UID) error {
	var latest string
	err := wait.PollUntilContextTimeout(ctx, stormServicePollInterval, stormServiceCleanupTimeout, true, func(ctx context.Context) (bool, error) {
		service, err := kubeClient.CoreV1().Services(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			latest = fmt.Sprintf("get error: %v", err)
			return false, nil
		}
		latest = fmt.Sprintf("ownedByStormService=%t %s", hasOwnerUID(service.OwnerReferences, ownerUID), describeService(service))
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("wait for Service %s/%s deletion: %w; latest observation: %s", namespace, name, err, latest)
	}
	return nil
}

func waitForPodGroupsDeleted(ctx context.Context, dynamicClient dynamic.Interface, namespace string, roleSetNames []string) error {
	for _, roleSetName := range roleSetNames {
		selector := fmt.Sprintf("%s=%s", controllerconstants.RoleSetNameLabelKey, roleSetName)
		var latest string
		err := wait.PollUntilContextTimeout(ctx, stormServicePollInterval, stormServiceCleanupTimeout, true, func(ctx context.Context) (bool, error) {
			podGroups, err := dynamicClient.Resource(volcanoPodGroupGVR).Namespace(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
			if err != nil {
				latest = fmt.Sprintf("list error: %v", err)
				return false, nil
			}
			latest = describeUnstructuredList(podGroups.Items)
			return len(podGroups.Items) == 0, nil
		})
		if err != nil {
			return fmt.Errorf("wait for Volcano PodGroups for RoleSet %q deletion: %w; latest observation: %s", roleSetName, err, latest)
		}
	}
	return nil
}

func hasOwnerUID(ownerReferences []metav1.OwnerReference, ownerUID types.UID) bool {
	for _, ownerReference := range ownerReferences {
		if ownerReference.UID == ownerUID {
			return true
		}
	}
	return false
}

func controllerRevisionsWithOwner(revisions []appsv1.ControllerRevision, ownerUID types.UID) []appsv1.ControllerRevision {
	owned := make([]appsv1.ControllerRevision, 0, len(revisions))
	for i := range revisions {
		if hasOwnerUID(revisions[i].OwnerReferences, ownerUID) {
			owned = append(owned, revisions[i])
		}
	}
	return owned
}

func podReady(pod *corev1.Pod) bool {
	for _, podCondition := range pod.Status.Conditions {
		if podCondition.Type == corev1.PodReady {
			return podCondition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func describeStormService(stormService *orchestrationv1alpha1.StormService) string {
	return fmt.Sprintf("generation=%d observedGeneration=%d replicas=%d readyReplicas=%d conditions=%v", stormService.Generation, stormService.Status.ObservedGeneration, stormService.Status.Replicas, stormService.Status.ReadyReplicas, stormService.Status.Conditions)
}

func describeUnstructuredList(items []unstructured.Unstructured) string {
	names := make([]string, 0, len(items))
	for i := range items {
		names = append(names, fmt.Sprintf("%s(uid=%s)", items[i].GetName(), items[i].GetUID()))
	}
	return fmt.Sprintf("count=%d items=%v", len(items), names)
}

func describePods(pods []corev1.Pod) string {
	descriptions := make([]string, 0, len(pods))
	for i := range pods {
		descriptions = append(descriptions, fmt.Sprintf("%s(uid=%s ready=%t phase=%s)", pods[i].Name, pods[i].UID, podReady(&pods[i]), pods[i].Status.Phase))
	}
	return fmt.Sprintf("count=%d pods=%v", len(pods), descriptions)
}

func describeService(service *corev1.Service) string {
	return fmt.Sprintf("uid=%s owners=%v clusterIP=%s", service.UID, service.OwnerReferences, service.Spec.ClusterIP)
}

func describeControllerRevisions(revisions []appsv1.ControllerRevision) string {
	descriptions := make([]string, 0, len(revisions))
	for i := range revisions {
		descriptions = append(descriptions, fmt.Sprintf("%s(uid=%s revision=%d)", revisions[i].Name, revisions[i].UID, revisions[i].Revision))
	}
	return fmt.Sprintf("count=%d revisions=%v", len(revisions), descriptions)
}
