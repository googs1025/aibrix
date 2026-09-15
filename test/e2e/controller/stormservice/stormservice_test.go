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
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	orchestrationv1alpha1 "github.com/vllm-project/aibrix/api/orchestration/v1alpha1"
	controllerconstants "github.com/vllm-project/aibrix/pkg/controller/constants"
	stormservicecontroller "github.com/vllm-project/aibrix/pkg/controller/stormservice"
)

func TestStormServiceReplicaLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	namespace := stormServiceNamespace()
	name := fmt.Sprintf("stormservice-replica-lifecycle-%d", time.Now().UnixNano())
	h := newStormServiceHarness(t, namespace)
	cleanupState := newStrictStormServiceCleanup(h, name, false)
	cleanup := func() {
		if cleanupState.completed {
			return
		}
		if t.Failed() && strings.EqualFold(strings.TrimSpace(os.Getenv(stormServiceE2EKeepOnFailureEnv)), "true") {
			h.logRetainedStormServiceResources(t, name, false)
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), stormServiceCleanupTimeout)
		defer cancel()
		if err := cleanupState.run(cleanupCtx); err != nil {
			t.Errorf("cleanup StormService %s/%s: %v", namespace, name, err)
		}
	}
	t.Cleanup(cleanup)

	created, err := h.stormServices.Create(ctx, newReplicaLifecycleStormService(namespace, name, 2), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create StormService %s/%s: %v", namespace, name, err)
	}

	_, err = waitForStormServiceState(ctx, h.stormServices, name, func(stormService *orchestrationv1alpha1.StormService) bool {
		return controllerutil.ContainsFinalizer(stormService, stormservicecontroller.StormServiceFinalizer)
	})
	if err != nil {
		t.Fatalf("wait for StormService finalizer: %v", err)
	}

	service, err := waitForOwnedService(ctx, h.kubeClient, namespace, name, created.UID)
	if err != nil {
		t.Fatalf("wait for headless Service: %v", err)
	}
	if service.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Errorf("Service ClusterIP = %q, want %q", service.Spec.ClusterIP, corev1.ClusterIPNone)
	}
	if !service.Spec.PublishNotReadyAddresses {
		t.Error("Service PublishNotReadyAddresses = false, want true")
	}
	wantServiceSelector := map[string]string{controllerconstants.StormServiceNameLabelKey: name}
	if !reflect.DeepEqual(service.Spec.Selector, wantServiceSelector) {
		t.Errorf("Service selector = %v, want %v", service.Spec.Selector, wantServiceSelector)
	}
	serviceHasControllerOwner := false
	for _, ownerReference := range service.OwnerReferences {
		if ownerReference.UID == created.UID && ownerReference.Controller != nil && *ownerReference.Controller {
			serviceHasControllerOwner = true
			break
		}
	}
	if !serviceHasControllerOwner {
		t.Errorf("Service controller owner UID does not match StormService UID %q: %v", created.UID, service.OwnerReferences)
	}

	roleSets, err := h.waitForRoleSets(ctx, name, 2)
	if err != nil {
		t.Fatalf("wait for two RoleSets: %v", err)
	}
	for i := range roleSets {
		if !roleSetHasStormServiceOwnerAndMetadata(&roleSets[i], created.UID) {
			t.Errorf("RoleSet %q does not have StormService controller ownership and required revision/index metadata: %v", roleSets[i].GetName(), roleSets[i].UnstructuredContent())
		}
	}

	if _, err := waitForPods(ctx, h.kubeClient, namespace, name, 2, true); err != nil {
		t.Fatalf("wait for two ready pods: %v", err)
	}
	ready, err := waitForStormServiceState(ctx, h.stormServices, name, func(stormService *orchestrationv1alpha1.StormService) bool {
		return stormServiceHasReplicaStatus(stormService, 2, "")
	})
	if err != nil {
		t.Fatalf("wait for StormService ready status at two replicas: %v", err)
	}
	updateRevision := ready.Status.UpdateRevision

	if _, err := updateStormService(ctx, h.stormServices, name, func(stormService *orchestrationv1alpha1.StormService) {
		stormService.Spec.Replicas = ptr.To(int32(3))
	}); err != nil {
		t.Fatalf("scale StormService to three replicas: %v", err)
	}
	roleSets, err = h.waitForRoleSets(ctx, name, 3)
	if err != nil {
		t.Fatalf("wait for three RoleSets: %v", err)
	}
	roleSetIndexes := make(map[string]string, len(roleSets))
	for i := range roleSets {
		if !roleSetHasStormServiceOwnerAndMetadata(&roleSets[i], created.UID) {
			t.Errorf("scaled RoleSet %q does not have StormService controller ownership and required revision/index metadata: %v", roleSets[i].GetName(), roleSets[i].UnstructuredContent())
			continue
		}
		index := roleSets[i].GetAnnotations()[controllerconstants.RoleSetIndexAnnotationKey]
		if existingRoleSet, found := roleSetIndexes[index]; found {
			t.Errorf("scaled RoleSets %q and %q share index %q", existingRoleSet, roleSets[i].GetName(), index)
			continue
		}
		roleSetIndexes[index] = roleSets[i].GetName()
	}
	if _, err := waitForPods(ctx, h.kubeClient, namespace, name, 3, true); err != nil {
		t.Fatalf("wait for three ready pods: %v", err)
	}
	if _, err := waitForStormServiceState(ctx, h.stormServices, name, func(stormService *orchestrationv1alpha1.StormService) bool {
		return stormServiceHasReplicaStatus(stormService, 3, updateRevision)
	}); err != nil {
		t.Fatalf("wait for StormService ready status at three replicas: %v", err)
	}

	if _, err := updateStormService(ctx, h.stormServices, name, func(stormService *orchestrationv1alpha1.StormService) {
		stormService.Spec.Replicas = ptr.To(int32(1))
	}); err != nil {
		t.Fatalf("scale StormService to one replica: %v", err)
	}
	if _, err := h.waitForRoleSets(ctx, name, 1); err != nil {
		t.Fatalf("wait for one RoleSet: %v", err)
	}
	if _, err := waitForPods(ctx, h.kubeClient, namespace, name, 1, true); err != nil {
		t.Fatalf("wait for one ready pod: %v", err)
	}
	if _, err := waitForStormServiceState(ctx, h.stormServices, name, func(stormService *orchestrationv1alpha1.StormService) bool {
		return stormServiceHasReplicaStatus(stormService, 1, updateRevision)
	}); err != nil {
		t.Fatalf("wait for StormService ready status at one replica: %v", err)
	}

	cleanup()
}

func stormServiceHasReplicaStatus(stormService *orchestrationv1alpha1.StormService, replicas int32, updateRevision string) bool {
	if stormService == nil ||
		stormService.Status.ObservedGeneration != stormService.Generation ||
		stormService.Status.Replicas != replicas ||
		stormService.Status.ReadyReplicas != replicas ||
		stormService.Status.UpdatedReplicas != replicas ||
		stormService.Status.UpdatedReadyReplicas != replicas ||
		stormService.Status.NotReadyReplicas != 0 ||
		stormService.Status.CurrentRevision == "" ||
		stormService.Status.CurrentRevision != stormService.Status.UpdateRevision ||
		(updateRevision != "" && stormService.Status.UpdateRevision != updateRevision) {
		return false
	}

	ready := condition(stormService.Status.Conditions, orchestrationv1alpha1.StormServiceReady)
	if ready == nil || ready.Status != corev1.ConditionTrue || ready.Reason != "Ready" {
		return false
	}
	for _, roleStatus := range stormService.Status.RoleStatuses {
		if roleStatus.Name == stormServiceWorkerRoleName && roleStatus.ReadyReplicas == replicas {
			return true
		}
	}
	return false
}

func roleSetHasStormServiceOwnerAndMetadata(roleSet *unstructured.Unstructured, stormServiceUID types.UID) bool {
	if roleSet == nil {
		return false
	}

	for _, owner := range roleSet.GetOwnerReferences() {
		if owner.UID == stormServiceUID &&
			owner.APIVersion == orchestrationv1alpha1.GroupVersion.String() &&
			owner.Kind == orchestrationv1alpha1.StormServiceKind &&
			owner.Controller != nil && *owner.Controller {
			annotations := roleSet.GetAnnotations()
			return annotations[controllerconstants.RoleSetIndexAnnotationKey] != "" &&
				annotations[controllerconstants.RoleSetRevisionAnnotationKey] != ""
		}
	}
	return false
}

func waitForServiceNotFound(ctx context.Context, kubeClient kubernetes.Interface, namespace, name string) error {
	var latest string
	err := wait.PollUntilContextTimeout(ctx, stormServicePollInterval, stormServiceCleanupTimeout, true, func(ctx context.Context) (bool, error) {
		service, err := kubeClient.CoreV1().Services(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, retryPollError(err, false, &latest)
		}
		latest = describeService(service)
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("wait for Service %s/%s strict deletion: %w; latest observation: %s", namespace, name, err, latest)
	}
	return nil
}

type strictStormServiceCleanup struct {
	harness        *stormServiceHarness
	name           string
	expectPodGroup bool
	completed      bool
}

func newStrictStormServiceCleanup(harness *stormServiceHarness, name string, expectPodGroup bool) *strictStormServiceCleanup {
	return &strictStormServiceCleanup{
		harness:        harness,
		name:           name,
		expectPodGroup: expectPodGroup,
	}
}

func (c *strictStormServiceCleanup) run(ctx context.Context) error {
	if c.completed {
		return nil
	}
	if err := c.harness.cleanupStormServiceResources(ctx, c.name, c.expectPodGroup); err != nil {
		return err
	}
	if err := waitForServiceNotFound(ctx, c.harness.kubeClient, c.harness.namespace, c.name); err != nil {
		return err
	}
	if _, err := c.harness.kubeClient.CoreV1().Services(c.harness.namespace).Get(ctx, c.name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		if err == nil {
			return fmt.Errorf("get Service after cleanup found %s/%s, want NotFound", c.harness.namespace, c.name)
		}
		return fmt.Errorf("get Service after cleanup: %w", err)
	}
	c.completed = true
	return nil
}
