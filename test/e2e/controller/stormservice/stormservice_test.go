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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	orchestrationv1alpha1 "github.com/vllm-project/aibrix/api/orchestration/v1alpha1"
	controllerconstants "github.com/vllm-project/aibrix/pkg/controller/constants"
	stormservicecontroller "github.com/vllm-project/aibrix/pkg/controller/stormservice"
)

func TestStormServiceReplicaLifecycle(t *testing.T) {
	if !strings.EqualFold(strings.TrimSpace(os.Getenv(stormServiceVolcanoE2EEnv)), "true") {
		t.Skipf("set %s=true to run StormService replica lifecycle e2e tests", stormServiceVolcanoE2EEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	namespace := stormServiceNamespace()
	name := fmt.Sprintf("stormservice-replica-lifecycle-%d", time.Now().UnixNano())
	h := newStormServiceHarness(t, namespace)
	cleanedUp := false
	cleanup := func() {
		if cleanedUp {
			return
		}
		cleanedUp = true
		h.cleanupStormService(t, name, false)
	}

	created, err := h.stormServices.Create(ctx, newReplicaLifecycleStormService(namespace, name, 2), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create StormService %s/%s: %v", namespace, name, err)
	}
	t.Cleanup(cleanup)

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
	if service.Spec.Selector[controllerconstants.StormServiceNameLabelKey] != name {
		t.Errorf("Service selector[%q] = %q, want %q", controllerconstants.StormServiceNameLabelKey, service.Spec.Selector[controllerconstants.StormServiceNameLabelKey], name)
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
	if _, err := h.waitForRoleSets(ctx, name, 3); err != nil {
		t.Fatalf("wait for three RoleSets: %v", err)
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
