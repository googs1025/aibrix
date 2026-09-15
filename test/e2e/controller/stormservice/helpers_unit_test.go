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
	"testing"

	orchestrationv1alpha1 "github.com/vllm-project/aibrix/api/orchestration/v1alpha1"
	aibrixfake "github.com/vllm-project/aibrix/pkg/client/clientset/versioned/fake"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestNewUpdateStormServiceUsesPooledInPlaceFixture(t *testing.T) {
	stormService := newUpdateStormService("default", "update")

	if stormService.Spec.Replicas == nil || *stormService.Spec.Replicas != 1 {
		t.Fatalf("replicas = %v, want explicit 1", stormService.Spec.Replicas)
	}
	if stormService.Spec.Mode != orchestrationv1alpha1.StormServicePooledMode {
		t.Fatalf("mode = %q, want %q", stormService.Spec.Mode, orchestrationv1alpha1.StormServicePooledMode)
	}
	if stormService.Spec.UpdateStrategy.Type != orchestrationv1alpha1.InPlaceUpdateStormServiceStrategyType {
		t.Fatalf("StormService update strategy = %q, want %q", stormService.Spec.UpdateStrategy.Type, orchestrationv1alpha1.InPlaceUpdateStormServiceStrategyType)
	}
	if stormService.Spec.Template.Spec.UpdateStrategy != orchestrationv1alpha1.ParallelRoleSetUpdateStrategyType {
		t.Fatalf("RoleSet update strategy = %q, want %q", stormService.Spec.Template.Spec.UpdateStrategy, orchestrationv1alpha1.ParallelRoleSetUpdateStrategyType)
	}
	if role := stormService.Spec.Template.Spec.Roles[0]; role.UpdateStrategy.Type != orchestrationv1alpha1.InPlaceIfPossibleRoleUpdateStrategyType {
		t.Fatalf("role update strategy = %q, want %q", role.UpdateStrategy.Type, orchestrationv1alpha1.InPlaceIfPossibleRoleUpdateStrategyType)
	}
}

func TestStormServiceHarnessRecordsRoleSetNamesAcrossObservations(t *testing.T) {
	harness := &stormServiceHarness{}
	harness.recordRoleSets("storm", []unstructured.Unstructured{
		{Object: map[string]interface{}{"metadata": map[string]interface{}{"name": "storm-roleset-a"}}},
	})
	harness.recordRoleSets("storm", []unstructured.Unstructured{
		{Object: map[string]interface{}{"metadata": map[string]interface{}{"name": "storm-roleset-b"}}},
	})

	names := harness.recordedRoleSetNames("storm")
	_, hasFirst := names["storm-roleset-a"]
	_, hasSecond := names["storm-roleset-b"]
	if len(names) != 2 || !hasFirst || !hasSecond {
		t.Fatalf("recorded RoleSet names = %v, want storm-roleset-a and storm-roleset-b", names)
	}
}

func TestStormServiceHarnessWaitForRoleSetsRecordsObservedNames(t *testing.T) {
	roleSet := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "orchestration.aibrix.ai/v1alpha1",
		"kind":       "RoleSet",
		"metadata": map[string]interface{}{
			"name":      "storm-roleset-a",
			"namespace": "default",
			"labels": map[string]interface{}{
				"storm-service-name": "storm",
			},
		},
	}}
	harness := &stormServiceHarness{
		namespace: "default",
		dynamicClient: dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
			runtime.NewScheme(),
			map[schema.GroupVersionResource]string{roleSetGVR: "RoleSetList"},
			roleSet,
		),
	}

	if _, err := harness.waitForRoleSets(context.Background(), "storm", 1); err != nil {
		t.Fatalf("wait for RoleSets: %v", err)
	}
	if _, found := harness.recordedRoleSetNames("storm")["storm-roleset-a"]; !found {
		t.Fatalf("harness did not record RoleSet observed through waitForRoleSets")
	}
}

func TestWaitForOwnedServiceFailsImmediatelyForForbidden(t *testing.T) {
	kubeClient := k8sfake.NewSimpleClientset()
	getCalls := 0
	kubeClient.PrependReactor("get", "services", func(k8stesting.Action) (bool, runtime.Object, error) {
		getCalls++
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "services"}, "storm", fmt.Errorf("denied"))
	})

	_, err := waitForOwnedService(context.Background(), kubeClient, "default", "storm", types.UID("storm-uid"))
	if !apierrors.IsForbidden(err) {
		t.Fatalf("wait error = %v, want Forbidden", err)
	}
	if getCalls != 1 {
		t.Fatalf("get calls = %d, want 1 for a permanent error", getCalls)
	}
}

func TestWaitForOwnedServiceRetriesNotFound(t *testing.T) {
	ownerUID := types.UID("storm-uid")
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name:      "storm",
		Namespace: "default",
		OwnerReferences: []metav1.OwnerReference{{
			UID: ownerUID,
		}},
	}}
	kubeClient := k8sfake.NewSimpleClientset(service)
	getCalls := 0
	kubeClient.PrependReactor("get", "services", func(k8stesting.Action) (bool, runtime.Object, error) {
		getCalls++
		if getCalls == 1 {
			return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "services"}, "storm")
		}
		return false, nil, nil
	})

	if _, err := waitForOwnedService(context.Background(), kubeClient, "default", "storm", ownerUID); err != nil {
		t.Fatalf("wait for owned Service: %v", err)
	}
	if getCalls != 2 {
		t.Fatalf("get calls = %d, want 2 after NotFound retry", getCalls)
	}
}

func TestWaitForOwnedServiceRetriesTransientError(t *testing.T) {
	ownerUID := types.UID("storm-uid")
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name:      "storm",
		Namespace: "default",
		OwnerReferences: []metav1.OwnerReference{{
			UID: ownerUID,
		}},
	}}
	kubeClient := k8sfake.NewSimpleClientset(service)
	getCalls := 0
	kubeClient.PrependReactor("get", "services", func(k8stesting.Action) (bool, runtime.Object, error) {
		getCalls++
		if getCalls == 1 {
			return true, nil, apierrors.NewServiceUnavailable("temporarily unavailable")
		}
		return false, nil, nil
	})

	if _, err := waitForOwnedService(context.Background(), kubeClient, "default", "storm", ownerUID); err != nil {
		t.Fatalf("wait for owned Service: %v", err)
	}
	if getCalls != 2 {
		t.Fatalf("get calls = %d, want 2 after transient retry", getCalls)
	}
}

func TestCleanupStormServiceContinuesAfterIdentityListFailure(t *testing.T) {
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		roleSetGVR:         "RoleSetList",
		podSetGVR:          "PodSetList",
		volcanoPodGroupGVR: "PodGroupList",
	})
	roleSetListCalls := 0
	dynamicClient.PrependReactor("list", "rolesets", func(k8stesting.Action) (bool, runtime.Object, error) {
		roleSetListCalls++
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: roleSetGVR.Group, Resource: roleSetGVR.Resource}, "", fmt.Errorf("denied"))
	})

	kubeClient := k8sfake.NewSimpleClientset()
	podListCalls := 0
	kubeClient.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		podListCalls++
		return false, nil, nil
	})
	aibrixClient := aibrixfake.NewSimpleClientset()
	deleteCalls := 0
	aibrixClient.Fake.PrependReactor("delete", "stormservices", func(k8stesting.Action) (bool, runtime.Object, error) {
		deleteCalls++
		return false, nil, nil
	})

	harness := &stormServiceHarness{
		namespace:     "default",
		kubeClient:    kubeClient,
		stormServices: aibrixClient.OrchestrationV1alpha1().StormServices("default"),
		dynamicClient: dynamicClient,
	}
	err := harness.cleanupStormServiceResources(context.Background(), "storm", false)
	if err == nil || !apierrors.IsForbidden(err) {
		t.Fatalf("cleanup error = %v, want joined Forbidden identity-list error", err)
	}
	if roleSetListCalls == 0 {
		t.Fatal("cleanup did not attempt the RoleSet identity list")
	}
	if deleteCalls != 1 {
		t.Fatalf("StormService delete calls = %d, want 1 despite identity-list failure", deleteCalls)
	}
	if podListCalls == 0 {
		t.Fatal("cleanup did not continue to the Pod check after identity-list failure")
	}
}
