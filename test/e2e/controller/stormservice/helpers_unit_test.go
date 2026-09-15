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
	"testing"

	orchestrationv1alpha1 "github.com/vllm-project/aibrix/api/orchestration/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
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
