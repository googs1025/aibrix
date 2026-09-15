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
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

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
