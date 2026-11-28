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

package modelclaim

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	modelv1alpha1 "github.com/vllm-project/aibrix/api/model/v1alpha1"
)

func TestRelocationCleanupBudgetDoesNotSilenceRuntime(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	c, runtimes := hangingRuntimes(1, func() time.Time { return now })
	runtime := runtimes[0]
	c.snapshotTimeout = 5 * time.Second
	ctx, cancel := relocationCleanupContext(context.Background())
	defer cancel()

	_, err := c.Snapshot(ctx, runtime.host, runtime.port)

	require.ErrorIs(t, err, context.Canceled)
	assert.NotContains(t, c.silence.runtimes, runtime.address())
	runtime.asked(t, 1)
}

func TestRelocationCleanupVerifiesOldInstance(t *testing.T) {
	for _, scenario := range []string{
		"source still recorded", "move called off", "Pod replaced",
		"same port reused", "engine stopping", "identity unavailable",
		"failed engine before marking", "failed engine before retry",
	} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			claim := claimWithCost(300, 100)
			claim.UID = "claim-uid"
			pod, snapshot := sizedWarmPod("warm-source", "10.0.0.1", 1000)
			pod.UID = "old-pod"
			modelName := servedModelName(claim)
			snapshot.Models = []RuntimeSnapshotModel{{
				ModelName: modelName, Port: 9001, InstanceID: "old-engine", Phase: runtimePhaseSleeping, Alive: true,
				ClaimRef: &ModelClaimRef{Namespace: claim.Namespace, Name: claim.Name, UID: string(claim.UID)},
			}}
			switch scenario {
			case "source still recorded":
				claim.Status.Instances = []modelv1alpha1.ModelClaimInstance{{Pod: pod.Name, Port: 9001, Reason: instanceReasonWakeFailed}}
			case "move called off":
				claim.Status.Instances = []modelv1alpha1.ModelClaimInstance{{Pod: pod.Name, Port: 9001, Phase: modelv1alpha1.ModelClaimSleeping}}

			}
			if scenario == "failed engine before marking" {
				snapshot.Models[0].Phase, snapshot.Models[0].Alive = runtimePhaseFailed, false
			}
			r, runtime := newReconciler(t, claim, pod)
			runtime.snapshots = map[string]*RuntimeSnapshot{pod.Status.PodIP: snapshot}
			require.NoError(t, r.recordPendingStop(ctx, claim, pod.Name))
			r.APIReader = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					err := c.Get(ctx, key, obj, opts...)
					if p, ok := obj.(*corev1.Pod); ok && err == nil && scenario == "Pod replaced" {
						p.UID = "new-pod"
						p.Annotations = nil
					}
					return err
				},
			})
			switch scenario {
			case "same port reused":
				snapshot.Models[0].InstanceID = "new-engine"
			case "engine stopping":
				snapshot.Models[0].Phase = runtimePhaseStopping
			case "identity unavailable":
				snapshot.Models[0].InstanceID = ""
			case "failed engine before retry":
				snapshot.Models[0].Phase, snapshot.Models[0].Alive = runtimePhaseFailed, false
			}
			r.retrySourceStops(ctx, claim)
			annotations := podNamed(t, r, pod.Name).Annotations
			key := pendingStopPrefix + string(claim.UID)
			if scenario == "failed engine before marking" || scenario == "failed engine before retry" {
				require.Len(t, runtime.deactivateCalls, 1)
				assert.Equal(t, "old-engine", runtime.deactivateCalls[0].InstanceID)
				return
			}
			assert.Empty(t, runtime.deactivateCalls)
			if scenario == "same port reused" || scenario == "move called off" {
				assert.NotContains(t, annotations, key)
			} else {
				assert.Contains(t, annotations, key)
			}
		})
	}
}
