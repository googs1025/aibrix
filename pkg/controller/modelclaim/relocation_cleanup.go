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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	modelv1alpha1 "github.com/vllm-project/aibrix/api/model/v1alpha1"
	"github.com/vllm-project/aibrix/pkg/constants"
)

const pendingStopPrefix = "model.aibrix.ai/pending-stop."

// Use cancellation so a short cleanup budget does not trip the runtime
// client's silence window for placement and health checks.
func relocationCleanupContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	timer := time.AfterFunc(2*time.Second, cancel)
	return ctx, func() {
		timer.Stop()
		cancel()
	}
}

// Record the old activation and withdraw its route in the same Pod patch,
// before replacing the instance in Claim status.
func (r *ModelClaimReconciler) recordPendingStop(ctx context.Context, claim *modelv1alpha1.ModelClaim, podName string) error {
	budget, cancel := relocationCleanupContext(ctx)
	defer cancel()
	pod, err := r.readPendingStopPod(budget, client.ObjectKey{Namespace: claim.Namespace, Name: podName})
	if err != nil {
		return client.IgnoreNotFound(err)
	}
	if !pod.DeletionTimestamp.IsZero() {
		return nil
	}
	if pod.Status.PodIP == "" {
		return fmt.Errorf("source Pod has no runtime address")
	}
	snapshot, err := r.Runtime.Snapshot(budget, pod.Status.PodIP, DefaultRuntimePort)
	if err != nil {
		return err
	}
	if snapshot == nil {
		return fmt.Errorf("source runtime returned no snapshot")
	}
	model := snapshotModelByName(snapshot, servedModelName(claim))
	patch := client.MergeFromWithOptions(pod.DeepCopy(), client.MergeFromWithOptimisticLock{})
	key := pendingStopPrefix + string(claim.UID)
	if model == nil || (model.ClaimRef != nil && model.ClaimRef.UID != "" && model.ClaimRef.UID != string(claim.UID)) {
		delete(pod.Annotations, key)
	} else {
		if model.InstanceID == "" || model.ClaimRef == nil || model.ClaimRef.Namespace != claim.Namespace ||
			model.ClaimRef.UID == "" {
			return fmt.Errorf("source runtime could not verify the old instance for %s", model.ModelName)
		}
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		pod.Annotations[key] = model.InstanceID
	}
	delete(pod.Annotations, constants.ModelClaimPodAnnotationPrefix+claim.Name)
	delete(pod.Annotations, constants.ModelClaimWakeAnnotationPrefix+claim.Name)
	return r.Patch(budget, pod, patch)
}

// Only inspect markers for this Claim. The cached listing also finds a
// source Pod that has since stopped matching the placement selector.
func (r *ModelClaimReconciler) retrySourceStops(ctx context.Context, claim *modelv1alpha1.ModelClaim) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(claim.Namespace)); err != nil {
		klog.ErrorS(err, "list Pods for pending ModelClaim stops", "claim", klog.KObj(claim))
		return
	}
	key := pendingStopPrefix + string(claim.UID)
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Annotations[key] != "" {
			r.retrySourceStop(ctx, claim, client.ObjectKeyFromObject(pod), key)
		}
	}
}

func (r *ModelClaimReconciler) retrySourceStop(parent context.Context, claim *modelv1alpha1.ModelClaim, podKey client.ObjectKey, key string) {
	ctx, cancel := relocationCleanupContext(parent)
	defer cancel()
	pod, err := r.readPendingStopPod(ctx, podKey)
	if err != nil || pod.Status.PodIP == "" || pod.Status.Phase != corev1.PodRunning || !pod.DeletionTimestamp.IsZero() {
		return
	}
	instanceID := pod.Annotations[key]
	if instanceID == "" {
		return
	}
	// The caller just persisted this status. Placement and cleanup share the
	// worker, so an unfinished move still has its source in this list.
	for _, instance := range claim.Status.Instances {
		if instance.Pod == pod.Name {
			if !movingReason(instance.Reason) {
				r.clearPendingStop(ctx, pod, key)
			}
			return
		}
	}
	snapshot, err := r.Runtime.Snapshot(ctx, pod.Status.PodIP, DefaultRuntimePort)
	if err != nil || snapshot == nil {
		return
	}
	modelName := servedModelName(claim)
	model := snapshotModelByName(snapshot, modelName)
	if model == nil || (model.InstanceID != "" && model.InstanceID != instanceID) {
		r.clearPendingStop(ctx, pod, key)
		return
	}
	if model.InstanceID == "" || model.Phase == runtimePhaseStopping {
		return
	}
	err = r.Runtime.Deactivate(ctx, pod.Status.PodIP, DefaultRuntimePort, &DeactivateRequest{
		ModelName: modelName, Mode: DeactivateStop, InstanceID: instanceID,
	})
	if err != nil {
		r.Recorder.Eventf(claim, corev1.EventTypeWarning, "RelocatedEngineStopRetry",
			"will retry stopping the old engine on %s: %v", pod.Name, err)
		return
	}
	r.Recorder.Eventf(claim, corev1.EventTypeNormal, "RelocatedEngineStopRequested",
		"requested a stop for the old engine on %s", pod.Name)
}

func snapshotModelByName(snapshot *RuntimeSnapshot, name string) *RuntimeSnapshotModel {
	for i := range snapshot.Models {
		if snapshot.Models[i].ModelName == name {
			return &snapshot.Models[i]
		}
	}
	return nil
}

func (r *ModelClaimReconciler) readPendingStopPod(ctx context.Context, key client.ObjectKey) (*corev1.Pod, error) {
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	pod := &corev1.Pod{}
	return pod, reader.Get(ctx, key, pod)
}

func (r *ModelClaimReconciler) clearPendingStop(ctx context.Context, pod *corev1.Pod, key string) {
	patch := client.MergeFromWithOptions(pod.DeepCopy(), client.MergeFromWithOptimisticLock{})
	delete(pod.Annotations, key)
	if err := r.Patch(ctx, pod, patch); err != nil {
		klog.ErrorS(err, "clear pending stop", "pod", klog.KObj(pod))
	}
}
