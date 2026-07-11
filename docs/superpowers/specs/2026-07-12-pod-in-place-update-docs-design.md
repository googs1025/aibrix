# Pod In-Place Update Documentation Design

## Goal

Document the pod in-place image rollout introduced by PR #2402 and provide
ready-to-use examples for both direct RoleSet use and StormService-managed
use.

## Scope

Update the existing StormService design documentation and add two manifests
under `samples/orchestration/`:

- a RoleSet example that enables pod-level in-place image updates;
- a StormService example that combines StormService-level and role-level
  in-place update strategies.

The change is documentation and samples only. It does not alter controllers,
APIs, CRDs, or generated files.

## Two Distinct Update Layers

The documentation must keep these strategies separate:

1. `StormService.spec.updateStrategy.type: InPlaceUpdate` updates managed
   RoleSet objects without replacing those RoleSets. By itself, it does not
   guarantee that Pods keep their identity.
2. `RoleSet.spec.roles[].updateStrategy.type: InPlaceIfPossible` attempts to
   patch live Pods when a role's container images change. It preserves Pod
   name and UID when the update is eligible and falls back to recreation when
   it is not.

For a StormService-driven pod in-place image rollout, both settings are
required: the outer strategy propagates the new template into existing
RoleSets, and the role strategy controls how each RoleSet updates its Pods.

The default role strategy is `Recreate`, so omitting `InPlaceIfPossible`
retains the existing replacement behavior.

## Supported Behavior and Fallback

The guide will describe image-only PodTemplate changes as eligible for
in-place update. During an eligible rollout, the controller patches the image
on the existing Pod and waits for the runtime container image ID and readiness
to reflect the new image before completing the role revision.

Changes outside the supported image fields fall back to Pod recreation. This
includes changes such as commands, environment, resources, volumes, or other
PodTemplate fields. Roles backed by a multi-Pod PodSet (`podGroupSize > 1`)
also fall back to recreation. Fallback is intentional for
`InPlaceIfPossible`: it prevents unsupported updates from blocking rollout.
The guide will show how to inspect the RoleSet `InPlaceFallback` event.

## Documentation Structure

Extend `docs/source/designs/aibrix-stormservice.rst` rather than creating a
separate feature page. Revise the current InPlace Update section so it no
longer implies that keeping RoleSets automatically means keeping Pods.

The section will contain:

- a comparison table for StormService `InPlaceUpdate`, role
  `InPlaceIfPossible`, and role `Recreate`;
- the required combined configuration for StormService-managed Pods;
- a direct RoleSet configuration;
- commands to apply an example and trigger an image update;
- commands to capture and compare Pod UID before and after the update;
- commands to inspect image, readiness, annotations, conditions, and events;
- supported-change, fallback, default, and availability-control notes;
- links to both sample manifests.

The prose and examples will use the exact API nesting from PR #2402.

## Samples

Create:

- `samples/orchestration/roleset-inplace-update.yaml`
- `samples/orchestration/stormservice-inplace-update.yaml`

Each manifest will be minimal and independently applicable. Comments will
identify the strategy layer and provide the exact `kubectl set image` or
`kubectl patch` command needed to change the image. The StormService sample
will visibly place `InPlaceUpdate` at the StormService level and
`InPlaceIfPossible` inside the role so users do not confuse the two.

The samples will use a lightweight public image with two stable tags suitable
for demonstrating an image-only update. They will set conservative rollout
limits (`maxUnavailable: 1`, `maxSurge: 0`) to avoid requiring extra capacity.

## Verification

Verify the result by:

- parsing both YAML manifests;
- building the Sphinx documentation, if the local documentation dependencies
  are available;
- checking the RST for broken local sample links;
- confirming all API field paths and enum values against PR #2402;
- running `git diff --check` and reviewing the final diff for unrelated
  changes.

Existing untracked or modified user files will not be included in the change.
