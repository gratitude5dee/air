import type { StudioMode } from "./commands.js";

/**
 * Air v2 deliberately has four fixed, single-step workflow definitions.  A
 * registry model fills the provider/model fields only after it has passed the
 * catalog, schema, privacy, and pricing review; users can never author a
 * workflow or add a second paid operation.
 */
export interface StudioWorkflowDefinition {
  readonly workflowId: `air:${StudioMode}:v1`;
  readonly version: 1;
  readonly mode: StudioMode;
  readonly paidSteps: readonly [
    {
      readonly stepId: "provider-generation";
      readonly kind: "provider-generation";
    },
  ];
}

const paidStep = Object.freeze([
  Object.freeze({ stepId: "provider-generation" as const, kind: "provider-generation" as const }),
] as const);

const WORKFLOWS: Readonly<Record<StudioMode, StudioWorkflowDefinition>> = Object.freeze({
  imagine: Object.freeze({
    workflowId: "air:imagine:v1",
    version: 1,
    mode: "imagine",
    paidSteps: paidStep,
  }),
  animate: Object.freeze({
    workflowId: "air:animate:v1",
    version: 1,
    mode: "animate",
    paidSteps: paidStep,
  }),
  "edit-image": Object.freeze({
    workflowId: "air:edit-image:v1",
    version: 1,
    mode: "edit-image",
    paidSteps: paidStep,
  }),
  "edit-video": Object.freeze({
    workflowId: "air:edit-video:v1",
    version: 1,
    mode: "edit-video",
    paidSteps: paidStep,
  }),
});

export function getStudioWorkflow(mode: StudioMode): StudioWorkflowDefinition {
  return WORKFLOWS[mode];
}

/** A narrow guard for future migrations that must never expand launch scope. */
export function isSinglePaidStepWorkflow(definition: StudioWorkflowDefinition): boolean {
  return definition.version === 1
    && definition.workflowId === `air:${definition.mode}:v1`
    && definition.paidSteps.length === 1
    && definition.paidSteps[0]?.stepId === "provider-generation"
    && definition.paidSteps[0]?.kind === "provider-generation";
}
