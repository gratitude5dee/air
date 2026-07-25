import { describe, expect, it, vi } from "vitest";

import type { StudioEnvironment } from "../../lib/studio/config.js";
import {
  createStudioDrainBudget,
  drainStudio,
} from "../../lib/studio/engine.js";
import type { StudioIMessageTransport } from "../../lib/studio/imessage-transport.js";
import type { StudioRuntime } from "../../lib/studio/runtime.js";

function deliveryEnvironment(): StudioEnvironment {
  return {
    deploymentMode: "preview",
    releasePhase: "canary",
    releaseGeneration: 1,
    releaseFenceSecret: "release-fence-secret",
    storageTransport: "rest",
    imessageProjectId: "project",
    imessageProjectSecret: "project-secret",
    imessageWebhookSecret: "webhook-secret",
    upstashRedisRestUrl: "https://redis.example.test",
    upstashRedisRestToken: "redis-token",
    idHashSecret: "identity-secret",
    queueEncryptionKey: Buffer.alloc(32, 11).toString("base64"),
    allowedSenderHashes: ["a".repeat(64) as StudioEnvironment["allowedSenderHashes"][number]],
    cronSecret: "cron-secret",
    publicBaseUrl: "https://air.example.test",
    modelRegistryVersion: "2.1.0",
    dailyUserJobLimit: 3,
    activeJobLimit: 1,
    canaryBudgetId: "drain-deadline-test-budget",
    policy: {
      acceptNewJobs: false,
      allowProviderSubmit: false,
      allowProviderRecovery: false,
      deliveryEnabled: true,
    },
    policyExplicit: true,
    spendCaps: {
      perJobCents: 0,
      dailyCents: 0,
      canaryTotalCents: 100,
      canaryFalCents: 0,
    },
  };
}

function fixture() {
  const claims: string[] = [];
  const claimNextInbox = vi.fn(async () => {
    claims.push("inbox");
    return null;
  });
  const claimNextOutbox = vi.fn(async () => {
    claims.push("outbox");
    return null;
  });
  const claimNextJob = vi.fn(async () => {
    claims.push("job");
    return null;
  });
  const transport = {
    verified: true,
    showTyping: vi.fn(async () => undefined),
    stopTyping: vi.fn(async () => undefined),
    sendText: vi.fn(async () => ({ kind: "sent" as const })),
    sendAttachment: vi.fn(async () => ({ kind: "sent" as const })),
    addLikeTapback: vi.fn(async () => ({ kind: "sent" as const })),
    fetchAttachment: vi.fn(async () => new Uint8Array()),
  } satisfies StudioIMessageTransport;
  const runtime = {
    environment: deliveryEnvironment(),
    store: { claimNextInbox, claimNextOutbox, claimNextJob },
    transport,
    provider: { verified: false },
    storageTransport: "rest",
    readiness: () => [],
    close: async () => undefined,
  } as unknown as StudioRuntime;
  return { runtime, claimNextInbox, claimNextOutbox, claimNextJob, transport, claims };
}

describe("Studio Cron invocation budget", () => {
  it("runs exactly one outbox pass per drain invocation", async () => {
    const { runtime, claimNextOutbox } = fixture();

    await drainStudio(runtime);

    // The only outbox pass occurs inside `drainStudioWithStats`; a job-phase
    // publication waits for the following minute Cron instead of reopening a
    // second unbounded Photon dispatch loop.
    expect(claimNextOutbox).toHaveBeenCalledTimes(1);
  });

  it("gives durable job recovery first access to a fresh Cron budget", async () => {
    const { runtime, claims } = fixture();

    await drainStudio(runtime);

    expect(claims).toEqual(["job", "inbox", "outbox"]);
  });

  it("shares one elapsed deadline across inbox, outbox, and job admissions", async () => {
    const { runtime, claimNextInbox, claimNextOutbox, claimNextJob, transport } = fixture();
    const budget = createStudioDrainBudget({ now: () => 10_000, deadlineAtMs: 10_000 });

    await drainStudio(runtime, budget);

    expect(claimNextInbox).not.toHaveBeenCalled();
    expect(claimNextOutbox).not.toHaveBeenCalled();
    expect(claimNextJob).not.toHaveBeenCalled();
    expect(transport.showTyping).not.toHaveBeenCalled();
    expect(transport.sendText).not.toHaveBeenCalled();
  });
});
