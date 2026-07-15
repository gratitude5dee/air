import { MongoClient, type Db, type MongoClientOptions } from "mongodb";

/**
 * Provenance: MusicRAG's reusable read-only MongoDB client pattern, adapted
 * for Air's bounded Vercel Function connection requirements.
 */

const DEFAULT_TIMEOUT_MS = 8_000;
const DEFAULT_DATABASE = "music_rag";

let client: MongoClient | undefined;
let connecting: Promise<MongoClient> | undefined;

function positiveEnvNumber(name: string, fallback: number): number {
  const value = Number(process.env[name]);
  return Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback;
}

export function mongoClientOptions(): MongoClientOptions {
  const serverSelectionTimeoutMS = positiveEnvNumber(
    "MONGODB_SERVER_SELECTION_TIMEOUT_MS",
    DEFAULT_TIMEOUT_MS,
  );

  return {
    // Air runs in Vercel Functions. Reuse one module-scoped client and keep
    // timeouts bounded; do not invent pool settings without production load
    // data and cluster capacity information.
    serverSelectionTimeoutMS,
    connectTimeoutMS: positiveEnvNumber(
      "MONGODB_CONNECT_TIMEOUT_MS",
      serverSelectionTimeoutMS,
    ),
  };
}

export function getMongoUri(env: NodeJS.ProcessEnv = process.env): string {
  const uri = env.MONGODB_URI?.trim();
  if (!uri) {
    throw new Error("MONGODB_URI is required for the read-only transcript corpus");
  }
  return uri;
}

/**
 * A module-scoped client survives warm Vercel invocations. It is intentionally
 * never closed after an individual request: closing it causes connection churn
 * and defeats the driver's pool reuse.
 */
export async function getMongoClient(): Promise<MongoClient> {
  if (!client) {
    client = new MongoClient(getMongoUri(), mongoClientOptions());
  }

  connecting ??= client.connect().then(
    () => client as MongoClient,
    (error) => {
      // Permit a later invocation to make a clean retry after a failed cold
      // connection; never include the driver's error text in user output.
      connecting = undefined;
      throw error;
    },
  );

  return connecting;
}

export async function getDb(): Promise<Db> {
  const mongo = await getMongoClient();
  return mongo.db(process.env.MONGODB_DB?.trim() || DEFAULT_DATABASE);
}

/** Safe text for a user-facing fallback; raw driver errors may contain hosts. */
export function publicMongoError(_error: unknown): string {
  return "The transcript database is temporarily unavailable. Please try again.";
}

/** Test-only reset hook. Application code must never call this per request. */
export async function __resetMongoForTests(): Promise<void> {
  const current = client;
  client = undefined;
  connecting = undefined;
  await current?.close().catch(() => undefined);
}
