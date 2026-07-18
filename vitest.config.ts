import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    include: [
      "tests/studio/**/*.test.ts",
      "tests/unit/crypto.test.ts",
      "tests/unit/redis-connection.test.ts",
    ],
    restoreMocks: true,
    clearMocks: true
  }
});
