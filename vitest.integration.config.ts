import { defineConfig } from "vitest/config";

// Real loopback integration tests: boot an actual http/ws server + connect a real
// client. Kept separate from the hermetic unit suite (see vitest.config.ts).
export default defineConfig({
  test: {
    include: ["test/integration/**/*.test.ts"],
    testTimeout: 15_000,
    hookTimeout: 15_000,
  },
});
