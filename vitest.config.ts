import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // Unit tests only. Integration tests (real servers/clients) live in
    // test/integration and run via `pnpm test:integration`.
    include: ["test/*.test.ts"],
  },
});
