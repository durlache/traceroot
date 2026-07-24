import { describe, it, expect, afterAll } from "vitest";
import { TenkiExecutor } from "../tenki.js";

/**
 * Live smoke test against the real Tenki API — exercises the SAME TenkiExecutor
 * the agent uses (not a parallel path), per the review guidance that a smoke
 * test must cover the real code path.
 *
 * Gated on TENKI_API_KEY (or TENKI_AUTH_TOKEN) so CI, which has no credentials,
 * skips it. Run locally with:
 *   TENKI_API_KEY=tk_... pnpm --filter @traceroot/agent exec vitest run tenki.integration
 * Set TENKI_PROJECT_ID (or TENKI_WORKSPACE_ID) if the key sees more than one project.
 */
const hasCreds = !!(process.env.TENKI_API_KEY || process.env.TENKI_AUTH_TOKEN);
const TWO_MIN = 120_000;

(hasCreds ? describe : describe.skip)("TenkiExecutor (live)", () => {
  const executor = new TenkiExecutor();

  afterAll(async () => {
    // Always attempt teardown so a failed assertion never leaks a microVM.
    await executor.destroy().catch(() => {});
  });

  it(
    "boots, runs commands as root, round-trips a file, clones, and tears down",
    async () => {
      await executor.init();
      expect(executor.isReady()).toBe(true);
      expect(executor.getWorkspacePath()).toBe("/workspace");

      // exec runs as root via sudo -E
      const whoami = await executor.exec("whoami");
      expect(whoami.code).toBe(0);
      expect(whoami.stdout.trim()).toBe("root");

      // writeFile → readFile round-trip through the guest workdir staging
      const marker = `hello-${Date.now()}`;
      await executor.writeFile("/workspace/notes/smoke.txt", marker);
      expect((await executor.readFile("/workspace/notes/smoke.txt")).trim()).toBe(marker);

      // and the file is visible to exec at the absolute path
      const cat = await executor.exec('cat "$P"', { env: { P: "/workspace/notes/smoke.txt" } });
      expect(cat.stdout.trim()).toBe(marker);

      // a failing command reports a non-zero code (not a false success)
      const fail = await executor.exec("exit 7");
      expect(fail.code).toBe(7);

      // clone a small public repo (no token needed)
      await executor.cloneRepo("https://github.com/octocat/Hello-World.git", "/workspace/repos/hw");
      const ls = await executor.exec("ls /workspace/repos/hw/.git");
      expect(ls.code).toBe(0);

      await executor.destroy();
      expect(executor.isReady()).toBe(false);
    },
    TWO_MIN,
  );
});
