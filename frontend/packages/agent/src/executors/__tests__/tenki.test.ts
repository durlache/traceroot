import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock the Tenki SDK. The executor drives commands through the low-level
// `session.run()` handle (so it can cancel), so the mock models that handle.
const mockSession = {
  id: "sbx-test",
  run: vi.fn(),
  writeFile: vi.fn(),
  readFile: vi.fn(),
  close: vi.fn(),
  closeIfOpen: vi.fn(),
};

const mockClient = {
  whoAmI: vi.fn(),
  create: vi.fn().mockResolvedValue(mockSession),
  close: vi.fn(),
};

vi.mock("@tenkicloud/sandbox", () => ({
  TenkiSandbox: vi.fn().mockImplementation(() => mockClient),
}));

import { TenkiExecutor } from "../tenki.js";

const enc = (s: string) => new TextEncoder().encode(s);

/** A ReadableStream that emits `text` (if any) then closes. */
function streamOf(text: string): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      if (text) controller.enqueue(enc(text));
      controller.close();
    },
  });
}

type RunResult = {
  exitCode?: number;
  stdout?: string;
  stderr?: string;
  signal?: string;
  reason?: string;
};

/** Build a ProcessRunHandle-like mock that resolves to a ProcessRunResult. */
function runHandle(r: RunResult = {}) {
  const { exitCode = 0, stdout = "", stderr = "", signal, reason } = r;
  const result = {
    exitCode,
    stdout: enc(stdout),
    stderr: enc(stderr),
    signal,
    reason,
    durationMs: 1,
  };
  return {
    pid: Promise.resolve(1),
    stdout: streamOf(stdout),
    stderr: streamOf(stderr),
    stdin: { close: () => Promise.resolve() },
    signal: vi.fn().mockResolvedValue(undefined),
    kill: vi.fn().mockResolvedValue(undefined),
    // PromiseLike<ProcessRunResult>
    then: (onF: (v: typeof result) => unknown, onR?: (e: unknown) => unknown) =>
      Promise.resolve(result).then(onF, onR),
  };
}

/** A handle whose streams never close and never resolves — used to test cancellation. */
function hangingRunHandle() {
  return {
    pid: Promise.resolve(1),
    stdout: new ReadableStream<Uint8Array>({ start() {} }),
    stderr: new ReadableStream<Uint8Array>({ start() {} }),
    stdin: { close: () => Promise.resolve() },
    signal: vi.fn().mockResolvedValue(undefined),
    kill: vi.fn().mockResolvedValue(undefined),
    then: () => new Promise(() => {}),
  };
}

/** session.run is always called as run(argv, { env }). */
type RunCall = [string[], { env?: Record<string, string> }];

const lastRun = () => mockSession.run.mock.lastCall as RunCall;
const runCmds = () => (mockSession.run.mock.calls as RunCall[]).map((c) => c[0].at(-1));

describe("TenkiExecutor", () => {
  let executor: TenkiExecutor;

  beforeEach(() => {
    vi.clearAllMocks();
    delete process.env.TENKI_PROJECT_ID;
    delete process.env.TENKI_WORKSPACE_ID;
    executor = new TenkiExecutor();

    // Default mocks (SDK 0.4.0: whoAmI workspaces carry projects[]).
    mockClient.create.mockResolvedValue(mockSession);
    mockClient.whoAmI.mockResolvedValue({
      workspaces: [{ id: "ws-1", name: "default", projects: [{ id: "proj-123" }] }],
    });
    mockSession.run.mockImplementation(() => runHandle());
  });

  describe("init()", () => {
    it("resolves the project via whoAmI and creates an outbound-enabled sandbox", async () => {
      await executor.init();

      expect(mockClient.whoAmI).toHaveBeenCalled();
      expect(mockClient.create).toHaveBeenCalledWith(
        expect.objectContaining({
          projectId: "proj-123",
          allowOutbound: true, // SDK default is networking OFF; clones need egress
          cpuCores: 2,
          memoryMb: 4096,
          idleTimeoutMinutes: 30, // configurable default
          maxDurationMs: 2 * 60 * 60 * 1000, // finite backstop, not 600s
        }),
      );
      expect(executor.getWorkspacePath()).toBe("/workspace");
      expect(executor.isReady()).toBe(true);
    });

    it("honors TENKI_PROJECT_ID without calling whoAmI", async () => {
      process.env.TENKI_PROJECT_ID = "proj-explicit";
      await executor.init();

      expect(mockClient.whoAmI).not.toHaveBeenCalled();
      expect(mockClient.create).toHaveBeenCalledWith(
        expect.objectContaining({ projectId: "proj-explicit" }),
      );
    });

    it("creates the /workspace layout at init", async () => {
      await executor.init();
      expect(runCmds().some((c) => c?.includes("mkdir -p /workspace/repos"))).toBe(true);
    });

    it("throws when no project is visible for the key", async () => {
      mockClient.whoAmI.mockResolvedValueOnce({ workspaces: [] });
      await expect(executor.init()).rejects.toThrow(/no project visible/);
    });

    it("refuses to guess when multiple projects are visible", async () => {
      mockClient.whoAmI.mockResolvedValueOnce({
        workspaces: [{ id: "w1", name: "a", projects: [{ id: "p1" }, { id: "p2" }] }],
      });
      await expect(executor.init()).rejects.toThrow(/multiple projects.*TENKI_PROJECT_ID/s);
      expect(mockClient.create).not.toHaveBeenCalled();
    });

    it("narrows to a single project via TENKI_WORKSPACE_ID", async () => {
      process.env.TENKI_WORKSPACE_ID = "w2";
      mockClient.whoAmI.mockResolvedValueOnce({
        workspaces: [
          { id: "w1", name: "a", projects: [{ id: "p1" }] },
          { id: "w2", name: "b", projects: [{ id: "p2" }] },
        ],
      });
      await executor.init();
      expect(mockClient.create).toHaveBeenCalledWith(expect.objectContaining({ projectId: "p2" }));
    });

    it("is idempotent — a second init() after success does not create another sandbox", async () => {
      await executor.init();
      await executor.init();
      expect(mockClient.create).toHaveBeenCalledTimes(1);
    });

    it("is single-flight under concurrent first use — one sandbox, not two", async () => {
      // Two overlapping first calls must share one initialization, or each would
      // create a microVM and destroy() could only reach one (reproduced live).
      const [a, b] = [executor.init(), executor.init()];
      await Promise.all([a, b]);
      expect(mockClient.create).toHaveBeenCalledTimes(1);
    });

    it("tears the sandbox down if post-create setup fails (no leak)", async () => {
      mockSession.run.mockImplementationOnce(() =>
        runHandle({ exitCode: 1, stderr: "mkdir: permission denied" }),
      );

      await expect(executor.init()).rejects.toThrow(/workspace setup failed/);
      expect(mockSession.closeIfOpen).toHaveBeenCalled();
      expect(mockClient.close).toHaveBeenCalled();
      expect(executor.isReady()).toBe(false);
    });

    it("releases the client if project resolution fails before create", async () => {
      mockClient.whoAmI.mockResolvedValueOnce({ workspaces: [] });
      await expect(executor.init()).rejects.toThrow(/no project visible/);
      expect(mockClient.close).toHaveBeenCalled();
      expect(mockClient.create).not.toHaveBeenCalled();
    });
  });

  describe("exec()", () => {
    it("runs commands as root via sudo -E … bash -lc and maps the result", async () => {
      await executor.init();
      mockSession.run.mockImplementationOnce(() =>
        runHandle({ exitCode: 0, stdout: "hello world", stderr: "warn" }),
      );

      const result = await executor.exec("echo hello world");

      expect(result).toEqual({ stdout: "hello world", stderr: "warn", code: 0 });
      const [argv] = lastRun();
      expect(argv.slice(0, 2)).toEqual(["sudo", "-E"]);
      expect(argv.slice(-2)).toEqual(["-lc", "cd /workspace && echo hello world"]);
    });

    it("always bounds execution in-guest with coreutils timeout (default when none given)", async () => {
      await executor.init();
      await executor.exec("do thing");
      const [argv] = lastRun();
      expect(argv).toEqual([
        "sudo",
        "-E",
        "timeout",
        "900",
        "bash",
        "-lc",
        "cd /workspace && do thing",
      ]);
    });

    it("uses the caller's timeout when supplied", async () => {
      await executor.init();
      await executor.exec("sleep 1", { timeout: 10 });
      const [argv] = lastRun();
      expect(argv).toEqual([
        "sudo",
        "-E",
        "timeout",
        "10",
        "bash",
        "-lc",
        "cd /workspace && sleep 1",
      ]);
    });

    it("passes env out-of-band, never in the command string", async () => {
      await executor.init();
      await executor.exec('echo "$SECRET"', { env: { SECRET: "s3cr3t" } });
      const [argv, opts] = lastRun();
      expect(opts.env).toEqual({ SECRET: "s3cr3t" });
      expect(argv.at(-1)).not.toContain("s3cr3t");
    });

    it("preserves exact output — no trimming of indentation/whitespace", async () => {
      await executor.init();
      const body = "  indented first line\nsecond\n\n"; // leading indent + trailing blank line
      mockSession.run.mockImplementationOnce(() => runHandle({ stdout: body }));
      const result = await executor.exec("cat file");
      expect(result.stdout).toBe(body);
    });

    it("cancels the guest process via kill() when the signal aborts", async () => {
      await executor.init();
      const handle = hangingRunHandle();
      mockSession.run.mockReturnValueOnce(handle);

      const ac = new AbortController();
      const p = executor.exec("sleep 999", { signal: ac.signal });
      ac.abort();

      await expect(p).rejects.toThrow();
      expect(handle.kill).toHaveBeenCalled();
    });

    it("throws immediately and never runs when the signal is already aborted", async () => {
      await executor.init();
      mockSession.run.mockClear();
      const ac = new AbortController();
      ac.abort();
      await expect(executor.exec("x", { signal: ac.signal })).rejects.toThrow();
      expect(mockSession.run).not.toHaveBeenCalled();
    });

    it("does not let a signaled command (exitCode 0) read as success", async () => {
      await executor.init();
      mockSession.run.mockImplementationOnce(() =>
        runHandle({ exitCode: 0, signal: "KILL", reason: "signaled" }),
      );
      const result = await executor.exec("run");
      expect(result.code).toBe(1);
      expect(result.stderr).toBe("command signaled"); // folded in only when no other output
    });

    it("does not clobber real output on a status-only failure", async () => {
      // The clone path merges stderr into stdout (2>&1); a synthetic message
      // must not shadow that real diagnostic.
      await executor.init();
      mockSession.run.mockImplementationOnce(() =>
        runHandle({ exitCode: 128, stdout: "fatal: repository not found" }),
      );
      const result = await executor.exec("git clone ...");
      expect(result.code).toBe(128);
      expect(result.stdout).toBe("fatal: repository not found");
      expect(result.stderr).toBe("");
    });

    it("scopes commands into /workspace after init, but not the bootstrap", async () => {
      await executor.init();
      // the init mkdir bootstrap runs before workDir is set → no cd prefix
      // (/workspace doesn't exist yet, so we must stay in the guest default)
      const bootstrap = (mockSession.run.mock.calls as RunCall[]).find((c) =>
        c[0].at(-1)?.includes("mkdir -p /workspace/repos"),
      );
      expect(bootstrap![0].at(-1)).not.toContain("cd ");

      // subsequent commands are cd'd into /workspace (the SDK run() cwd option is
      // ignored through sudo + the login shell, verified live), matching the bash
      // tool's advertised working directory.
      await executor.exec("ls repos");
      expect(lastRun()[0].at(-1)).toBe("cd /workspace && ls repos");
    });

    it("treats a non-positive timeout as unset and applies the default (never `timeout 0`)", async () => {
      await executor.init();
      await executor.exec("thing", { timeout: 0 });
      const [argv] = lastRun();
      expect(argv).toEqual([
        "sudo",
        "-E",
        "timeout",
        "900",
        "bash",
        "-lc",
        "cd /workspace && thing",
      ]);
    });

    it("surfaces a timeout (exit 124) as a clear message, not 'command exit'", async () => {
      await executor.init();
      mockSession.run.mockImplementationOnce(() => runHandle({ exitCode: 124, reason: "exit" }));
      const result = await executor.exec("slow", { timeout: 5 });
      expect(result.code).toBe(124);
      expect(result.stderr).toBe("command timed out after 5s");
    });

    it("has a host-side deadline so an unresponsive guest cannot hang the caller", async () => {
      vi.useFakeTimers();
      try {
        await executor.init();
        const handle = hangingRunHandle();
        mockSession.run.mockReturnValueOnce(handle);

        const p = executor.exec("wedged", { timeout: 5 }); // no AbortSignal
        const assertion = expect(p).rejects.toThrow(/host deadline/);
        // advance past timeout(5s) + grace(30s)
        await vi.advanceTimersByTimeAsync(36_000);
        await assertion;
        expect(handle.kill).toHaveBeenCalled();
      } finally {
        vi.useRealTimers();
      }
    });

    it("throws if not initialized", async () => {
      await expect(executor.exec("ls")).rejects.toThrow("not initialized");
    });
  });

  describe("writeFile()", () => {
    // Native SDK file ops are confined to the guest workdir (/home/tenki), so
    // writes stage there and are moved into place with exec.
    it("stages via native writeFile then moves into place with env-quoted paths", async () => {
      await executor.init();
      vi.clearAllMocks();
      mockSession.run.mockImplementation(() => runHandle());

      await executor.writeFile("/tmp/test.txt", "hello");

      const [stagePath, content] = mockSession.writeFile.mock.lastCall as [string, string];
      expect(stagePath).toMatch(/^\.traceroot-stage-/);
      expect(content).toBe("hello");

      const [argv, opts] = lastRun();
      expect(argv.at(-1)).toContain('mv "$SRC" "$DEST"');
      expect(opts.env).toMatchObject({ DEST: "/tmp/test.txt" });
      expect(opts.env?.SRC).toContain(stagePath);
    });

    it("throws when the move fails", async () => {
      await executor.init();
      mockSession.run.mockImplementationOnce(() =>
        runHandle({ exitCode: 1, stderr: "mv: cannot move" }),
      );
      await expect(executor.writeFile("/tmp/x", "y")).rejects.toThrow(/writeFile failed/);
    });

    it("throws if not initialized", async () => {
      await expect(executor.writeFile("/tmp/x", "y")).rejects.toThrow("not initialized");
    });
  });

  describe("readFile()", () => {
    it("copies into the workdir, reads natively, and cleans up", async () => {
      await executor.init();
      vi.clearAllMocks();
      mockSession.run.mockImplementation(() => runHandle());
      mockSession.readFile.mockResolvedValueOnce(Buffer.from("file contents"));

      const result = await executor.readFile("/tmp/test.txt");

      expect(result).toBe("file contents");
      const calls = mockSession.run.mock.calls as RunCall[];
      const cp = calls.find((c) => c[0].at(-1)?.includes('cp "$SRC"'));
      expect(cp?.[1].env).toMatchObject({ SRC: "/tmp/test.txt" });
      const rm = calls.find((c) => c[0].at(-1)?.includes('rm -f "$DEST"'));
      expect(rm).toBeTruthy();
    });

    it("throws if not initialized", async () => {
      await expect(executor.readFile("/tmp/x")).rejects.toThrow("not initialized");
    });
  });

  describe("cloneRepo()", () => {
    // Same contract as DaytonaExecutor: system `git` CLI + GIT_ASKPASS, token
    // never in argv / URL / .git/config.
    function cloneCall(): [string, Record<string, string>] {
      const call = (mockSession.run.mock.calls as RunCall[]).find((c) =>
        c[0].at(-1)?.includes("clone"),
      );
      expect(call).toBeTruthy();
      return [call![0].at(-1)!, call![1].env ?? {}];
    }

    it("writes an askpass helper and passes the token via env, never in argv", async () => {
      await executor.init();
      vi.clearAllMocks();
      mockSession.run.mockImplementation(() => runHandle());

      await executor.cloneRepo("https://github.com/foo/bar.git", "/repos/bar", {
        ref: "main",
        username: "x-access-token",
        password: "dummy_token",
      });

      const askpassUpload = (mockSession.writeFile.mock.calls as [string, string][]).find((c) =>
        c[1].includes("GIT_PASSWORD"),
      );
      expect(askpassUpload).toBeTruthy();

      const [cmd, env] = cloneCall();
      expect(cmd).toContain('"$GIT_URL"');
      expect(cmd).toContain('"$GIT_DEST"');
      expect(cmd).not.toContain("https://github.com/foo/bar.git");
      expect(cmd).not.toContain("dummy_token");
      expect(env).toMatchObject({
        GIT_ASKPASS: "/tmp/git-askpass.sh",
        GIT_TERMINAL_PROMPT: "0",
        GIT_USERNAME: "x-access-token",
        GIT_PASSWORD: "dummy_token",
        GIT_URL: "https://github.com/foo/bar.git",
        GIT_DEST: "/repos/bar",
        GIT_REF: "main",
      });
    });

    it("does not inject shell from a hostile ref", async () => {
      await executor.init();
      vi.clearAllMocks();
      mockSession.run.mockImplementation(() => runHandle());

      const hostile = 'main"; rm -rf / #';
      await executor.cloneRepo("https://github.com/foo/bar.git", "/repos/bar", {
        ref: hostile,
        password: "dummy_token",
      });

      const [cmd, env] = cloneCall();
      expect(cmd).not.toContain("rm -rf");
      expect(env.GIT_REF).toBe(hostile);
    });

    it("clones a branch with --branch (no checkout step)", async () => {
      await executor.init();
      await executor.cloneRepo("https://github.com/foo/bar.git", "/repos/bar", {
        ref: "main",
        password: "dummy_token",
      });
      const [cmd, env] = cloneCall();
      expect(cmd).toContain('--branch "$GIT_REF"');
      expect(cmd).not.toContain("checkout");
      expect(env.GIT_REF).toBe("main");
    });

    it("clones then checks out a commit SHA (not --branch)", async () => {
      await executor.init();
      const sha = "29b242d1b96aab9ac17e37350e6c7dc54033f61b";
      await executor.cloneRepo("https://github.com/foo/bar.git", "/repos/bar", {
        ref: sha,
        password: "dummy_token",
      });
      const [cmd, env] = cloneCall();
      expect(cmd).not.toContain("--branch");
      expect(cmd).toContain('checkout "$GIT_REF"');
      expect(env.GIT_REF).toBe(sha);
    });

    it("shallow-clones the default branch when no ref is given", async () => {
      await executor.init();
      await executor.cloneRepo("https://github.com/foo/bar.git", "/repos/bar", {
        password: "dummy_token",
      });
      const [cmd] = cloneCall();
      expect(cmd).toContain("--depth 1");
      expect(cmd).not.toContain("--branch");
      expect(cmd).not.toContain("checkout");
    });

    it("throws a redacted error when the clone fails", async () => {
      await executor.init();
      vi.clearAllMocks();
      // Only the `git clone` run fails; askpass staging/mv/chmod succeed.
      mockSession.run.mockImplementation((argv: string[]) =>
        argv.at(-1)?.includes("clone")
          ? runHandle({ exitCode: 128, stdout: "fatal: could not read Password dummy_token" })
          : runHandle(),
      );

      await expect(
        executor.cloneRepo("https://github.com/foo/bar.git", "/repos/bar", {
          password: "dummy_token",
        }),
      ).rejects.toThrow(/git clone failed.*\[REDACTED\]/s);
    });
  });

  describe("hasNativeGit()", () => {
    it("returns true", () => {
      expect(executor.hasNativeGit()).toBe(true);
    });
  });

  describe("destroy()", () => {
    it("closes the session (terminate) and resets state", async () => {
      await executor.init();
      expect(executor.isReady()).toBe(true);

      await executor.destroy();

      expect(mockSession.closeIfOpen).toHaveBeenCalled();
      expect(mockClient.close).toHaveBeenCalled();
      expect(executor.isReady()).toBe(false);
    });

    it("no-ops if not initialized", async () => {
      await executor.destroy();
      expect(mockSession.closeIfOpen).not.toHaveBeenCalled();
    });

    it("surfaces a failed terminate and retains the handle for retry", async () => {
      await executor.init();
      mockSession.closeIfOpen.mockRejectedValueOnce(new Error("terminate failed"));

      await expect(executor.destroy()).rejects.toThrow(/terminate failed/);
      expect(executor.isReady()).toBe(true);

      mockSession.closeIfOpen.mockResolvedValueOnce(undefined);
      await executor.destroy();
      expect(executor.isReady()).toBe(false);
    });

    it("tears down a sandbox created by an in-flight init (no leak on concurrent destroy)", async () => {
      // Hold create() open so destroy() arrives while init is mid-flight.
      let releaseCreate: (s: typeof mockSession) => void;
      mockClient.create.mockReturnValueOnce(
        new Promise((resolve) => {
          releaseCreate = resolve;
        }),
      );

      const initP = executor.init(); // starts, awaiting create()
      const destroyP = executor.destroy(); // must await the in-flight init, not no-op
      releaseCreate!(mockSession); // create resolves → init finishes → destroy adopts the session

      await Promise.all([initP, destroyP]);
      expect(mockSession.closeIfOpen).toHaveBeenCalled(); // the created VM was torn down
      expect(executor.isReady()).toBe(false);
    });
  });

  describe("failed init recovery", () => {
    it("does not report a half-initialized sandbox as ready, and a retry re-inits", async () => {
      // First init: setup fails AND cleanup close also fails — the session must
      // still be cleared so the guard can't report the broken sandbox as ready.
      mockSession.run.mockImplementationOnce(() => runHandle({ exitCode: 1, stderr: "boom" }));
      mockSession.closeIfOpen.mockRejectedValueOnce(new Error("close failed too"));

      await expect(executor.init()).rejects.toThrow(/workspace setup failed/);
      expect(executor.isReady()).toBe(false);

      // Retry: a fresh create() runs (guard did not short-circuit on a stale session).
      await executor.init();
      expect(mockClient.create).toHaveBeenCalledTimes(2);
      expect(executor.isReady()).toBe(true);
    });
  });
});
