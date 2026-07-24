import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock the Tenki SDK
const mockSession = {
  id: "sbx-test",
  exec: vi.fn(),
  writeFile: vi.fn(),
  readFile: vi.fn(),
  close: vi.fn(),
};

const mockClient = {
  whoAmI: vi.fn(),
  create: vi.fn().mockResolvedValue(mockSession),
  close: vi.fn(),
};

vi.mock("@tenkicloud/sandbox", () => ({
  TenkiSandbox: vi.fn().mockImplementation(() => mockClient),
  stdoutText: (r: { stdout: string }) => r.stdout ?? "",
  stderrText: (r: { stderr: string }) => r.stderr ?? "",
  isSuccess: (status: string) => status === "SUCCEEDED",
}));

import { TenkiExecutor } from "../tenki.js";

/** The exec wrapper always calls session.exec("sudo", { args, env, ... }). */
type ExecCall = [string, { args: string[]; env?: Record<string, string>; timeoutMs?: number }];

describe("TenkiExecutor", () => {
  let executor: TenkiExecutor;

  beforeEach(() => {
    vi.clearAllMocks();
    delete process.env.TENKI_PROJECT_ID;
    delete process.env.TENKI_WORKSPACE_ID;
    executor = new TenkiExecutor();

    // Default mock returns (SDK 0.4.0: whoAmI workspaces carry projects[]).
    mockClient.whoAmI.mockResolvedValue({
      workspaces: [{ id: "ws-1", name: "default", projects: [{ id: "proj-123" }] }],
    });
    mockSession.exec.mockResolvedValue({
      exitCode: 0,
      stdout: "",
      stderr: "",
      status: "SUCCEEDED",
    });
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
      // workDir is always /workspace, consistent with Docker/Daytona executors
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

      const cmds = (mockSession.exec.mock.calls as ExecCall[]).map((c) => c[1].args.at(-1));
      expect(cmds.some((c) => c?.includes("mkdir -p /workspace/repos"))).toBe(true);
    });

    it("throws when no project is visible for the key", async () => {
      mockClient.whoAmI.mockResolvedValueOnce({ workspaces: [] });
      await expect(executor.init()).rejects.toThrow(/no project visible/);
    });

    it("refuses to guess when multiple projects are visible", async () => {
      // Silently picking the first of many is the footgun the sibling Tenki
      // PRs were told to fix — ambiguity must point at the override instead.
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

    it("is idempotent — a second init() does not create a second sandbox", async () => {
      await executor.init();
      await executor.init();
      expect(mockClient.create).toHaveBeenCalledTimes(1);
    });

    it("tears the sandbox down if post-create setup fails (no leak)", async () => {
      // create() succeeded, so a later failure must terminate the session rather
      // than leak a running microVM until its idle timeout.
      mockSession.exec.mockResolvedValueOnce({
        exitCode: 1,
        stdout: "",
        stderr: "mkdir: permission denied",
        status: "FAILED",
      });

      await expect(executor.init()).rejects.toThrow(/workspace setup failed/);
      expect(mockSession.close).toHaveBeenCalled();
      expect(mockClient.close).toHaveBeenCalled();
      expect(executor.isReady()).toBe(false);
    });

    it("releases the client if project resolution fails before create", async () => {
      mockClient.whoAmI.mockResolvedValueOnce({ workspaces: [] });
      await expect(executor.init()).rejects.toThrow(/no project visible/);
      // No session was created, but the client's control-plane channel is closed.
      expect(mockClient.close).toHaveBeenCalled();
      expect(mockClient.create).not.toHaveBeenCalled();
    });
  });

  describe("exec()", () => {
    it("runs commands as root via sudo -E bash -lc and maps the result", async () => {
      await executor.init();
      mockSession.exec.mockResolvedValueOnce({
        exitCode: 0,
        stdout: "hello world",
        stderr: "warn",
        status: "SUCCEEDED",
      });

      const result = await executor.exec("echo hello world");

      expect(result).toEqual({ stdout: "hello world", stderr: "warn", code: 0 });
      const [program, opts] = mockSession.exec.mock.lastCall as ExecCall;
      expect(program).toBe("sudo");
      expect(opts.args).toEqual(["-E", "bash", "-lc", "echo hello world"]);
    });

    it("enforces timeout in-guest via coreutils timeout (SDK timeoutMs is only a backstop)", async () => {
      await executor.init();

      await executor.exec("sleep 1", { timeout: 10 });

      const [, opts] = mockSession.exec.mock.lastCall as ExecCall;
      expect(opts.args).toEqual(["-E", "timeout", "10", "bash", "-lc", "sleep 1"]);
      expect(opts.timeoutMs).toBe(40_000); // (timeout + 30s) backstop
    });

    it("passes env out-of-band, never in the command string", async () => {
      await executor.init();

      await executor.exec('echo "$SECRET"', { env: { SECRET: "s3cr3t" } });

      const [, opts] = mockSession.exec.mock.lastCall as ExecCall;
      expect(opts.env).toEqual({ SECRET: "s3cr3t" });
      expect(opts.args.at(-1)).not.toContain("s3cr3t");
    });

    it("does not let a timed-out command (exitCode 0, TIMED_OUT) read as success", async () => {
      await executor.init();
      mockSession.exec.mockResolvedValueOnce({
        exitCode: 0,
        stdout: "",
        stderr: "",
        status: "TIMED_OUT",
      });

      const result = await executor.exec("sleep 999");

      expect(result.code).toBe(124);
      expect(result.stderr).toBe("command timed_out"); // status folded in only when there's no other output
    });

    it("does not clobber real output on a status-only failure", async () => {
      // The clone path merges stderr into stdout (2>&1); a synthetic status
      // message must not shadow that real diagnostic.
      await executor.init();
      mockSession.exec.mockResolvedValueOnce({
        exitCode: 128,
        stdout: "fatal: repository not found",
        stderr: "",
        status: "FAILED",
      });

      const result = await executor.exec("git clone ...");

      expect(result.code).toBe(128);
      expect(result.stdout).toBe("fatal: repository not found");
      expect(result.stderr).toBe(""); // left empty so the caller reads stdout
    });

    it("maps a FAILED status with exitCode 0 to a failure", async () => {
      await executor.init();
      mockSession.exec.mockResolvedValueOnce({
        exitCode: 0,
        stdout: "",
        stderr: "",
        status: "FAILED",
      });

      const result = await executor.exec("run");

      expect(result.code).toBe(1);
      expect(result.stderr).toBe("command failed");
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
      mockSession.exec.mockResolvedValue({
        exitCode: 0,
        stdout: "",
        stderr: "",
        status: "SUCCEEDED",
      });

      await executor.writeFile("/tmp/test.txt", "hello");

      // staged into the workdir with a relative path
      const [stagePath, content] = mockSession.writeFile.mock.lastCall as [string, string];
      expect(stagePath).toMatch(/^\.traceroot-stage-/);
      expect(content).toBe("hello");

      // moved with paths in env, not interpolated
      const [, opts] = mockSession.exec.mock.lastCall as ExecCall;
      expect(opts.args.at(-1)).toContain('mv "$SRC" "$DEST"');
      expect(opts.env).toMatchObject({ DEST: "/tmp/test.txt" });
      expect(opts.env?.SRC).toContain(stagePath);
    });

    it("throws when the move fails", async () => {
      await executor.init();
      mockSession.exec.mockResolvedValueOnce({
        exitCode: 1,
        stdout: "",
        stderr: "mv: cannot move",
        status: "FAILED",
      });

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
      mockSession.exec.mockResolvedValue({
        exitCode: 0,
        stdout: "",
        stderr: "",
        status: "SUCCEEDED",
      });
      mockSession.readFile.mockResolvedValueOnce(Buffer.from("file contents"));

      const result = await executor.readFile("/tmp/test.txt");

      expect(result).toBe("file contents");
      const calls = mockSession.exec.mock.calls as ExecCall[];
      const cp = calls.find((c) => c[1].args.at(-1)?.includes('cp "$SRC"'));
      expect(cp?.[1].env).toMatchObject({ SRC: "/tmp/test.txt" });
      const rm = calls.find((c) => c[1].args.at(-1)?.includes('rm -f "$DEST"'));
      expect(rm).toBeTruthy();
    });

    it("throws if not initialized", async () => {
      await expect(executor.readFile("/tmp/x")).rejects.toThrow("not initialized");
    });
  });

  describe("cloneRepo()", () => {
    // Same contract as DaytonaExecutor: system `git` CLI + GIT_ASKPASS, token
    // never in argv / URL / .git/config.

    /** Find the exec call that runs `git ... clone` and return [command, env]. */
    function cloneCall(): [string, Record<string, string>] {
      const call = (mockSession.exec.mock.calls as ExecCall[]).find((c) =>
        c[1].args.at(-1)?.includes("clone"),
      );
      expect(call).toBeTruthy();
      return [call![1].args.at(-1)!, call![1].env ?? {}];
    }

    it("writes an askpass helper and passes the token via env, never in argv", async () => {
      await executor.init();
      vi.clearAllMocks();
      mockSession.exec.mockResolvedValue({
        exitCode: 0,
        stdout: "",
        stderr: "",
        status: "SUCCEEDED",
      });

      await executor.cloneRepo("https://github.com/foo/bar.git", "/repos/bar", {
        ref: "main",
        username: "x-access-token",
        password: "dummy_token",
      });

      // askpass script staged via native writeFile
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
      mockSession.exec.mockResolvedValue({
        exitCode: 0,
        stdout: "",
        stderr: "",
        status: "SUCCEEDED",
      });

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
      // Only the `git clone` exec fails; the askpass staging/mv/chmod succeed.
      mockSession.exec.mockImplementation((_prog: string, opts: { args: string[] }) =>
        Promise.resolve(
          opts.args.at(-1)?.includes("clone")
            ? {
                exitCode: 128,
                stdout: "fatal: could not read Password dummy_token",
                stderr: "",
                status: "FAILED",
              }
            : { exitCode: 0, stdout: "", stderr: "", status: "SUCCEEDED" },
        ),
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

      expect(mockSession.close).toHaveBeenCalled();
      expect(mockClient.close).toHaveBeenCalled();
      expect(executor.isReady()).toBe(false);
    });

    it("no-ops if not initialized", async () => {
      await executor.destroy(); // should not throw
      expect(mockSession.close).not.toHaveBeenCalled();
    });

    it("surfaces a failed terminate and retains the handle for retry", async () => {
      // A failed terminate must NOT read as success with the handle dropped —
      // that leaks the microVM permanently. The handle is kept so a later
      // destroy() can retry.
      await executor.init();
      mockSession.close.mockRejectedValueOnce(new Error("terminate failed"));

      await expect(executor.destroy()).rejects.toThrow(/terminate failed/);
      expect(executor.isReady()).toBe(true); // still tracked

      // A retry now succeeds and clears state.
      mockSession.close.mockResolvedValueOnce(undefined);
      await executor.destroy();
      expect(executor.isReady()).toBe(false);
    });
  });
});
