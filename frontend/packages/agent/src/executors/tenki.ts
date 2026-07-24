import { randomUUID } from "node:crypto";
import { TenkiSandbox, stdoutText, stderrText, isSuccess } from "@tenkicloud/sandbox";
import type { Session } from "@tenkicloud/sandbox";
import type { Executor, ExecResult, ExecOptions } from "./interface.js";

// Lifetime backstops (env-overridable). A finite maxDuration is the last-resort
// guard against a stuck microVM (a recurring review finding); the idle timeout
// pauses a sandbox left unused. NOTE: a single long exec may not refresh the
// idle timer server-side, so the idle default is generous enough to cover a
// long build/test step — bump TENKI_IDLE_TIMEOUT_MINUTES if agents run longer.
const IDLE_TIMEOUT_MINUTES = Number(process.env.TENKI_IDLE_TIMEOUT_MINUTES) || 30;
const MAX_DURATION_MS = Number(process.env.TENKI_MAX_DURATION_MS) || 2 * 60 * 60 * 1000;

export class TenkiExecutor implements Executor {
  private client: TenkiSandbox | null = null;
  private session: Session | null = null;
  private workDir = "";

  async init(): Promise<void> {
    // Idempotent: a second init() while a session is live would orphan the first
    // microVM (a recurring review finding — "repeated start() leaks the previous
    // sandbox"). Callers guard with isReady(), but the guard belongs here too.
    if (this.session) return;

    console.log("[TenkiExecutor] Creating sandbox...");

    // Env-driven auth: TENKI_AUTH_TOKEN, then TENKI_API_KEY (and TENKI_API_ENDPOINT
    // for the base URL). Mirrors DaytonaExecutor's env-key pattern.
    this.client = new TenkiSandbox();

    // Everything after the client exists is failure-atomic: if project
    // resolution, create(), or workspace setup fails, we tear down whatever was
    // allocated rather than leaking a running microVM until its idle timeout.
    // (The sibling Tenki integrations were all flagged for leaking here.)
    try {
      const projectId = await this.resolveProjectId();

      // Default microVM image is Ubuntu 24.04 with git/jq/curl/gh preinstalled, so
      // unlike Daytona there is no runtime apt-get step. Networking defaults to OFF
      // in the SDK — allowOutbound is required for git clone / package installs.
      this.session = await this.client.create({
        name: "traceroot-session",
        projectId,
        cpuCores: 2,
        memoryMb: 4096,
        allowOutbound: true,
        idleTimeoutMinutes: IDLE_TIMEOUT_MINUTES,
        maxDurationMs: MAX_DURATION_MS,
        metadata: { "traceroot.session": "true" },
      });

      // Use /workspace consistently with the Docker/Daytona executors.
      this.workDir = "/workspace";
      const setup = await this.exec("mkdir -p /workspace/repos /workspace/traces /workspace/notes");
      if (setup.code !== 0) {
        throw new Error(`TenkiExecutor: workspace setup failed: ${setup.stderr || setup.stdout}`);
      }

      console.log(`[TenkiExecutor] Sandbox ready (${this.session.id}), workDir: ${this.workDir}`);
    } catch (err) {
      // Best-effort teardown of whatever was allocated, then surface the
      // original failure — a cleanup error must not mask why init() failed.
      await this.destroy().catch((cleanupErr) => {
        console.warn("[TenkiExecutor] cleanup after failed init() also failed", cleanupErr);
      });
      throw err;
    }
  }

  // Tenki requires a project on create even for single-project accounts. Resolve
  // it explicitly (TENKI_PROJECT_ID), else from the account. Auto-resolution only
  // fires when exactly one project is visible: silently picking the first of many
  // is the footgun the sibling PRs were all told to fix, so ambiguity is a hard
  // error pointing at the override. TENKI_WORKSPACE_ID narrows a multi-workspace
  // account first.
  private async resolveProjectId(): Promise<string> {
    const explicit = process.env.TENKI_PROJECT_ID;
    if (explicit) return explicit;

    if (!this.client) throw new Error("Sandbox not initialized");
    const identity = await this.client.whoAmI();

    const workspaceFilter = process.env.TENKI_WORKSPACE_ID;
    const workspaces = (identity.workspaces ?? []).filter(
      (w) => !workspaceFilter || w.id === workspaceFilter,
    );
    const projectIds = workspaces.flatMap((w) => (w.projects ?? []).map((p) => p.id));

    if (projectIds.length === 0) {
      throw new Error("TenkiExecutor: no project visible for this API key (set TENKI_PROJECT_ID)");
    }
    if (projectIds.length > 1) {
      throw new Error(
        `TenkiExecutor: multiple projects visible (${projectIds.slice(0, 8).join(", ")}); ` +
          "set TENKI_PROJECT_ID (or TENKI_WORKSPACE_ID) to disambiguate",
      );
    }
    return projectIds[0];
  }

  async exec(command: string, options?: ExecOptions): Promise<ExecResult> {
    if (!this.session) throw new Error("Sandbox not initialized");

    // Tenki's exec is argv-based and runs as the unprivileged `tenki` user, while
    // the Docker/Daytona executors run commands as root — `sudo -E` restores that
    // parity (and -E keeps the out-of-band env, so secrets still never hit argv).
    // The deadline is enforced in-guest via coreutils `timeout` (exit 124) because
    // the SDK's timeoutMs is not enforced server-side as of v0.4.0; timeoutMs is
    // still passed through as a backstop for when that lands.
    const timeoutSecs = options?.timeout;
    const args = [
      "-E",
      ...(timeoutSecs ? ["timeout", String(timeoutSecs)] : []),
      "bash",
      "-lc",
      command,
    ];

    const result = await this.session.exec("sudo", {
      args,
      env: options?.env,
      timeoutMs: timeoutSecs ? (timeoutSecs + 30) * 1000 : undefined,
      signal: options?.signal,
    });

    const stdout = stdoutText(result);
    let stderr = stderrText(result);
    let code = result.exitCode ?? 1;

    // Trust the command's status, not exitCode alone: a signaled or timed-out
    // command can come back with exitCode 0, and reading that as success is a
    // recurring review finding ("exit_code=0 + SIGKILL reads as success"). When
    // such a failure carries no output at all, fold the status in so the one
    // useful diagnostic isn't lost — but never shadow real output (the clone
    // path merges git's error onto stdout via 2>&1).
    const ok = isSuccess(result.status);
    if (!ok && code === 0) {
      code = result.status === "TIMED_OUT" ? 124 : 1;
    }
    if (code !== 0 && !ok && !stderr && !stdout) {
      stderr = `command ${result.status.toLowerCase()}`;
    }

    return { stdout, stderr, code };
  }

  getWorkspacePath(): string {
    return this.workDir;
  }

  // File ops: the SDK's native writeFile/readFile are confined to the guest
  // workdir (/home/tenki) — absolute paths like /tmp/... or /workspace/... are
  // rejected with "path outside workdir". So stage through the workdir with the
  // native (streaming) transfer and move into place with exec. Paths flow through
  // env as quoted "$VARS", never interpolated into the command string.

  async writeFile(path: string, content: string): Promise<void> {
    if (!this.session) throw new Error("Sandbox not initialized");
    const stage = `.traceroot-stage-${randomUUID()}`;
    await this.session.writeFile(stage, content);
    const result = await this.exec(`mkdir -p "$(dirname "$DEST")" && mv "$SRC" "$DEST"`, {
      env: { SRC: `/home/tenki/${stage}`, DEST: path },
    });
    if (result.code !== 0) {
      throw new Error(`writeFile failed for ${path}: ${result.stderr || result.stdout}`);
    }
  }

  async readFile(path: string): Promise<string> {
    if (!this.session) throw new Error("Sandbox not initialized");
    const stage = `.traceroot-stage-${randomUUID()}`;
    const stagePath = `/home/tenki/${stage}`;
    const result = await this.exec(`cp "$SRC" "$DEST" && chown tenki:tenki "$DEST"`, {
      env: { SRC: path, DEST: stagePath },
    });
    if (result.code !== 0) {
      throw new Error(`readFile failed for ${path}: ${result.stderr || result.stdout}`);
    }
    try {
      const buf = await this.session.readFile(stage);
      return Buffer.from(buf).toString();
    } finally {
      await this.exec(`rm -f "$DEST"`, { env: { DEST: stagePath } });
    }
  }

  isReady(): boolean {
    return this.session !== null;
  }

  async destroy(): Promise<void> {
    // Terminate the session, and only clear the handle once close() actually
    // succeeds. Swallowing a failed terminate AND dropping the handle is a
    // recurring review finding: the microVM keeps running, the failure reads as
    // success, and no later call can retry. So on failure we keep the handle
    // (isReady() stays true) and re-throw; a subsequent destroy() retries.
    if (this.session) {
      console.log("[TenkiExecutor] Destroying sandbox...");
      await this.session.close(); // close() terminates the session; may throw
      this.session = null;
    }
    // Release the client's control-plane channel only once no session remains
    // (a failed init() can leave a client with no session).
    if (this.client) {
      try {
        this.client.close();
      } catch {
        // The channel is best-effort to close; the session is what matters.
      }
      this.client = null;
    }
  }

  // Git Support
  //
  // Same design as DaytonaExecutor: the executor owns cloning (hasNativeGit=true)
  // and uses the system `git` CLI — full LFS/submodule/protocol-v2 behavior for
  // arbitrary customer repos. The token is passed via GIT_ASKPASS + env
  // (out-of-band), so it never lands in argv, the clone URL, or .git/config.

  hasNativeGit(): boolean {
    return true;
  }

  async cloneRepo(
    url: string,
    path: string,
    options?: { ref?: string; username?: string; password?: string },
  ): Promise<void> {
    if (!this.session) throw new Error("Sandbox not initialized");

    const username = options?.username || "x-access-token";
    const password = options?.password ?? "";
    const ref = options?.ref;

    // Askpass helper: git calls this for credential prompts; it echoes the
    // creds we hand it via env. Keeps secrets out of argv and the URL.
    const askpassPath = "/tmp/git-askpass.sh";
    await this.writeFile(
      askpassPath,
      [
        "#!/bin/sh",
        'case "$1" in',
        '  Username*) printf "%s" "$GIT_USERNAME" ;;',
        '  Password*) printf "%s" "$GIT_PASSWORD" ;;',
        "esac",
        "",
      ].join("\n"),
    );
    await this.exec(`chmod +x ${askpassPath}`);

    // URL/ref/path flow through env and are referenced as quoted "$VARS", never
    // interpolated into the command string — so a hostile branch name or ref
    // (e.g. from a trace's git_ref) can't break quoting or inject shell.
    // credential.helper= and core.hooksPath=/dev/null neutralize any inherited
    // credential helper / repo hooks (defense-in-depth).
    const base = `git -c credential.helper= -c core.hooksPath=/dev/null`;
    const isCommitSha = ref !== undefined && /^[0-9a-f]{7,40}$/i.test(ref);

    let inner: string;
    if (!ref) {
      inner = `${base} clone --depth 1 -- "$GIT_URL" "$GIT_DEST"`;
    } else if (isCommitSha) {
      // A SHA isn't a fetchable ref name — full clone, then checkout.
      inner = `${base} clone -- "$GIT_URL" "$GIT_DEST" && ${base} -C "$GIT_DEST" checkout "$GIT_REF"`;
    } else {
      // Branch or tag.
      inner = `${base} clone --depth 1 --branch "$GIT_REF" -- "$GIT_URL" "$GIT_DEST"`;
    }

    // Merge stderr→stdout so git's progress + errors land in one place, matching
    // the output shape of the other executors.
    const result = await this.exec(`( ${inner} ) 2>&1`, {
      timeout: 180,
      env: {
        GIT_ASKPASS: askpassPath,
        GIT_TERMINAL_PROMPT: "0",
        GIT_USERNAME: username,
        GIT_PASSWORD: password,
        GIT_URL: url,
        GIT_DEST: path,
        ...(ref ? { GIT_REF: ref } : {}),
      },
    });

    if (result.code !== 0) {
      const output = result.stderr || result.stdout || "";
      const sanitized = password ? output.replaceAll(password, "[REDACTED]") : output;
      throw new Error(`git clone failed (exit ${result.code}): ${sanitized.trim()}`);
    }
  }
}
