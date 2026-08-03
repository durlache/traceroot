import { randomUUID } from "node:crypto";
import { TenkiSandbox } from "@tenkicloud/sandbox";
import type { Session, ProcessRunHandle } from "@tenkicloud/sandbox";
import type { Executor, ExecResult, ExecOptions } from "./interface.js";

// Lifetime backstops (env-overridable). A finite maxDuration is the last-resort
// guard against a stuck microVM (a recurring review finding); the idle timeout
// pauses a sandbox left unused. NOTE: a single long exec may not refresh the
// idle timer server-side, so the idle default is generous enough to cover a
// long build/test step — bump TENKI_IDLE_TIMEOUT_MINUTES if agents run longer.
const IDLE_TIMEOUT_MINUTES = Number(process.env.TENKI_IDLE_TIMEOUT_MINUTES) || 30;
const MAX_DURATION_MS = Number(process.env.TENKI_MAX_DURATION_MS) || 2 * 60 * 60 * 1000;

// Every exec is bounded in-guest by coreutils `timeout` (the SDK enforces neither
// timeoutMs nor the abort signal — still true in 0.5.1, whose exec() passes
// neither option through to run()). This ceiling applies when a caller supplies
// no timeout, so nothing runs unbounded.
const DEFAULT_EXEC_TIMEOUT_SECS = Number(process.env.TENKI_EXEC_TIMEOUT_SECS) || 900;

// Host-side deadline = the in-guest timeout plus this grace. The in-guest
// `timeout` handles the normal case; this backstop unblocks callers if the guest
// itself goes unresponsive (paused by idle timeout, torn down mid-exec) so
// `await proc` would otherwise never settle.
const HOST_TIMEOUT_GRACE_SECS = 30;

// Cap retained stdout/stderr per stream (parity with DockerExecutor). We keep
// draining past this so the guest process never blocks on a full pipe, but a
// noisy command can't grow the agent's memory without bound.
const MAX_OUTPUT_BYTES = 10 * 1024 * 1024; // 10MB

// Root-side recursive tree-kill (post-order: children before parent, so nothing
// reparents mid-walk). proc.kill() only reaps the direct child, leaving the
// sudo'd timeout→bash→command tree orphaned but alive (verified live); this
// runs as root because those processes are root-owned. $TARGET_PID is passed
// out-of-band via env, never interpolated.
const TREE_KILL_SCRIPT =
  'kt(){ for c in $(pgrep -P "$1" 2>/dev/null); do kt "$c"; done; kill -KILL "$1" 2>/dev/null; }; kt "$TARGET_PID"';

/**
 * Drain a byte stream to completion (so the guest process never blocks on a full
 * pipe) while retaining at most `capBytes`.
 */
async function readAll(stream: ReadableStream<Uint8Array>, capBytes: number): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let retained = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value && retained < capBytes) {
        const slice =
          retained + value.length <= capBytes ? value : value.subarray(0, capBytes - retained);
        chunks.push(slice);
        retained += slice.length;
      }
      // beyond the cap: keep reading to drain, but discard
    }
  } finally {
    reader.releaseLock();
  }
  const out = new Uint8Array(retained);
  let offset = 0;
  for (const c of chunks) {
    out.set(c, offset);
    offset += c.length;
  }
  return out;
}

const asAbortError = (reason: unknown): Error =>
  reason instanceof Error ? reason : new Error("exec aborted");

export class TenkiExecutor implements Executor {
  private client: TenkiSandbox | null = null;
  private session: Session | null = null;
  private workDir = "";
  private initPromise: Promise<void> | null = null;
  // Sessions whose close failed during init cleanup. The handles are retained —
  // dropping them would strand a live VM until the maxDuration backstop while a
  // retry creates a second one — and every later init()/destroy() retries them.
  private orphanedSessions: Session[] = [];

  async init(): Promise<void> {
    // Single-flight: a plain `if (this.session) return` guard is not safe under
    // concurrent first use — two callers can both pass it during the awaits
    // before `this.session` is assigned, each create a microVM, and destroy()
    // can then only reach one (reproduced live in review). Share one in-flight
    // promise so concurrent callers join the same initialization; already-ready
    // callers short-circuit. On failure the promise is cleared so a later init()
    // retries; on success this guard short-circuits.
    //
    // The short-circuit must be isReady() — full readiness — not `this.session`:
    // mid-init the session exists before workspace setup finishes, and a caller
    // returned early then would hit exec()'s readiness gate and fail spuriously
    // (caught by the concurrent-first-use stress test). Not-yet-ready callers
    // fall through and join the in-flight promise below.
    if (this.isReady()) return;
    if (!this.initPromise) {
      this.initPromise = this.doInit().finally(() => {
        this.initPromise = null;
      });
    }
    return this.initPromise;
  }

  private async doInit(): Promise<void> {
    console.log("[TenkiExecutor] Creating sandbox...");

    // Retry closing any session a previous failed init could not tear down,
    // before allocating another VM on top of it.
    await this.closeOrphans();

    // Env-driven auth: TENKI_AUTH_TOKEN, then TENKI_API_KEY (and TENKI_API_ENDPOINT
    // for the base URL). Mirrors DaytonaExecutor's env-key pattern.
    this.client = new TenkiSandbox();

    // Everything after the client exists is failure-atomic: if workspace
    // resolution, create(), or workspace setup fails, we tear down whatever was
    // allocated rather than leaking a running microVM until its idle timeout.
    // (The sibling Tenki integrations were all flagged for leaking here.)
    try {
      const workspaceId = await this.resolveWorkspaceId();

      // Default microVM image is Ubuntu 24.04 with git/jq/curl/gh preinstalled, so
      // unlike Daytona there is no runtime apt-get step. Networking defaults to OFF
      // in the SDK — allowOutbound is required for git clone / package installs.
      this.session = await this.client.create({
        name: "traceroot-session",
        workspaceId,
        cpuCores: 2,
        memoryMb: 4096,
        allowOutbound: true,
        idleTimeoutMinutes: IDLE_TIMEOUT_MINUTES,
        maxDurationMs: MAX_DURATION_MS,
        metadata: { "traceroot.session": "true" },
      });

      // Create the /workspace layout BEFORE adopting it as the working dir: the
      // bootstrap must run in the guest default (/home/tenki) because exec() sets
      // cwd to this.workDir and /workspace doesn't exist yet. Only once it exists
      // do we switch, so every later command runs in /workspace — matching the
      // Docker/Daytona executors and the bash tool's advertised working dir.
      // (execRaw, not exec: the public exec() refuses to run until /workspace
      // is adopted, exactly so nothing else can slip in during this window.)
      const setup = await this.execRaw(
        "mkdir -p /workspace/repos /workspace/traces /workspace/notes",
      );
      if (setup.code !== 0) {
        throw new Error(`TenkiExecutor: workspace setup failed: ${setup.stderr || setup.stdout}`);
      }
      this.workDir = "/workspace";

      console.log(`[TenkiExecutor] Sandbox ready (${this.session.id}), workDir: ${this.workDir}`);
    } catch (err) {
      // Force a full reset on failure so a retry re-inits cleanly. Unlike
      // destroy() (which retains a live handle for retry), a half-initialized
      // session must NOT survive: leaving this.session set would make the init
      // guard report a broken sandbox as ready.
      await this.resetAfterFailedInit();
      throw err;
    }
  }

  // Teardown that ALWAYS clears the ready-state fields, used only on the init
  // failure path: no half-initialized session may survive to satisfy the
  // init/isReady guard. If the close itself fails, the handle is NOT dropped —
  // it moves to orphanedSessions so the next init()/destroy() can retry the
  // close instead of stranding a live VM until the maxDuration backstop while
  // a retry boots a second one.
  private async resetAfterFailedInit(): Promise<void> {
    if (this.session) {
      try {
        await this.session.closeIfOpen();
      } catch (err) {
        this.orphanedSessions.push(this.session);
        console.warn(
          "[TenkiExecutor] cleanup after failed init() failed; close will be retried on next init()/destroy()",
          err,
        );
      }
      this.session = null;
    }
    if (this.client) {
      try {
        // Safe while orphans remain: each Session carries its own RPC client,
        // so closing this control-plane handle doesn't sever theirs.
        this.client.close();
      } catch {
        // best-effort
      }
      this.client = null;
    }
    this.workDir = "";
  }

  // Retry closing sessions orphaned by a failed init cleanup. Never throws:
  // callers (init retry, destroy) must proceed regardless; sessions that still
  // fail stay queued for the next attempt and are bounded by the maxDuration
  // backstop in the worst case.
  private async closeOrphans(): Promise<void> {
    if (this.orphanedSessions.length === 0) return;
    const stillOrphaned: Session[] = [];
    for (const session of this.orphanedSessions) {
      try {
        await session.closeIfOpen();
      } catch {
        stillOrphaned.push(session);
      }
    }
    this.orphanedSessions = stillOrphaned;
    if (stillOrphaned.length > 0) {
      console.warn(
        `[TenkiExecutor] ${stillOrphaned.length} orphaned sandbox(es) still failing to close; ` +
          "will retry, VM(s) linger until backstop otherwise",
      );
    }
  }

  // Sandboxes are workspace-scoped (SDK 0.5.x removed the project concept).
  // Resolve the workspace explicitly (TENKI_WORKSPACE_ID), else from the
  // account. Auto-resolution only fires when exactly one workspace is visible:
  // silently picking the first of many is the footgun the sibling PRs were all
  // told to fix, so ambiguity is a hard error pointing at the override.
  private async resolveWorkspaceId(): Promise<string> {
    if (process.env.TENKI_PROJECT_ID) {
      throw new Error(
        "TenkiExecutor: TENKI_PROJECT_ID is no longer supported (Tenki SDK 0.5.x removed " +
          "projects); set TENKI_WORKSPACE_ID instead",
      );
    }
    const explicit = process.env.TENKI_WORKSPACE_ID;
    if (explicit) return explicit;

    if (!this.client) throw new Error("Sandbox not initialized");
    const identity = await this.client.whoAmI();
    const workspaces = identity.workspaces ?? [];

    if (workspaces.length === 0) {
      throw new Error(
        "TenkiExecutor: no workspace visible for this API key (set TENKI_WORKSPACE_ID)",
      );
    }
    if (workspaces.length > 1) {
      throw new Error(
        `TenkiExecutor: multiple workspaces visible (${workspaces
          .slice(0, 8)
          .map((w) => w.id)
          .join(", ")}); set TENKI_WORKSPACE_ID to disambiguate`,
      );
    }
    return workspaces[0].id;
  }

  async exec(command: string, options?: ExecOptions): Promise<ExecResult> {
    // Require BOTH fields: `session` alone is set mid-init, before /workspace
    // exists, and a command admitted then would run in /home/tenki instead of
    // the advertised working dir (reproduced in review under concurrent first
    // use). isReady() applies the same test, so callers doing
    // `isReady() || init()` land here only once setup has finished.
    if (!this.session || !this.workDir) throw new Error("Sandbox not initialized");
    return this.execRaw(command, options);
  }

  // exec() without the readiness gate — the init bootstrap must run before
  // /workspace exists (in the guest default cwd). Everything else goes through
  // exec().
  private async execRaw(command: string, options?: ExecOptions): Promise<ExecResult> {
    if (!this.session) throw new Error("Sandbox not initialized");
    const signal = options?.signal;
    if (signal?.aborted) throw asAbortError(signal.reason);

    // Run via the low-level handle (not session.exec) so we can actually cancel:
    // the SDK reads neither ExecOptions.signal nor timeoutMs (verified against
    // 0.5.1 — exec() forwards neither to run()), so this method bounds every
    // command in-guest with coreutils `timeout` (exit 124) and enforces the
    // abort signal itself by killing the guest process.
    //
    // Commands run as root via `sudo -E`: the Docker/Daytona executors run as
    // root, and Tenki's guest user is unprivileged `tenki`; -E keeps the
    // out-of-band env so secrets never hit argv.
    // Treat a non-positive timeout as "unset" and fall back to the default —
    // `?? ` would pass 0 through, and coreutils `timeout 0` means NO limit,
    // silently reintroducing the unbounded case (and differing from Docker).
    const requested = options?.timeout;
    const timeoutSecs = requested && requested > 0 ? requested : DEFAULT_EXEC_TIMEOUT_SECS;

    // Run in the workspace dir so relative-path commands behave as the bash tool
    // advertises ("Working directory is /workspace"). The SDK's run() `cwd`
    // option is NOT honored through our `sudo -E … bash -lc` wrapper (sudo + the
    // login shell land in /home/tenki — verified live), so cd explicitly.
    // this.workDir is a fixed literal (never user input); it's empty only during
    // the init bootstrap, before /workspace exists, when the guest default is
    // correct.
    const scoped = this.workDir ? `cd ${this.workDir} && ${command}` : command;
    const argv = ["sudo", "-E", "timeout", String(timeoutSecs), "bash", "-lc", scoped];

    const proc = this.session.run(argv, { env: options?.env });
    // This executor never sends stdin; close it so commands that read stdin
    // (e.g. `cat`) see EOF immediately instead of blocking until the timeout.
    void proc.stdin.close().catch(() => {});

    let onAbort: (() => void) | undefined;
    const aborted = signal
      ? new Promise<never>((_, reject) => {
          onAbort = () => {
            // Real cancellation: reap the whole guest process tree (proc.kill()
            // alone orphans the sudo'd children — verified live), and only reject
            // once that's done so the caller doesn't unblock while work continues.
            // If the reaper stalls, the host deadline below still rejects the race.
            void this.reapTree(proc).finally(() => reject(asAbortError(signal.reason)));
          };
          signal.addEventListener("abort", onAbort, { once: true });
        })
      : null;

    // Host-side backstop: the in-guest `timeout` needs a live guest to fire, so
    // guard against an unresponsive one (paused/torn down) that would leave
    // `await proc` hanging forever — especially for callers with no AbortSignal.
    let hostTimer: ReturnType<typeof setTimeout> | undefined;
    const hostDeadline = new Promise<never>((_, reject) => {
      hostTimer = setTimeout(
        () => {
          void proc.kill().catch(() => {});
          reject(
            new Error(`exec exceeded host deadline (guest unresponsive after ${timeoutSecs}s)`),
          );
        },
        (timeoutSecs + HOST_TIMEOUT_GRACE_SECS) * 1000,
      );
    });

    const collect = (async () => {
      const [stdoutBuf, stderrBuf] = await Promise.all([
        readAll(proc.stdout, MAX_OUTPUT_BYTES),
        readAll(proc.stderr, MAX_OUTPUT_BYTES),
      ]);
      const result = await proc;
      return { stdoutBuf, stderrBuf, result };
    })();

    // Reaping the tree on abort closes the streams, so `collect` resolves with
    // the killed command's result. That must NOT win the race — an aborted call
    // rejects, it doesn't return a partial result. When the signal is set, hand
    // the race a never-settling stand-in so `aborted` (which rejects only after
    // reapTree finishes) is the outcome.
    const guardedCollect = collect.then((r) =>
      signal?.aborted ? new Promise<typeof r>(() => {}) : r,
    );

    try {
      const { stdoutBuf, stderrBuf, result } = await Promise.race(
        aborted ? [guardedCollect, hostDeadline, aborted] : [guardedCollect, hostDeadline],
      );

      // Decode the raw bytes WITHOUT trimming. The SDK's stdoutText/stderrText
      // helpers `.trim()`, which would drop leading indentation and trailing
      // whitespace the `read` tool depends on.
      const stdout = new TextDecoder().decode(stdoutBuf);
      let stderr = new TextDecoder().decode(stderrBuf);
      let code = result.exitCode ?? 1;

      // Trust the outcome, not exitCode alone: a signaled command can report
      // exitCode 0 ("exit_code=0 + SIGKILL reads as success" was a review
      // finding). Surface a timeout clearly (coreutils `timeout` exits 124), and
      // otherwise fold the guest reason into empty output — but never shadow real
      // output (the clone path merges git's error onto stdout via 2>&1).
      const ok = code === 0 && !result.signal;
      if (!ok && code === 0) code = 1;
      if (code === 124) {
        const note = `command timed out after ${timeoutSecs}s`;
        stderr = stderr ? `${stderr}\n${note}` : note;
      } else if (code !== 0 && !stderr && !stdout) {
        stderr = result.reason ? `command ${result.reason}` : "command failed";
      }

      return { stdout, stderr, code };
    } finally {
      if (hostTimer) clearTimeout(hostTimer);
      if (signal && onAbort) signal.removeEventListener("abort", onAbort);
    }
  }

  // Terminate the whole guest process tree spawned for `proc`, as root, before it
  // can orphan — proc.kill() alone reaps only the direct child, leaving the
  // sudo'd timeout→bash→command subtree running (verified live).
  private async reapTree(proc: ProcessRunHandle): Promise<void> {
    try {
      if (!this.session) return;
      const pid = await proc.pid;
      const killer = this.session.run(["sudo", "-E", "bash", "-c", TREE_KILL_SCRIPT], {
        env: { TARGET_PID: String(pid) },
      });
      void killer.stdin.close().catch(() => {});
      await killer;
    } catch {
      // best-effort — the caller unblocks regardless, and the host deadline backstops
    } finally {
      await proc.kill().catch(() => {});
    }
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
    // Not just `session !== null`: the session handle exists before workspace
    // setup finishes, and reporting ready then lets a concurrent caller skip
    // init() and exec() in the wrong working directory (review blocker).
    // workDir is assigned last in doInit, so both set ⇒ fully initialized.
    return this.session !== null && this.workDir !== "";
  }

  async destroy(): Promise<void> {
    // If an init() is still in flight, let it settle first: otherwise destroy()
    // sees no session, no-ops, and the microVM that create() returns moments
    // later is assigned to a handle nobody holds — a leak. Awaiting adopts that
    // session so we actually tear it down (or it already cleaned up on failure).
    if (this.initPromise) {
      await this.initPromise.catch(() => {});
    }

    // Retry any sessions a failed init could not close (never throws — a still-
    // failing orphan must not block tearing down the live session below).
    await this.closeOrphans();

    // Terminate the session, clearing the handle only once teardown succeeds.
    // Swallowing a failed terminate AND dropping the handle is a review finding
    // (the VM keeps running, the failure reads as success, nothing can retry) —
    // so on a transient failure we keep the handle and re-throw for retry. But
    // use closeIfOpen(): an already-terminated sandbox (hit its max-duration,
    // GC'd server-side) is the desired end state, not a retryable error, so it
    // must not throw and wedge the session as permanently undeletable.
    if (this.session) {
      console.log("[TenkiExecutor] Destroying sandbox...");
      await this.session.closeIfOpen(); // no-ops if already gone; throws only on transient failure
      this.session = null;
      // Clear workDir with the session (ready-state fields live and die
      // together): left stale, a re-init on this executor would prefix the new
      // VM's bootstrap with `cd /workspace` before that directory exists and
      // wedge setup. Cleared only on successful teardown — a failed destroy
      // retains the handle AND ready state for retry.
      this.workDir = "";
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
