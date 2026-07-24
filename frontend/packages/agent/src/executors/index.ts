import type { Executor } from "./interface.js";
import { DockerExecutor } from "./docker.js";
import { DaytonaExecutor } from "./daytona.js";
import { TenkiExecutor } from "./tenki.js";

export type { Executor, ExecResult, ExecOptions } from "./interface.js";

export function createExecutor(): Executor {
  const provider = process.env.SANDBOX_PROVIDER || "docker";

  switch (provider) {
    case "docker":
      return new DockerExecutor();
    case "daytona":
      return new DaytonaExecutor();
    case "tenki":
      return new TenkiExecutor();
    default:
      throw new Error(`Unknown sandbox provider: ${provider}`);
  }
}
