#!/usr/bin/env node --enable-source-maps

import { SQSClient } from "@aws-sdk/client-sqs";
import { amain } from "../src/app.ts";
import type { Logger, Orchestrator } from "../src/models.ts";
import { JobSQS, WatchdogQueue } from "../src/queues.ts";
import z from "zod";

type HasStatus = { status: number };
class WebOrchestrator implements Orchestrator {
  readonly domain: string;
  constructor(domain: string) {
    this.domain = domain;
  }
  public async schedule(msg: string): Promise<string> {
    await fetch(`${this.domain}/${msg}`, { method: "PUT" });
    return msg;
  }

  public async read(jobReceipt: string): Promise<boolean> {
    const res = await fetch(`${this.domain}/${jobReceipt}`, { method: "GET" });
    return ((await res.json()) as HasStatus).status === 2;
  }
}

class ConsoleLogger implements Logger {
  readonly context: Record<string, any>;
  constructor(ctx: Record<string, any>) {
    this.context = ctx;
  }
  public child(ctx: Record<string, any>) {
    return new ConsoleLogger({ ...ctx, ...this.context });
  }
  trace(message: string): void {
    console.log("TRACE " + message, { ...this.context });
  }
  debug(message: string): void {
    console.log("DEBUG " + message, { ...this.context });
  }
  info(message: string): void {
    console.log("INFO " + message, { ...this.context });
  }
  warn(message: string): void {
    console.log("WARN " + message, { ...this.context });
  }
  error(err: Error | unknown | string): void {
    console.log("ERROR ", err, { ...this.context });
  }
}

const envsSchema = z.object({
  orchestratorDomain: z.string(),
  jobUrl: z.string(),
  watchdogUrl: z.string(),
  jobVisibilityTimeoutSecs: z.coerce.number(),
  watchdogIntervalSecs: z.coerce.number(),
  maxWatchdogAgeSecs: z.coerce.number(),
}) satisfies z.ZodType<Envs>;

type Envs = {
  orchestratorDomain: string;
  jobUrl: string;
  watchdogUrl: string;
  jobVisibilityTimeoutSecs: number;
  watchdogIntervalSecs: number;
  maxWatchdogAgeSecs: number;
};

const main = async () => {
  const envs: Envs = envsSchema.parse(process.env);
  const l = new ConsoleLogger({}).child(envs);
  l.info("loaded environment variables");

  const o = new WebOrchestrator(envs.orchestratorDomain);

  const sqsc = new SQSClient();
  const job = new JobSQS(sqsc, envs.jobUrl, envs.jobVisibilityTimeoutSecs, l);
  const watchdawg = new WatchdogQueue(
    sqsc,
    envs.watchdogUrl,
    envs.watchdogIntervalSecs,
    l,
  );

  await amain(o, job, watchdawg, envs.maxWatchdogAgeSecs, l);
};
await main();
