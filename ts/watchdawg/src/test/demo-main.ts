#!/usr/bin/env node --enable-source-maps

// Copyright © 2026 Anterior <tech@anterior.com>
// SPDX-License-Identifier: AGPL-3.0-only

import { SQSClient } from "@aws-sdk/client-sqs";
import { amain } from "../app.ts";
import type {
  Logger,
  Orchestrator,
  WatchdogAction,
  WatchdogMsg,
} from "../models.ts";
import { JobSQS, WatchdogQueue } from "../queues.ts";
import z from "zod";

type HasStatus = { status: number };
class WebOrchestrator implements Orchestrator {
  readonly domain: string;
  readonly logger: Logger;
  constructor(domain: string) {
    this.domain = domain;
    this.logger = new ConsoleLogger({});
  }
  public async schedule(msg: string): Promise<string> {
    await fetch(`${this.domain}/${msg}`, { method: "PUT" });
    return msg;
  }

  public async read(msg: WatchdogMsg): Promise<{ action: WatchdogAction }> {
    const res = await fetch(`${this.domain}/${msg.job_receipt}`, {
      method: "GET",
    });
    if (((await res.json()) as HasStatus).status === 2) {
      return { action: "DONE" };
    }

    const expiry =
      +(msg.watchdog_msg_attributes?.SentTimestamp || "0") / 1e3 +
      msg.max_age_secs;
    const now = Date.now() / 1e3;
    this.logger
      .child({ expiry, now, watchdogMsg: msg })
      .debug(`watchdog message expiry= ${expiry} time now= ${now}`);
    if (expiry < now) return { action: "STALE" }; // causes a toplevel retry

    // There isn't actually a concept in the dummy orchestrator if something
    // is still working, so we just say it's working for the maximum time.
    // In a real system you would have something like a heartbeat
    // sidechannel to communicate in the distributed system if a worker is
    // still working or it died.
    // TODO: implement and test some concept of "working" vs "dead" in the
    // dummy orchestrator.
    // BenZ 202605
    return { action: "WORKING" };
  }

  // TODO: add test that uses approx receive count on the job to e.g.
  // optionally exclude some jobs from the dlq.
  // BenZ 202605
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
