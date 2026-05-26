#!/usr/bin/env node --enable-source-maps

// Copyright © 2026 Anterior <tech@anterior.com>
// SPDX-License-Identifier: AGPL-3.0-only

import assert from "node:assert/strict";
import { setTimeout } from "timers/promises";
import { test } from "node:test";
import bencodelib from "bencode";
import { TextDecoder } from "node:util";
import z from "zod";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";

const envsSchema = z.object({
  orchestratorDomain: z.string(),
  jobUrl: z.string(),
});

const _ = test("test happy path", { timeout: 120e3 }, async () => {
  const slug = `${Math.random().toString()}x`;
  const getId = (idx: number): string => `${slug}${idx.toString()}`;
  const JOBS = 10;
  const envs = envsSchema.parse(process.env);

  const td = new TextDecoder();

  const makeMsg = (jobId: string): string => {
    return td.decode(bencodelib.encode({ msg: jobId }));
  };

  type HasStatus = { status: number };
  const checkJob = async (jobId: string): Promise<number> => {
    const res = await fetch(`${envs.orchestratorDomain}/${jobId}`, {
      method: "GET",
    });
    return ((await res.json()) as HasStatus).status;
  };
  const setJobComplete = async (jobId: string) => {
    await fetch(`${envs.orchestratorDomain}/${jobId}`, {
      method: "POST",
    });
  };

  const sqsc = new SQSClient();

  // schedule all the jobs
  const sent = new Map<string, string>(); // this should be a set
  for (let i = 0; i < JOBS; i++) {
    await sqsc.send(
      new SendMessageCommand({
        QueueUrl: envs.jobUrl,
        MessageBody: makeMsg(getId(i)),
      }),
    );
    sent.set(getId(i), getId(i));
  }
  // check jobs are scheduled
  const progress = new Map<string, string>(); // should be a set
  while (sent.size > 0) {
    for (const [k, v] of sent) {
      if ((await checkJob(v)) === 1) {
        sent.delete(k);
        progress.set(k, v);
      }
    }
    await setTimeout(10);
  }

  // set jobs to complete
  for (let i = 0; i < JOBS; i++) {
    await setJobComplete(getId(i));
    sent.set(getId(i), getId(i));
  }
  // check jobs completed
  while (progress.size > 0) {
    for (const [k, v] of progress) {
      if ((await checkJob(v)) === 2) {
        progress.delete(k);
      }
    }
    await setTimeout(10);
  }

  // TODO: ensure job and watchdog queue are empty

  assert(true);
  console.log("DONE TEST!");
});
