import {
  watchdogMsgRawPreParsedSchema,
  type Logger,
  type WatchdogMsgRaw,
} from "./models.ts";

import { type JobMsg, type JobMsgRaw, type Orchestrator } from "./models.ts";
import { filter, takeWhile } from "./pipeline.ts";
import { JobSQS, WatchdogQueue } from "./queues.ts";

export { amain, type JobMsgRaw };

// This should really be typed with Promise<never> but ts doesn't like that.  In our hearts.
async function amain(
  o: Orchestrator,
  jobsQueue: JobSQS,
  watchdogQueue: WatchdogQueue,
  maxWatchdogAgeSecs: number, // default max age if not passed explicitly with a job
  logger: Logger,
) {
  logger = logger.child({
    jobsQueueUrl: jobsQueue.sqsUrl,
    watchdogQueueUrl: watchdogQueue.sqsUrl,
    maxWatchdogAgeSecs,
    jobVisibilityTimeoutSecs: jobsQueue.jobVisibilityTimeoutSecs,
    watchdogIntervalSecs: watchdogQueue.watchdogIntervalSecs,
  });
  logger.info(`starting agent. listening on jobQueue= ${jobsQueue.sqsUrl}`);

  // Effectively a select statement on the job queue and watchdog queue.  We
  // long poll here for 1 message each, first to get a message will start being
  // handled.  Note that we also handle all watchdog messages after we handle 1
  // job in the job loop to prioritize the watchdog messages
  await Promise.all([
    watchdogLoopForever(o, jobsQueue, watchdogQueue, logger),
    handle1JobForever(o, jobsQueue, watchdogQueue, maxWatchdogAgeSecs, logger),
  ]);
}

/**
 * Keep exhausting the watchdog queue with a long poll on the watchdog queue,
 * deleting old messages.
 */
async function watchdogLoopForever(
  o: Orchestrator,
  jobsQueue: JobSQS,
  watchdogQueue: WatchdogQueue,
  logger: Logger,
): Promise<never> {
  for (;;) {
    await watchdogLoop(o, jobsQueue, watchdogQueue, logger, true);
  }
}

/**
 * Handle one job from the job queue if exists, then exhaust the entire
 * watchdog queue.
 */
async function handle1JobForever(
  o: Orchestrator,
  jobsQueue: JobSQS,
  watchdogQueue: WatchdogQueue,
  maxWatchdogAgeSecs: number,
  logger: Logger,
): Promise<never> {
  for (;;) {
    const job: JobMsg | null = await jobsQueue.receive();
    if (!job) {
      continue;
    }

    const jobLogger = logger.child({ job });
    jobLogger.debug(`found job in job queue!`);

    const jobReceipt = await o.schedule(job.msg);
    jobLogger
      .child({ jobReceipt })
      .info(`scheduled job to orchestrator. jobReceipt= ${jobReceipt}`);

    const watchdogMsg: WatchdogMsgRaw = watchdogMsgRawPreParsedSchema.parse({
      job_msg_handle: job.msg_handle,
      max_age_secs: job.max_age_secs ?? maxWatchdogAgeSecs,
      job_receipt: jobReceipt,
    } satisfies WatchdogMsgRaw);
    jobLogger
      .child({ watchdogMsg })
      .debug(`enqueuing message on watchdog queue. ${jobReceipt}`);
    await watchdogQueue.send(watchdogMsg);

    // For every 1 message received, we exhaust the watchdog queue.  This
    // functions as a sort of rate limiter -- we cannot start new jobs when we
    // have watchdog messages to deal with.  But we do a short non blocking poll
    // here.
    await watchdogLoop(o, jobsQueue, watchdogQueue, logger, false);
  }
}

/**
 * Exhaust the watchdog queue.
 *
 * The forever boolean indicates whether this should be a single drain (false), or a forever
 * blocking read loop (true).
 */
async function watchdogLoop(
  o: Orchestrator,
  jobsQueue: JobSQS,
  watchdogQueue: WatchdogQueue,
  logger: Logger,
  forever: boolean,
): Promise<void> {
  const source = watchdogQueue.receive(forever);
  const msgs = forever
    ? filter(source, (msg) => msg !== null)
    : takeWhile(source, (msg) => msg !== null);
  for await (const watchdogMsg of msgs) {
    const l = logger.child({ watchdogMsg });

    // for both drain and read loop
    if (await watchdogQueue.deleteIfStale(watchdogMsg)) continue;

    l.info(
      `watchdog queue has received a message jobReceipt= ${watchdogMsg.job_receipt} . Reading status from Orchestrator.`,
    );
    if (await o.read(watchdogMsg.job_receipt)) {
      l.debug(
        `job with orchestrator jobReceipt= ${watchdogMsg.job_receipt} complete, deleting from both queues`,
      );
      await Promise.all([
        jobsQueue.delete(watchdogMsg.job_msg_handle),
        watchdogQueue.delete(watchdogMsg.watchdog_msg_handle),
      ]);
    } else {
      l.debug(
        `job with orchestrator jobReceipt: ${watchdogMsg.job_receipt} not complete, punting in job queue for ${jobsQueue.jobVisibilityTimeoutSecs} seconds`,
      );
      await jobsQueue.punt(watchdogMsg.job_msg_handle);
    }
  }
}
