import {
  watchdogMsgRawPreParsedSchema,
  type Logger,
  type WatchdogMsgRaw,
} from "./models.ts";

import {
  type JobMsg,
  type JobMsgRaw,
  type Orchestrator,
  type WatchdogMsg,
} from "./models.ts";
import { JobSQS, SQSBouncer } from "./queues.ts";

export { amain, type JobMsgRaw };

// this should really be typed with Promise<never> but ts doesn't like that. in our hearts.
async function amain(
  o: Orchestrator,
  jobsQueue: JobSQS,
  watchdogQueue: SQSBouncer,
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

  // effectively a select statement on the job queue and watchdog queue
  // we long poll here for 1 message each, first to get a message will start being handled.
  // note that we also handle all watchdog messages after we handle 1 job in the job loop
  // to prioritize the watchdog messages
  await Promise.all([
    watchdogLoopForever(o, jobsQueue, watchdogQueue, logger),
    handle1JobForever(o, jobsQueue, watchdogQueue, maxWatchdogAgeSecs, logger),
  ]);
}

/** keep exhausting the watchdog queue with a long poll on the watchdog queue, deleting old messages */
async function watchdogLoopForever(
  o: Orchestrator,
  jobsQueue: JobSQS,
  watchdogQueue: SQSBouncer,
  logger: Logger,
): Promise<never> {
  for (;;) {
    await watchdogLoop(o, jobsQueue, watchdogQueue, logger);
  }
}

/**  handle one job from the job queue if exists,
 * then exhaust the entire watchdog queue
 */
async function handle1JobForever(
  o: Orchestrator,
  jobsQueue: JobSQS,
  watchdogQueue: SQSBouncer,
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
    jobLogger.info(`scheduled job to orchestrator. jobReceipt= ${jobReceipt}`, {
      jobReceipt,
    });

    const watchdogMsg: WatchdogMsgRaw = watchdogMsgRawPreParsedSchema.parse({
      job_msg_handle: job.msg_handle,
      max_age_secs: job.max_age_secs ?? maxWatchdogAgeSecs,
      job_receipt: jobReceipt,
    } satisfies WatchdogMsgRaw);
    jobLogger.debug(`enqueuing message on watchdog queue. ${jobReceipt}`, {
      watchdogMsg,
    });
    await watchdogQueue.send(watchdogMsg);

    // for every 1 message received, we exhaust the watchdog queue.
    // this functions as a sort of rate limiter.
    // can't start new jobs when we have watchdog msgs to deal with.
    // but we do a short non blocking poll here.
    await watchdogLoop(o, jobsQueue, watchdogQueue, logger, false);
  }
}

/**  exhaust the watchdog queue */
async function watchdogLoop(
  o: Orchestrator,
  jobsQueue: JobSQS,
  watchdogQueue: SQSBouncer,
  logger: Logger,
  longPoll = true,
): Promise<void> {
  for (
    let watchdogMsg: WatchdogMsg | null = await watchdogQueue.receive(longPoll);
    watchdogMsg;
    watchdogMsg = await watchdogQueue.receive(longPoll)
  ) {
    logger = logger.child({ watchdogMsg });
    logger.info(
      `watchdog queue has received a message jobReceipt= ${watchdogMsg.job_receipt} . Reading status from Orchestrator.`,
    );
    if (await o.read(watchdogMsg.job_receipt)) {
      logger.debug(
        `job with orchestrator jobReceipt= ${watchdogMsg.job_receipt} complete, deleting from both queues`,
      );
      await Promise.all([
        jobsQueue.delete(watchdogMsg.job_msg_handle),
        watchdogQueue.delete(watchdogMsg.watchdog_msg_handle),
      ]);
    } else {
      logger.debug(
        `job with orchestrator jobReceipt: ${watchdogMsg.job_receipt} not complete, punting in job queue for ${jobsQueue.jobVisibilityTimeoutSecs.toString()} seconds`,
      );
      await jobsQueue.punt(watchdogMsg.job_msg_handle);
    }
  }
}
