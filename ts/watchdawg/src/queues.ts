import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SQSClient,
  SendMessageCommand,
} from "@aws-sdk/client-sqs";

import {
  bdecodeFromString,
  bencodeAsString,
  jobMsgSchema,
  watchdogMsgSchema,
  type Logger,
} from "./models.ts";

import {
  jobMsgRawSchema,
  watchdogMsgRawSchema,
  type JobMsg,
  type WatchdogMsg,
  type WatchdogMsgRaw,
} from "./models.ts";

export { JobSQS, WatchdogQueue };

/**
 * WatchdogQueue reads messages from SQS, with options for long or short
 * polling, and returns those messages in an AsyncGenerator.  This is for the
 * watchdog queue.  Messages are put here when a job is scheduled with the
 * orchestrator so we can check the return status and reschedule jobs if not
 * completed. It doesn't manually punt, but just allows messages to become
 * visible again after e.g. 1m.  When a message is deemed too old to be waited
 * on, the consumer can delete the message from this queue which will have us
 * stop punting the job queue message, and therefore the job will become
 * visible again in the job queue and the main loop can schedule it again.
 */
class WatchdogQueue {
  readonly sqsc: SQSClient;
  readonly sqsUrl: string;
  readonly watchdogIntervalSecs: number;
  readonly logger: Logger;
  constructor(
    sqsc: SQSClient,
    sqsUrl: string,
    watchdogIntervalSecs: number,
    logger: Logger,
  ) {
    this.sqsc = sqsc;
    this.sqsUrl = sqsUrl;
    this.watchdogIntervalSecs = watchdogIntervalSecs;
    this.logger = logger.child({ sqsUrl, watchdogIntervalSecs });
  }

  async send(job: WatchdogMsgRaw): Promise<void> {
    const msg = bencodeAsString(job);
    await this.sqsc.send(
      new SendMessageCommand({ QueueUrl: this.sqsUrl, MessageBody: msg }),
    );
  }

  /**
   * `receive` which is an async generator of WatchdogMsg with enough information to delete/punt or
   * determine if the message is "stale".
   */
  async *receive(longPoll: boolean): AsyncGenerator<WatchdogMsg | null> {
    for (;;) {
      const res = await this.sqsc.send(
        new ReceiveMessageCommand({
          QueueUrl: this.sqsUrl,
          MessageSystemAttributeNames: ["All"],
          VisibilityTimeout: this.watchdogIntervalSecs,
          WaitTimeSeconds: longPoll ? 20 : 0,
        }),
      );

      // no messages in the queue
      if (!res.Messages?.[0]) {
        yield null;
        continue;
      }

      const msg = res.Messages[0];
      let logger = this.logger.child({
        watchdogReceiptHandle: msg.ReceiptHandle,
        watchdog_msg_attributes: msg.Attributes,
        longPoll,
      });
      logger.child({ rawBody: msg.Body }).debug("watchdog queue raw message.");

      if (!(msg.Body && msg.ReceiptHandle)) {
        throw new Error(
          "watchdog found message with no body or no receipt handle in the queue.",
        );
      }

      const parsed = watchdogMsgRawSchema.parse(bdecodeFromString(msg.Body));
      logger
        .child({ watchdogMsg: parsed })
        .debug("message received on watchdog queue");

      yield watchdogMsgSchema.parse({
        max_age_secs: parsed.max_age_secs,
        watchdog_msg_attributes: msg.Attributes,
        watchdog_msg_handle: msg.ReceiptHandle,
        job_msg_handle: parsed.job_msg_handle,
        job_receipt: parsed.job_receipt,
      } satisfies WatchdogMsg);
    }
  }

  async delete(msgHandle: string): Promise<void> {
    await this.sqsc.send(
      new DeleteMessageCommand({
        QueueUrl: this.sqsUrl,
        ReceiptHandle: msgHandle,
      }),
    );
  }

  async deleteIfStale(msg: WatchdogMsg): Promise<boolean> {
    const logger = this.logger.child({ watchdogMsg: msg });

    // take in time as param? or di clock?
    const expiry =
      +(msg.watchdog_msg_attributes?.SentTimestamp || "0") / 1e3 +
      msg.max_age_secs;
    const now = Date.now() / 1e3;
    logger
      .child({ expiry, now })
      .debug(`watchdog message expiry= ${expiry} time now= ${now}`);

    if (expiry < now) {
      logger.info(
        `deleting old message from watchdog queue so it's retried in the job queue or sent to the DLQ. orchestrator jobReceipt: ${msg.job_receipt}`,
      );
      await this.delete(msg.watchdog_msg_handle);
      return true;
    }

    return false;
  }
}

/**
 * The job queue.
 *
 * The job queue is very simple.  Receive messages other ppl scheduled for us
 * to turn into orchestrator jobs.  We punt messages (increase the visibility
 * timeout) we're still waiting for a value in the orchestrator.  We delete
 * messages when the job is complete in the orchestrator.
 */
class JobSQS {
  readonly sqsc: SQSClient;
  readonly sqsUrl: string;
  readonly logger: Logger;
  readonly jobVisibilityTimeoutSecs: number;
  constructor(
    sqsc: SQSClient,
    sqsUrl: string,
    jobVisibilityTimeoutSecs: number,
    logger: Logger,
  ) {
    this.sqsUrl = sqsUrl;
    this.sqsc = sqsc;
    this.jobVisibilityTimeoutSecs = jobVisibilityTimeoutSecs;
    this.logger = logger.child({ sqsUrl, jobVisibilityTimeoutSecs });
  }

  async receive(): Promise<JobMsg | null> {
    const res = await this.sqsc.send(
      new ReceiveMessageCommand({
        QueueUrl: this.sqsUrl,
        MessageSystemAttributeNames: ["All"],
        VisibilityTimeout: this.jobVisibilityTimeoutSecs,
        WaitTimeSeconds: 20,
      }),
    );

    if (!res.Messages?.[0]?.Body) return null;

    this.logger
      .child({ rawBody: res.Messages[0].Body })
      .debug("message received on job queue");

    const parsed = jobMsgRawSchema.safeParse(
      bdecodeFromString(res.Messages[0].Body),
    );
    if (!parsed.success) {
      this.logger
        .child({ res, parsed })
        .error(
          "could not parse orchestrator job message wrapper! allow it to go to dlq. " +
            `message body: ${res.Messages[0].Body} parse error: ${parsed.error.message}`,
        );
      // if this is null just return null and it'll be ignored and make its way
      // to the dlq if we throw, we risk ruining the queue with this evil
      // message
      return null;
    }

    if (!res.Messages[0].ReceiptHandle) {
      throw new Error("no receiptHandle in job sqs, no idea how to proceed");
    }

    return jobMsgSchema.parse({
      ...parsed.data,
      msg_handle: res.Messages[0].ReceiptHandle,
    } satisfies JobMsg);
  }

  async delete(msgHandle: string): Promise<void> {
    await this.sqsc.send(
      new DeleteMessageCommand({
        QueueUrl: this.sqsUrl,
        ReceiptHandle: msgHandle,
      }),
    );
  }

  async punt(msgHandle: string): Promise<void> {
    await this.sqsc.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: this.sqsUrl,
        ReceiptHandle: msgHandle,
        VisibilityTimeout: this.jobVisibilityTimeoutSecs,
      }),
    );
  }
}
