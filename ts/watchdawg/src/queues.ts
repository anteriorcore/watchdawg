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

export { JobSQS, SQSBouncer };

/**
 * SQSBouncer
 * reads messages from SQS, deleting any messages it encounters
 * which are older than the message's max age.  This is for the watchdog queue.
 * Messages are put here when a job is scheduled with the orchestrator so we
 * can check up on the return status and reschedule jobs if not completed.  It
 * doesn't manually punt, but just allows messages to become visible again
 * after e.g. 1m.  When a message is deemed too old to be waited on, we delete
 * the message from this queue which will have us stop punting the job queue
 * message, meaning the job will become visible again in the job queue and the
 * main loop can schedule it again.
 */
class SQSBouncer {
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

  /**  `receive` which deletes messages that are too old ("max age") and keep
   * receiving new messages until we find a valid-aged message or the queue is
   * exhausted
   *
   * this is the bouncer functionality
   */
  async receive(longPoll: boolean): Promise<WatchdogMsg | null> {
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
      if (!res.Messages?.[0]) return null;
      const msg = res.Messages[0];
      this.logger.debug("message received on watchdog queue", {
        rawBody: msg.Body,
        watchdogHandle: msg.ReceiptHandle,
      });

      if (!(msg.Body && msg.ReceiptHandle)) {
        throw new Error(
          "watchdog found message with no body or no receipt handle in the queue.",
        );
      }

      const parsed = watchdogMsgRawSchema.parse(bdecodeFromString(msg.Body));

      let logger = this.logger.child({
        watchdogMsg: parsed,
        watchdogReceiptHandle: msg.ReceiptHandle,
        longPoll,
      });
      logger.debug("message received on watchdog queue", { rawBody: msg.Body });

      // take in time as param? or di clock?
      const expiry =
        +(msg.Attributes?.SentTimestamp || "0") / 1e3 + parsed.max_age_secs;
      const now = Date.now() / 1e3;
      logger.debug(`watchdog message expiry= ${expiry} time now= ${now}`, {
        expiry,
        now,
      });
      if (expiry < now) {
        logger.info(
          `deleting old message from watchdog queue so it's retried in the job queue. orchestrator jobReceipt: ${parsed.job_receipt}`,
        );
        await this.delete(msg.ReceiptHandle);
        continue;
      }

      return watchdogMsgSchema.parse({
        watchdog_msg_handle: msg.ReceiptHandle,
        job_msg_handle: parsed.job_msg_handle,
        job_receipt: parsed.job_receipt,
      } satisfies WatchdogMsg);
    }
  }

  /**
   * Drain all messages from the receive queue until none are left.
   *
   * Compare this to calling a short poll receive in a loop until it returns null.
   */
  async *drain(): AsyncGenerator<WatchdogMsg> {
    for (;;) {
      const msg = await this.receive(false);
      if (!msg) {
        break;
      }
      yield msg;
    }
  }

  /**
   * Keep reading messages from the queue, long polling, forever and ever.
   */
  async *readAll(): AsyncGenerator<WatchdogMsg> {
    for (;;) {
      const msg = await this.receive(true);
      if (msg) {
        yield msg;
      }
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
}

/** the job queue
 *
 * the job queue is very simple.  receive messages others scheduled for
 * watchdawg to turn into orchestrator jobs.  we punt messages (increase the
 * visibility timeout) we're still waiting for a value in the orchestrator.  we
 * delete messages when the job is complete in the orchestrator.
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

    this.logger.debug("message received on job queue", {
      rawBody: res.Messages[0].Body,
    });

    const parsed = jobMsgRawSchema.safeParse(
      bdecodeFromString(res.Messages[0].Body),
    );
    if (!parsed.success) {
      this.logger.error(
        "could not parse orchestrator job message wrapper! allow it to go to dlq. " +
          `message body: ${res.Messages[0].Body}` +
          `parse error: ${parsed.error.message}`,
        { res, parsed },
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
