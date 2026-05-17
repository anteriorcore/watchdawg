import { SendMessageCommand, type SQSClient } from "@aws-sdk/client-sqs";
import { bencodeAsString, type JobMsgRaw } from "./models.ts";

export { put };

/** for the user of this library:
 * enqueue a message on the job queue for processing by your Orchestrator, guaranteeing at least
 * once delivery. */
const put = async (
  sqs: SQSClient,
  jobQueueUrl: string,
  jobMsgRaw: JobMsgRaw,
): Promise<void> => {
  await sqs.send(
    new SendMessageCommand({
      QueueUrl: jobQueueUrl,
      MessageBody: bencodeAsString(jobMsgRaw),
    }),
  );
};
