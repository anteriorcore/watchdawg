import { MessageSystemAttributeName } from "@aws-sdk/client-sqs";
import bencodelib from "bencode";
import { TextDecoder } from "node:util";
import { z } from "zod";

export {
  bdecodeFromString,
  bencodeAsString,
  bytesToStr,
  jobMsgRawSchema,
  jobMsgSchema,
  watchdogMsgRawPreParsedSchema,
  watchdogMsgRawSchema,
  watchdogMsgSchema,
  type JobMsg,
  type JobMsgRaw,
  type Logger,
  type Orchestrator,
  type WatchdogMsg,
  type WatchdogMsgRaw,
};

const td = new TextDecoder("utf8", { fatal: true });

const bencodeAsString = (data: unknown): string => {
  return td.decode(bencodelib.encode(data));
};
const bdecodeFromString = (data: string): unknown => {
  return bencodelib.decode(Buffer.from(data));
};

// bencoded strings are encoded as utf8 bytes and need to be decoded into str
const bytesToStr = (bytes: Uint8Array, ctx: z.RefinementCtx) => {
  try {
    return td.decode(bytes);
  } catch (e) {
    ctx.addIssue({
      code: "custom",
      message:
        `expected field to be utf8 string: ` +
        (e instanceof Error ? e.message : String(e)),
      fatal: true,
    });
    return z.NEVER;
  }
};

/**
 * The msg in a JobMsgRaw can be anything you can later parse out that includes
 * the information to schedule a job.  Note that the string in msg MUST be
 * compatible with SQS's subset of accepted string characters:
 * https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html
 * */
type JobMsgRaw = { msg: string; max_age_secs?: number | undefined };
const jobMsgRawSchema = z.object({
  msg: z.instanceof(Uint8Array).transform((x, ctx) => bytesToStr(x, ctx)),

  max_age_secs: z.union([z.int().positive(), z.undefined()]),
}) satisfies z.ZodType<JobMsgRaw>;

type JobMsg = JobMsgRaw & { msg_handle: string };
const jobMsgSchema = z.object({
  msg: z.string(),
  max_age_secs: z.union([z.int().positive(), z.undefined()]),
  msg_handle: z.string(),
}) satisfies z.ZodType<JobMsg>;

// with max_age for use by the bouncer
type WatchdogMsgRaw = {
  max_age_secs: number;
  job_msg_handle: string;
  job_receipt: string;
};
const watchdogMsgRawSchema = z.object({
  max_age_secs: z.int().positive(),

  job_msg_handle: z
    .instanceof(Uint8Array)
    .transform((x, ctx) => bytesToStr(x, ctx)),

  job_receipt: z
    .instanceof(Uint8Array)
    .transform((x, ctx) => bytesToStr(x, ctx)),
}) satisfies z.ZodType<WatchdogMsgRaw>;
/** same as watchdogMsgRawSchema except doesn't need parsing from bytes to string */
const watchdogMsgRawPreParsedSchema = z.object({
  max_age_secs: z.int().positive(),

  job_msg_handle: z.string(),

  job_receipt: z.string(),
}) satisfies z.ZodType<WatchdogMsgRaw>;

type WatchdogMsg = WatchdogMsgRaw & {
  watchdog_msg_handle: string;
  // type lifted from AWS SDK
  watchdog_msg_attributes:
    | Partial<Record<MessageSystemAttributeName, string>>
    | undefined;
};
const watchdogMsgSchema = z.object({
  watchdog_msg_handle: z.string(),

  watchdog_msg_attributes: z.union([
    z.partialRecord(z.enum(MessageSystemAttributeName), z.string()),
    z.undefined(),
  ]),

  // from raw
  max_age_secs: z.int().positive(),
  job_msg_handle: z.string(),
  job_receipt: z.string(),
}) satisfies z.ZodType<WatchdogMsg>;

/**
 * Orchestrator is provided by user of this program.  It exposes a method which
 * takes the `msg` from the JobMsg and schedules it, returning a job receipt.
 * That job receipt is later used by the exposed method read which takes that
 * jobReceipt (after it goes through the watchdog queue in a WatchdogMsg) and
 * returns a boolean indicating if the orchestrated task is completed / has a
 * value.
 * */
type Orchestrator = {
  /** schedule is called after receiving a JobMsg from the job queue. The received JobMsg.msg is
   * passed as the parameter. It returns a job receipt that can be used to check on the status of
   * the task later. */
  schedule: (msg: string) => Promise<string>;
  /** read is called after receiving a message on the watchdog queue. The WatchdogMsg.job_receipt is
   * passed as the parameter and the function returns a boolean if the task is complete. */
  read: (jobReceipt: string) => Promise<boolean>;
};

/**
 * Standalone logger definition so this can be used with any logger (with a
 * shim).  These are the basic log functions that are required.
 */
type Logger = {
  /**
   * Fork with bound context.
   * Can also use inline to add context like logger.child({foo: 123}).info("hello")
   */
  child: (ctx: Record<string, any>) => Logger;

  trace: (message: string) => void;
  debug: (message: string) => void;
  info: (message: string) => void;
  warn: (message: string) => void;
  // Allow unknown because technically in javascript you could throw anything and therefore catch
  // anything.  But assume it’s an Error.
  error: (err: Error | unknown | string) => void;
};
