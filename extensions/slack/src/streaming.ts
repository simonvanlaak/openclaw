/**
 * Slack native text streaming helpers.
 *
 * Uses the Slack SDK's `ChatStreamer` (via `client.chatStream()`) to stream
 * text responses word-by-word in a single updating message, matching Slack's
 * "Agents & AI Apps" streaming UX.
 *
 * @see https://docs.slack.dev/ai/developing-ai-apps#streaming
 * @see https://docs.slack.dev/reference/methods/chat.startStream
 * @see https://docs.slack.dev/reference/methods/chat.appendStream
 * @see https://docs.slack.dev/reference/methods/chat.stopStream
 */

import type { WebAPICallResult, WebClient } from "@slack/web-api";
import type { ChatStreamer } from "@slack/web-api/dist/chat-stream.js";
import { logVerbose } from "openclaw/plugin-sdk/runtime-env";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type SlackStreamSession = {
  /** The SDK ChatStreamer instance managing this stream. */
  streamer: ChatStreamer;
  /** Channel this stream lives in. */
  channel: string;
  /** Thread timestamp (required for streaming). */
  threadTs: string;
  /** True once stop() has been called. */
  stopped: boolean;
};

export type SlackStreamChunk =
  | {
      type: "markdown_text";
      markdown_text: string;
    }
  | {
      type: "task_update";
      id: string;
      title: string;
      status: "pending" | "in_progress" | "complete" | "completed" | "error";
    };

export type SlackChunkStreamSession = {
  client: WebClient;
  channel: string;
  threadTs: string;
  messageTs: string;
  stopped: boolean;
};

export type StartSlackStreamParams = {
  client: WebClient;
  channel: string;
  threadTs: string;
  /** Optional initial markdown text to include in the stream start. */
  text?: string;
  /**
   * The team ID of the workspace this stream belongs to.
   * Required by the Slack API for `chat.startStream` / `chat.stopStream`.
   * Obtain from `auth.test` response (`team_id`).
   */
  teamId?: string;
  /**
   * The user ID of the message recipient (required for DM streaming).
   * Without this, `chat.stopStream` fails with `missing_recipient_user_id`
   * in direct message conversations.
   */
  userId?: string;
};

export type AppendSlackStreamParams = {
  session: SlackStreamSession;
  text: string;
};

export type StopSlackStreamParams = {
  session: SlackStreamSession;
  /** Optional final markdown text to append before stopping. */
  text?: string;
};

export type StartSlackChunkStreamParams = {
  client: WebClient;
  channel: string;
  threadTs: string;
  teamId?: string;
  userId?: string;
  taskDisplayMode?: "plan" | "task_update";
  chunks?: SlackStreamChunk[];
};

export type AppendSlackChunkStreamParams = {
  session: SlackChunkStreamSession;
  chunks: SlackStreamChunk[];
};

export type StopSlackChunkStreamParams = {
  session: SlackChunkStreamSession;
  chunks?: SlackStreamChunk[];
};

type SlackApiClient = WebClient & {
  apiCall: (method: string, args: Record<string, unknown>) => Promise<Record<string, unknown>>;
};

// ---------------------------------------------------------------------------
// Stream lifecycle
// ---------------------------------------------------------------------------

/**
 * Start a new Slack text stream.
 *
 * Returns a {@link SlackStreamSession} that should be passed to
 * {@link appendSlackStream} and {@link stopSlackStream}.
 *
 * The first chunk of text can optionally be included via `text`.
 */
export async function startSlackStream(
  params: StartSlackStreamParams,
): Promise<SlackStreamSession> {
  const { client, channel, threadTs, text, teamId, userId } = params;

  logVerbose(
    `slack-stream: starting stream in ${channel} thread=${threadTs}${teamId ? ` team=${teamId}` : ""}${userId ? ` user=${userId}` : ""}`,
  );

  const streamer = client.chatStream({
    channel,
    thread_ts: threadTs,
    ...(teamId ? { recipient_team_id: teamId } : {}),
    ...(userId ? { recipient_user_id: userId } : {}),
  });

  const session: SlackStreamSession = {
    streamer,
    channel,
    threadTs,
    stopped: false,
  };

  // If initial text is provided, send it as the first append which will
  // trigger the ChatStreamer to call chat.startStream under the hood.
  if (text) {
    await streamer.append({ markdown_text: text });
    logVerbose(`slack-stream: appended initial text (${text.length} chars)`);
  }

  return session;
}

/**
 * Append markdown text to an active Slack stream.
 */
export async function appendSlackStream(params: AppendSlackStreamParams): Promise<void> {
  const { session, text } = params;

  if (session.stopped) {
    logVerbose("slack-stream: attempted to append to a stopped stream, ignoring");
    return;
  }

  if (!text) {
    return;
  }

  await session.streamer.append({ markdown_text: text });
  logVerbose(`slack-stream: appended ${text.length} chars`);
}

/**
 * Stop (finalize) a Slack stream.
 *
 * After calling this the stream message becomes a normal Slack message.
 * Optionally include final text to append before stopping.
 */
export async function stopSlackStream(params: StopSlackStreamParams): Promise<void> {
  const { session, text } = params;

  if (session.stopped) {
    logVerbose("slack-stream: stream already stopped, ignoring duplicate stop");
    return;
  }

  session.stopped = true;

  logVerbose(
    `slack-stream: stopping stream in ${session.channel} thread=${session.threadTs}${
      text ? ` (final text: ${text.length} chars)` : ""
    }`,
  );

  await session.streamer.stop(text ? { markdown_text: text } : undefined);

  logVerbose("slack-stream: stream stopped");
}

function resolveSlackStreamMessageTs(
  response: WebAPICallResult & { ts?: string; message_ts?: string },
): string {
  const ts = response.ts;
  if (typeof ts === "string" && ts.length > 0) {
    return ts;
  }
  const messageTs = response.message_ts;
  if (typeof messageTs === "string" && messageTs.length > 0) {
    return messageTs;
  }
  throw new TypeError("Slack stream response missing message timestamp");
}

export async function startSlackChunkStream(
  params: StartSlackChunkStreamParams,
): Promise<SlackChunkStreamSession> {
  const { client, channel, threadTs, teamId, userId, taskDisplayMode, chunks } = params;

  logVerbose(
    `slack-stream: starting chunk stream in ${channel} thread=${threadTs}${
      taskDisplayMode ? ` mode=${taskDisplayMode}` : ""
    }`,
  );

  const apiClient = client as SlackApiClient;
  const response = await apiClient.apiCall("chat.startStream", {
    channel,
    thread_ts: threadTs,
    ...(teamId ? { recipient_team_id: teamId } : {}),
    ...(userId ? { recipient_user_id: userId } : {}),
    ...(taskDisplayMode ? { task_display_mode: taskDisplayMode } : {}),
    ...(chunks?.length ? { chunks } : {}),
  });

  return {
    client,
    channel,
    threadTs,
    messageTs: resolveSlackStreamMessageTs(response),
    stopped: false,
  };
}

export async function appendSlackChunkStream(params: AppendSlackChunkStreamParams): Promise<void> {
  const { session, chunks } = params;
  if (session.stopped || chunks.length === 0) {
    return;
  }
  const apiClient = session.client as SlackApiClient;
  await apiClient.apiCall("chat.appendStream", {
    channel: session.channel,
    thread_ts: session.threadTs,
    ts: session.messageTs,
    chunks,
  });
}

export async function stopSlackChunkStream(params: StopSlackChunkStreamParams): Promise<void> {
  const { session, chunks } = params;
  if (session.stopped) {
    logVerbose("slack-stream: chunk stream already stopped, ignoring duplicate stop");
    return;
  }
  session.stopped = true;
  const apiClient = session.client as SlackApiClient;
  await apiClient.apiCall("chat.stopStream", {
    channel: session.channel,
    thread_ts: session.threadTs,
    ts: session.messageTs,
    ...(chunks?.length ? { chunks } : {}),
  });
}
