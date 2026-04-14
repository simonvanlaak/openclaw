import { describe, expect, it, vi } from "vitest";
import {
  appendSlackChunkStream,
  startSlackChunkStream,
  stopSlackChunkStream,
  type SlackChunkStreamSession,
} from "./streaming.js";

describe("Slack chunk streaming", () => {
  it("uses ts returned by chat.startStream for subsequent append/stop calls", async () => {
    const apiCall = vi
      .fn()
      .mockResolvedValueOnce({ ok: true, ts: "1776187898.610739" })
      .mockResolvedValueOnce({ ok: true })
      .mockResolvedValueOnce({ ok: true });
    const client = {
      apiCall,
    } as never;

    const session = await startSlackChunkStream({
      client,
      channel: "D0AQQAVC50U",
      threadTs: "1776166086.341639",
      taskDisplayMode: "plan",
    });

    await appendSlackChunkStream({
      session,
      chunks: [
        {
          type: "task_update",
          id: "understand_request",
          title: "Understand request",
          status: "in_progress",
        },
      ],
    });
    await stopSlackChunkStream({ session });

    expect(apiCall).toHaveBeenNthCalledWith(
      1,
      "chat.startStream",
      expect.objectContaining({
        channel: "D0AQQAVC50U",
        thread_ts: "1776166086.341639",
        task_display_mode: "plan",
      }),
    );
    expect(apiCall).toHaveBeenNthCalledWith(
      2,
      "chat.appendStream",
      expect.objectContaining({
        channel: "D0AQQAVC50U",
        thread_ts: "1776166086.341639",
        ts: "1776187898.610739",
      }),
    );
    expect(apiCall).toHaveBeenNthCalledWith(
      3,
      "chat.stopStream",
      expect.objectContaining({
        channel: "D0AQQAVC50U",
        thread_ts: "1776166086.341639",
        ts: "1776187898.610739",
      }),
    );
    expect(apiCall).not.toHaveBeenCalledWith(
      "chat.appendStream",
      expect.objectContaining({ message_ts: expect.anything() }),
    );
    expect(apiCall).not.toHaveBeenCalledWith(
      "chat.stopStream",
      expect.objectContaining({ message_ts: expect.anything() }),
    );
  });

  it("accepts flat task_update chunks", async () => {
    const apiCall = vi.fn().mockResolvedValue({ ok: true });
    const session: SlackChunkStreamSession = {
      client: { apiCall } as never,
      channel: "C123",
      threadTs: "thread-1",
      messageTs: "stream-1",
      stopped: false,
    };

    await appendSlackChunkStream({
      session,
      chunks: [
        {
          type: "task_update",
          id: "compose_response",
          title: "Compose response",
          status: "complete",
        },
      ],
    });

    expect(apiCall).toHaveBeenCalledWith(
      "chat.appendStream",
      expect.objectContaining({
        chunks: [
          {
            type: "task_update",
            id: "compose_response",
            title: "Compose response",
            status: "complete",
          },
        ],
      }),
    );
  });
});
