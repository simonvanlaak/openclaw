import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import type { PreparedSlackMessage } from "./types.js";

const FINAL_REPLY_TEXT = "final answer";
const THREAD_TS = "thread-1";

const deliverRepliesMock = vi.fn(async () => {});
const startSlackChunkStreamMock = vi.fn(async () => ({
  client: {} as never,
  channel: "C123",
  threadTs: THREAD_TS,
  messageTs: "171234.567",
  stopped: false,
}));
const appendSlackChunkStreamMock = vi.fn(async () => {});
const stopSlackChunkStreamMock = vi.fn(async () => {});

function createPreparedSlackMessage() {
  const setSlackThreadStatus = vi.fn(async () => undefined);
  return {
    ctx: {
      cfg: {},
      runtime: {},
      botToken: "xoxb-test",
      app: { client: {} },
      teamId: "T1",
      textLimit: 4000,
      typingReaction: "",
      removeAckAfterReply: false,
      historyLimit: 0,
      channelHistories: new Map(),
      allowFrom: [],
      setSlackThreadStatus,
    },
    account: {
      accountId: "default",
      config: {},
    },
    message: {
      channel: "C123",
      ts: "171234.111",
      thread_ts: THREAD_TS,
      user: "U123",
    },
    route: {
      agentId: "agent-1",
      accountId: "default",
      mainSessionKey: "main",
    },
    channelConfig: null,
    replyTarget: "channel:C123",
    ctxPayload: {
      MessageThreadId: THREAD_TS,
    },
    replyToMode: "all",
    isDirectMessage: false,
    isRoomish: false,
    historyKey: "history-key",
    preview: "",
    ackReactionValue: "eyes",
    ackReactionPromise: null,
  } as unknown as PreparedSlackMessage & {
    ctx: PreparedSlackMessage["ctx"] & {
      setSlackThreadStatus: typeof setSlackThreadStatus;
    };
  };
}

vi.mock("openclaw/plugin-sdk/agent-runtime", () => ({
  resolveHumanDelayConfig: () => undefined,
}));

vi.mock("openclaw/plugin-sdk/channel-feedback", () => ({
  DEFAULT_TIMING: {
    doneHoldMs: 0,
    errorHoldMs: 0,
  },
  createStatusReactionController: () => ({
    setQueued: async () => {},
    setThinking: async () => {},
    setTool: async () => {},
    setError: async () => {},
    setDone: async () => {},
    clear: async () => {},
    restoreInitial: async () => {},
  }),
  logAckFailure: () => {},
  logTypingFailure: () => {},
  removeAckReactionAfterReply: () => {},
}));

vi.mock("openclaw/plugin-sdk/channel-reply-pipeline", () => ({
  createChannelReplyPipeline: () => ({
    typingCallbacks: {
      onIdle: vi.fn(),
    },
    onModelSelected: undefined,
  }),
}));

vi.mock("openclaw/plugin-sdk/channel-streaming", () => ({
  resolveChannelStreamingBlockEnabled: () => false,
  resolveChannelStreamingNativeTransport: () => false,
}));

vi.mock("openclaw/plugin-sdk/outbound-runtime", () => ({
  resolveAgentOutboundIdentity: () => undefined,
}));

vi.mock("openclaw/plugin-sdk/reply-history", () => ({
  clearHistoryEntriesIfEnabled: () => {},
}));

vi.mock("openclaw/plugin-sdk/reply-payload", () => ({
  resolveSendableOutboundReplyParts: (payload: { text?: string }) => ({
    text: payload.text ?? "",
    trimmedText: (payload.text ?? "").trim(),
    hasText: Boolean(payload.text?.trim()),
    hasMedia: false,
    mediaUrls: [],
    hasContent: Boolean(payload.text?.trim()),
  }),
}));

vi.mock("openclaw/plugin-sdk/runtime-env", () => ({
  danger: (message: string) => message,
  logVerbose: () => {},
  shouldLogVerbose: () => false,
}));

vi.mock("openclaw/plugin-sdk/security-runtime", () => ({
  resolvePinnedMainDmOwnerFromAllowlist: () => undefined,
}));

vi.mock("openclaw/plugin-sdk/text-runtime", () => ({
  normalizeOptionalLowercaseString: (value?: string) => value?.toLowerCase(),
}));

vi.mock("../../actions.js", () => ({
  reactSlackMessage: async () => {},
  removeSlackReaction: async () => {},
}));

vi.mock("../../draft-stream.js", () => ({
  createSlackDraftStream: () => undefined,
}));

vi.mock("../../format.js", () => ({
  normalizeSlackOutboundText: (value: string) => value.trim(),
}));

vi.mock("../../interactive-replies.js", () => ({
  compileSlackInteractiveReplies: (payload: unknown) => payload,
  isSlackInteractiveRepliesEnabled: () => false,
}));

vi.mock("../../limits.js", () => ({
  SLACK_TEXT_LIMIT: 4000,
}));

vi.mock("../../sent-thread-cache.js", () => ({
  recordSlackThreadParticipation: () => {},
}));

vi.mock("../../stream-mode.js", () => ({
  applyAppendOnlyStreamUpdate: ({ incoming }: { incoming: string }) => ({
    changed: true,
    rendered: incoming,
    source: incoming,
  }),
  buildStatusFinalPreviewText: () => "status",
  resolveSlackStreamingConfig: () => ({
    mode: "progress",
    nativeStreaming: false,
    draftMode: "append",
  }),
}));

vi.mock("../../streaming.js", () => ({
  appendSlackChunkStream: appendSlackChunkStreamMock,
  appendSlackStream: async () => {},
  startSlackChunkStream: startSlackChunkStreamMock,
  startSlackStream: async () => ({
    threadTs: THREAD_TS,
    stopped: false,
  }),
  stopSlackChunkStream: stopSlackChunkStreamMock,
  stopSlackStream: async () => {},
}));

vi.mock("../../threading.js", () => ({
  resolveSlackThreadTargets: () => ({
    statusThreadTs: THREAD_TS,
    isThreadReply: true,
  }),
}));

vi.mock("../allow-list.js", () => ({
  normalizeSlackAllowOwnerEntry: (value: string) => value,
}));

vi.mock("../config.runtime.js", () => ({
  resolveStorePath: () => "/tmp/openclaw-store.json",
  updateLastRoute: async () => {},
}));

vi.mock("../replies.js", () => ({
  createSlackReplyDeliveryPlan: () => ({
    nextThreadTs: () => THREAD_TS,
    markSent: () => {},
  }),
  deliverReplies: deliverRepliesMock,
  readSlackReplyBlocks: () => undefined,
  resolveSlackThreadTs: () => THREAD_TS,
}));

vi.mock("../reply.runtime.js", () => ({
  createReplyDispatcherWithTyping: (params: {
    deliver: (payload: unknown, info: { kind: "tool" | "block" | "final" }) => Promise<void>;
  }) => ({
    dispatcher: {
      deliver: params.deliver,
    },
    replyOptions: {},
    markDispatchIdle: () => {},
  }),
  dispatchInboundMessage: async (params: {
    replyOptions?: {
      onToolStart?: (payload: { name?: string }) => Promise<void>;
      onItemEvent?: (payload: {
        itemId?: string;
        kind?: string;
        title?: string;
        phase?: string;
        status?: string;
      }) => Promise<void>;
      onAssistantMessageStart?: () => Promise<void>;
    };
    dispatcher: {
      deliver: (payload: { text: string }, info: { kind: "final" }) => Promise<void>;
    };
  }) => {
    await params.replyOptions?.onToolStart?.({ name: "exec" });
    await params.replyOptions?.onItemEvent?.({
      itemId: "tool:exec-1",
      kind: "tool",
      title:
        "exec lin issue query --all-teams --state completed --json --limit 100 --updated-after 2026-04-13 (in ~/.openclaw/workspace)",
      phase: "start",
      status: "running",
    });
    await params.replyOptions?.onItemEvent?.({
      itemId: "tool:exec-1",
      kind: "tool",
      title:
        "exec lin issue query --all-teams --state completed --json --limit 100 --updated-after 2026-04-13 (in ~/.openclaw/workspace)",
      phase: "end",
      status: "completed",
    });
    await params.replyOptions?.onToolStart?.({ name: "mcp" });
    await params.replyOptions?.onItemEvent?.({
      itemId: "tool:mcp-1",
      kind: "tool",
      title: "Using tool",
      phase: "start",
      status: "running",
    });
    await params.replyOptions?.onItemEvent?.({
      itemId: "tool:mcp-1",
      kind: "tool",
      title: "Using tool",
      phase: "end",
      status: "completed",
    });
    await params.replyOptions?.onAssistantMessageStart?.();
    await params.dispatcher.deliver({ text: FINAL_REPLY_TEXT }, { kind: "final" });
    return {
      queuedFinal: false,
      counts: { final: 1 },
    };
  },
}));

vi.mock("./preview-finalize.js", () => ({
  finalizeSlackPreviewEdit: async () => {},
}));

let dispatchPreparedSlackMessage: typeof import("./dispatch.js").dispatchPreparedSlackMessage;

describe("dispatchPreparedSlackMessage progress plan streaming", () => {
  beforeAll(async () => {
    ({ dispatchPreparedSlackMessage } = await import("./dispatch.js"));
  });

  beforeEach(() => {
    deliverRepliesMock.mockReset();
    startSlackChunkStreamMock.mockClear();
    appendSlackChunkStreamMock.mockClear();
    stopSlackChunkStreamMock.mockClear();
  });

  it("starts a plan stream and appends per-tool task updates", async () => {
    const prepared = createPreparedSlackMessage();

    await dispatchPreparedSlackMessage(prepared);

    expect(startSlackChunkStreamMock).toHaveBeenCalledTimes(1);
    expect(startSlackChunkStreamMock).toHaveBeenCalledWith(
      expect.objectContaining({
        channel: "C123",
        threadTs: THREAD_TS,
        taskDisplayMode: "plan",
        chunks: [
          expect.objectContaining({
            type: "task_update",
            id: "understand_request",
            title: "Thinking...",
            status: "in_progress",
          }),
        ],
      }),
    );

    const appendedTasks = appendSlackChunkStreamMock.mock.calls.flatMap((call: unknown[]) => {
      const first = call[0] as
        | {
            chunks?: Array<{ id?: string; title?: string; status?: string }>;
          }
        | undefined;
      return first?.chunks ?? [];
    });

    expect(appendedTasks).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          title: "Thinking...",
          status: "complete",
        }),
        expect.objectContaining({
          title: "Using Linear",
          status: "in_progress",
        }),
        expect.objectContaining({
          title: "Using Linear",
          status: "complete",
        }),
        expect.objectContaining({
          title: "Analyzing tool results",
          status: "in_progress",
        }),
        expect.objectContaining({
          title: "Analyzing tool results",
          status: "complete",
        }),
        expect.objectContaining({
          title: "Using tool",
          status: "in_progress",
        }),
        expect.objectContaining({
          title: "Using tool",
          status: "complete",
        }),
        expect.objectContaining({
          title: "Compose response",
          status: "in_progress",
        }),
        expect.objectContaining({
          title: "Compose response",
          status: "complete",
        }),
      ]),
    );

    expect(stopSlackChunkStreamMock).toHaveBeenCalledTimes(1);
    expect(deliverRepliesMock).toHaveBeenCalledTimes(1);
  });

  it("does not publish tool status text into assistant thread status", async () => {
    const prepared = createPreparedSlackMessage();

    await dispatchPreparedSlackMessage(prepared);

    const setStatusMock = prepared.ctx.setSlackThreadStatus;
    expect(setStatusMock).not.toHaveBeenCalledWith(
      expect.objectContaining({
        status: "Running command...",
      }),
    );
  });
});
