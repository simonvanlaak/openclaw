import { resolveHumanDelayConfig } from "openclaw/plugin-sdk/agent-runtime";
import {
  createStatusReactionController,
  DEFAULT_TIMING,
  logAckFailure,
  logTypingFailure,
  removeAckReactionAfterReply,
  type StatusReactionAdapter,
} from "openclaw/plugin-sdk/channel-feedback";
import { createChannelReplyPipeline } from "openclaw/plugin-sdk/channel-reply-pipeline";
import {
  resolveChannelStreamingBlockEnabled,
  resolveChannelStreamingNativeTransport,
} from "openclaw/plugin-sdk/channel-streaming";
import { resolveAgentOutboundIdentity } from "openclaw/plugin-sdk/outbound-runtime";
import { clearHistoryEntriesIfEnabled } from "openclaw/plugin-sdk/reply-history";
import { resolveSendableOutboundReplyParts } from "openclaw/plugin-sdk/reply-payload";
import type { ReplyDispatchKind, ReplyPayload } from "openclaw/plugin-sdk/reply-runtime";
import { danger, logVerbose, shouldLogVerbose } from "openclaw/plugin-sdk/runtime-env";
import { resolvePinnedMainDmOwnerFromAllowlist } from "openclaw/plugin-sdk/security-runtime";
import { normalizeOptionalLowercaseString } from "openclaw/plugin-sdk/text-runtime";
import { reactSlackMessage, removeSlackReaction } from "../../actions.js";
import { createSlackDraftStream } from "../../draft-stream.js";
import { normalizeSlackOutboundText } from "../../format.js";
import {
  compileSlackInteractiveReplies,
  isSlackInteractiveRepliesEnabled,
} from "../../interactive-replies.js";
import { SLACK_TEXT_LIMIT } from "../../limits.js";
import { recordSlackThreadParticipation } from "../../sent-thread-cache.js";
import {
  applyAppendOnlyStreamUpdate,
  buildStatusFinalPreviewText,
  resolveSlackStreamingConfig,
} from "../../stream-mode.js";
import type {
  SlackChunkStreamSession,
  SlackStreamChunk,
  SlackStreamSession,
} from "../../streaming.js";
import {
  appendSlackChunkStream,
  appendSlackStream,
  startSlackChunkStream,
  startSlackStream,
  stopSlackChunkStream,
  stopSlackStream,
} from "../../streaming.js";
import { resolveSlackThreadTargets } from "../../threading.js";
import { normalizeSlackAllowOwnerEntry } from "../allow-list.js";
import { resolveStorePath, updateLastRoute } from "../config.runtime.js";
import {
  createSlackReplyDeliveryPlan,
  deliverReplies,
  readSlackReplyBlocks,
  resolveSlackThreadTs,
} from "../replies.js";
import { createReplyDispatcherWithTyping, dispatchInboundMessage } from "../reply.runtime.js";
import { finalizeSlackPreviewEdit } from "./preview-finalize.js";
import type { PreparedSlackMessage } from "./types.js";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Slack reactions.add/remove expect shortcode names, not raw unicode emoji.
const UNICODE_TO_SLACK: Record<string, string> = {
  "👀": "eyes",
  "🤔": "thinking_face",
  "🔥": "fire",
  "👨‍💻": "male-technologist",
  "👨💻": "male-technologist",
  "👩‍💻": "female-technologist",
  "⚡": "zap",
  "🌐": "globe_with_meridians",
  "✅": "white_check_mark",
  "👍": "thumbsup",
  "❌": "x",
  "😱": "scream",
  "🥱": "yawning_face",
  "😨": "fearful",
  "⏳": "hourglass_flowing_sand",
  "⚠️": "warning",
  "✍": "writing_hand",
  "🧠": "brain",
  "🛠️": "hammer_and_wrench",
  "💻": "computer",
};

function toSlackEmojiName(emoji: string): string {
  const trimmed = emoji.trim().replace(/^:+|:+$/g, "");
  return UNICODE_TO_SLACK[trimmed] ?? trimmed;
}

const TOOL_STATUS_LABELS: Record<string, string> = {
  exec: "Running command...",
  Read: "Reading files...",
  Edit: "Editing files...",
  Write: "Writing files...",
  web_search: "Searching the web...",
  web_fetch: "Fetching page...",
  memory_search: "Checking memory...",
  memory_get: "Reading memory...",
  browser: "Using browser...",
  message: "Sending message...",
  tts: "Converting to speech...",
  image: "Analyzing image...",
  sessions_spawn: "Spawning sub-agent...",
  sessions_send: "Messaging sub-agent...",
  sessions_list: "Checking sessions...",
  sessions_history: "Reading session history...",
  session_status: "Checking status...",
  cron: "Managing schedule...",
  canvas: "Updating canvas...",
  nodes: "Checking nodes...",
  gateway: "Managing gateway...",
  whatsapp_login: "WhatsApp login...",
  agents_list: "Listing agents...",
  process: "Managing process...",
};

export function toolStatusLabel(toolName: string): string {
  return TOOL_STATUS_LABELS[toolName] ?? `Using ${toolName}...`;
}

export function isSlackStreamingEnabled(params: {
  mode: "off" | "partial" | "block" | "progress";
  nativeStreaming: boolean;
}): boolean {
  if (params.mode !== "partial") {
    return false;
  }
  return params.nativeStreaming;
}

export function shouldEnableSlackPreviewStreaming(params: {
  mode: "off" | "partial" | "block" | "progress";
  isDirectMessage: boolean;
  threadTs?: string;
}): boolean {
  if (params.mode === "off") {
    return false;
  }
  if (!params.isDirectMessage) {
    return true;
  }
  return Boolean(params.threadTs);
}

export function shouldInitializeSlackDraftStream(params: {
  previewStreamingEnabled: boolean;
  useStreaming: boolean;
}): boolean {
  return params.previewStreamingEnabled && !params.useStreaming;
}

export function resolveSlackStreamingThreadHint(params: {
  replyToMode: "off" | "first" | "all" | "batched";
  incomingThreadTs: string | undefined;
  messageTs: string | undefined;
  isThreadReply?: boolean;
}): string | undefined {
  return resolveSlackThreadTs({
    replyToMode: params.replyToMode,
    incomingThreadTs: params.incomingThreadTs,
    messageTs: params.messageTs,
    hasReplied: false,
    isThreadReply: params.isThreadReply,
  });
}

type SlackTurnDeliveryAttempt = {
  kind: ReplyDispatchKind;
  payload: ReplyPayload;
  threadTs?: string;
  textOverride?: string;
};

function buildSlackTurnDeliveryKey(params: SlackTurnDeliveryAttempt): string | null {
  const reply = resolveSendableOutboundReplyParts(params.payload, {
    text: params.textOverride,
  });
  const slackBlocks = readSlackReplyBlocks(params.payload);
  if (!reply.hasContent && !slackBlocks?.length) {
    return null;
  }
  return JSON.stringify({
    kind: params.kind,
    threadTs: params.threadTs ?? "",
    replyToId: params.payload.replyToId ?? null,
    text: reply.trimmedText,
    mediaUrls: reply.mediaUrls,
    blocks: slackBlocks ?? null,
  });
}

export function createSlackTurnDeliveryTracker() {
  const deliveredKeys = new Set<string>();
  return {
    hasDelivered(params: SlackTurnDeliveryAttempt) {
      const key = buildSlackTurnDeliveryKey(params);
      return key ? deliveredKeys.has(key) : false;
    },
    markDelivered(params: SlackTurnDeliveryAttempt) {
      const key = buildSlackTurnDeliveryKey(params);
      if (key) {
        deliveredKeys.add(key);
      }
    },
  };
}

function shouldUseStreaming(params: {
  streamingEnabled: boolean;
  threadTs: string | undefined;
}): boolean {
  if (!params.streamingEnabled) {
    return false;
  }
  if (!params.threadTs) {
    logVerbose("slack-stream: streaming disabled — no reply thread target available");
    return false;
  }
  return true;
}

function shouldUseSlackProgressPlanStream(params: {
  mode: "off" | "partial" | "block" | "progress";
  threadTs?: string;
}): boolean {
  return (
    params.mode === "progress" && typeof params.threadTs === "string" && params.threadTs.length > 0
  );
}

function createSlackTaskUpdateChunk(params: {
  taskId: string;
  title: string;
  status: "pending" | "in_progress" | "complete" | "error";
}): SlackStreamChunk {
  return {
    type: "task_update",
    task: {
      task_id: params.taskId,
      title: params.title,
      status: params.status,
    },
  };
}

export async function dispatchPreparedSlackMessage(prepared: PreparedSlackMessage) {
  const { ctx, account, message, route } = prepared;
  const cfg = ctx.cfg;
  const runtime = ctx.runtime;

  // Resolve agent identity for Slack chat:write.customize overrides.
  const outboundIdentity = resolveAgentOutboundIdentity(cfg, route.agentId);
  const slackIdentity = outboundIdentity
    ? {
        username: outboundIdentity.name,
        iconUrl: outboundIdentity.avatarUrl,
        iconEmoji: outboundIdentity.emoji,
      }
    : undefined;

  if (prepared.isDirectMessage) {
    const sessionCfg = cfg.session;
    const storePath = resolveStorePath(sessionCfg?.store, {
      agentId: route.agentId,
    });
    const pinnedMainDmOwner = resolvePinnedMainDmOwnerFromAllowlist({
      dmScope: cfg.session?.dmScope,
      allowFrom: ctx.allowFrom,
      normalizeEntry: normalizeSlackAllowOwnerEntry,
    });
    const senderRecipient = normalizeOptionalLowercaseString(message.user);
    const skipMainUpdate =
      pinnedMainDmOwner &&
      senderRecipient &&
      normalizeOptionalLowercaseString(pinnedMainDmOwner) !== senderRecipient;
    if (skipMainUpdate) {
      logVerbose(
        `slack: skip main-session last route for ${senderRecipient} (pinned owner ${pinnedMainDmOwner})`,
      );
    } else {
      await updateLastRoute({
        storePath,
        sessionKey: route.mainSessionKey,
        deliveryContext: {
          channel: "slack",
          to: `user:${message.user}`,
          accountId: route.accountId,
          threadId: prepared.ctxPayload.MessageThreadId,
        },
        ctx: prepared.ctxPayload,
      });
    }
  }

  const { statusThreadTs, isThreadReply } = resolveSlackThreadTargets({
    message,
    replyToMode: prepared.replyToMode,
  });

  const reactionMessageTs = prepared.ackReactionMessageTs;
  const messageTs = message.ts ?? message.event_ts;
  const incomingThreadTs = message.thread_ts;
  let didSetStatus = false;
  const statusReactionsEnabled =
    Boolean(prepared.ackReactionPromise) &&
    Boolean(reactionMessageTs) &&
    cfg.messages?.statusReactions?.enabled !== false;
  const slackStatusAdapter: StatusReactionAdapter = {
    setReaction: async (emoji) => {
      await reactSlackMessage(message.channel, reactionMessageTs ?? "", toSlackEmojiName(emoji), {
        token: ctx.botToken,
        client: ctx.app.client,
      }).catch((err) => {
        if (String(err).includes("already_reacted")) {
          return;
        }
        throw err;
      });
    },
    removeReaction: async (emoji) => {
      await removeSlackReaction(message.channel, reactionMessageTs ?? "", toSlackEmojiName(emoji), {
        token: ctx.botToken,
        client: ctx.app.client,
      }).catch((err) => {
        if (String(err).includes("no_reaction")) {
          return;
        }
        throw err;
      });
    },
  };
  const statusReactionTiming = {
    ...DEFAULT_TIMING,
    ...cfg.messages?.statusReactions?.timing,
  };
  const statusReactions = createStatusReactionController({
    enabled: statusReactionsEnabled,
    adapter: slackStatusAdapter,
    initialEmoji: prepared.ackReactionValue || "eyes",
    emojis: cfg.messages?.statusReactions?.emojis,
    timing: cfg.messages?.statusReactions?.timing,
    onError: (err) => {
      logAckFailure({
        log: logVerbose,
        channel: "slack",
        target: `${message.channel}/${message.ts}`,
        error: err,
      });
    },
  });

  if (statusReactionsEnabled) {
    void statusReactions.setQueued();
  }

  // Shared mutable ref for "replyToMode=first". Both tool + auto-reply flows
  // mark this to ensure only the first reply is threaded.
  const hasRepliedRef = { value: false };
  const replyPlan = createSlackReplyDeliveryPlan({
    replyToMode: prepared.replyToMode,
    incomingThreadTs,
    messageTs,
    hasRepliedRef,
    isThreadReply,
  });

  const typingTarget = statusThreadTs ? `${message.channel}/${statusThreadTs}` : message.channel;
  const typingReaction = ctx.typingReaction;
  const { onModelSelected, ...replyPipeline } = createChannelReplyPipeline({
    cfg,
    agentId: route.agentId,
    channel: "slack",
    accountId: route.accountId,
    transformReplyPayload: (payload) =>
      isSlackInteractiveRepliesEnabled({ cfg, accountId: route.accountId })
        ? compileSlackInteractiveReplies(payload)
        : payload,
    typing: {
      start: async () => {
        if (!didSetStatus && !progressPlanStreamingEnabled) {
          didSetStatus = true;
          await ctx.setSlackThreadStatus({
            channelId: message.channel,
            threadTs: statusThreadTs,
            status: "is typing...",
          });
        }
        if (typingReaction && message.ts) {
          await reactSlackMessage(message.channel, message.ts, typingReaction, {
            token: ctx.botToken,
            client: ctx.app.client,
          }).catch(() => {});
        }
      },
      stop: async () => {
        if (!didSetStatus) {
          return;
        }
        didSetStatus = false;
        await ctx.setSlackThreadStatus({
          channelId: message.channel,
          threadTs: statusThreadTs,
          status: "",
        });
        if (typingReaction && message.ts) {
          await removeSlackReaction(message.channel, message.ts, typingReaction, {
            token: ctx.botToken,
            client: ctx.app.client,
          }).catch(() => {});
        }
      },
      onStartError: (err) => {
        logTypingFailure({
          log: (message) => runtime.error?.(danger(message)),
          channel: "slack",
          action: "start",
          target: typingTarget,
          error: err,
        });
      },
      onStopError: (err) => {
        logTypingFailure({
          log: (message) => runtime.error?.(danger(message)),
          channel: "slack",
          action: "stop",
          target: typingTarget,
          error: err,
        });
      },
    },
  });

  const slackStreaming = resolveSlackStreamingConfig({
    streaming: account.config.streaming,
    nativeStreaming: resolveChannelStreamingNativeTransport(account.config),
  });
  const streamThreadHint = resolveSlackStreamingThreadHint({
    replyToMode: prepared.replyToMode,
    incomingThreadTs,
    messageTs,
    isThreadReply,
  });
  const previewStreamingEnabled = shouldEnableSlackPreviewStreaming({
    mode: slackStreaming.mode,
    isDirectMessage: prepared.isDirectMessage,
    threadTs: streamThreadHint,
  });
  const streamingEnabled = isSlackStreamingEnabled({
    mode: slackStreaming.mode,
    nativeStreaming: slackStreaming.nativeStreaming,
  });
  const progressPlanStreamingEnabled = shouldUseSlackProgressPlanStream({
    mode: slackStreaming.mode,
    threadTs: streamThreadHint,
  });
  const useStreaming = shouldUseStreaming({
    streamingEnabled,
    threadTs: streamThreadHint,
  });
  const shouldUseDraftStream = shouldInitializeSlackDraftStream({
    previewStreamingEnabled,
    useStreaming,
  });
  let streamSession: SlackStreamSession | null = null;
  let progressStreamSession: SlackChunkStreamSession | null = null;
  let streamFailed = false;
  let usedReplyThreadTs: string | undefined;
  let observedReplyDelivery = false;
  const deliveryTracker = createSlackTurnDeliveryTracker();
  let progressUpdateChain: Promise<void> = Promise.resolve();
  let progressPlanStarted = false;
  let progressUnderstandCompleted = false;
  let progressToolsActivated = false;
  let progressComposeStarted = false;
  let progressComposeCompleted = false;
  const activeProgressToolTasks: string[] = [];
  const progressToolTaskIdsByItemId = new Map<string, string>();
  const progressToolTaskTitles = new Map<string, string>();

  const queueProgressUpdate = (callback: () => Promise<void>) => {
    progressUpdateChain = progressUpdateChain.then(callback).catch((err) => {
      runtime.error?.(danger(`slack progress stream failed: ${String(err)}`));
    });
    return progressUpdateChain;
  };

  const ensureProgressPlanStream = async () => {
    if (!progressPlanStreamingEnabled || progressPlanStarted) {
      return;
    }
    if (!streamThreadHint) {
      return;
    }
    progressStreamSession = await startSlackChunkStream({
      client: ctx.app.client,
      channel: message.channel,
      threadTs: streamThreadHint,
      teamId: ctx.teamId,
      userId: message.user,
      taskDisplayMode: "plan",
      chunks: [
        createSlackTaskUpdateChunk({
          taskId: "understand_request",
          title: "Understand request",
          status: "in_progress",
        }),
        createSlackTaskUpdateChunk({
          taskId: "run_tools",
          title: "Run tools",
          status: "pending",
        }),
        createSlackTaskUpdateChunk({
          taskId: "compose_response",
          title: "Compose response",
          status: "pending",
        }),
      ],
    });
    progressPlanStarted = true;
    didSetStatus = false;
    await ctx.setSlackThreadStatus({
      channelId: message.channel,
      threadTs: streamThreadHint,
      status: "",
    });
  };

  const appendProgressTaskUpdates = async (chunks: SlackStreamChunk[]) => {
    if (!progressPlanStreamingEnabled) {
      return;
    }
    await ensureProgressPlanStream();
    if (!progressStreamSession || chunks.length === 0) {
      return;
    }
    await appendSlackChunkStream({
      session: progressStreamSession,
      chunks,
    });
  };

  const completeProgressUnderstand = async () => {
    if (progressUnderstandCompleted) {
      return;
    }
    progressUnderstandCompleted = true;
    await appendProgressTaskUpdates([
      createSlackTaskUpdateChunk({
        taskId: "understand_request",
        title: "Understand request",
        status: "complete",
      }),
    ]);
  };

  const setProgressToolsStatus = async (
    status: "pending" | "in_progress" | "complete" | "error",
  ) => {
    await appendProgressTaskUpdates([
      createSlackTaskUpdateChunk({
        taskId: "run_tools",
        title: "Run tools",
        status,
      }),
    ]);
  };

  const setProgressComposeStatus = async (
    status: "pending" | "in_progress" | "complete" | "error",
  ) => {
    if (status === "in_progress") {
      progressComposeStarted = true;
    }
    if (status === "complete") {
      progressComposeCompleted = true;
    }
    await appendProgressTaskUpdates([
      createSlackTaskUpdateChunk({
        taskId: "compose_response",
        title: "Compose response",
        status,
      }),
    ]);
  };

  const deliverNormally = async (params: {
    payload: ReplyPayload;
    kind: ReplyDispatchKind;
    forcedThreadTs?: string;
  }): Promise<void> => {
    const replyThreadTs = params.forcedThreadTs ?? replyPlan.nextThreadTs();
    if (
      deliveryTracker.hasDelivered({
        kind: params.kind,
        payload: params.payload,
        threadTs: replyThreadTs,
      })
    ) {
      logVerbose("slack: suppressed duplicate normal delivery within the same turn");
      return;
    }
    await deliverReplies({
      replies: [params.payload],
      target: prepared.replyTarget,
      token: ctx.botToken,
      accountId: account.accountId,
      runtime,
      textLimit: ctx.textLimit,
      replyThreadTs,
      replyToMode: prepared.replyToMode,
      ...(slackIdentity ? { identity: slackIdentity } : {}),
    });
    observedReplyDelivery = true;
    // Record the thread ts only after confirmed delivery success.
    if (replyThreadTs) {
      usedReplyThreadTs ??= replyThreadTs;
    }
    replyPlan.markSent();
    deliveryTracker.markDelivered({
      kind: params.kind,
      payload: params.payload,
      threadTs: replyThreadTs,
    });
  };

  const deliverWithStreaming = async (params: {
    payload: ReplyPayload;
    kind: ReplyDispatchKind;
  }): Promise<void> => {
    const reply = resolveSendableOutboundReplyParts(params.payload);
    if (
      streamFailed ||
      reply.hasMedia ||
      readSlackReplyBlocks(params.payload)?.length ||
      !reply.hasText
    ) {
      await deliverNormally({
        payload: params.payload,
        kind: params.kind,
        forcedThreadTs: streamSession?.threadTs,
      });
      return;
    }

    const text = reply.trimmedText;
    let plannedThreadTs: string | undefined;
    try {
      if (!streamSession) {
        const streamThreadTs = replyPlan.nextThreadTs();
        plannedThreadTs = streamThreadTs;
        if (!streamThreadTs) {
          logVerbose(
            "slack-stream: no reply thread target for stream start, falling back to normal delivery",
          );
          streamFailed = true;
          await deliverNormally({ payload: params.payload, kind: params.kind });
          return;
        }
        if (
          deliveryTracker.hasDelivered({
            kind: params.kind,
            payload: params.payload,
            threadTs: streamThreadTs,
            textOverride: text,
          })
        ) {
          logVerbose("slack-stream: suppressed duplicate stream start payload");
          return;
        }

        streamSession = await startSlackStream({
          client: ctx.app.client,
          channel: message.channel,
          threadTs: streamThreadTs,
          text,
          teamId: ctx.teamId,
          userId: message.user,
        });
        observedReplyDelivery = true;
        usedReplyThreadTs ??= streamThreadTs;
        replyPlan.markSent();
        deliveryTracker.markDelivered({
          kind: params.kind,
          payload: params.payload,
          threadTs: streamThreadTs,
          textOverride: text,
        });
        return;
      }
      if (
        deliveryTracker.hasDelivered({
          kind: params.kind,
          payload: params.payload,
          threadTs: streamSession.threadTs,
          textOverride: text,
        })
      ) {
        logVerbose("slack-stream: suppressed duplicate append payload");
        return;
      }

      await appendSlackStream({
        session: streamSession,
        text: "\n" + text,
      });
      deliveryTracker.markDelivered({
        kind: params.kind,
        payload: params.payload,
        threadTs: streamSession.threadTs,
        textOverride: text,
      });
    } catch (err) {
      runtime.error?.(
        danger(`slack-stream: streaming API call failed: ${String(err)}, falling back`),
      );
      streamFailed = true;
      await deliverNormally({
        payload: params.payload,
        kind: params.kind,
        forcedThreadTs: streamSession?.threadTs ?? plannedThreadTs,
      });
    }
  };

  const { dispatcher, replyOptions, markDispatchIdle } = createReplyDispatcherWithTyping({
    ...replyPipeline,
    humanDelay: resolveHumanDelayConfig(cfg, route.agentId),
    deliver: async (payload, info) => {
      if (useStreaming) {
        await deliverWithStreaming({ payload, kind: info.kind });
        return;
      }

      const reply = resolveSendableOutboundReplyParts(payload);
      const slackBlocks = readSlackReplyBlocks(payload);
      const draftMessageId = draftStream?.messageId();
      const draftChannelId = draftStream?.channelId();
      const trimmedFinalText = reply.trimmedText;
      const canFinalizeViaPreviewEdit =
        previewStreamingEnabled &&
        streamMode !== "status_final" &&
        !reply.hasMedia &&
        !payload.isError &&
        (trimmedFinalText.length > 0 || Boolean(slackBlocks?.length)) &&
        typeof draftMessageId === "string" &&
        typeof draftChannelId === "string";

      if (canFinalizeViaPreviewEdit) {
        const finalThreadTs = usedReplyThreadTs ?? statusThreadTs;
        if (deliveryTracker.hasDelivered({ kind: info.kind, payload, threadTs: finalThreadTs })) {
          observedReplyDelivery = true;
          return;
        }
        draftStream?.stop();
        try {
          await finalizeSlackPreviewEdit({
            client: ctx.app.client,
            token: ctx.botToken,
            accountId: account.accountId,
            channelId: draftChannelId,
            messageId: draftMessageId,
            text: normalizeSlackOutboundText(trimmedFinalText),
            ...(slackBlocks?.length ? { blocks: slackBlocks } : {}),
            threadTs: finalThreadTs,
          });
          observedReplyDelivery = true;
          deliveryTracker.markDelivered({ kind: info.kind, payload, threadTs: finalThreadTs });
          return;
        } catch (err) {
          logVerbose(
            `slack: preview final edit failed; falling back to standard send (${String(err)})`,
          );
        }
      } else if (previewStreamingEnabled && streamMode === "status_final" && hasStreamedMessage) {
        try {
          const statusChannelId = draftStream?.channelId();
          const statusMessageId = draftStream?.messageId();
          if (statusChannelId && statusMessageId) {
            await ctx.app.client.chat.update({
              token: ctx.botToken,
              channel: statusChannelId,
              ts: statusMessageId,
              text: "Status: complete. Final answer posted below.",
            });
          }
        } catch (err) {
          logVerbose(`slack: status_final completion update failed (${String(err)})`);
        }
      } else if (reply.hasMedia) {
        await draftStream?.clear();
        hasStreamedMessage = false;
      }

      await deliverNormally({ payload, kind: info.kind });
    },
    onError: (err, info) => {
      runtime.error?.(danger(`slack ${info.kind} reply failed: ${String(err)}`));
      replyPipeline.typingCallbacks?.onIdle?.();
    },
  });

  const draftStream = shouldUseDraftStream
    ? createSlackDraftStream({
        target: prepared.replyTarget,
        token: ctx.botToken,
        accountId: account.accountId,
        maxChars: Math.min(ctx.textLimit, SLACK_TEXT_LIMIT),
        resolveThreadTs: () => {
          const ts = replyPlan.nextThreadTs();
          if (ts) {
            usedReplyThreadTs ??= ts;
          }
          return ts;
        },
        onMessageSent: () => replyPlan.markSent(),
        log: logVerbose,
        warn: logVerbose,
      })
    : undefined;
  let hasStreamedMessage = false;
  const streamMode = slackStreaming.draftMode;
  let appendRenderedText = "";
  let appendSourceText = "";
  let statusUpdateCount = 0;
  const updateDraftFromPartial = (text?: string) => {
    const trimmed = text?.trimEnd();
    if (!trimmed) {
      return;
    }

    if (streamMode === "append") {
      const next = applyAppendOnlyStreamUpdate({
        incoming: trimmed,
        rendered: appendRenderedText,
        source: appendSourceText,
      });
      appendRenderedText = next.rendered;
      appendSourceText = next.source;
      if (!next.changed) {
        return;
      }
      draftStream?.update(next.rendered);
      hasStreamedMessage = true;
      return;
    }

    if (streamMode === "status_final") {
      statusUpdateCount += 1;
      if (statusUpdateCount > 1 && statusUpdateCount % 4 !== 0) {
        return;
      }
      draftStream?.update(buildStatusFinalPreviewText(statusUpdateCount));
      hasStreamedMessage = true;
      return;
    }

    draftStream?.update(trimmed);
    hasStreamedMessage = true;
  };
  const onDraftBoundary = !shouldUseDraftStream
    ? undefined
    : async () => {
        if (hasStreamedMessage) {
          draftStream?.forceNewMessage();
          hasStreamedMessage = false;
          appendRenderedText = "";
          appendSourceText = "";
          statusUpdateCount = 0;
        }
      };

  if (progressPlanStreamingEnabled) {
    await queueProgressUpdate(async () => {
      await ensureProgressPlanStream();
    });
  }

  let dispatchError: unknown;
  let queuedFinal = false;
  let counts: { final?: number; block?: number } = {};
  try {
    const result = await dispatchInboundMessage({
      ctx: prepared.ctxPayload,
      cfg,
      dispatcher,
      replyOptions: {
        ...replyOptions,
        skillFilter: prepared.channelConfig?.skills,
        hasRepliedRef,
        disableBlockStreaming: useStreaming
          ? true
          : typeof resolveChannelStreamingBlockEnabled(account.config) === "boolean"
            ? !resolveChannelStreamingBlockEnabled(account.config)
            : undefined,
        onModelSelected,
        onPartialReply: useStreaming
          ? undefined
          : !previewStreamingEnabled
            ? undefined
            : async (payload) => {
                updateDraftFromPartial(payload.text);
              },
        onAssistantMessageStart: async () => {
          await onDraftBoundary?.();
          if (!progressPlanStreamingEnabled) {
            return;
          }
          await queueProgressUpdate(async () => {
            await completeProgressUnderstand();
            if (!progressComposeStarted) {
              for (const taskId of activeProgressToolTasks.splice(0)) {
                await appendProgressTaskUpdates([
                  createSlackTaskUpdateChunk({
                    taskId,
                    title: progressToolTaskTitles.get(taskId) ?? "Use tool",
                    status: "complete",
                  }),
                ]);
                progressToolTaskTitles.delete(taskId);
              }
              await setProgressToolsStatus("complete");
              await setProgressComposeStatus("in_progress");
            }
          });
        },
        onReasoningEnd: onDraftBoundary,
        onReasoningStream: statusReactionsEnabled
          ? async () => {
              await statusReactions.setThinking();
            }
          : undefined,
        onToolStart: async (payload) => {
          if (statusReactionsEnabled) {
            await statusReactions.setTool(payload.name);
          }
          if (progressPlanStreamingEnabled) {
            return;
          }
          if (!payload.name || !statusThreadTs) {
            return;
          }
          didSetStatus = true;
          await ctx.setSlackThreadStatus({
            channelId: message.channel,
            threadTs: statusThreadTs,
            status: toolStatusLabel(payload.name),
          });
        },
        onItemEvent: async (payload) => {
          if (!progressPlanStreamingEnabled) {
            return;
          }
          await queueProgressUpdate(async () => {
            if (payload.kind !== "tool" || !payload.itemId || !payload.title) {
              return;
            }
            await completeProgressUnderstand();
            if (!progressToolsActivated) {
              progressToolsActivated = true;
              await setProgressToolsStatus("in_progress");
            }

            let taskId = progressToolTaskIdsByItemId.get(payload.itemId);
            if (!taskId) {
              taskId =
                payload.itemId
                  .replace(/[^a-z0-9]+/gi, "_")
                  .replace(/^_+|_+$/g, "")
                  .toLowerCase() || "tool";
              progressToolTaskIdsByItemId.set(payload.itemId, taskId);
              progressToolTaskTitles.set(taskId, payload.title);
              activeProgressToolTasks.push(taskId);
            }

            if (
              payload.phase === "start" ||
              payload.phase === "update" ||
              payload.status === "running"
            ) {
              await appendProgressTaskUpdates([
                createSlackTaskUpdateChunk({
                  taskId,
                  title: payload.title,
                  status: "in_progress",
                }),
              ]);
              return;
            }

            if (
              payload.phase === "end" ||
              payload.status === "completed" ||
              payload.status === "failed"
            ) {
              const isError = payload.status === "failed";
              await appendProgressTaskUpdates([
                createSlackTaskUpdateChunk({
                  taskId,
                  title: progressToolTaskTitles.get(taskId) ?? payload.title,
                  status: isError ? "error" : "complete",
                }),
              ]);
              progressToolTaskIdsByItemId.delete(payload.itemId);
              progressToolTaskTitles.delete(taskId);
              const index = activeProgressToolTasks.indexOf(taskId);
              if (index >= 0) {
                activeProgressToolTasks.splice(index, 1);
              }
              if (activeProgressToolTasks.length === 0 && !progressComposeStarted) {
                await setProgressToolsStatus(isError ? "error" : "complete");
              }
            }
          });
        },
      },
    });
    queuedFinal = result.queuedFinal;
    counts = result.counts;
  } catch (err) {
    dispatchError = err;
  } finally {
    await draftStream?.flush();
    draftStream?.stop();
    markDispatchIdle();
  }

  // -----------------------------------------------------------------------
  // Finalize the stream if one was started
  // -----------------------------------------------------------------------
  const finalStream = streamSession as SlackStreamSession | null;
  if (finalStream && !finalStream.stopped) {
    try {
      await stopSlackStream({ session: finalStream });
    } catch (err) {
      runtime.error?.(danger(`slack-stream: failed to stop stream: ${String(err)}`));
    }
  }

  await progressUpdateChain;
  const finalProgressStream = progressStreamSession as SlackChunkStreamSession | null;
  if (finalProgressStream && !finalProgressStream.stopped) {
    try {
      if (dispatchError) {
        await appendProgressTaskUpdates([
          createSlackTaskUpdateChunk({
            taskId: progressComposeStarted ? "compose_response" : "understand_request",
            title: progressComposeStarted ? "Compose response" : "Understand request",
            status: "error",
          }),
        ]);
        if (progressToolsActivated && activeProgressToolTasks.length > 0) {
          for (const taskId of activeProgressToolTasks.splice(0)) {
            await appendProgressTaskUpdates([
              createSlackTaskUpdateChunk({
                taskId,
                title: progressToolTaskTitles.get(taskId) ?? "Use tool",
                status: "error",
              }),
            ]);
            progressToolTaskTitles.delete(taskId);
          }
          await setProgressToolsStatus("error");
        }
      } else {
        if (!progressUnderstandCompleted) {
          await completeProgressUnderstand();
        }
        if (!progressToolsActivated) {
          await setProgressToolsStatus("complete");
        }
        if (!progressComposeStarted) {
          await setProgressComposeStatus("in_progress");
        }
        if (!progressComposeCompleted) {
          await setProgressComposeStatus("complete");
        }
      }
      await stopSlackChunkStream({ session: finalProgressStream });
    } catch (err) {
      runtime.error?.(danger(`slack progress stream failed to stop: ${String(err)}`));
    }
  }

  const anyReplyDelivered =
    observedReplyDelivery || queuedFinal || (counts.block ?? 0) > 0 || (counts.final ?? 0) > 0;

  if (statusReactionsEnabled) {
    if (dispatchError) {
      await statusReactions.setError();
      if (ctx.removeAckAfterReply) {
        void (async () => {
          await sleep(statusReactionTiming.errorHoldMs);
          if (anyReplyDelivered) {
            await statusReactions.clear();
            return;
          }
          await statusReactions.restoreInitial();
        })();
      } else {
        void statusReactions.restoreInitial();
      }
    } else if (anyReplyDelivered) {
      await statusReactions.setDone();
      if (ctx.removeAckAfterReply) {
        void (async () => {
          await sleep(statusReactionTiming.doneHoldMs);
          await statusReactions.clear();
        })();
      } else {
        void statusReactions.restoreInitial();
      }
    } else {
      // Silent success should preserve queued state and clear any stall timers
      // instead of transitioning to terminal/stall reactions after return.
      await statusReactions.restoreInitial();
    }
  }

  if (dispatchError) {
    throw dispatchError;
  }

  // Record thread participation only when we actually delivered a reply and
  // know the thread ts that was used (set by deliverNormally, streaming start,
  // or draft stream). Falls back to statusThreadTs for edge cases.
  const participationThreadTs = usedReplyThreadTs ?? statusThreadTs;
  if (anyReplyDelivered && participationThreadTs) {
    recordSlackThreadParticipation(account.accountId, message.channel, participationThreadTs);
  }

  if (!anyReplyDelivered) {
    await draftStream?.clear();
    if (prepared.isRoomish) {
      clearHistoryEntriesIfEnabled({
        historyMap: ctx.channelHistories,
        historyKey: prepared.historyKey,
        limit: ctx.historyLimit,
      });
    }
    return;
  }

  if (shouldLogVerbose()) {
    const finalCount = counts.final;
    logVerbose(
      `slack: delivered ${finalCount} reply${finalCount === 1 ? "" : "ies"} to ${prepared.replyTarget}`,
    );
  }

  if (!statusReactionsEnabled) {
    removeAckReactionAfterReply({
      removeAfterReply: ctx.removeAckAfterReply && anyReplyDelivered,
      ackReactionPromise: prepared.ackReactionPromise,
      ackReactionValue: prepared.ackReactionValue,
      remove: () =>
        removeSlackReaction(
          message.channel,
          prepared.ackReactionMessageTs ?? "",
          prepared.ackReactionValue,
          {
            token: ctx.botToken,
            client: ctx.app.client,
          },
        ),
      onError: (err) => {
        logAckFailure({
          log: logVerbose,
          channel: "slack",
          target: `${message.channel}/${message.ts}`,
          error: err,
        });
      },
    });
  }

  if (prepared.isRoomish) {
    clearHistoryEntriesIfEnabled({
      historyMap: ctx.channelHistories,
      historyKey: prepared.historyKey,
      limit: ctx.historyLimit,
    });
  }
}
