/**
 * WeCom (企业微信智能机器人) Streaming Message Controller
 *
 * Implements typewriter-style progressive delivery over the WeCom aibot
 * WebSocket long-connection using `WSClient.replyStream()`.
 *
 * Protocol (see @wecom/aibot-node-sdk):
 *   replyStream(frame, streamId, content, finish)
 *   - `frame`   : the ORIGINAL inbound WsFrame (carries headers.req_id) — the
 *                 reply is bound to that req_id, so the connection stashes the
 *                 latest inbound frame per chatId and hands it in via `sendStream`.
 *   - `streamId`: a stable id from generateReqId('stream'), constant per session.
 *   - `content` : FULL markdown text so far (replace semantics — each call
 *                 overwrites the streaming bubble, unlike QQ's strict prefix rule).
 *   - `finish`  : false for incremental chunks, true exactly once at the end.
 *
 * Lifecycle: idle → streaming → completing/aborting → completed/aborted
 * Fallback : if replyStream fails (e.g. no inbound frame / rate limit), falls
 *            back to a plain `sendMessage` with the final text.
 *
 * Transient status vs final text
 * ──────────────────────────────
 * HappyClaw pushes transient runtime status (e.g. "上下文压缩中") via
 * setSystemStatus()/setThinking(). WeCom's replace semantics let us render that
 * as a blockquote status line ABOVE the streamed body while `finish=false`, and
 * then drop it entirely on the DONE chunk — so the status never leaks into the
 * final message body. This is the whole point of the streaming path vs the old
 * single plain sendMessage (which appended the status to the text).
 */

import { logger } from './logger.js';

// ─── Constants ───────────────────────────────────────────────

/** Throttle between streaming replyStream calls (ms). WeCom queues per req_id
 *  (default cap 500), so keep updates coarse to avoid backpressure. */
const STREAM_UPDATE_INTERVAL = 700;

/** Official maximum for one WeCom intelligent-bot markdown payload. */
export const WECOM_MARKDOWN_MAX_BYTES = 20_480;

function utf8Bytes(value: string): number {
  return Buffer.byteLength(value, 'utf8');
}

/** Keep a streaming preview inside the byte limit without splitting Unicode. */
export function truncateWeComUtf8(
  value: string,
  maxBytes = WECOM_MARKDOWN_MAX_BYTES,
  suffix = '\n\n> 内容较长，完成后将分段发送…',
): string {
  if (utf8Bytes(value) <= maxBytes) return value;
  const budget = Math.max(0, maxBytes - utf8Bytes(suffix));
  let result = '';
  let bytes = 0;
  for (const char of value) {
    const size = utf8Bytes(char);
    if (bytes + size > budget) break;
    result += char;
    bytes += size;
  }
  return `${result.trimEnd()}${suffix}`;
}

// ─── Callback types ──────────────────────────────────────────

/** Send one streaming frame. Resolves on ack; rejects on transport error
 *  (e.g. no stashed inbound frame for this chat). */
export type WeComSendStreamFn = (
  content: string,
  finish: boolean,
) => Promise<void>;

/** Plain fallback send (non-streaming) used when streaming is impossible. */
export type WeComFallbackSendFn = (text: string) => Promise<void>;

type StreamingState =
  | 'idle'
  | 'streaming'
  | 'completing'
  | 'aborting'
  | 'completed'
  | 'aborted';

// ─── Controller ──────────────────────────────────────────────

export class WeComStreamingController {
  private state: StreamingState = 'idle';
  private accumulatedText = '';
  private sentChunkCount = 0;

  // Throttle / serialization (single flush in flight at a time)
  private lastUpdateTime = 0;
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private currentFlushPromise: Promise<void> | null = null;
  private flushPending = false;

  // Dependencies
  private readonly chatId: string;
  private readonly sendStream: WeComSendStreamFn;
  private readonly fallbackSend: WeComFallbackSendFn;
  private fallbackPromise: Promise<void> | null = null;

  // Transient status (rendered in streaming view only, NEVER in final text)
  private systemStatus: string | null = null;
  private thinking = false;
  private thinkingText = '';

  // Auxiliary tracking (kept for parity with the streaming-session surface;
  // not surfaced to the WeCom bubble to keep the status line compact).
  private tools = new Map<
    string,
    {
      name: string;
      status: 'running' | 'complete' | 'error';
      startTime: number;
      summary?: string;
    }
  >();
  private recentEvents: string[] = [];

  private static readonly MAX_THINKING_CHARS = 300;

  constructor(opts: {
    chatId: string;
    sendStream: WeComSendStreamFn;
    fallbackSend: WeComFallbackSendFn;
  }) {
    this.chatId = opts.chatId;
    this.sendStream = opts.sendStream;
    this.fallbackSend = opts.fallbackSend;
  }

  // ─── StreamingSession interface ─────────────────────────────

  isActive(): boolean {
    return this.state === 'idle' || this.state === 'streaming';
  }

  append(text: string): void {
    if (!this.isActive()) return;
    const isFirst = this.accumulatedText.length === 0;
    this.accumulatedText = text;
    // Real text has arrived — the thinking indicator is no longer relevant.
    this.thinking = false;
    this.thinkingText = '';
    if (isFirst) {
      logger.debug(
        { chatId: this.chatId, textLen: text.length },
        'WeCom streaming first append',
      );
    }
    this.scheduleFlush();
  }

  async complete(finalText: string): Promise<void> {
    if (
      this.state === 'completed' ||
      this.state === 'aborted' ||
      this.state === 'completing' ||
      this.state === 'aborting'
    ) {
      return;
    }
    // Block late append()s during the awaits below.
    this.state = 'completing';
    this.clearTimers();
    if (this.currentFlushPromise)
      await this.currentFlushPromise.catch(() => {});

    this.accumulatedText = finalText;
    logger.info(
      {
        chatId: this.chatId,
        sentChunks: this.sentChunkCount,
        textLen: finalText.length,
      },
      'WeCom streaming complete() entry',
    );

    // Final bubble carries ONLY the answer text — no transient status line.
    const finalBody = finalText.trim();
    if (!finalBody) {
      // Nothing to say. If we already opened a stream, close it empty so the
      // bubble is finalized; otherwise there is simply nothing to send.
      if (this.sentChunkCount > 0) {
        await this.sendStream(this.accumulatedText || '', true);
      }
      this.state = 'completed';
      return;
    }

    // A stream is one provider message and cannot be paginated. Close an
    // already-open preview, then deliver the complete answer through the
    // ACK-governed proactive pagination path.
    if (utf8Bytes(finalBody) > WECOM_MARKDOWN_MAX_BYTES) {
      if (this.sentChunkCount > 0) {
        await this.sendStream('内容较长，完整回复将分段发送。', true).catch(
          (err: any) => {
            logger.warn(
              { err: err?.message, chatId: this.chatId },
              'WeCom oversized stream close failed; continuing with pagination',
            );
          },
        );
      }
      await this.tryFallback(finalBody);
      this.state = 'completed';
      return;
    }

    try {
      await this.sendStream(finalBody, true); // DONE
      this.sentChunkCount++;
      this.state = 'completed';
      logger.info(
        { chatId: this.chatId, chunks: this.sentChunkCount },
        'WeCom streaming completed',
      );
    } catch (err: any) {
      logger.warn(
        { err: err?.message, chatId: this.chatId },
        'WeCom streaming finalize failed, using fallback',
      );
      // Preview bubble already exists. A plain sendMessage would deliver a
      // second full copy of the same reply.
      if (this.sentChunkCount === 0) {
        await this.tryFallback(finalBody);
      }
      this.state = 'completed';
    }
  }

  async abort(reason?: string): Promise<void> {
    if (
      this.state === 'completed' ||
      this.state === 'aborted' ||
      this.state === 'completing' ||
      this.state === 'aborting'
    ) {
      return;
    }
    this.state = 'aborting';
    this.clearTimers();
    if (this.currentFlushPromise)
      await this.currentFlushPromise.catch(() => {});

    const notice = `⚠️ 已中断: ${reason ?? '用户取消'}`;
    const body = this.accumulatedText.trim()
      ? `${this.accumulatedText}\n\n${notice}`
      : notice;
    try {
      await this.sendStream(body, true); // DONE
      this.sentChunkCount++;
    } catch (err: any) {
      logger.debug(
        { err: err?.message, chatId: this.chatId },
        'WeCom streaming abort chunk failed, using fallback',
      );
      await this.tryFallback(body);
    }
    this.state = 'aborted';
  }

  dispose(): void {
    this.clearTimers();
  }

  // ─── Transient status / auxiliary surface ───────────────────

  setSystemStatus(status: string | null): void {
    this.systemStatus = status;
    // Refresh the status line live while streaming.
    if (this.state === 'streaming' || this.state === 'idle')
      this.scheduleFlush();
  }

  setThinking(): void {
    this.thinking = true;
    if (this.state === 'streaming' || this.state === 'idle')
      this.scheduleFlush();
  }

  appendThinking(text: string): void {
    this.thinkingText += text;
    if (
      this.thinkingText.length > WeComStreamingController.MAX_THINKING_CHARS
    ) {
      this.thinkingText =
        '...' +
        this.thinkingText.slice(
          -(WeComStreamingController.MAX_THINKING_CHARS - 3),
        );
    }
    this.thinking = true;
    if (this.state === 'streaming' || this.state === 'idle')
      this.scheduleFlush();
  }

  setHook(_hook: { hookName: string; hookEvent: string } | null): void {
    // Not surfaced for WeCom plain-text streaming.
  }

  setTodos(
    _todos: Array<{ id: string; content: string; status: string }>,
  ): void {
    // Too verbose for the compact WeCom status line.
  }

  pushRecentEvent(text: string): void {
    this.recentEvents.push(text);
    if (this.recentEvents.length > 5)
      this.recentEvents = this.recentEvents.slice(-5);
  }

  startTool(toolId: string, toolName: string): void {
    this.tools.set(toolId, {
      name: toolName,
      status: 'running',
      startTime: Date.now(),
    });
  }

  endTool(toolId: string, isError: boolean): void {
    const tc = this.tools.get(toolId);
    if (tc) tc.status = isError ? 'error' : 'complete';
  }

  updateToolSummary(toolId: string, summary: string): void {
    const tc = this.tools.get(toolId);
    if (tc) tc.summary = summary;
  }

  getToolInfo(toolId: string): { name: string } | undefined {
    return this.tools.get(toolId);
  }

  async patchUsageNote(_usage: {
    inputTokens: number;
    outputTokens: number;
    costUSD: number;
    durationMs: number;
    numTurns: number;
  }): Promise<void> {}

  getAllMessageIds(): string[] {
    return [];
  }

  // ─── Internal ───────────────────────────────────────────────

  /** Compose the transient status line shown ONLY during streaming. */
  private statusLine(): string | null {
    if (this.systemStatus) return `⏳ ${this.systemStatus}`;
    if (this.thinking) {
      return this.thinkingText ? `💭 ${this.thinkingText}` : '💭 思考中…';
    }
    return null;
  }

  /** Render the streaming bubble: status line (transient) + body. */
  private renderStreaming(): string {
    const status = this.statusLine();
    const body = this.accumulatedText;
    if (status && body) return `> ${status}\n\n${body}`;
    if (status) return `> ${status}`;
    return body;
  }

  private scheduleFlush(): void {
    this.flushPending = true;
    // Serialize: at most one flush in flight or scheduled.
    if (this.flushTimer || this.currentFlushPromise) return;
    const elapsed = Date.now() - this.lastUpdateTime;
    const delay = Math.max(0, STREAM_UPDATE_INTERVAL - elapsed);
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null;
      this.flushPending = false;
      this.currentFlushPromise = this.doFlush()
        .catch((err: any) => {
          logger.debug(
            { err: err?.message, chatId: this.chatId },
            'WeCom streaming flush failed',
          );
        })
        .finally(() => {
          this.currentFlushPromise = null;
          if (this.flushPending && this.isActive()) this.scheduleFlush();
        });
    }, delay);
  }

  private async doFlush(): Promise<void> {
    if (!this.isActive()) return;
    const content = this.renderStreaming();
    if (!content.trim()) return;
    await this.sendStream(truncateWeComUtf8(content), false); // GENERATING
    this.sentChunkCount++;
    this.lastUpdateTime = Date.now();
    if (this.state === 'idle') this.state = 'streaming';
  }

  private async tryFallback(text: string): Promise<void> {
    this.fallbackPromise ??= this.fallbackSend(text);
    try {
      await this.fallbackPromise;
    } catch (err: any) {
      logger.warn(
        { err: err?.message, chatId: this.chatId },
        'WeCom streaming fallback send also failed',
      );
      throw err;
    }
  }

  private clearTimers(): void {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
  }
}
