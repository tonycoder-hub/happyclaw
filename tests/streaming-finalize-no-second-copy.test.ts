import { afterEach, describe, expect, test, vi } from 'vitest';

const dingtalkHttps = vi.hoisted(() => {
  let failFinalize = false;
  const requests: Array<{
    hostname: string;
    path: string;
    method: string;
    body?: Record<string, unknown>;
  }> = [];

  return {
    requests,
    setFailFinalize(value: boolean) {
      failFinalize = value;
    },
    reset() {
      failFinalize = false;
      requests.length = 0;
    },
    request(options: any, cb: (res: any) => void) {
      const chunks: Buffer[] = [];
      const req = {
        on() {
          return req;
        },
        write(data: string) {
          chunks.push(Buffer.from(data));
        },
        end() {
          const bodyStr = Buffer.concat(chunks).toString('utf-8');
          let body: Record<string, unknown> | undefined;
          try {
            body = bodyStr ? JSON.parse(bodyStr) : undefined;
          } catch {
            body = undefined;
          }
          requests.push({
            hostname: options.hostname,
            path: options.path,
            method: options.method,
            body,
          });

          let payload: Record<string, unknown> = { success: true };
          if (options.hostname === 'oapi.dingtalk.com') {
            payload = {
              errcode: 0,
              access_token: 'tok',
              expires_in: 7200,
            };
          } else if (
            String(options.path).includes('/card/streaming') &&
            body?.isFinalize &&
            failFinalize
          ) {
            payload = { code: 'InternalError', message: 'finalize failed' };
          }

          const listeners: Record<string, Array<(arg?: unknown) => void>> = {
            data: [],
            end: [],
            error: [],
          };
          const res = {
            statusCode: 200,
            on(event: string, handler: (arg?: unknown) => void) {
              (listeners[event] ??= []).push(handler);
              return res;
            },
          };
          queueMicrotask(() => {
            cb(res);
            queueMicrotask(() => {
              const buf = Buffer.from(JSON.stringify(payload));
              for (const handler of listeners.data) handler(buf);
              for (const handler of listeners.end) handler();
            });
          });
        },
      };
      return req;
    },
  };
});

vi.mock('node:https', () => ({
  default: { request: dingtalkHttps.request },
}));

vi.mock('../src/logger.js', () => ({
  logger: {
    info: vi.fn(),
    warn: vi.fn(),
    debug: vi.fn(),
    error: vi.fn(),
  },
}));

import { DiscordStreamingEditController } from '../src/discord-streaming-edit.js';
import {
  DingTalkStreamingCardController,
  type DingTalkStreamingCardConfig,
  type DingTalkCardTarget,
} from '../src/dingtalk-streaming-card.js';
import { QQStreamingController } from '../src/qq-streaming-card.js';
import { WeComStreamingController } from '../src/wecom-streaming.js';

afterEach(() => {
  vi.useRealTimers();
  vi.clearAllMocks();
  dingtalkHttps.reset();
});

describe('streaming finalize must not send a second full copy', () => {
  test('Discord: preview flush + failed final edit does not fallback-send', async () => {
    vi.useFakeTimers();

    const fallbackSend = vi.fn(async () => {});
    let editCount = 0;
    const message = {
      id: 'msg-preview',
      edit: vi.fn(async (_content: string) => {
        editCount += 1;
        // First edit is the preview flush; the finalize edit fails.
        if (editCount > 1) throw new Error('discord finalize edit failed');
        return message;
      }),
    };
    const channel = {
      send: vi.fn(async (_content: string) => message),
    };

    const ctrl = new DiscordStreamingEditController(channel as any, {
      fallbackSend,
    });
    ctrl.append('Hello from the preview');
    await vi.advanceTimersByTimeAsync(600);

    expect(channel.send).toHaveBeenCalledTimes(1);
    expect(message.edit).toHaveBeenCalledTimes(1);
    expect(fallbackSend).not.toHaveBeenCalled();

    await ctrl.complete('Hello from the preview — final');

    expect(message.edit).toHaveBeenCalledTimes(2);
    expect(fallbackSend).not.toHaveBeenCalled();
    expect(channel.send).toHaveBeenCalledTimes(1);
  });

  test('Discord: failed finalize without a flushed preview still fallback-sends', async () => {
    const fallbackSend = vi.fn(async () => {});
    const message = {
      id: 'msg-placeholder',
      edit: vi.fn(async () => {
        throw new Error('discord finalize edit failed');
      }),
    };
    const channel = {
      send: vi.fn(async (_content: string) => message),
    };

    const ctrl = new DiscordStreamingEditController(channel as any, {
      fallbackSend,
    });
    await ctrl.complete('Only the final text');

    expect(fallbackSend).toHaveBeenCalledTimes(1);
    expect(fallbackSend).toHaveBeenCalledWith('Only the final text');
  });

  test('QQ: preview flush + failed DONE chunk does not fallback-send', async () => {
    vi.useFakeTimers();

    const fallbackSend = vi.fn(async () => {});
    const streamCalls: Array<{ input_state: number; content_raw: string }> = [];
    const sendStreamChunk = vi.fn(async (_openid: string, params: any) => {
      streamCalls.push({
        input_state: params.input_state,
        content_raw: params.content_raw,
      });
      if (params.input_state === 10) {
        throw new Error('qq finalize DONE failed');
      }
      return { id: 'stream-msg-1' };
    });

    const ctrl = new QQStreamingController({
      openid: 'user-openid',
      msgSeq: 1,
      sendStreamChunk,
      fallbackSend,
      passiveMsgId: 'passive-1',
    });
    ctrl.append('Hello from the preview');
    await vi.advanceTimersByTimeAsync(600);

    expect(streamCalls).toHaveLength(1);
    expect(streamCalls[0].input_state).toBe(1);
    expect(fallbackSend).not.toHaveBeenCalled();

    await ctrl.complete('Hello from the preview — final');

    expect(streamCalls.some((c) => c.input_state === 10)).toBe(true);
    expect(fallbackSend).not.toHaveBeenCalled();
  });

  test('WeCom: preview flush + failed DONE stream does not fallback-send', async () => {
    vi.useFakeTimers();

    const fallbackSend = vi.fn(async () => {});
    const streamCalls: Array<{ content: string; finish: boolean }> = [];
    const sendStream = vi.fn(async (content: string, finish: boolean) => {
      streamCalls.push({ content, finish });
      if (finish) throw new Error('wecom finalize DONE failed');
    });

    const ctrl = new WeComStreamingController({
      chatId: 'chat-1',
      sendStream,
      fallbackSend,
    });
    ctrl.append('Hello from the preview');
    await vi.advanceTimersByTimeAsync(800);

    expect(streamCalls).toEqual([
      { content: 'Hello from the preview', finish: false },
    ]);
    expect(fallbackSend).not.toHaveBeenCalled();

    await ctrl.complete('Hello from the preview — final');

    expect(streamCalls).toContainEqual({
      content: 'Hello from the preview — final',
      finish: true,
    });
    expect(fallbackSend).not.toHaveBeenCalled();
  });

  test('WeCom: failed finalize without a flushed preview still fallback-sends', async () => {
    const fallbackSend = vi.fn(async () => {});
    const sendStream = vi.fn(async () => {
      throw new Error('wecom finalize DONE failed');
    });

    const ctrl = new WeComStreamingController({
      chatId: 'chat-1',
      sendStream,
      fallbackSend,
    });
    await ctrl.complete('Only the final text');

    expect(fallbackSend).toHaveBeenCalledTimes(1);
    expect(fallbackSend).toHaveBeenCalledWith('Only the final text');
  });

  test('DingTalk: preview flush + failed finalize does not fallback-send', async () => {
    vi.useFakeTimers();
    dingtalkHttps.setFailFinalize(false);

    const fallbackSend = vi.fn(async () => {});
    const config: DingTalkStreamingCardConfig = {
      clientId: 'test_client_id',
      clientSecret: 'test_client_secret',
    };
    const target: DingTalkCardTarget = {
      type: 'group',
      openConversationId: 'cidXXXX',
    };
    const ctrl = new DingTalkStreamingCardController(config, target, {
      fallbackSend,
    });

    ctrl.append('Hello from the preview');
    await vi.advanceTimersByTimeAsync(600);
    await vi.waitFor(() => {
      expect(
        dingtalkHttps.requests.some((req) =>
          String(req.path).includes('/card/streaming'),
        ),
      ).toBe(true);
    });
    expect(fallbackSend).not.toHaveBeenCalled();

    dingtalkHttps.setFailFinalize(true);
    await ctrl.complete('Hello from the preview — final');

    expect(
      dingtalkHttps.requests.some(
        (req) =>
          String(req.path).includes('/card/streaming') &&
          req.body?.isFinalize === true,
      ),
    ).toBe(true);
    expect(fallbackSend).not.toHaveBeenCalled();
  });
});
