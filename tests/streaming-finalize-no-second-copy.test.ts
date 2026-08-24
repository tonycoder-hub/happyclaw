import { afterEach, describe, expect, test, vi } from 'vitest';

vi.mock('../src/logger.js', () => ({
  logger: {
    info: vi.fn(),
    warn: vi.fn(),
    debug: vi.fn(),
    error: vi.fn(),
  },
}));

import { DiscordStreamingEditController } from '../src/discord-streaming-edit.js';
import { QQStreamingController } from '../src/qq-streaming-card.js';
import { WeComStreamingController } from '../src/wecom-streaming.js';

afterEach(() => {
  vi.useRealTimers();
  vi.clearAllMocks();
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
});
