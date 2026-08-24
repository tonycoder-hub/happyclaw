import type { Dispatcher } from 'undici';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';

import type {
  WeChatContextTokenClaimInput,
  WeChatContextTokenRecord,
  WeChatContextTokenReleaseInput,
  WeChatContextTokenStore,
} from '../src/wechat-context-token.js';

const dbCalls = vi.hoisted(() => ({
  storeChatMetadata: vi.fn(),
  storeMessageDirect: vi.fn(),
  updateChatName: vi.fn(),
}));

vi.mock('../src/db.js', () => ({
  ...dbCalls,
  isDatabaseInitialized: () => false,
}));
vi.mock('../src/message-notifier.js', () => ({
  notifyNewImMessage: vi.fn(),
}));
vi.mock('../src/web.js', () => ({ broadcastNewMessage: vi.fn() }));
vi.mock('../src/logger.js', () => ({
  logger: { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));

const { createWeChatConnection } = await import('../src/wechat.js');

beforeEach(() => {
  vi.clearAllMocks();
  dbCalls.storeMessageDirect.mockImplementation(() => undefined);
});

class SharedStore implements WeChatContextTokenStore {
  record: WeChatContextTokenRecord | undefined;

  list(accountId: string): WeChatContextTokenRecord[] {
    return this.record?.accountId === accountId ? [{ ...this.record }] : [];
  }

  upsert(input: {
    accountId: string;
    userId: string;
    token: string;
    refreshedAtMs: number;
    sourceMessageId?: string | null;
    sourceSequence?: number | null;
  }): WeChatContextTokenRecord {
    this.record = { ...input, sendCount: 0, lastSentAtMs: null };
    return { ...this.record };
  }

  claim(
    input: WeChatContextTokenClaimInput,
  ):
    | { status: 'claimed'; record: WeChatContextTokenRecord }
    | { status: 'missing' | 'changed' | 'expired' | 'quota_exhausted' } {
    const record = this.record;
    if (!record) return { status: 'missing' };
    if (
      record.token !== input.expectedToken ||
      record.refreshedAtMs !== input.expectedRefreshedAtMs ||
      (input.expectedSourceMessageId !== undefined &&
        (record.sourceMessageId ?? null) !== input.expectedSourceMessageId)
    ) {
      return { status: 'changed' };
    }
    const claimed = {
      ...record,
      sendCount: record.sendCount + input.claimCount,
      lastSentAtMs: input.nowMs,
    };
    this.record = claimed;
    return { status: 'claimed', record: { ...claimed } };
  }

  release(
    input: WeChatContextTokenReleaseInput,
  ):
    | { status: 'released'; record: WeChatContextTokenRecord }
    | { status: 'missing' | 'changed' } {
    const record = this.record;
    if (!record) return { status: 'missing' };
    if (
      record.token !== input.expectedToken ||
      record.refreshedAtMs !== input.expectedRefreshedAtMs ||
      (input.expectedSourceMessageId !== undefined &&
        (record.sourceMessageId ?? null) !== input.expectedSourceMessageId) ||
      record.sendCount < input.releaseCount
    ) {
      return { status: 'changed' };
    }
    const released = {
      ...record,
      sendCount: record.sendCount - input.releaseCount,
    };
    this.record = released;
    return { status: 'released', record: { ...released } };
  }

  delete(input: {
    accountId: string;
    userId: string;
    expectedToken?: string;
    expectedRefreshedAtMs?: number;
    expectedSourceMessageId?: string | null;
  }): boolean {
    if (
      !this.record ||
      this.record.accountId !== input.accountId ||
      this.record.userId !== input.userId ||
      (input.expectedToken !== undefined &&
        (this.record.token !== input.expectedToken ||
          this.record.refreshedAtMs !== input.expectedRefreshedAtMs ||
          (input.expectedSourceMessageId !== undefined &&
            (this.record.sourceMessageId ?? null) !==
              input.expectedSourceMessageId)))
    ) {
      return false;
    }
    this.record = undefined;
    return true;
  }
}

function waitUntilAborted(signal?: AbortSignal | null): Promise<Response> {
  return new Promise((_resolve, reject) => {
    const abort = () =>
      reject(Object.assign(new Error('aborted'), { name: 'AbortError' }));
    if (signal?.aborted) return abort();
    signal?.addEventListener('abort', abort, { once: true });
  });
}

describe('WeChat connection durable context_token integration', () => {
  test('does not consume send quota when media upload fails before sendmessage', async () => {
    const store = new SharedStore();
    store.record = {
      accountId: 'account',
      userId: 'peer',
      token: 'durable-secret',
      refreshedAtMs: Date.now(),
      sourceMessageId: 'inbound-1',
      sourceSequence: 1,
      sendCount: 0,
      lastSentAtMs: null,
    };
    const dispatcher = {
      close: vi.fn(async () => undefined),
    } as unknown as Dispatcher;
    const uploadFailure = new Error('CDN unavailable');
    const uploadMedia = vi.fn(async () => {
      throw uploadFailure;
    });
    const connection = createWeChatConnection(
      {
        botToken: 'bot-token',
        ilinkBotId: 'bot-id',
        logContext: { accountId: 'account' },
      },
      {
        fetch: vi.fn((_url, init) =>
          waitUntilAborted(init?.signal),
        ) as typeof fetch,
        createDispatcher: () => dispatcher,
        contextTokenStore: store,
        uploadMediaBuffer: uploadMedia,
      },
    );
    await connection.connect({ onNewChat: vi.fn() });

    await expect(
      connection.sendImage(
        'peer',
        Buffer.from('image'),
        'image/png',
        'caption',
      ),
    ).rejects.toBe(uploadFailure);
    expect(store.record?.sendCount).toBe(0);
    const filePath = path.join(
      fs.mkdtempSync(path.join(os.tmpdir(), 'happyclaw-wechat-upload-')),
      'report.txt',
    );
    fs.writeFileSync(filePath, 'report');
    try {
      await expect(
        connection.sendFile('peer', filePath, 'report.txt'),
      ).rejects.toBe(uploadFailure);
      expect(store.record?.sendCount).toBe(0);
      expect(uploadMedia).toHaveBeenCalledTimes(2);
    } finally {
      fs.rmSync(path.dirname(filePath), { recursive: true, force: true });
    }

    await connection.disconnect();
  });

  test('multi-chunk send does not burn unused slots or resend a delivered prefix after a mid-batch 502', async () => {
    const store = new SharedStore();
    store.record = {
      accountId: 'account',
      userId: 'peer',
      token: 'durable-secret',
      refreshedAtMs: Date.now(),
      sourceMessageId: 'inbound-1',
      sourceSequence: 1,
      sendCount: 0,
      lastSentAtMs: null,
    };
    const dispatcher = {
      close: vi.fn(async () => undefined),
    } as unknown as Dispatcher;
    const attemptedTexts: string[] = [];
    const ackedTexts: string[] = [];
    let sendAttempts = 0;
    const fetchMock = vi.fn(
      async (
        url: string,
        init?: { body?: unknown; signal?: AbortSignal | null },
      ) => {
        if (url.includes('sendmessage')) {
          sendAttempts += 1;
          const body = JSON.parse(String(init?.body));
          const text = String(body.msg.item_list[0].text_item.text);
          attemptedTexts.push(text);
          if (sendAttempts === 2) {
            return new Response('bad gateway', {
              status: 502,
              statusText: 'Bad Gateway',
            });
          }
          ackedTexts.push(text);
          return Response.json({ ret: 0 });
        }
        return waitUntilAborted(init?.signal);
      },
    );
    const connection = createWeChatConnection(
      {
        botToken: 'bot-token',
        ilinkBotId: 'bot-id',
        logContext: { accountId: 'account' },
      },
      {
        fetch: fetchMock as typeof fetch,
        createDispatcher: () => dispatcher,
        contextTokenStore: store,
      },
    );
    await connection.connect({ onNewChat: vi.fn() });

    const longText = 'A'.repeat(2500);
    await expect(connection.sendMessage('peer', longText)).rejects.toThrow(
      'HTTP 502',
    );

    const prefix = ackedTexts[0];
    expect(prefix.length).toBeGreaterThan(0);
    expect(prefix.length).toBeLessThan(longText.length);
    expect(attemptedTexts).toHaveLength(2);
    expect(store.record?.sendCount).toBe(1);
    expect(attemptedTexts.filter((text) => text === prefix)).toHaveLength(1);

    await expect(connection.sendMessage('peer', longText)).resolves.toBe(
      undefined,
    );
    expect(attemptedTexts.filter((text) => text === prefix)).toHaveLength(1);
    expect(ackedTexts.join('')).toBe(longText);
    expect(store.record?.sendCount).toBe(2);
    expect(sendAttempts).toBe(3);

    await connection.disconnect();
  });

  test('persists inbound token, restores after restart, and invalidates ret=-2 without tokenless retry', async () => {
    const store = new SharedStore();
    const dispatcher = {
      close: vi.fn(async () => undefined),
    } as unknown as Dispatcher;
    let firstPoll = true;
    const firstFetch = vi.fn(
      async (_url: string, init?: { signal?: AbortSignal | null }) => {
        if (firstPoll) {
          firstPoll = false;
          return Response.json({
            get_updates_buf: 'cursor-1',
            msgs: [
              {
                message_id: 1,
                from_user_id: 'peer',
                message_type: 1,
                create_time_ms: Date.now(),
                context_token: 'durable-secret',
                item_list: [{ type: 1, text_item: { text: 'hello' } }],
              },
            ],
          });
        }
        return waitUntilAborted(init?.signal);
      },
    );
    const first = createWeChatConnection(
      {
        botToken: 'bot-token',
        ilinkBotId: 'bot-id',
        logContext: { accountId: 'account' },
      },
      {
        fetch: firstFetch as typeof fetch,
        createDispatcher: () => dispatcher,
        contextTokenStore: store,
      },
    );
    await first.connect({
      onNewChat: vi.fn(),
      isChatAuthorized: () => true,
    });
    await vi.waitFor(() => expect(store.record?.token).toBe('durable-secret'));
    await first.disconnect();

    let sendAttempts = 0;
    const secondFetch = vi.fn(
      async (url: string, init?: { signal?: AbortSignal | null }) => {
        if (url.includes('sendmessage')) {
          sendAttempts += 1;
          const body = JSON.parse(String((init as { body?: unknown })?.body));
          expect(body.msg.context_token).toBe('durable-secret');
          return Response.json(
            sendAttempts === 1 ? { ret: 0 } : { ret: -2, errmsg: '' },
          );
        }
        return waitUntilAborted(init?.signal);
      },
    );
    const restarted = createWeChatConnection(
      {
        botToken: 'bot-token',
        ilinkBotId: 'bot-id',
        logContext: { accountId: 'account' },
      },
      {
        fetch: secondFetch as typeof fetch,
        createDispatcher: () => dispatcher,
        contextTokenStore: store,
      },
    );
    await restarted.connect({ onNewChat: vi.fn() });
    await expect(restarted.sendMessage('peer', 'after restart')).resolves.toBe(
      undefined,
    );
    await expect(restarted.sendMessage('peer', 'stale now')).rejects.toThrow(
      'ret=-2',
    );
    expect(store.record).toBeUndefined();
    await expect(
      restarted.sendMessage('peer', 'must not fall back'),
    ).rejects.toThrow('请让该用户先向机器人发送一条新消息');
    expect(sendAttempts).toBe(2);
    await restarted.disconnect();
  });
});

describe('WeChat inbound replay classification', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  test('keeps the old cursor and replays after an infrastructure failure', async () => {
    const store = new SharedStore();
    const onUpdatesBuf = vi.fn();
    const dispatcher = {
      close: vi.fn(async () => undefined),
    } as unknown as Dispatcher;
    const batch = {
      get_updates_buf: 'cursor-after-message',
      msgs: [
        {
          message_id: 42,
          from_user_id: 'peer',
          message_type: 1,
          create_time_ms: Date.now(),
          context_token: 'retry-token',
          item_list: [{ type: 1, text_item: { text: 'retry me' } }],
        },
      ],
    };
    let polls = 0;
    const fetchMock = vi.fn(
      async (_url: string, init?: { signal?: AbortSignal | null }) => {
        polls += 1;
        if (polls <= 2) return Response.json(batch);
        return waitUntilAborted(init?.signal);
      },
    );
    dbCalls.storeMessageDirect
      .mockImplementationOnce(() => {
        throw new Error('database temporarily unavailable');
      })
      .mockImplementation(() => undefined);

    const connection = createWeChatConnection(
      {
        botToken: 'bot-token',
        ilinkBotId: 'bot-id',
        logContext: { accountId: 'account' },
      },
      {
        fetch: fetchMock as typeof fetch,
        createDispatcher: () => dispatcher,
        contextTokenStore: store,
        random: () => 0.5,
      },
    );
    await connection.connect({
      onNewChat: vi.fn(),
      isChatAuthorized: () => true,
      onUpdatesBuf,
    });

    await vi.advanceTimersByTimeAsync(0);
    expect(dbCalls.storeMessageDirect).toHaveBeenCalledTimes(1);
    expect(onUpdatesBuf).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(3000);
    expect(dbCalls.storeMessageDirect).toHaveBeenCalledTimes(2);
    expect(onUpdatesBuf).toHaveBeenCalledTimes(1);
    expect(onUpdatesBuf).toHaveBeenCalledWith('cursor-after-message');
    await connection.disconnect();
  });

  test('acknowledges an intentional terminal ignore without persistence', async () => {
    const onUpdatesBuf = vi.fn();
    const dispatcher = {
      close: vi.fn(async () => undefined),
    } as unknown as Dispatcher;
    let firstPoll = true;
    const fetchMock = vi.fn(
      async (_url: string, init?: { signal?: AbortSignal | null }) => {
        if (firstPoll) {
          firstPoll = false;
          return Response.json({
            get_updates_buf: 'cursor-after-bot-message',
            msgs: [{ message_id: 7, message_type: 2 }],
          });
        }
        return waitUntilAborted(init?.signal);
      },
    );
    const connection = createWeChatConnection(
      { botToken: 'bot-token', ilinkBotId: 'bot-id' },
      {
        fetch: fetchMock as typeof fetch,
        createDispatcher: () => dispatcher,
        contextTokenStore: null,
      },
    );
    await connection.connect({ onNewChat: vi.fn(), onUpdatesBuf });

    await vi.advanceTimersByTimeAsync(0);
    expect(dbCalls.storeMessageDirect).not.toHaveBeenCalled();
    expect(onUpdatesBuf).toHaveBeenCalledWith('cursor-after-bot-message');
    await connection.disconnect();
  });
});
