import { describe, expect, test } from 'vitest';

import {
  WECHAT_CONTEXT_TOKEN_MAX_AGE_MS,
  WeChatContextTokenError,
  WeChatContextTokenManager,
  type WeChatContextTokenClaimInput,
  type WeChatContextTokenRecord,
  type WeChatContextTokenReleaseInput,
  type WeChatContextTokenStore,
} from '../src/wechat-context-token.js';

class MemoryStore implements WeChatContextTokenStore {
  readonly rows = new Map<string, WeChatContextTokenRecord>();

  private key(accountId: string, userId: string): string {
    return `${accountId}:${userId}`;
  }

  list(accountId: string): WeChatContextTokenRecord[] {
    return [...this.rows.values()]
      .filter((record) => record.accountId === accountId)
      .map((record) => ({ ...record }));
  }

  upsert(input: {
    accountId: string;
    userId: string;
    token: string;
    refreshedAtMs: number;
    sourceMessageId?: string | null;
    sourceSequence?: number | null;
  }): WeChatContextTokenRecord {
    const record: WeChatContextTokenRecord = {
      ...input,
      sendCount: 0,
      lastSentAtMs: null,
    };
    this.rows.set(this.key(input.accountId, input.userId), record);
    return { ...record };
  }

  claim(
    input: WeChatContextTokenClaimInput,
  ):
    | { status: 'claimed'; record: WeChatContextTokenRecord }
    | { status: 'missing' | 'changed' | 'expired' | 'quota_exhausted' } {
    const key = this.key(input.accountId, input.userId);
    const record = this.rows.get(key);
    if (!record) return { status: 'missing' };
    if (
      record.token !== input.expectedToken ||
      record.refreshedAtMs !== input.expectedRefreshedAtMs ||
      (input.expectedSourceMessageId !== undefined &&
        (record.sourceMessageId ?? null) !== input.expectedSourceMessageId)
    ) {
      return { status: 'changed' };
    }
    if (input.nowMs - record.refreshedAtMs >= input.maxAgeMs) {
      return { status: 'expired' };
    }
    if (record.sendCount + input.claimCount > input.maxSendCount) {
      return { status: 'quota_exhausted' };
    }
    const claimed = {
      ...record,
      sendCount: record.sendCount + input.claimCount,
      lastSentAtMs: input.nowMs,
    };
    this.rows.set(key, claimed);
    return { status: 'claimed', record: { ...claimed } };
  }

  release(
    input: WeChatContextTokenReleaseInput,
  ):
    | { status: 'released'; record: WeChatContextTokenRecord }
    | { status: 'missing' | 'changed' } {
    const key = this.key(input.accountId, input.userId);
    const record = this.rows.get(key);
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
    this.rows.set(key, released);
    return { status: 'released', record: { ...released } };
  }

  delete(input: {
    accountId: string;
    userId: string;
    expectedToken?: string;
    expectedRefreshedAtMs?: number;
    expectedSourceMessageId?: string | null;
  }): boolean {
    const key = this.key(input.accountId, input.userId);
    const record = this.rows.get(key);
    if (!record) return false;
    if (
      input.expectedToken !== undefined &&
      (record.token !== input.expectedToken ||
        record.refreshedAtMs !== input.expectedRefreshedAtMs ||
        (input.expectedSourceMessageId !== undefined &&
          (record.sourceMessageId ?? null) !== input.expectedSourceMessageId))
    ) {
      return false;
    }
    return this.rows.delete(key);
  }
}

describe('WeChat context_token lifecycle', () => {
  test('survives a connection restart without crossing account boundaries', () => {
    const store = new MemoryStore();
    let now = 1_000;
    const first = new WeChatContextTokenManager({
      accountId: 'account-a',
      store,
      now: () => now,
    });
    first.refresh('peer', 'secret-token', now);
    expect(first.claim('peer', 2)).toMatchObject({
      token: 'secret-token',
      sendCount: 2,
    });
    first.clearMemory();

    now += 5_000;
    const restarted = new WeChatContextTokenManager({
      accountId: 'account-a',
      store,
      now: () => now,
    });
    expect(restarted.restore()).toBe(1);
    expect(restarted.claim('peer')).toMatchObject({ sendCount: 3 });

    const otherAccount = new WeChatContextTokenManager({
      accountId: 'account-b',
      store,
      now: () => now,
    });
    expect(otherAccount.restore()).toBe(0);
    expect(() => otherAccount.claim('peer')).toThrow(WeChatContextTokenError);
  });

  test('reserves the ten-send budget atomically and inbound refresh resets it', () => {
    const store = new MemoryStore();
    let now = 10_000;
    const manager = new WeChatContextTokenManager({
      accountId: 'account',
      store,
      now: () => now,
    });
    manager.refresh('peer', 'token-1', now);
    expect(manager.claim('peer', 9).sendCount).toBe(9);
    expect(manager.claim('peer').sendCount).toBe(10);
    expect(() => manager.claim('peer')).toThrowError(
      expect.objectContaining({ reason: 'quota_exhausted' }),
    );

    now += 1_000;
    manager.refresh('peer', 'token-2', now);
    expect(manager.claim('peer')).toMatchObject({
      token: 'token-2',
      sendCount: 1,
    });
  });

  test('expires conservatively and removes the durable stale credential', () => {
    const store = new MemoryStore();
    let now = 100_000;
    const manager = new WeChatContextTokenManager({
      accountId: 'account',
      store,
      now: () => now,
    });
    manager.refresh('peer', 'token', now);
    now += WECHAT_CONTEXT_TOKEN_MAX_AGE_MS;

    expect(() => manager.claim('peer')).toThrowError(
      expect.objectContaining({ reason: 'expired' }),
    );
    expect(store.rows.size).toBe(0);
  });

  test('an old failed send cannot delete a concurrent inbound refresh', () => {
    const store = new MemoryStore();
    let now = 100;
    const manager = new WeChatContextTokenManager({
      accountId: 'account',
      store,
      now: () => now,
    });
    manager.refresh('peer', 'same-token', now);
    const oldGeneration = manager.claim('peer');

    now += 1;
    manager.refresh('peer', 'same-token', now);
    expect(manager.invalidate(oldGeneration)).toBe(false);
    expect(manager.claim('peer')).toMatchObject({
      token: 'same-token',
      refreshedAtMs: 101,
      sendCount: 1,
    });
  });

  test('keeps exact replay quota but accepts a distinct same-millisecond generation', () => {
    const store = new MemoryStore();
    const manager = new WeChatContextTokenManager({
      accountId: 'account',
      store,
      now: () => 10_000,
    });
    manager.refresh('peer', 'token-1', 10_000, {
      messageId: 'message-1',
      sequence: 1,
    });
    expect(manager.claim('peer', 7).sendCount).toBe(7);

    manager.refresh('peer', 'changed-on-replay', 10_000, {
      messageId: 'message-1',
      sequence: 1,
    });
    expect(manager.peek('peer')).toMatchObject({
      token: 'token-1',
      sendCount: 7,
    });

    manager.refresh('peer', 'token-2', 10_000, {
      messageId: 'message-2',
      sequence: 2,
    });
    expect(manager.peek('peer')).toMatchObject({
      token: 'token-2',
      sendCount: 0,
    });
    manager.refresh('peer', 'old-replay', 10_000, {
      messageId: 'message-1',
      sequence: 1,
    });
    expect(manager.peek('peer')).toMatchObject({
      token: 'token-2',
      sendCount: 0,
    });
  });

  test('classifies the first same-millisecond generation after a legacy v70 restore by token', () => {
    const store = new MemoryStore();
    const manager = new WeChatContextTokenManager({
      accountId: 'account',
      store,
      now: () => 10_000,
    });
    manager.refresh('peer', 'legacy-token', 10_000);
    expect(manager.claim('peer', 4).sendCount).toBe(4);

    manager.refresh('peer', 'legacy-token', 10_000, {
      messageId: 'message-1',
      sequence: 1,
    });
    expect(manager.peek('peer')).toMatchObject({
      sendCount: 4,
      sourceMessageId: null,
    });

    manager.refresh('peer', 'new-token', 10_000, {
      messageId: 'message-2',
      sequence: 2,
    });
    expect(manager.peek('peer')).toMatchObject({
      token: 'new-token',
      sendCount: 0,
      sourceMessageId: 'message-2',
    });
  });

  test('releases unused reservations on the current generation only', () => {
    const store = new MemoryStore();
    const manager = new WeChatContextTokenManager({
      accountId: 'account',
      store,
      now: () => 10_000,
    });
    manager.refresh('peer', 'token', 10_000, {
      messageId: 'message-1',
      sequence: 1,
    });
    const claimed = manager.claim('peer', 2);
    expect(claimed.sendCount).toBe(2);
    expect(manager.release(claimed, 1)).toBe(true);
    expect(manager.peek('peer')).toMatchObject({
      token: 'token',
      sendCount: 1,
    });
    expect(store.rows.values().next().value).toMatchObject({ sendCount: 1 });

    manager.refresh('peer', 'token-2', 10_001, {
      messageId: 'message-2',
      sequence: 2,
    });
    expect(manager.release(claimed, 1)).toBe(false);
    expect(manager.peek('peer')).toMatchObject({
      token: 'token-2',
      sendCount: 0,
    });
  });
});
