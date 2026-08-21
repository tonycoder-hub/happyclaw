import {
  claimWeChatContextToken,
  deleteWeChatContextToken,
  isDatabaseInitialized,
  listWeChatContextTokens,
  releaseWeChatContextToken,
  upsertWeChatContextToken,
  type StoredWeChatContextToken,
  type WeChatContextTokenClaimResult,
  type WeChatContextTokenReleaseResult,
} from './db.js';

// Tencent does not publish a stable machine-readable contract for these
// limits. The official plugin issue tracker and observed iLink behavior agree
// on a 24-hour window and ten sendmessage calls per inbound refresh. Keep a
// one-hour safety margin because field reports show expiry can occur early.
export const WECHAT_CONTEXT_TOKEN_MAX_AGE_MS = 23 * 60 * 60 * 1000;
export const WECHAT_CONTEXT_TOKEN_MAX_SENDS = 10;

export interface WeChatContextTokenRecord {
  accountId: string;
  userId: string;
  token: string;
  refreshedAtMs: number;
  sourceMessageId?: string | null;
  sourceSequence?: number | null;
  sendCount: number;
  lastSentAtMs: number | null;
}

export interface WeChatContextTokenClaimInput {
  accountId: string;
  userId: string;
  expectedToken: string;
  expectedRefreshedAtMs: number;
  expectedSourceMessageId?: string | null;
  claimCount: number;
  maxSendCount: number;
  maxAgeMs: number;
  nowMs: number;
}

export interface WeChatContextTokenReleaseInput {
  accountId: string;
  userId: string;
  expectedToken: string;
  expectedRefreshedAtMs: number;
  expectedSourceMessageId?: string | null;
  releaseCount: number;
}

export interface WeChatContextTokenStore {
  list(accountId: string): WeChatContextTokenRecord[];
  upsert(input: {
    accountId: string;
    userId: string;
    token: string;
    refreshedAtMs: number;
    sourceMessageId?: string | null;
    sourceSequence?: number | null;
  }): WeChatContextTokenRecord;
  claim(
    input: WeChatContextTokenClaimInput,
  ):
    | { status: 'claimed'; record: WeChatContextTokenRecord }
    | { status: 'missing' | 'changed' | 'expired' | 'quota_exhausted' };
  release(
    input: WeChatContextTokenReleaseInput,
  ):
    | { status: 'released'; record: WeChatContextTokenRecord }
    | { status: 'missing' | 'changed' };
  delete(input: {
    accountId: string;
    userId: string;
    expectedToken?: string;
    expectedRefreshedAtMs?: number;
    expectedSourceMessageId?: string | null;
  }): boolean;
}

export type WeChatContextTokenFailureReason =
  | 'missing'
  | 'expired'
  | 'quota_exhausted';

export class WeChatContextTokenError extends Error {
  readonly code = 'WECHAT_CONTEXT_REFRESH_REQUIRED';

  constructor(
    readonly reason: WeChatContextTokenFailureReason,
    readonly userId: string,
  ) {
    const reasonText =
      reason === 'missing'
        ? '缺少回复凭证'
        : reason === 'expired'
          ? '回复凭证已过期'
          : '本轮 10 条回复额度已用完';
    super(`微信${reasonText}，请让该用户先向机器人发送一条新消息后再重试`);
    this.name = 'WeChatContextTokenError';
  }
}

function fromStored(
  record: StoredWeChatContextToken,
): WeChatContextTokenRecord {
  return {
    accountId: record.channel_account_id,
    userId: record.user_id,
    token: record.context_token,
    refreshedAtMs: record.refreshed_at_ms,
    sourceMessageId: record.source_message_id,
    sourceSequence: record.source_sequence,
    sendCount: record.send_count,
    lastSentAtMs: record.last_sent_at_ms,
  };
}

function fromClaim(
  result: WeChatContextTokenClaimResult,
): ReturnType<WeChatContextTokenStore['claim']> {
  return result.status === 'claimed'
    ? { status: 'claimed', record: fromStored(result.record) }
    : result;
}

function fromRelease(
  result: WeChatContextTokenReleaseResult,
): ReturnType<WeChatContextTokenStore['release']> {
  return result.status === 'released'
    ? { status: 'released', record: fromStored(result.record) }
    : result;
}

export function createDatabaseWeChatContextTokenStore(): WeChatContextTokenStore | null {
  if (!isDatabaseInitialized()) return null;
  return {
    list: (accountId) => listWeChatContextTokens(accountId).map(fromStored),
    upsert: (input) =>
      fromStored(
        upsertWeChatContextToken({
          channelAccountId: input.accountId,
          userId: input.userId,
          contextToken: input.token,
          refreshedAtMs: input.refreshedAtMs,
          sourceMessageId: input.sourceMessageId,
          sourceSequence: input.sourceSequence,
        }),
      ),
    claim: (input) =>
      fromClaim(
        claimWeChatContextToken({
          channelAccountId: input.accountId,
          userId: input.userId,
          expectedToken: input.expectedToken,
          expectedRefreshedAtMs: input.expectedRefreshedAtMs,
          expectedSourceMessageId: input.expectedSourceMessageId,
          claimCount: input.claimCount,
          maxSendCount: input.maxSendCount,
          maxAgeMs: input.maxAgeMs,
          nowMs: input.nowMs,
        }),
      ),
    release: (input) =>
      fromRelease(
        releaseWeChatContextToken({
          channelAccountId: input.accountId,
          userId: input.userId,
          expectedToken: input.expectedToken,
          expectedRefreshedAtMs: input.expectedRefreshedAtMs,
          expectedSourceMessageId: input.expectedSourceMessageId,
          releaseCount: input.releaseCount,
        }),
      ),
    delete: (input) =>
      deleteWeChatContextToken({
        channelAccountId: input.accountId,
        userId: input.userId,
        expectedToken: input.expectedToken,
        expectedRefreshedAtMs: input.expectedRefreshedAtMs,
        expectedSourceMessageId: input.expectedSourceMessageId,
      }),
  };
}

export interface WeChatContextTokenManagerOptions {
  accountId?: string;
  store?: WeChatContextTokenStore | null;
  now?: () => number;
  maxAgeMs?: number;
  maxSendCount?: number;
}

export interface WeChatContextTokenGeneration {
  messageId?: string;
  sequence?: number;
}

function shouldReplaceCachedGeneration(
  current: WeChatContextTokenRecord,
  next: Pick<
    WeChatContextTokenRecord,
    'token' | 'refreshedAtMs' | 'sourceMessageId' | 'sourceSequence'
  >,
): boolean {
  const currentMessageId = current.sourceMessageId ?? null;
  const nextMessageId = next.sourceMessageId ?? null;
  const currentSequence = current.sourceSequence ?? null;
  const nextSequence = next.sourceSequence ?? null;

  if (nextMessageId === null) {
    return (
      currentMessageId === null && next.refreshedAtMs > current.refreshedAtMs
    );
  }
  // Stable message identity makes an exact batch replay quota-idempotent even
  // if the upstream token or local observation time changes on the replay.
  if (currentMessageId === nextMessageId) return false;
  // A legacy v70 row has no message identity. At an equal timestamp, an
  // unchanged per-message token is the only available replay signal; a changed
  // token establishes the first durable provider generation.
  if (currentMessageId === null) {
    return (
      next.refreshedAtMs > current.refreshedAtMs ||
      (next.refreshedAtMs === current.refreshedAtMs &&
        next.token !== current.token)
    );
  }
  if (nextSequence !== null && currentSequence !== null) {
    return nextSequence > currentSequence;
  }
  if (nextSequence !== null) {
    return next.refreshedAtMs >= current.refreshedAtMs;
  }
  return currentSequence === null
    ? next.refreshedAtMs >= current.refreshedAtMs
    : next.refreshedAtMs > current.refreshedAtMs;
}

/**
 * Connection-local cache backed by an account-scoped durable store. The
 * manager owns lifetime/quota enforcement and compare-and-delete invalidation;
 * transport code only ever receives a token after a successful reservation.
 */
export class WeChatContextTokenManager {
  private readonly cache = new Map<string, WeChatContextTokenRecord>();
  private readonly accountId?: string;
  private readonly store: WeChatContextTokenStore | null;
  private readonly now: () => number;
  private readonly maxAgeMs: number;
  private readonly maxSendCount: number;

  constructor(options: WeChatContextTokenManagerOptions) {
    this.accountId = options.accountId;
    this.store = options.store ?? null;
    this.now = options.now ?? Date.now;
    this.maxAgeMs = options.maxAgeMs ?? WECHAT_CONTEXT_TOKEN_MAX_AGE_MS;
    this.maxSendCount = options.maxSendCount ?? WECHAT_CONTEXT_TOKEN_MAX_SENDS;
  }

  restore(): number {
    this.cache.clear();
    if (!this.accountId || !this.store) return 0;
    const nowMs = this.now();
    for (const record of this.store.list(this.accountId)) {
      if (nowMs - record.refreshedAtMs >= this.maxAgeMs) {
        this.store.delete({
          accountId: this.accountId,
          userId: record.userId,
          expectedToken: record.token,
          expectedRefreshedAtMs: record.refreshedAtMs,
          expectedSourceMessageId: record.sourceMessageId ?? null,
        });
        continue;
      }
      this.cache.set(record.userId, record);
    }
    return this.cache.size;
  }

  refresh(
    userId: string,
    token: string,
    inboundAtMs: number,
    generation: WeChatContextTokenGeneration = {},
  ): void {
    const nowMs = this.now();
    const refreshedAtMs = Math.min(nowMs, Math.max(0, inboundAtMs));
    const sourceMessageId = generation.messageId ?? null;
    const sourceSequence = Number.isSafeInteger(generation.sequence)
      ? generation.sequence!
      : null;
    const current = this.cache.get(userId);
    if (
      current &&
      !shouldReplaceCachedGeneration(current, {
        token,
        refreshedAtMs,
        sourceMessageId,
        sourceSequence,
      })
    ) {
      return;
    }
    const record =
      this.accountId && this.store
        ? this.store.upsert({
            accountId: this.accountId,
            userId,
            token,
            refreshedAtMs,
            sourceMessageId,
            sourceSequence,
          })
        : {
            accountId: this.accountId ?? '',
            userId,
            token,
            refreshedAtMs,
            sourceMessageId,
            sourceSequence,
            sendCount: 0,
            lastSentAtMs: null,
          };
    this.cache.set(userId, record);
  }

  claim(userId: string, claimCount = 1): WeChatContextTokenRecord {
    if (!Number.isInteger(claimCount) || claimCount <= 0) {
      throw new Error('WeChat context_token claimCount must be positive');
    }
    for (let attempt = 0; attempt < 2; attempt++) {
      const record = this.cache.get(userId);
      if (!record) throw new WeChatContextTokenError('missing', userId);
      const nowMs = this.now();
      if (nowMs - record.refreshedAtMs >= this.maxAgeMs) {
        this.invalidate(record);
        throw new WeChatContextTokenError('expired', userId);
      }
      if (record.sendCount + claimCount > this.maxSendCount) {
        throw new WeChatContextTokenError('quota_exhausted', userId);
      }

      if (!this.accountId || !this.store) {
        const claimed = {
          ...record,
          sendCount: record.sendCount + claimCount,
          lastSentAtMs: nowMs,
        };
        this.cache.set(userId, claimed);
        return claimed;
      }

      const result = this.store.claim({
        accountId: this.accountId,
        userId,
        expectedToken: record.token,
        expectedRefreshedAtMs: record.refreshedAtMs,
        expectedSourceMessageId: record.sourceMessageId ?? null,
        claimCount,
        maxSendCount: this.maxSendCount,
        maxAgeMs: this.maxAgeMs,
        nowMs,
      });
      if (result.status === 'claimed') {
        this.cache.set(userId, result.record);
        return result.record;
      }
      if (result.status === 'changed') {
        const latest = this.store
          .list(this.accountId)
          .find((candidate) => candidate.userId === userId);
        if (latest) this.cache.set(userId, latest);
        else this.cache.delete(userId);
        continue;
      }
      if (result.status === 'expired') this.invalidate(record);
      throw new WeChatContextTokenError(result.status, userId);
    }
    throw new WeChatContextTokenError('missing', userId);
  }

  release(record: WeChatContextTokenRecord, releaseCount = 1): boolean {
    if (!Number.isInteger(releaseCount) || releaseCount <= 0) {
      throw new Error('WeChat context_token releaseCount must be positive');
    }
    const current = this.cache.get(record.userId);
    if (
      !current ||
      current.token !== record.token ||
      current.refreshedAtMs !== record.refreshedAtMs ||
      (current.sourceMessageId ?? null) !== (record.sourceMessageId ?? null) ||
      current.sendCount < releaseCount
    ) {
      return false;
    }

    if (!this.accountId || !this.store) {
      this.cache.set(record.userId, {
        ...current,
        sendCount: current.sendCount - releaseCount,
      });
      return true;
    }

    const result = this.store.release({
      accountId: this.accountId,
      userId: record.userId,
      expectedToken: record.token,
      expectedRefreshedAtMs: record.refreshedAtMs,
      expectedSourceMessageId: record.sourceMessageId ?? null,
      releaseCount,
    });
    if (result.status === 'released') {
      this.cache.set(record.userId, result.record);
      return true;
    }
    if (result.status === 'changed') {
      const latest = this.store
        .list(this.accountId)
        .find((candidate) => candidate.userId === record.userId);
      if (latest) this.cache.set(record.userId, latest);
      else this.cache.delete(record.userId);
    }
    return false;
  }

  peek(userId: string): WeChatContextTokenRecord | undefined {
    const record = this.cache.get(userId);
    if (!record) return undefined;
    if (this.now() - record.refreshedAtMs >= this.maxAgeMs) {
      this.invalidate(record);
      return undefined;
    }
    return record;
  }

  invalidate(record: WeChatContextTokenRecord): boolean {
    const current = this.cache.get(record.userId);
    if (
      !current ||
      current.token !== record.token ||
      current.refreshedAtMs !== record.refreshedAtMs ||
      (current.sourceMessageId ?? null) !== (record.sourceMessageId ?? null)
    ) {
      return false;
    }
    if (this.accountId && this.store) {
      this.store.delete({
        accountId: this.accountId,
        userId: record.userId,
        expectedToken: record.token,
        expectedRefreshedAtMs: record.refreshedAtMs,
        expectedSourceMessageId: record.sourceMessageId ?? null,
      });
    }
    this.cache.delete(record.userId);
    return true;
  }

  clearMemory(): void {
    this.cache.clear();
  }
}
