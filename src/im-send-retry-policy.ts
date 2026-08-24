export interface ImSendFailurePolicy {
  retryable: boolean;
  countsTowardChannelRemoval: boolean;
}

const REFRESH_REQUIRED_CODE = 'WECHAT_CONTEXT_REFRESH_REQUIRED';

function errorChainHasCode(error: unknown, expectedCode: string): boolean {
  let current = error;
  const seen = new Set<unknown>();
  while (current && typeof current === 'object' && !seen.has(current)) {
    seen.add(current);
    if ((current as { code?: unknown }).code === expectedCode) return true;
    current = (current as { cause?: unknown }).cause;
  }
  return false;
}

/**
 * Decide whether repeating a failed IM send can make progress and whether the
 * failure is evidence that the concrete chat binding is unhealthy.
 */
export function imSendFailurePolicy(error: unknown): ImSendFailurePolicy {
  if (errorChainHasCode(error, REFRESH_REQUIRED_CODE)) {
    return {
      retryable: false,
      countsTowardChannelRemoval: false,
    };
  }
  return {
    retryable: true,
    countsTowardChannelRemoval: true,
  };
}

/**
 * A timeout after the physical send started cannot prove the provider
 * rejected the message. The request may already have been accepted.
 */
export function isUncertainAfterAcceptImError(error: unknown): boolean {
  return errorChainHasCode(error, 'ETIMEDOUT');
}

export interface RetryUnscopedImSendOptions {
  maxAttempts?: number;
  delayMs?: number;
  sleep?: (ms: number) => Promise<void>;
  onAttemptFailure?: (error: unknown, attempt: number) => void;
}

/**
 * Unscoped (no outbox) IM send. Pre-accept failures may still retry;
 * an ETIMEDOUT after the physical send started must not resend.
 */
export async function retryUnscopedImSend(
  send: () => Promise<void>,
  options: RetryUnscopedImSendOptions = {},
): Promise<{ ok: boolean; error?: unknown }> {
  const maxAttempts = options.maxAttempts ?? 3;
  const delayMs = options.delayMs ?? 2_000;
  const sleep =
    options.sleep ??
    ((ms: number) => new Promise((resolve) => setTimeout(resolve, ms)));
  let lastError: unknown;
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      await send();
      return { ok: true };
    } catch (error) {
      lastError = error;
      options.onAttemptFailure?.(error, attempt);
      // After the physical send started, ETIMEDOUT cannot prove rejection.
      // Retrying would deliver a second visible copy of the same notice.
      if (
        isUncertainAfterAcceptImError(error) ||
        !imSendFailurePolicy(error).retryable
      ) {
        return { ok: false, error };
      }
      if (attempt < maxAttempts - 1) {
        await sleep(delayMs * (attempt + 1));
      }
    }
  }
  return { ok: false, error: lastError };
}
