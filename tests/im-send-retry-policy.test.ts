import { describe, expect, test } from 'vitest';

import {
  imSendFailurePolicy,
  isUncertainAfterAcceptImError,
} from '../src/im-send-retry-policy.js';
import { WeChatContextTokenError } from '../src/wechat-context-token.js';

describe('imSendFailurePolicy', () => {
  test.each(['missing', 'expired', 'quota_exhausted'] as const)(
    'does not retry or remove a healthy WeChat chat for %s context',
    (reason) => {
      expect(
        imSendFailurePolicy(new WeChatContextTokenError(reason, 'peer')),
      ).toEqual({
        retryable: false,
        countsTowardChannelRemoval: false,
      });
    },
  );

  test('recognizes a refresh requirement wrapped as an error cause', () => {
    const cause = new WeChatContextTokenError('missing', 'peer');
    expect(imSendFailurePolicy(new Error('adapter failed', { cause }))).toEqual(
      {
        retryable: false,
        countsTowardChannelRemoval: false,
      },
    );
  });

  test('keeps transient transport failures retryable and health-counted', () => {
    expect(imSendFailurePolicy(new Error('connection reset'))).toEqual({
      retryable: true,
      countsTowardChannelRemoval: true,
    });
  });

  test('treats ETIMEDOUT in the error chain as uncertain after accept', () => {
    const timeout = Object.assign(new Error('socket hang up'), {
      code: 'ETIMEDOUT',
    });
    expect(isUncertainAfterAcceptImError(timeout)).toBe(true);
    expect(
      isUncertainAfterAcceptImError(
        new Error('adapter failed', { cause: timeout }),
      ),
    ).toBe(true);
    expect(isUncertainAfterAcceptImError(new Error('connection reset'))).toBe(
      false,
    );
  });
});
