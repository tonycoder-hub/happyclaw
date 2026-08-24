import fs from 'node:fs';
import path from 'node:path';

import { describe, expect, test } from 'vitest';

import { retryUnscopedImSend } from '../src/im-send-retry-policy.js';

function etimedoutAfterAccept(): NodeJS.ErrnoException {
  const error = new Error(
    'ETIMEDOUT after provider accepted the task notice',
  ) as NodeJS.ErrnoException;
  error.code = 'ETIMEDOUT';
  return error;
}

describe('unscoped task notice send without outbox', () => {
  test('ETIMEDOUT-after-accept on a task notice stays 1 copy (no extra physical resend)', async () => {
    let copies = 0;
    const result = await retryUnscopedImSend(
      async () => {
        // The provider accepted and delivered the notice. Only the ACK
        // timed out. Another attempt would be a second visible copy.
        copies += 1;
        throw etimedoutAfterAccept();
      },
      { sleep: async () => {} },
    );

    expect(copies).toBe(1);
    expect(result.ok).toBe(false);
    expect((result.error as NodeJS.ErrnoException | undefined)?.code).toBe(
      'ETIMEDOUT',
    );
  });

  test('pre-accept transport failures may still retry', async () => {
    let attempts = 0;
    const refused = Object.assign(new Error('connect ECONNREFUSED'), {
      code: 'ECONNREFUSED',
    });
    const result = await retryUnscopedImSend(
      async () => {
        attempts += 1;
        throw refused;
      },
      { sleep: async () => {} },
    );

    expect(attempts).toBe(3);
    expect(result.ok).toBe(false);
  });

  test('sendImWithRetry else-branch and retryTaskNotification use the unscoped helper', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'src/index.ts'),
      'utf8',
    );
    const sendStart = source.indexOf('async function sendImWithRetry(');
    const sendEnd = source.indexOf(
      'const CHANNEL_MANUAL_RECONCILIATION_NOTICE',
      sendStart,
    );
    expect(sendStart).toBeGreaterThanOrEqual(0);
    expect(sendEnd).toBeGreaterThan(sendStart);
    const sendImWithRetry = source.slice(sendStart, sendEnd);
    const elseBranch = sendImWithRetry.slice(
      sendImWithRetry.indexOf('} else {'),
    );
    expect(elseBranch).toContain('retryUnscopedImSend(');
    expect(elseBranch).not.toContain('retryImOperation(');

    const retryStart = source.indexOf(
      'retryTaskNotification: async (payload) => {',
    );
    const retryEnd = source.indexOf(
      'assistantName: ASSISTANT_NAME',
      retryStart,
    );
    expect(retryStart).toBeGreaterThanOrEqual(0);
    expect(retryEnd).toBeGreaterThan(retryStart);
    const retryTaskNotification = source.slice(retryStart, retryEnd);
    expect(retryTaskNotification).toContain('success = await sendImWithRetry(');
    expect(retryTaskNotification).toContain(
      'success = await sendImWithRetry(targetJid!, payload.text, [])',
    );

    // Scheduled-task IPC notices also go through the unscoped path.
    expect(source).toContain('sendImWithRetry(targetJid, data.text, [])');
  });
});
