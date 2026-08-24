import { describe, expect, test } from 'vitest';

import { parseDingTalkC2cSendResponse } from '../src/dingtalk.js';

describe('DingTalk C2C / sessionWebhook strict ACK', () => {
  test.each(['', '<html>ok</html>', '{broken'])(
    'rejects malformed 2xx response %j',
    (body) => {
      expect(() => parseDingTalkC2cSendResponse(200, body)).toThrow();
    },
  );

  test('rejects HTTP failures', () => {
    expect(() =>
      parseDingTalkC2cSendResponse(
        503,
        JSON.stringify({ errcode: 0, errmsg: 'ok' }),
      ),
    ).toThrow('HTTP failed (503)');
  });

  test('rejects a non-zero errcode on HTTP 200', () => {
    expect(() =>
      parseDingTalkC2cSendResponse(
        200,
        JSON.stringify({ errcode: 88, errmsg: 'forbidden' }),
      ),
    ).toThrow('88');
  });

  test.each([{ processQueryKey: 'query-c2c-1' }, { errcode: 0, errmsg: 'ok' }])(
    'accepts a recognized success envelope %#',
    (response) => {
      expect(
        parseDingTalkC2cSendResponse(200, JSON.stringify(response)),
      ).toEqual(response);
    },
  );
});
