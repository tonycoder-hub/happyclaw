import net from 'node:net';
import { afterEach, describe, expect, test } from 'vitest';

import { batchSendToUser, sendViaGroupMessagesAPI } from '../src/dingtalk.js';

async function listenBlackhole(): Promise<{
  server: net.Server;
  port: number;
  close: () => Promise<void>;
}> {
  const sockets = new Set<net.Socket>();
  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
    // Accept TCP and stay silent so TLS/HTTP never complete.
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => resolve());
  });
  const addr = server.address();
  if (!addr || typeof addr === 'string') {
    throw new Error('blackhole server has no TCP port');
  }
  return {
    server,
    port: addr.port,
    close: async () => {
      for (const socket of sockets) {
        socket.destroy();
      }
      await new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      });
    },
  };
}

describe('DingTalk HTTPS send timeout against a blackhole TCP peer', () => {
  const closers: Array<() => Promise<void>> = [];

  afterEach(async () => {
    await Promise.allSettled(closers.splice(0).map((close) => close()));
  });

  test('sendViaGroupMessagesAPI rejects with timeout instead of hanging', async () => {
    const blackhole = await listenBlackhole();
    closers.push(blackhole.close);
    const started = Date.now();

    await expect(
      sendViaGroupMessagesAPI(
        'cidXXXX',
        'robot-code',
        'token',
        'sampleText',
        JSON.stringify({ content: 'hi' }),
        {
          hostname: '127.0.0.1',
          port: blackhole.port,
          timeoutMs: 250,
          rejectUnauthorized: false,
        },
      ),
    ).rejects.toThrow(/timed out/i);

    expect(Date.now() - started).toBeLessThan(2000);
  });

  test('batchSendToUser rejects with timeout instead of hanging', async () => {
    const blackhole = await listenBlackhole();
    closers.push(blackhole.close);
    const started = Date.now();

    await expect(
      batchSendToUser(
        ['staff-1'],
        'robot-code',
        'token',
        'sampleText',
        JSON.stringify({ content: 'hi' }),
        {
          hostname: '127.0.0.1',
          port: blackhole.port,
          timeoutMs: 250,
          rejectUnauthorized: false,
        },
      ),
    ).rejects.toThrow(/timed out/i);

    expect(Date.now() - started).toBeLessThan(2000);
  });
});
