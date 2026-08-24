import { beforeEach, describe, expect, test, vi } from 'vitest';

const sdk = vi.hoisted(() => {
  class MockDWClient {
    static instances: MockDWClient[] = [];
    listener:
      | ((downstream: {
          headers?: { messageId?: string };
          data: string;
        }) => Promise<void> | void)
      | null = null;
    registerCallbackListener = vi.fn(
      (
        _topic: string,
        listener: (downstream: {
          headers?: { messageId?: string };
          data: string;
        }) => Promise<void> | void,
      ) => {
        this.listener = listener;
        return this;
      },
    );
    socketCallBackResponse = vi.fn();
    connect = vi.fn(async () => undefined);
    disconnect = vi.fn();
    constructor(public options: Record<string, unknown>) {
      MockDWClient.instances.push(this);
    }
  }
  return { MockDWClient };
});

vi.mock('dingtalk-stream', () => ({
  DWClient: sdk.MockDWClient,
  TOPIC_ROBOT: '/v1.0/im/bot/messages/get',
}));

vi.mock('../src/db.js', () => ({
  storeChatMetadata: vi.fn(),
  storeMessageDirect: vi.fn(),
}));

vi.mock('../src/message-notifier.js', () => ({
  notifyNewImMessage: vi.fn(),
}));

vi.mock('../src/logger.js', () => ({
  logger: {
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
  },
}));

import { storeMessageDirect } from '../src/db.js';
import { createDingTalkConnection } from '../src/dingtalk.js';

type MockClient = InstanceType<typeof sdk.MockDWClient>;

function robotDownstream(input: {
  messageId?: string;
  msgId?: string;
  content?: string;
  senderId?: string;
}) {
  const senderId = input.senderId ?? 'staff-1';
  return {
    headers: { messageId: input.messageId ?? 'stream-msg-1' },
    data: JSON.stringify({
      conversationId: senderId,
      conversationType: '1',
      msgId: input.msgId ?? 'dt-msg-1',
      senderId,
      senderNick: '钉钉用户',
      senderStaffId: senderId,
      createAt: Date.now(),
      msgtype: 'text',
      text: { content: input.content ?? 'hello from dingtalk' },
    }),
  };
}

async function connect(): Promise<{
  client: MockClient;
  connection: ReturnType<typeof createDingTalkConnection>;
}> {
  const connection = createDingTalkConnection({
    clientId: 'ding-client',
    clientSecret: 'ding-secret',
  });
  const ok = await connection.connect({
    onNewChat: vi.fn(),
    isChatAuthorized: () => true,
    resolveEffectiveChatJid: (jid: string) => ({
      effectiveJid: jid,
      agentId: null,
    }),
    onMessagePersisted: vi.fn(),
  });
  expect(ok).toBe(true);
  const client = sdk.MockDWClient.instances.at(-1);
  if (!client?.listener) {
    throw new Error('DingTalk callback listener was not registered');
  }
  return { client, connection };
}

async function deliver(client: MockClient) {
  await client.listener!(robotDownstream({}));
}

describe('DingTalk inbound Stream ACK must not precede handle', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    sdk.MockDWClient.instances.length = 0;
    vi.mocked(storeMessageDirect).mockImplementation(() => 'stored');
  });

  test('does not ACK when storeMessageDirect throws, then persists and ACKs on redelivery', async () => {
    vi.mocked(storeMessageDirect)
      .mockImplementationOnce(() => {
        throw new Error('database temporarily unavailable');
      })
      .mockImplementation(() => 'stored');

    const { client } = await connect();

    await deliver(client);
    await vi.waitFor(() => expect(storeMessageDirect).toHaveBeenCalledTimes(1));

    expect(client.socketCallBackResponse).not.toHaveBeenCalled();

    await deliver(client);
    await vi.waitFor(() => expect(storeMessageDirect).toHaveBeenCalledTimes(2));

    expect(client.socketCallBackResponse).toHaveBeenCalledTimes(1);
    expect(client.socketCallBackResponse).toHaveBeenCalledWith('stream-msg-1', {
      success: true,
    });
  });

  test('ACKs only after inbound persist succeeds', async () => {
    const order: string[] = [];
    vi.mocked(storeMessageDirect).mockImplementation(() => {
      order.push('store');
      return 'stored';
    });

    const { client } = await connect();
    client.socketCallBackResponse.mockImplementation(() => {
      order.push('ack');
    });

    await deliver(client);
    await vi.waitFor(() => expect(storeMessageDirect).toHaveBeenCalledTimes(1));

    expect(order).toEqual(['store', 'ack']);
    expect(client.socketCallBackResponse).toHaveBeenCalledWith('stream-msg-1', {
      success: true,
    });
  });
});
