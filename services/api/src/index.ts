import Fastify from 'fastify';
import fastifyCors from '@fastify/cors';
import { connect, JSONCodec } from 'nats';
import { nanoid } from 'nanoid';

const port = Number(process.env.PORT || 4000);
const natsUrl = process.env.NATS_URL || 'nats://localhost:4222';

async function start() {
  const server = Fastify();
  await server.register(fastifyCors, { origin: true });

  const nc = await connect({ servers: natsUrl });
  const js = nc.jetstream();
  const codec = JSONCodec();

  server.post('/api/chat', async (request, reply) => {
    const body = request.body as { sessionId?: string; message?: string };
    const sessionId = body.sessionId || nanoid();
    const message = String(body.message || '').trim();

    if (!message) {
      return reply.status(400).send({ error: 'message is required' });
    }

    const payload = { sessionId, message, timestamp: new Date().toISOString() };
    await js.publish('chat.incoming', codec.encode(payload));

    return { status: 'queued', messageId: nanoid(), sessionId };
  });

  server.get('/api/chat/stream', async (request, reply) => {
    const sessionId = String((request.query as any).sessionId || '');
    if (!sessionId) {
      return reply.status(400).send({ error: 'sessionId query parameter is required' });
    }

    reply.raw.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
    });
    reply.raw.write(': connected\n\n');

    const sub = nc.subscribe('chat.response', { queue: 'response-stream' });

    (async () => {
      for await (const msg of sub) {
        const data = codec.decode(msg.data) as any;
        if (data.sessionId === sessionId) {
          reply.raw.write(`event: message\ndata: ${JSON.stringify(data)}\n\n`);
        }
      }
    })().catch(() => {
      reply.raw.end();
    });

    request.raw.on('close', () => {
      reply.raw.end();
      sub.unsubscribe();
    });

    return reply;
  });

  server.listen({ port, host: '0.0.0.0' }).then(() => {
    console.log(`API running on http://0.0.0.0:${port}`);
  });
}

start().catch((error) => {
  console.error('Failed to start API service', error);
  process.exit(1);
});
  const body = request.body as { sessionId?: string; message?: string };
  const sessionId = body.sessionId || nanoid();
  const message = String(body.message || '').trim();

  if (!message) {
    return reply.status(400).send({ error: 'message is required' });
  }

  const payload = { sessionId, message, timestamp: new Date().toISOString() };
  await js.publish('chat.incoming', codec.encode(payload));

  return { status: 'queued', messageId: nanoid(), sessionId };
});

server.get('/api/chat/stream', async (request, reply) => {
  const sessionId = String((request.query as any).sessionId || '');
  if (!sessionId) {
    return reply.status(400).send({ error: 'sessionId query parameter is required' });
  }

  reply.raw.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });
  reply.raw.write(': connected\n\n');

  const sub = nc.subscribe('chat.response', { queue: 'response-stream' });

  (async () => {
    for await (const msg of sub) {
      const data = codec.decode(msg.data) as any;
      if (data.sessionId === sessionId) {
        reply.raw.write(`event: message\ndata: ${JSON.stringify(data)}\n\n`);
      }
    }
  })().catch(() => {
    reply.raw.end();
  });

  request.raw.on('close', () => {
    reply.raw.end();
    sub.unsubscribe();
  });

  return reply;
});

server.listen({ port, host: '0.0.0.0' }).then(() => {
  console.log(`API running on http://0.0.0.0:${port}`);
});
