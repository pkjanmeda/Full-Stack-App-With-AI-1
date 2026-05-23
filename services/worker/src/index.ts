import { connect, JSONCodec } from 'nats';

const natsUrl = process.env.NATS_URL || 'nats://localhost:4222';
const langgraphMode = process.env.LANGGRAPH_MODE || 'local';

async function start() {
  const nc = await connect({ servers: natsUrl });
  const js = nc.jetstream();
  const codec = JSONCodec();

  console.log('Worker starting:', { natsUrl, langgraphMode });

  const sub = nc.subscribe('chat.incoming', { queue: 'chat-workers' });

  for await (const msg of sub) {
    const payload = codec.decode(msg.data) as { sessionId: string; message: string };
    const response = runAiAgent(payload.message, langgraphMode);
    const result = {
      sessionId: payload.sessionId,
      reply: response,
      originalMessage: payload.message,
      timestamp: new Date().toISOString(),
    };

    await js.publish('chat.response', codec.encode(result));
    if (typeof msg.ack === 'function') {
      msg.ack();
    }
  }
}

start().catch((error) => {
  console.error('Worker failed to start', error);
  process.exit(1);
});

function runAiAgent(message: string, mode: string): string {
  const tools = [
    randomGreeting,
    emphasizePhrase,
    appendChanceFact,
  ];
  const enriched = tools.reduce((text, tool) => tool(text), message);
  return `[${mode} agent] ${enriched}`;
}

function randomGreeting(text: string): string {
  const greetings = ['Hey there!', 'Good news:', 'Hot take:'];
  return `${greetings[Math.floor(Math.random() * greetings.length)]} ${text}`;
}

function emphasizePhrase(text: string): string {
  return text.replace(/\b(\w+)\b/, (match) => `**${match}**`);
}

function appendChanceFact(text: string): string {
  const facts = [
    'Psst — did you know cats can learn to use iPads?',
    'Fun fact: this message was polished by a worker.',
    'Random tip: always save your work early.',
  ];
  return `${text} ${facts[Math.floor(Math.random() * facts.length)]}`;
}
