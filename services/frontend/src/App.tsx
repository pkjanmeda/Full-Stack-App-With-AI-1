import { useEffect, useRef, useState } from 'react';

type ChatLine = {
  sender: 'user' | 'agent';
  text: string;
};

type StreamMessage = {
  reply?: string;
  isPartial?: boolean;
};

function App() {
  const [sessionId] = useState(() => `session-${Math.random().toString(36).slice(2, 8)}`);
  const [message, setMessage] = useState('');
  const [chat, setChat] = useState<ChatLine[]>([]);
  const [connected, setConnected] = useState(false);
  const eventSourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    const source = new EventSource(`/api/chat/stream?sessionId=${sessionId}`);
    source.onopen = () => setConnected(true);
    source.onerror = () => setConnected(false);
    source.onmessage = (event) => {
      const data = JSON.parse(event.data) as StreamMessage;
      if (typeof data.reply !== 'string') return;

      setChat((prev) => {
        if (data.isPartial) {
          const last = prev[prev.length - 1];
          if (last?.sender === 'agent') {
            return [...prev.slice(0, -1), { ...last, text: data.reply! }];
          }
          return [...prev, { sender: 'agent', text: data.reply! }];
        }

        const last = prev[prev.length - 1];
        if (last?.sender === 'agent') {
          return [...prev.slice(0, -1), { ...last, text: data.reply }];
        }
        return [...prev, { sender: 'agent', text: data.reply }];
      });
    };
    eventSourceRef.current = source;

    return () => {
      source.close();
    };
  }, [sessionId]);

  const sendMessage = async () => {
    if (!message.trim()) return;
    setChat((prev) => [...prev, { sender: 'user', text: message }]);

    await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sessionId, message }),
    });

    setMessage('');
  };

  return (
    <div className="app-shell">
      <header>
        <h1>AI Chat Stream</h1>
        <p>Status: {connected ? 'Connected' : 'Disconnected'}</p>
      </header>
      <section className="chat-window">
        {chat.map((line, idx) => (
          <div key={idx} className={`chat-line ${line.sender}`}>
            <span>{line.sender === 'user' ? 'You' : 'Agent'}</span>
            <p>{line.text}</p>
          </div>
        ))}
      </section>
      <footer>
        <input
          value={message}
          onChange={(event) => setMessage(event.target.value)}
          onKeyDown={(event) => event.key === 'Enter' && sendMessage()}
          placeholder="Type a message..."
        />
        <button onClick={sendMessage}>Send</button>
      </footer>
    </div>
  );
}

export default App;
