import { useEffect, useRef, useState } from 'react';

type ChatLine = {
  sender: 'user' | 'agent';
  text: string;
};

type StreamMessage = {
  type?: string;
  status?: string;
  orchestration?: string | null;
  sessionId?: string;
  message?: string;
  reply?: string;
  isPartial?: boolean;
};

function App() {
  const [sessionId] = useState(() => `session-${Math.random().toString(36).slice(2, 8)}`);
  const [message, setMessage] = useState('');
  const [chat, setChat] = useState<ChatLine[]>([]);
  const [connected, setConnected] = useState(false);
  const [hasSubmittedFirstMessage, setHasSubmittedFirstMessage] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);

  const applyAgentChunk = (data: StreamMessage) => {
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

  const connectWebSocket = () => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      return wsRef.current;
    }

    const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
    const ws = new WebSocket(`${protocol}://${window.location.host}/api/chat/ws/${sessionId}`);
    ws.onopen = () => setConnected(true);
    ws.onerror = () => setConnected(false);
    ws.onclose = () => setConnected(false);
    ws.onmessage = (event) => {
      const data = JSON.parse(event.data) as StreamMessage;
      if (data.type === 'ack' || data.type === 'error') {
        return;
      }
      applyAgentChunk(data);
    };
    wsRef.current = ws;
    return ws;
  };

  const ensureWebSocketConnected = async (): Promise<WebSocket> => {
    const existing = wsRef.current;
    if (existing && existing.readyState === WebSocket.OPEN) {
      return existing;
    }

    const ws = connectWebSocket();
    if (ws.readyState === WebSocket.OPEN) {
      return ws;
    }

    await new Promise<void>((resolve, reject) => {
      const timeout = window.setTimeout(() => {
        reject(new Error('websocket connection timeout'));
      }, 5000);

      const onOpen = () => {
        window.clearTimeout(timeout);
        ws.removeEventListener('open', onOpen);
        ws.removeEventListener('error', onError);
        resolve();
      };

      const onError = () => {
        window.clearTimeout(timeout);
        ws.removeEventListener('open', onOpen);
        ws.removeEventListener('error', onError);
        reject(new Error('websocket connection failed'));
      };

      ws.addEventListener('open', onOpen);
      ws.addEventListener('error', onError);
    });

    return ws;
  };

  useEffect(() => {
    return () => {
      wsRef.current?.close();
    };
  }, []);

  const sendMessage = async () => {
    if (!message.trim()) return;
    const outgoingMessage = message;
    setChat((prev) => [...prev, { sender: 'user', text: outgoingMessage }]);
    setMessage('');

    if (!hasSubmittedFirstMessage) {
      await ensureWebSocketConnected();
      await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sessionId, message: outgoingMessage }),
      });

      setHasSubmittedFirstMessage(true);
      connectWebSocket();
      return;
    }

    const ws = await ensureWebSocketConnected();
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'chat', sessionId, message: outgoingMessage }));
      return;
    }

    await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sessionId, message: outgoingMessage }),
    });
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
