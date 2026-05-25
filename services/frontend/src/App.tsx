import { useEffect, useMemo, useRef, useState } from 'react';

type ChatLine = {
  sender: 'user' | 'agent';
  text: string;
  cacheHit?: boolean;
};

type StreamMessage = {
  type?: string;
  status?: string;
  orchestration?: string | null;
  sessionId?: string;
  message?: string;
  reply?: string;
  isPartial?: boolean;
  cacheHit?: boolean;
};

type Conversation = {
  id: string;
  title: string;
  chat: ChatLine[];
  connected: boolean;
  hasSubmittedFirstMessage: boolean;
};

function createConversation(index: number): Conversation {
  return {
    id: `thread-${Math.random().toString(36).slice(2, 10)}`,
    title: `Conversation ${index}`,
    chat: [],
    connected: false,
    hasSubmittedFirstMessage: false,
  };
}

function App() {
  const [conversations, setConversations] = useState<Conversation[]>(() => [createConversation(1)]);
  const [activeConversationId, setActiveConversationId] = useState<string>('');
  const [message, setMessage] = useState('');
  const wsRef = useRef<Record<string, WebSocket>>({});
  const chatWindowRef = useRef<HTMLElement | null>(null);

  const activeConversation = useMemo(
    () => conversations.find((conversation) => conversation.id === activeConversationId) ?? conversations[0],
    [activeConversationId, conversations],
  );

  const applyAgentChunk = (conversationId: string, data: StreamMessage) => {
      if (typeof data.reply !== 'string') return;
      const reply = data.reply;

      setConversations((prev) =>
        prev.map((conversation) => {
          if (conversation.id !== conversationId) return conversation;

          const currentChat = conversation.chat;
          if (data.isPartial) {
            const last = currentChat[currentChat.length - 1];
            if (last?.sender === 'agent') {
              return {
                ...conversation,
                chat: [
                  ...currentChat.slice(0, -1),
                  {
                    ...last,
                    text: reply,
                    cacheHit: typeof data.cacheHit === 'boolean' ? data.cacheHit : last.cacheHit,
                  },
                ],
              };
            }
            return {
              ...conversation,
              chat: [...currentChat, { sender: 'agent', text: reply, cacheHit: data.cacheHit }],
            };
          }

          const last = currentChat[currentChat.length - 1];
          if (last?.sender === 'agent') {
            return {
              ...conversation,
              chat: [
                ...currentChat.slice(0, -1),
                {
                  ...last,
                  text: reply,
                  cacheHit: typeof data.cacheHit === 'boolean' ? data.cacheHit : last.cacheHit,
                },
              ],
            };
          }
          return {
            ...conversation,
            chat: [...currentChat, { sender: 'agent', text: reply, cacheHit: data.cacheHit }],
          };
        }),
      );
  };

  const connectWebSocket = (conversationId: string) => {
    const existing = wsRef.current[conversationId];
    if (existing && existing.readyState === WebSocket.OPEN) {
      return existing;
    }

    const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
    const ws = new WebSocket(`${protocol}://${window.location.host}/api/chat/ws/${conversationId}`);
    ws.onopen = () => {
      setConversations((prev) =>
        prev.map((conversation) =>
          conversation.id === conversationId ? { ...conversation, connected: true } : conversation,
        ),
      );
    };
    ws.onerror = () => {
      setConversations((prev) =>
        prev.map((conversation) =>
          conversation.id === conversationId ? { ...conversation, connected: false } : conversation,
        ),
      );
    };
    ws.onclose = () => {
      setConversations((prev) =>
        prev.map((conversation) =>
          conversation.id === conversationId ? { ...conversation, connected: false } : conversation,
        ),
      );
    };
    ws.onmessage = (event) => {
      const data = JSON.parse(event.data) as StreamMessage;
      if (data.type === 'ack' || data.type === 'error') {
        return;
      }
      applyAgentChunk(conversationId, data);
    };
    wsRef.current[conversationId] = ws;
    return ws;
  };

  const ensureWebSocketConnected = async (conversationId: string): Promise<WebSocket> => {
    const existing = wsRef.current[conversationId];
    if (existing && existing.readyState === WebSocket.OPEN) {
      return existing;
    }

    const ws = connectWebSocket(conversationId);
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
      Object.values(wsRef.current).forEach((socket) => socket.close());
    };
  }, []);

  useEffect(() => {
    const chatWindow = chatWindowRef.current;
    if (!chatWindow) return;
    chatWindow.scrollTop = chatWindow.scrollHeight;
  }, [activeConversation?.chat]);

  useEffect(() => {
    if (!activeConversation) return;
    setMessage('');
  }, [activeConversationId, activeConversation]);

  useEffect(() => {
    if (!activeConversationId && conversations.length > 0) {
      setActiveConversationId(conversations[0].id);
    }
  }, [activeConversationId, conversations]);

  const createNewConversation = () => {
    const next = createConversation(conversations.length + 1);
    setConversations((prev) => [...prev, next]);
    setActiveConversationId(next.id);
  };

  const sendMessage = async () => {
    if (!message.trim() || !activeConversation) return;

    const outgoingMessage = message.trim();
    const conversationId = activeConversation.id;
    setConversations((prev) =>
      prev.map((conversation) =>
        conversation.id === conversationId
          ? { ...conversation, chat: [...conversation.chat, { sender: 'user', text: outgoingMessage }] }
          : conversation,
      ),
    );
    setMessage('');

    if (!activeConversation.hasSubmittedFirstMessage) {
      await ensureWebSocketConnected(conversationId);
      await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sessionId: conversationId, message: outgoingMessage }),
      });

      setConversations((prev) =>
        prev.map((conversation) =>
          conversation.id === conversationId
            ? { ...conversation, hasSubmittedFirstMessage: true }
            : conversation,
        ),
      );
      connectWebSocket(conversationId);
      return;
    }

    const ws = await ensureWebSocketConnected(conversationId);
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'chat', sessionId: conversationId, message: outgoingMessage }));
      return;
    }

    await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sessionId: conversationId, message: outgoingMessage }),
    });
  };

  if (!activeConversation) {
    return null;
  }

  return (
    <div className="app-shell">
      <aside className="conversation-panel">
        <div className="conversation-panel-header">
          <h2>Conversations</h2>
          <button type="button" onClick={createNewConversation}>
            New
          </button>
        </div>
        <div className="conversation-list">
          {conversations.map((conversation) => (
            <button
              key={conversation.id}
              type="button"
              className={`conversation-item ${conversation.id === activeConversationId ? 'active' : ''}`}
              onClick={() => setActiveConversationId(conversation.id)}
            >
              <strong>{conversation.title}</strong>
              <span>{conversation.id}</span>
            </button>
          ))}
        </div>
      </aside>

      <main className="chat-panel">
        <header>
          <h1>AI Chat Stream</h1>
          <p>
            Thread: {activeConversation.id} | Status: {activeConversation.connected ? 'Connected' : 'Disconnected'}
          </p>
        </header>
        <section ref={chatWindowRef} className="chat-window">
          {activeConversation.chat.map((line, idx) => (
            <div key={idx} className={`chat-line ${line.sender}`}>
              <span>{line.sender === 'user' ? 'You' : 'Agent'}</span>
              {line.sender === 'agent' && line.cacheHit && <span> (cache)</span>}
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
      </main>
    </div>
  );
}

export default App;
