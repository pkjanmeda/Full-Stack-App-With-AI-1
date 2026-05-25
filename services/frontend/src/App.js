import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useEffect, useRef, useState } from 'react';
function App() {
    const [sessionId] = useState(() => `session-${Math.random().toString(36).slice(2, 8)}`);
    const [message, setMessage] = useState('');
    const [chat, setChat] = useState([]);
    const [connected, setConnected] = useState(false);
    const [hasSubmittedFirstMessage, setHasSubmittedFirstMessage] = useState(false);
    const wsRef = useRef(null);
    const chatWindowRef = useRef(null);
    const applyAgentChunk = (data) => {
        if (typeof data.reply !== 'string')
            return;
        const reply = data.reply;
        setChat((prev) => {
            if (data.isPartial) {
                const last = prev[prev.length - 1];
                if (last?.sender === 'agent') {
                    return [
                        ...prev.slice(0, -1),
                        {
                            ...last,
                            text: reply,
                            cacheHit: typeof data.cacheHit === 'boolean' ? data.cacheHit : last.cacheHit,
                        },
                    ];
                }
                return [...prev, { sender: 'agent', text: reply, cacheHit: data.cacheHit }];
            }
            const last = prev[prev.length - 1];
            if (last?.sender === 'agent') {
                return [
                    ...prev.slice(0, -1),
                    {
                        ...last,
                        text: reply,
                        cacheHit: typeof data.cacheHit === 'boolean' ? data.cacheHit : last.cacheHit,
                    },
                ];
            }
            return [...prev, { sender: 'agent', text: reply, cacheHit: data.cacheHit }];
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
            const data = JSON.parse(event.data);
            if (data.type === 'ack' || data.type === 'error') {
                return;
            }
            applyAgentChunk(data);
        };
        wsRef.current = ws;
        return ws;
    };
    const ensureWebSocketConnected = async () => {
        const existing = wsRef.current;
        if (existing && existing.readyState === WebSocket.OPEN) {
            return existing;
        }
        const ws = connectWebSocket();
        if (ws.readyState === WebSocket.OPEN) {
            return ws;
        }
        await new Promise((resolve, reject) => {
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
    useEffect(() => {
        const chatWindow = chatWindowRef.current;
        if (!chatWindow)
            return;
        chatWindow.scrollTop = chatWindow.scrollHeight;
    }, [chat]);
    const sendMessage = async () => {
        if (!message.trim())
            return;
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
    return (_jsxs("div", { className: "app-shell", children: [_jsxs("header", { children: [_jsx("h1", { children: "AI Chat Stream" }), _jsxs("p", { children: ["Status: ", connected ? 'Connected' : 'Disconnected'] })] }), _jsx("section", { ref: chatWindowRef, className: "chat-window", children: chat.map((line, idx) => (_jsxs("div", { className: `chat-line ${line.sender}`, children: [_jsx("span", { children: line.sender === 'user' ? 'You' : 'Agent' }), line.sender === 'agent' && line.cacheHit && _jsx("span", { children: " (cache)" }), _jsx("p", { children: line.text })] }, idx))) }), _jsxs("footer", { children: [_jsx("input", { value: message, onChange: (event) => setMessage(event.target.value), onKeyDown: (event) => event.key === 'Enter' && sendMessage(), placeholder: "Type a message..." }), _jsx("button", { onClick: sendMessage, children: "Send" })] })] }));
}
export default App;
