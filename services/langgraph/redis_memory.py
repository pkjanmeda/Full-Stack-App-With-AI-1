import hashlib
import json
import math
import re
from datetime import datetime

import redis.asyncio as redis


class RedisConversationMemory:
    def __init__(
        self,
        redis_url: str,
        ttl_seconds: int = 3600,
        max_turns: int = 50,
        similarity_threshold: float = 0.72,
        vector_size: int = 256,
    ):
        self.redis_url = redis_url
        self.ttl_seconds = ttl_seconds
        self.max_turns = max_turns
        self.similarity_threshold = similarity_threshold
        self.vector_size = vector_size
        self.client = None

    async def connect(self):
        self.client = redis.from_url(self.redis_url, decode_responses=True)
        await self.client.ping()

    async def close(self):
        if self.client is not None:
            await self.client.close()

    def _session_key(self, session_id: str) -> str:
        return f'chat:memory:{session_id}'

    def _tokenize(self, text: str) -> list[str]:
        return re.findall(r"[a-z0-9']+", text.lower())

    def _vectorize(self, text: str) -> dict[int, float]:
        counts = {}
        for token in self._tokenize(text):
            bucket = int(hashlib.sha1(token.encode()).hexdigest(), 16) % self.vector_size
            counts[bucket] = counts.get(bucket, 0.0) + 1.0

        magnitude = math.sqrt(sum(value * value for value in counts.values()))
        if magnitude == 0:
            return {}

        return {index: value / magnitude for index, value in counts.items()}

    def _cosine_similarity(self, a: dict[int, float], b: dict[int, float]) -> float:
        if not a or not b:
            return 0.0
        common = set(a.keys()) & set(b.keys())
        if not common:
            return 0.0
        return sum(a[index] * b[index] for index in common)

    async def add_turn(self, session_id: str, user_message: str, assistant_reply: str, source: str):
        if self.client is None:
            return

        key = self._session_key(session_id)
        payload = {
            'sessionId': session_id,
            'userMessage': user_message,
            'assistantReply': assistant_reply,
            'source': source,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
        }
        await self.client.lpush(key, json.dumps(payload))
        await self.client.ltrim(key, 0, self.max_turns - 1)
        await self.client.expire(key, self.ttl_seconds)

    async def search_similar(self, session_id: str, query: str):
        if self.client is None:
            return None

        key = self._session_key(session_id)
        entries = await self.client.lrange(key, 0, self.max_turns - 1)
        if not entries:
            return None

        query_vector = self._vectorize(query)
        best_match = None
        best_score = 0.0

        for raw in entries:
            try:
                item = json.loads(raw)
            except json.JSONDecodeError:
                continue

            user_message = str(item.get('userMessage', ''))
            response_text = str(item.get('assistantReply', ''))
            if not user_message or not response_text:
                continue

            score = self._cosine_similarity(query_vector, self._vectorize(user_message))
            if score > best_score:
                best_score = score
                best_match = item

        if best_match is None or best_score < self.similarity_threshold:
            return None

        return {
            'score': round(best_score, 4),
            'reply': best_match.get('assistantReply'),
            'matchedQuestion': best_match.get('userMessage'),
            'source': best_match.get('source', 'memory'),
        }