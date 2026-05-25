import hashlib
import json
import math
import re
from datetime import datetime
from typing import Any, cast

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
        client = cast(Any, self.client)
        await client.ping()

    async def close(self):
        if self.client is not None:
            client = cast(Any, self.client)
            await client.close()

    def _session_key(self, session_id: str) -> str:
        return f'chat:memory:{session_id}'

    def _profile_key(self, session_id: str) -> str:
        return f'chat:profile:{session_id}'

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

        client = cast(Any, self.client)
        key = self._session_key(session_id)
        payload = {
            'sessionId': session_id,
            'userMessage': user_message,
            'assistantReply': assistant_reply,
            'source': source,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
        }
        await client.lpush(key, json.dumps(payload))
        await client.ltrim(key, 0, self.max_turns - 1)
        await client.expire(key, self.ttl_seconds)

    async def search_similar(self, session_id: str, query: str):
        if self.client is None:
            return None

        client = cast(Any, self.client)
        key = self._session_key(session_id)
        entries = await client.lrange(key, 0, self.max_turns - 1)
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

    async def set_user_name(self, session_id: str, user_name: str):
        if self.client is None:
            return

        client = cast(Any, self.client)
        profile_key = self._profile_key(session_id)
        await client.hset(profile_key, mapping={'userName': user_name})
        await client.expire(profile_key, self.ttl_seconds)

    async def get_user_name(self, session_id: str):
        if self.client is None:
            return None

        client = cast(Any, self.client)
        profile_key = self._profile_key(session_id)
        value = await client.hget(profile_key, 'userName')
        if value is None:
            return None
        name = str(value).strip()
        return name or None