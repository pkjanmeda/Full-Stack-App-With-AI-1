import argparse
import asyncio
import json
import uuid
from pathlib import Path
from typing import Any

import websockets


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


async def replay_row(ws_base: str, row: dict[str, Any], timeout_s: float) -> dict[str, Any]:
    session_id = str(uuid.uuid4())
    uri = f"{ws_base.rstrip('/')}/{session_id}"

    async with websockets.connect(uri, open_timeout=timeout_s, close_timeout=timeout_s) as ws:
        await ws.send(json.dumps({"type": "chat", "message": row["input"]}))

        ack_received = False
        partial_count = 0
        final_event: dict[str, Any] | None = None

        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=timeout_s)
            event = json.loads(raw)

            if event.get("type") == "ack":
                ack_received = True
                continue

            if event.get("isPartial") is True:
                partial_count += 1
                continue

            if event.get("isPartial") is False:
                final_event = event
                break

            # Fallback for non-streamed payloads.
            if "reply" in event and "isPartial" not in event:
                final_event = event
                break

        if final_event is None:
            raise RuntimeError("No final event received for row")

        return {
            "sessionId": session_id,
            "input": row.get("input", ""),
            "expected_route": row.get("expected_route"),
            "expected_source": row.get("expected_source"),
            "expected_decline": row.get("expected_decline"),
            "expected_contains": row.get("expected_contains"),
            "cache_eligible": row.get("cache_eligible"),
            "ack_received": ack_received,
            "partial_count": partial_count,
            "observed_source": final_event.get("responseSource"),
            "observed_cache_hit": bool(final_event.get("cacheHit", False)),
            "observed_similarity_score": final_event.get("similarityScore"),
            "reply": final_event.get("reply", ""),
            "raw_final_event": final_event,
        }


async def replay_dataset(
    dataset_path: Path,
    ws_base: str,
    output_path: Path,
    timeout_s: float,
    pause_ms: int,
) -> None:
    rows = load_jsonl(dataset_path)
    results: list[dict[str, Any]] = []

    for index, row in enumerate(rows, start=1):
        result = await replay_row(ws_base=ws_base, row=row, timeout_s=timeout_s)
        results.append(result)
        print(f"[{index}/{len(rows)}] replayed: {row.get('input', '')}")
        if pause_ms > 0:
            await asyncio.sleep(pause_ms / 1000)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        "\n".join(json.dumps(item, ensure_ascii=False) for item in results) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote {len(results)} replay results to {output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay eval JSONL prompts through chat WebSocket API.")
    parser.add_argument(
        "--dataset",
        default="datasets/evals/starter-evals.jsonl",
        help="Input JSONL dataset path.",
    )
    parser.add_argument(
        "--ws-base",
        default="ws://localhost:8000/api/chat/ws",
        help="Base WebSocket URL without sessionId suffix.",
    )
    parser.add_argument(
        "--output",
        default="datasets/evals/observed-results.jsonl",
        help="Output JSONL path for observed replay results.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=25.0,
        help="Timeout in seconds for each websocket receive operation.",
    )
    parser.add_argument(
        "--pause-ms",
        type=int,
        default=50,
        help="Pause between requests in milliseconds.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        asyncio.run(
            replay_dataset(
                dataset_path=Path(args.dataset),
                ws_base=args.ws_base,
                output_path=Path(args.output),
                timeout_s=args.timeout,
                pause_ms=args.pause_ms,
            )
        )
        return 0
    except Exception as exc:
        print(f"Replay failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
