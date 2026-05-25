import argparse
import subprocess
import sys


def run_command(args: list[str]) -> int:
    print(f"Running: {' '.join(args)}")
    completed = subprocess.run(args, check=False)
    return completed.returncode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="One-command local Phoenix regression runner.")
    parser.add_argument("--dataset", default="datasets/evals/starter-evals.jsonl")
    parser.add_argument("--ws-base", default="ws://localhost:8000/api/chat/ws")
    parser.add_argument("--observed", default="datasets/evals/observed-results.jsonl")
    parser.add_argument("--scored", default="datasets/evals/scored-results.jsonl")
    parser.add_argument("--summary", default="datasets/evals/score-summary.json")
    parser.add_argument("--timeout", type=float, default=25.0)
    parser.add_argument("--pause-ms", type=int, default=50)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    replay_cmd = [
        sys.executable,
        "scripts/replay_eval_ws.py",
        "--dataset",
        args.dataset,
        "--ws-base",
        args.ws_base,
        "--output",
        args.observed,
        "--timeout",
        str(args.timeout),
        "--pause-ms",
        str(args.pause_ms),
    ]

    score_cmd = [
        sys.executable,
        "scripts/score_eval_results.py",
        "--input",
        args.observed,
        "--output",
        args.scored,
        "--summary",
        args.summary,
    ]

    replay_rc = run_command(replay_cmd)
    if replay_rc != 0:
        return replay_rc

    return run_command(score_cmd)


if __name__ == "__main__":
    raise SystemExit(main())
