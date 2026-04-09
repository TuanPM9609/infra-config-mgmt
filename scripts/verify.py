#!/usr/bin/env python3
"""verify.py — Re-collect sau apply, kiểm tra diff còn lại = 0."""
import argparse, yaml, subprocess, sys

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--system", required=True)
    parser.add_argument("--env",    required=True)
    args = parser.parse_args()

    # Re-collect từ target env
    subprocess.run([
        "python", "scripts/collect.py",
        "--system", args.system, "--env", args.env,
        "--output", f"tmp/{args.system}_{args.env}_verify_state.yaml"
    ], check=True)

    # Re-diff: so sánh lại với expected
    subprocess.run([
        "python", "scripts/diff.py",
        "--system", args.system, "--env", args.env,
        "--output", f"tmp/{args.system}_{args.env}_verify_diff.yaml"
    ], check=True)

    verify_diff = yaml.safe_load(open(f"tmp/{args.system}_{args.env}_verify_diff.yaml"))

    remaining_drifts = [
        x for x in verify_diff["db_params"] + verify_diff["os_params"]
        if x["status"] in ("DRIFT", "MISSING")
    ]

    if remaining_drifts:
        print(f"[verify] FAILED — {len(remaining_drifts)} params still drifted after apply:")
        for d in remaining_drifts:
            print(f"  {d['param']}: expected={d['expected']}, actual={d['actual']}")
        sys.exit(1)   # Jenkins đánh dấu stage FAIL
    else:
        print(f"[verify] PASSED — {args.system}/{args.env} fully in sync")

if __name__ == "__main__":
    main()
