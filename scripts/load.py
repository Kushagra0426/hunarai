#!/usr/bin/env python3
"""Open N concurrent sessions through the load balancer and report what happened.

Connections are opened simultaneously rather than in sequence, because a limit
that holds when connections trickle in proves nothing -- the interesting case is
many arriving at once across different nodes.

    python scripts/load.py --org acme --count 600
    python scripts/load.py --org acme --count 50 --hold 60
"""

import argparse
import asyncio
import json
import sys
from collections import Counter

try:
    import websockets
except ImportError:
    sys.exit("pip install websockets")

# Close codes from connsessions/consumers.py and drain.py.
REASONS = {
    4404: "unknown org",
    4403: "org inactive",
    4429: "org at its limit",
    4503: "platform full",
    4504: "node draining",
    4409: "duplicate session",
    4500: "server error",
}


async def open_session(url, index, hold, established, rejected, sessions):
    try:
        ws = await websockets.connect(url, open_timeout=20)
    except Exception as exc:
        code = getattr(getattr(exc, "rcvd", None), "code", None)
        rejected[code or type(exc).__name__] += 1
        return

    # The handshake succeeds even when the session is refused: the server accepts
    # so it can send a close code the client can act on, then closes. So an
    # established session is one that received session.established, not one that
    # merely connected.
    try:
        msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=20))
        established.append(msg)
        sessions.append(ws)
        if hold:
            await asyncio.sleep(hold)
    except websockets.exceptions.ConnectionClosed as exc:
        rejected[exc.rcvd.code if exc.rcvd else "closed"] += 1
    except Exception as exc:
        rejected[type(exc).__name__] += 1
    finally:
        if not hold:
            await ws.close()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="ws://localhost:8090")
    parser.add_argument("--org", default="acme")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument(
        "--hold",
        type=float,
        default=0,
        help="Seconds to keep sessions open. 0 closes immediately.",
    )
    args = parser.parse_args()

    url = f"{args.url}/ws/{args.org}/"
    established, rejected, sessions = [], Counter(), []

    print(f"opening {args.count} simultaneous connections to {url}")

    await asyncio.gather(
        *(
            open_session(f"{url}?client_id=c{i}", i, args.hold,
                         established, rejected, sessions)
            for i in range(args.count)
        )
    )

    print(f"\n  established : {len(established)}")
    print(f"  rejected    : {sum(rejected.values())}")
    for code, n in sorted(rejected.items(), key=lambda kv: -kv[1]):
        label = REASONS.get(code, "")
        print(f"      {code} {label:20s} {n}")

    if established:
        by_node = Counter(m.get("node_id") for m in established)
        print("\n  spread across nodes:")
        for node, n in sorted(by_node.items()):
            print(f"      {node}: {n}")
        limit = established[0].get("org_limit")
        if limit:
            # Compare the peak concurrent count, not the cumulative total. With
            # --hold 0 each client closes as soon as it connects, so later
            # clients legitimately reuse freed slots and the total admitted can
            # far exceed the limit without it ever having been breached. Only
            # the high-water mark answers "were too many live at once".
            peak = max(m.get("org_sessions", 0) for m in established)
            print(f"\n  org limit: {limit}")
            print(f"  total admitted: {len(established)} (cumulative, slots get reused)")
            print(f"  peak concurrent: {peak}")
            if peak > limit:
                print("  LIMIT BREACHED")
                return 1
            print("  limit held")
            if not args.hold and len(established) > limit:
                print("  (run with --hold to keep sessions open and see the cap bind)")

    for ws in sessions:
        await ws.close()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
