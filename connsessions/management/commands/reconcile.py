"""Close audit rows whose sessions are not in Redis.

The reaper handles the normal case: a node stops heartbeating, its sessions are
released and its rows marked node_lost. But a node that dies while it is not in
nodes:alive at all -- killed before its first heartbeat, or during a Redis outage
-- is never discovered, and its rows stay open forever while the live count is
correct.

Redis is the authority on what is live, so a row with no session in Redis is by
definition finished. Only the audit trail is repaired here; live counts are never
touched, because guessing at them is exactly the bug this system exists to avoid.

Not run on a timer: it is a repair tool, and a scheduled job that quietly fixes
drift would hide whatever is causing the drift. Run it after a known incident.

    manage.py reconcile --dry-run
    manage.py reconcile
"""

import asyncio

from django.core.management.base import BaseCommand
from django.utils import timezone

from connsessions import registry
from connsessions.models import SessionRecord


class Command(BaseCommand):
    help = "Close audit rows for sessions Redis no longer knows about."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report what would be closed without changing anything.",
        )
        parser.add_argument(
            "--min-age-sec",
            type=int,
            default=60,
            help=(
                "Ignore rows younger than this. Guards against closing a session "
                "that is mid-handshake: the row is written before Redis is read "
                "back, so a brand new row can legitimately have no session yet."
            ),
        )

    def handle(self, *args, **options):
        cutoff = timezone.now() - timezone.timedelta(seconds=options["min_age_sec"])
        open_rows = list(
            SessionRecord.objects.filter(
                ended_at__isnull=True, started_at__lt=cutoff
            ).values_list("id", "session_id", "node_id")
        )

        if not open_rows:
            self.stdout.write("nothing to reconcile")
            return

        live = asyncio.run(self._live_session_ids([str(s) for _, s, _ in open_rows]))

        orphaned = [(pk, sid, node) for pk, sid, node in open_rows if str(sid) not in live]

        self.stdout.write(
            f"open rows: {len(open_rows)}  still live in redis: {len(live)}  "
            f"orphaned: {len(orphaned)}"
        )

        if not orphaned:
            return

        by_node = {}
        for _, _, node in orphaned:
            by_node[node] = by_node.get(node, 0) + 1
        for node, count in sorted(by_node.items(), key=lambda kv: -kv[1]):
            self.stdout.write(f"  {node}: {count}")

        if options["dry_run"]:
            self.stdout.write(self.style.WARNING("dry run, nothing changed"))
            return

        updated = SessionRecord.objects.filter(
            id__in=[pk for pk, _, _ in orphaned]
        ).update(ended_at=timezone.now(), end_reason=SessionRecord.EndReason.NODE_LOST)

        self.stdout.write(self.style.SUCCESS(f"closed {updated} orphaned rows"))

    async def _live_session_ids(self, session_ids):
        """Which of these sessions still exist in Redis."""
        r = registry.get_redis()
        live = set()
        # Pipelined: one round-trip per batch rather than per session, which
        # matters when reconciling thousands of rows after an incident.
        for start in range(0, len(session_ids), 500):
            batch = session_ids[start : start + 500]
            async with r.pipeline(transaction=False) as pipe:
                for sid in batch:
                    pipe.exists(registry.session_key(sid))
                results = await pipe.execute()
            live.update(sid for sid, exists in zip(batch, results) if exists)
        await registry.close_redis()
        return live
