"""Seed tenants for demos and the chaos scripts.

Idempotent, so it is safe to re-run against a live database. The default set is
chosen to exercise requirement 4 -- a mix of usage patterns, not three identical
orgs: acme is the big steady tenant, globex is the one that spikes and would
starve the others without a reserved floor, initech is small and is what the
floor protects.
"""

from django.core.management.base import BaseCommand

from connsessions.models import Organization

DEFAULTS = [
    # slug, name, max_sessions, reserved_floor
    ("acme", "Acme Corp", 500, 100),
    ("globex", "Globex Corporation", 800, 50),
    ("initech", "Initech", 100, 25),
]


class Command(BaseCommand):
    help = "Create or update the demo organizations."

    def add_arguments(self, parser):
        parser.add_argument(
            "--reset-limits",
            action="store_true",
            help="Overwrite limits on orgs that already exist.",
        )

    def handle(self, *args, **options):
        for slug, name, max_sessions, floor in DEFAULTS:
            org, created = Organization.objects.get_or_create(
                slug=slug,
                defaults={
                    "name": name,
                    "max_sessions": max_sessions,
                    "reserved_floor": floor,
                },
            )
            if created:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"created {slug}: max={max_sessions} floor={floor}"
                    )
                )
            elif options["reset_limits"]:
                org.max_sessions = max_sessions
                org.reserved_floor = floor
                org.is_active = True
                org.save()
                self.stdout.write(f"updated {slug}: max={max_sessions} floor={floor}")
            else:
                self.stdout.write(
                    f"exists  {slug}: max={org.max_sessions} "
                    f"floor={org.reserved_floor} (use --reset-limits to change)"
                )
