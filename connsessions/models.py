"""Durable state: who the tenants are, and what happened to every session.

Deliberately not the enforcement path. Live counts and admission decisions live
in Redis (see registry.py) because a per-connect transaction here would contend
exactly when load peaks. Postgres answers "what happened" and "what is this org
allowed"; Redis answers "what is true right now".
"""

from django.core.validators import MinValueValidator
from django.db import models


class Organization(models.Model):
    """A tenant, and its slice of platform capacity.

    Two numbers, doing different jobs:

    max_sessions is a ceiling -- the org cannot exceed it no matter how empty
    the platform is. It is what the org pays for.

    reserved_floor is a guarantee -- capacity this org can always reach even
    when the platform is otherwise full. Without it, one org arriving first
    could take every free slot and a paying tenant would be locked out despite
    being well under its own limit. Above the floor, orgs compete first-come
    for whatever global capacity is left.
    """

    slug = models.SlugField(
        max_length=64,
        unique=True,
        help_text="Stable identifier used in Redis keys and the connect URL.",
    )
    name = models.CharField(max_length=200)

    max_sessions = models.PositiveIntegerField(
        default=100,
        validators=[MinValueValidator(0)],
        help_text="Hard ceiling on concurrent sessions for this org.",
    )
    reserved_floor = models.PositiveIntegerField(
        default=0,
        validators=[MinValueValidator(0)],
        help_text=(
            "Concurrent sessions this org can always reach, even when global "
            "capacity is exhausted. Must not exceed max_sessions."
        ),
    )

    is_active = models.BooleanField(
        default=True,
        help_text="Inactive orgs are refused at connect time.",
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["slug"]

    def __str__(self):
        return f"{self.name} ({self.slug})"

    def clean(self):
        from django.core.exceptions import ValidationError

        # A floor above the ceiling is incoherent: it would promise capacity the
        # org is not allowed to use. Caught here so admin and fixtures both
        # reject it rather than leaving Redis to arbitrate nonsense.
        if self.reserved_floor > self.max_sessions:
            raise ValidationError(
                {"reserved_floor": "reserved_floor cannot exceed max_sessions."}
            )


class SessionRecord(models.Model):
    """Audit trail: one row per session, written on connect, closed on end.

    Rows are never deleted, so a reviewer can ask what happened after the fact --
    which node held a session, how it ended, how long it lasted. Live state is
    in Redis; this is history.
    """

    class EndReason(models.TextChoices):
        CLIENT = "client", "Client disconnected"
        NODE_LOST = "node_lost", "Node stopped heartbeating"
        DRAINED = "drained", "Node drained for shutdown"
        REJECTED = "rejected", "Refused at admission"

    session_id = models.UUIDField(unique=True, db_index=True)
    organization = models.ForeignKey(
        Organization,
        on_delete=models.CASCADE,
        related_name="session_records",
    )

    # Plain text, not a FK: nodes are ephemeral and a node that has gone away
    # forever must not stop us recording the sessions it once held.
    node_id = models.CharField(max_length=128, db_index=True)
    client_id = models.CharField(
        max_length=128,
        blank=True,
        help_text="Caller-supplied end-user identifier, opaque to this layer.",
    )

    started_at = models.DateTimeField()
    ended_at = models.DateTimeField(null=True, blank=True)
    end_reason = models.CharField(
        max_length=16,
        choices=EndReason.choices,
        blank=True,
    )

    class Meta:
        indexes = [
            # The hot query: live sessions for one org. ended_at IS NULL is the
            # liveness test, so lead with the org and keep it covering.
            models.Index(fields=["organization", "ended_at"]),
            models.Index(fields=["node_id", "ended_at"]),
        ]
        ordering = ["-started_at"]

    def __str__(self):
        state = "live" if self.ended_at is None else self.end_reason or "ended"
        return f"{self.session_id} [{state}]"

    @property
    def is_live(self):
        return self.ended_at is None
