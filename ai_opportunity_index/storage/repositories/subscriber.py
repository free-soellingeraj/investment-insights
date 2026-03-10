"""Async repository for subscriber, refresh request, and notification operations."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime

from sqlalchemy import select, update

from ai_opportunity_index.domains import (
    Notification,
    NotificationStatus,
    RefreshRequest,
    RefreshStatus,
    Subscriber,
)
from ai_opportunity_index.storage.models import (
    NotificationModel,
    RefreshRequestModel,
    SubscriberModel,
)
from ai_opportunity_index.storage.repositories.base import BaseRepository

logger = logging.getLogger(__name__)


class SubscriberRepository(BaseRepository[SubscriberModel]):
    """Async repository for subscribers, refresh requests, and notifications."""

    model_class = SubscriberModel

    @staticmethod
    def _subscriber_to_domain(m: SubscriberModel) -> Subscriber:
        return Subscriber(
            id=m.id,
            email=m.email,
            stripe_customer_id=m.stripe_customer_id,
            stripe_subscription_id=m.stripe_subscription_id,
            status=m.status,
            plan_tier=m.plan_tier,
            access_token=m.access_token,
            created_at=m.created_at,
        )

    @staticmethod
    def _refresh_request_to_domain(m: RefreshRequestModel) -> RefreshRequest:
        return RefreshRequest(
            id=m.id,
            subscriber_id=m.subscriber_id,
            company_id=m.company_id,
            dimensions=m.dimensions or [],
            status=m.status,
            pipeline_run_id=m.pipeline_run_id,
            requested_at=m.requested_at,
            completed_at=m.completed_at,
        )

    @staticmethod
    def _notification_to_domain(m: NotificationModel) -> Notification:
        return Notification(
            id=m.id,
            subscriber_id=m.subscriber_id,
            notification_type=m.notification_type,
            channel=m.channel,
            subject=m.subject,
            body=m.body,
            payload=m.payload or {},
            status=m.status,
            created_at=m.created_at,
        )

    # ── Subscribers ────────────────────────────────────────────────────────

    async def create_subscriber(
        self,
        email: str,
        stripe_customer_id: str | None,
        stripe_subscription_id: str | None,
    ) -> str:
        """Create or update a subscriber, returning the access token."""
        result = await self.session.execute(
            select(SubscriberModel).where(SubscriberModel.email == email)
        )
        existing = result.scalar_one_or_none()

        if existing:
            existing.stripe_customer_id = stripe_customer_id
            existing.stripe_subscription_id = stripe_subscription_id
            existing.status = "active"
            await self.session.flush()
            return existing.access_token

        token = uuid.uuid4().hex
        subscriber = SubscriberModel(
            email=email,
            stripe_customer_id=stripe_customer_id,
            stripe_subscription_id=stripe_subscription_id,
            status="active",
            access_token=token,
        )
        self.session.add(subscriber)
        await self.session.flush()
        return token

    async def get_subscriber_by_token(self, token: str) -> Subscriber | None:
        """Look up a subscriber by access token."""
        result = await self.session.execute(
            select(SubscriberModel).where(SubscriberModel.access_token == token)
        )
        model = result.scalar_one_or_none()
        return self._subscriber_to_domain(model) if model else None

    async def get_subscriber_by_email(self, email: str) -> Subscriber | None:
        """Look up a subscriber by email."""
        result = await self.session.execute(
            select(SubscriberModel).where(SubscriberModel.email == email)
        )
        model = result.scalar_one_or_none()
        return self._subscriber_to_domain(model) if model else None

    async def update_subscriber_status(
        self, stripe_subscription_id: str, new_status: str
    ) -> None:
        """Update subscriber status by Stripe subscription ID."""
        result = await self.session.execute(
            select(SubscriberModel).where(
                SubscriberModel.stripe_subscription_id == stripe_subscription_id
            )
        )
        model = result.scalar_one_or_none()
        if model:
            model.status = new_status
            await self.session.flush()
            logger.info("Subscriber %s status -> %s", model.email, new_status)

    # ── Refresh Requests ───────────────────────────────────────────────────

    async def create_refresh_request(self, req: RefreshRequest) -> RefreshRequest:
        """Create a refresh request."""
        model = RefreshRequestModel(
            subscriber_id=req.subscriber_id,
            company_id=req.company_id,
            dimensions=req.dimensions,
            status=req.status,
        )
        self.session.add(model)
        await self.session.flush()
        return self._refresh_request_to_domain(model)

    async def get_pending_refresh_requests(
        self, limit: int = 10
    ) -> list[RefreshRequest]:
        """Get pending refresh requests ordered by requested_at."""
        result = await self.session.execute(
            select(RefreshRequestModel)
            .where(RefreshRequestModel.status == RefreshStatus.PENDING)
            .order_by(RefreshRequestModel.requested_at)
            .limit(limit)
        )
        models = result.scalars().all()
        return [self._refresh_request_to_domain(m) for m in models]

    async def update_refresh_request_status(
        self,
        request_id: int,
        status: RefreshStatus | str,
        pipeline_run_id: int | None = None,
    ) -> None:
        """Update a refresh request's status."""
        values: dict = {"status": status}
        if pipeline_run_id is not None:
            values["pipeline_run_id"] = pipeline_run_id
        if status in (RefreshStatus.COMPLETED, RefreshStatus.FAILED):
            values["completed_at"] = datetime.utcnow()

        await self.session.execute(
            update(RefreshRequestModel)
            .where(RefreshRequestModel.id == request_id)
            .values(**values)
        )
        await self.session.flush()

    # ── Notifications ──────────────────────────────────────────────────────

    async def create_notification(self, notif: Notification) -> None:
        """Create a notification."""
        model = NotificationModel(
            subscriber_id=notif.subscriber_id,
            notification_type=notif.notification_type,
            channel=notif.channel,
            subject=notif.subject,
            body=notif.body,
            payload=notif.payload,
            status=notif.status,
        )
        self.session.add(model)
        await self.session.flush()

    async def get_pending_notifications(self, limit: int = 50) -> list[Notification]:
        """Get pending notifications."""
        result = await self.session.execute(
            select(NotificationModel)
            .where(NotificationModel.status == NotificationStatus.PENDING)
            .order_by(NotificationModel.created_at)
            .limit(limit)
        )
        models = result.scalars().all()
        return [self._notification_to_domain(m) for m in models]

    async def mark_notification_sent(self, notification_id: int) -> None:
        """Mark a notification as sent."""
        await self.session.execute(
            update(NotificationModel)
            .where(NotificationModel.id == notification_id)
            .values(status=NotificationStatus.SENT)
        )
        await self.session.flush()
