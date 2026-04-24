from __future__ import annotations

ACTIVE_STRIPE_STATUSES = {"active", "trialing"}


def handle_stripe_webhook(event, user_store):
    if event.get("type") != "customer.subscription.updated":
        return {"updated": False, "reason": "ignored_event"}

    subscription = event.get("data", {}).get("object", {})
    customer_id = subscription.get("customer")
    stripe_status = subscription.get("status")
    if not customer_id or not stripe_status:
        raise ValueError("subscription event requires customer and status")

    user_status = "active" if stripe_status in ACTIVE_STRIPE_STATUSES else "inactive"
    user_store.setdefault(customer_id, {})["status"] = user_status
    return {
        "updated": True,
        "customer_id": customer_id,
        "stripe_status": stripe_status,
        "user_status": user_status,
    }
