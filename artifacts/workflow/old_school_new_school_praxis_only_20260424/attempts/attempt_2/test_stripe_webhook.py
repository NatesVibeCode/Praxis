from __future__ import annotations

import unittest

from stripe_webhook import handle_stripe_webhook


def subscription_event(customer_id, status):
    return {
        "type": "customer.subscription.updated",
        "data": {"object": {"customer": customer_id, "status": status}},
    }


class StripeWebhookTests(unittest.TestCase):
    def test_active_subscription_marks_user_active(self):
        users = {"cus_123": {"status": "inactive"}}
        result = handle_stripe_webhook(subscription_event("cus_123", "active"), users)
        self.assertTrue(result["updated"])
        self.assertEqual(users["cus_123"]["status"], "active")

    def test_canceled_subscription_marks_user_inactive(self):
        users = {"cus_123": {"status": "active"}}
        result = handle_stripe_webhook(subscription_event("cus_123", "canceled"), users)
        self.assertTrue(result["updated"])
        self.assertEqual(result["user_status"], "inactive")
        self.assertEqual(users["cus_123"]["status"], "inactive")

    def test_unrelated_event_is_ignored(self):
        users = {}
        result = handle_stripe_webhook({"type": "invoice.paid"}, users)
        self.assertEqual(result, {"updated": False, "reason": "ignored_event"})
        self.assertEqual(users, {})


if __name__ == "__main__":
    unittest.main()
