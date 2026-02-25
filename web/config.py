"""Web configuration — loaded from environment variables."""

import os

STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET")
STRIPE_PRICE_ID = os.environ.get("STRIPE_PRICE_ID")
BASE_URL = os.environ.get("BASE_URL", "http://localhost:8080")
RESEND_API_KEY = os.environ.get("RESEND_API_KEY")
FROM_EMAIL = os.environ.get("FROM_EMAIL", "hello@winonaquantitative.com")
ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "asoellin@gmail.com")
DATABASE_URL = os.environ.get("DATABASE_URL")
