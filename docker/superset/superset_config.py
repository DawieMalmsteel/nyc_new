# Local Superset configuration overrides.
import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "local-dev-key-change-me")

# Disable CSRF for bootstrap script (POST without CSRF token).
WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False
ENABLE_CSP = False
