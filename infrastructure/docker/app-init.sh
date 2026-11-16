#!/bin/sh
# One-shot container: apply database migrations and create the login accounts from .env.
# Idempotent: existing accounts are left unchanged (their passwords are never reset).
set -eu
cd /app/backend
python -m app.cli migrate
for role in ADMIN ANALYST; do
  if [ "$role" = ADMIN ]; then user="${FRAUDSTREAM_ADMIN_USERNAME:-}"; pwvar=FRAUDSTREAM_ADMIN_PASSWORD
  else user="${FRAUDSTREAM_ANALYST_USERNAME:-}"; pwvar=FRAUDSTREAM_ANALYST_PASSWORD; fi
  if [ -z "$user" ]; then echo "no $role username configured - skipped"; continue; fi
  printf '%s (%s): ' "$user" "$role"
  python -m app.cli create-user --username "$user" --role "$role" --full-name "Container $role" --password-env "$pwvar"
done
echo "app-init complete"
