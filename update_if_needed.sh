#!/bin/bash

REPO_DIR="EDIT_HERE_WORKING_DIR"
BRANCH="main"
SERVICE_NAME="bixi.service"

cd "$REPO_DIR" || exit 1

git fetch origin "$BRANCH"

LOCAL=$(git rev-parse "$BRANCH")
REMOTE=$(git rev-parse "origin/$BRANCH")

if [ "$LOCAL" != "$REMOTE" ]; then
  echo "[$(date)] New updates found. Pulling and restarting..."
  git reset --hard "origin/$BRANCH"
  systemctl restart "$SERVICE_NAME"
else
  echo "[$(date)] No updates. Nothing to do."
fi
