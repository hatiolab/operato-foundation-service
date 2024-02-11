#!/usr/bin/env bash
APP_VERSION=$(python src/version.py)
echo "Application Version: ${APP_VERSION}"
docker push hatiolab/locking-service:${APP_VERSION} && docker push hatiolab/locking-service:latest

