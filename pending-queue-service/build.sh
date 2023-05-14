#!/usr/bin/env bash

APP_VERSION=$(python src/version.py)
echo "Applicaton Version: ${APP_VERSION}"
docker image build --platform linux/amd64 -t hatiolab/pending-queue:${APP_VERSION} -t hatiolab/pending-queue:latest .
