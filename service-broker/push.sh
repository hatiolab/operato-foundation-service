#!/usr/bin/env bash
APP_VERSION=$(python src/version.py)
echo "Application Version: ${APP_VERSION}"
docker push hatiolab/service-broker:${APP_VERSION} && docker push hatiolab/service-broker:latest

