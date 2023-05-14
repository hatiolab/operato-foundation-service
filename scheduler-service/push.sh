#!/usr/bin/env bash
APP_VERSION=$(python src/version.py)
echo "Application Version: ${APP_VERSION}"
docker push hatiolab/schevt-mgr:${APP_VERSION} && docker push hatiolab/schevt-mgr:latest

