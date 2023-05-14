FROM python:3.9-buster
ARG DEBIAN_FRONTEND=noninteractive

WORKDIR /app
COPY src .

RUN pip install -r requirements.txt
CMD ["python", "main.py"]
# ENTRYPOINT ["gunicorn", "api.api_main:fast_api", "--workers", "1", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:9902"]
