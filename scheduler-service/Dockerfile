FROM python:3.9-buster
ARG DEBIAN_FRONTEND=noninteractive

WORKDIR /app
COPY src .

RUN pip install -r requirements.txt
CMD ["python", "main.py"]

