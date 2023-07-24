# Install

## Application Preparations

### Prerequistes

- python 3.9 or later


### Install dependent modules

```bash
pip install -r src/requirements.txt
```

## Local Database Setup

### start the local database

- start the database docker container
```bash
cd local-test-db
docker-compouse up -d

```

- connect to the database with the following configuration
```yaml
database:
  host: 127.0.0.1
  port: 55432
  id: postgres
  pw: abcd1234
```

- create database named with 'scheduler'
```sql
CREATE DATABASE scheduler;

```

## Application Run

```bash
cd src
python main.py

```

### Prepare ***config.yaml***

copy ***config/config.yaml*** to ***src/*** after modifying it if necessary.

## API Endpoint

```bash
{URL}:9902/docs

```

## Workaround 
#### failing to install psycopg2 on Mac M1

```bash
# please 
brew install postgresql
```


