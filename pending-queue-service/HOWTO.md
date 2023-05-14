# Install

## Setup

### Prerequistes

- python 3.9 or later


### Install the required modules

```bash
pip install -r src/requirements.txt
```

#### Workaround 
##### failing to install psycopg2 on Mac M1

```bash
# please 
brew install postgresql
```

   




### Prepare ***config.yaml***

copy ***config/config.yaml*** to ***src/*** after modifying it if necessary.


### Start API server

```bash
cd src
python3 main.py
```

### Run test codes.

```bash
cd src
python3 -m unittest discover -s test -p "*_test.py"
```

## Deployment

#### Build a docker image

```bash
# build a docker image
./build.sh

# push the docker image
./push.sh
```


## API Manual

```bash
{URL}:9903/docs

```

![Scalable Scheduler OpenAPI](./assets/scheduler-service-openapi.png "Scalable Scheduler OpenAPI")

### Schedule Type

#### Once
- now
  - no specific schedule 
  
  ```json
   {
  "client": {
    "name": "gangster",
    "group": "activity",
    "key": "abcdef"
  },
  "type": "now",
  "schedule": "",
  "task": {
    ...
  }
  ```

- date 
  - iso time format (ex. "2023-02-03T13:33:12") 
  
  ```json
   {
  "client": {
    "name": "gangster",
    "group": "activity",
    "key": "abcdef"
  },
  "type": "date",
  "schedule": "2023-02-03T13:33:12",
  "task": {
    ...
  }
  ```

- delay 
  - unit: seconds (ex. "10") 

  ```json
   {
  "name": "test03",
  "type": "delay",
  "schedule": "30",
  "task": {
    ...
  }
  ```


#### Recurring
- cron
  - ex. */1 * * * *

  ```json
   {
  "client": {
    "name": "gangster",
    "group": "activity",
    "key": "abcdef"
  },
  "type": "cron",
  "schedule": "*/1 * * * *",
  "task": {
    ...
  }
  ```

- delay-recur
  - unit: every seconds (ex "10")

  ```json
   {
  "client": {
    "name": "gangster",
    "group": "activity",
    "key": "abcdef"
  },
  "type": "delay-recur",
  "schedule": "60",
  "task": {
    ...
  }
  ```


### Task Type

- Rest

  ```json
   {
  "client": {
    "name": "gangster",
    "group": "activity",
    "key": "abcdef"
  },
  "type": "cron",
  "schedule": "*/1 * * * *",
  "task": {
    "type": "rest",
    "connection": {"host": "http://localhost:3000/api/unstable/run-scenario/test-scenario-1", "headers": {"Content-Type": "application/json", "accept": "*/*"}},
    "data": {"instanceName": "test-scenario-1", "variables": {}}
  }
  ```

- Kafka

  ```json
   {
  "client": {
    "name": "gangster",
    "group": "activity",
    "key": "abcdef"
  },
  "type": "cron",
  "schedule": "*/1 * * * *",
  "task": {
    "type": "kafka",
    "connection": {"host": "localhost:9092", "topic": "example-topic"},
    "data": { ... }
  }
  ```

- Redis

  ```json
   {
  "client": {
    "name": "gangster",
    "group": "activity",
    "key": "abcdef"
  },
  "type": "cron",
  "schedule": "*/1 * * * *",
  "task": {
    "type": "redis",
    "connection": {"host": "redis://localhost", "topic": "example-topic"},
    "data": { ... }
  }
  ```


#### Task Rest Example with Cron Schedule

```json
{
  "client": {
    "name": "gangster",
    "group": "activity",
    "key": "abcdef"
  },
  "type": "cron",
  "schedule": "*/1 * * * *",
  "task": {
    "type": "rest",
    "connection": {
      "host": "http://localhost:3000/api/unstable/run-scenario/DELAY-TEST", 
      "headers": {"Content-Type": "application/json", "accept": "*/*"},
      "data": {"instanceName": "delay-test", "variables": {}}
    }
  }
}
```

#### Task Kafka Example with Now Schedule

```json
{
  "client": {
    "name": "gangster",
    "group": "activity",
    "key": "abcdef"
  },
  "type": "now",
  "schedule": "",
  "task": {
    "type": "kafka",
    "connection": {
      "host": "localhost:9092", 
      "topic": "example-topic"},
    "data": {"id": "toboe"}
  }
}
```



## Additional Things

### k8s deployment

Please refer to k8s/*.

### Code Formatter

Black(https://github.com/psf/black)

```bash
pip install black
```
