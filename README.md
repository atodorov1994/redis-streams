# Multi-Module Messaging System

This repository contains a multi-module Java application for a message processing pipeline consisting of:

- **Producer**: Generates and publishes messages.
- **Bridge**: Intermediate service that recieves messages and forwards them to srtream.
- **Consumer**: Consumes input stream, executes logic and forwards to output stream.

## 🧱 Modules Overview

### 1. `producer/`
Responsible for generating messages and sending them to `messages::published` channel.
For the sake of the exercise, we assume that this is a third party application and we don't have access to it.

### 2. `bridge/`
Acts as an intermediary service that:
- Listens to messages from `messages::published`.
- Transforms `messages::published` channel to `messages::published` stream.
  
#### 📝 Note
This module is not designed to scale horizonally, because the distributed synchronization causes
too much overhead to the throughput. Instead we have one leader instance that can scale vertically and
one idle instance, that can continue the processing if the leader crashes.

### 3. `consumer/`
- Consumes messages from `messages::published` input stream
- Executes business logic
- Produces messages to `messages::processed` output stream
- MetricsService monitors the throughput

## 🧭 Architecture Diagram
![Diagram](https://github.com/atodorov1994/redis-streams/blob/master/diagram.png)

## 📦 Tech Stack

- Java 17+
- Spring Boot
- Spring Data Redis
- Maven (multi-module structure)

## 🚀 Getting Started

### Prerequisites

- Java 17
- Maven 3.6+
- Redis server
- Docker Environment (for tests)

### Running Services
```
.\mvnw clean install -DskipTests
```

```
.\mvnw spring-boot:run -pl bridge
```

```
.\mvnw spring-boot:run -pl consumer
```

```
.\mvnw spring-boot:run -pl producer
```
You can also run the project from your IDE.
Just make sure bridge and consumer modules are up before the producer, for optimal test results.

## ⚙️ Configuration

Each module uses `application.properties` file for configurations.
Example:
```
spring.application.name=consumer-${random.uuid}
server.port=8080

spring.data.redis.host=localhost
spring.data.redis.port=6379

redis.stream.key=${CHANNEL_TOPIC: messages:published}
redis.output.stream.key=${OUTPUT_TOPIC: messages:processed}
redis.consumer-group.id:${CONSUMER_ID: messages-published-group}
redis.consumer-group.size=${GROUP_SIZE: 4}
redis.active-subscription-key=${ACTIVE_SUBS_KEY: input-stream:active-subscriptions}
```

## 📈 Throughput Results

|  Bridge Threads  |  Consumer Threads  | AvrgThroughput (events/3sec) |
|------------------|--------------------|------------------------------|
| 1                | 1                  | 7000                         |
| 1                | 2                  | 10000                        |
| 1                | 4                  | 17000                        |
| 2                | 1                  | 8000                         |
| 2                | 2                  | 11000                        |
| 2                | 4                  | 18000                        |

### 📝 Notes

- Producer produces 3000 messages at rate between 100-200ms for 30s.
  Total messages produced on avrg = 270K
- All tests are conducted for single instance - multiple threads
- Results will vary, depending on the hardware
- 2-3 bridge threads is optimal.















