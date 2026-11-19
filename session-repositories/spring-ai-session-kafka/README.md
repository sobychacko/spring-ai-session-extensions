# Spring AI Session Kafka

A Kafka-based `SessionRepository` for the Spring AI session: sessions and their events are kept in Kafka.
It lets a team that already runs Kafka keep agent memory there, with no extra database.
It works with `DefaultSessionService` and the session advisors unchanged, and passes the same contract suite as the JDBC store.

Experimental: tested on an embedded single broker, and the store format is not versioned yet.

[Kafka Session Repository](../../docs/session-repositories/kafka.md) explains how it works, how to use it, and its guarantees and limits.

```shell
./mvnw verify -pl session-repositories/spring-ai-session-kafka -am
```
