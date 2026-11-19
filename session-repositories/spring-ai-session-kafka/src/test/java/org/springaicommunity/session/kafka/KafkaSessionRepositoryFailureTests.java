/*
 * Copyright 2023-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springaicommunity.session.kafka;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueProcessingExceptionHandler;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.springframework.ai.session.EventFilter;
import org.springframework.ai.session.Session;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 * What happens when things go wrong: a task that fails mid-request, requests the task
 * can't read, client settings and topics that don't fit, and a start that fails. Failures
 * are injected through the repository's package-private test hooks.
 *
 * @author Soby Chacko
 */
@EmbeddedKafka(brokerProperties = { "auto.create.topics.enable=false", "transaction.state.log.replication.factor=1",
		"transaction.state.log.min.isr=1" })
class KafkaSessionRepositoryFailureTests {

	@TempDir
	Path stateDirectory;

	private final EmbeddedKafkaBroker broker;

	KafkaSessionRepositoryFailureTests(EmbeddedKafkaBroker broker) {
		this.broker = broker;
	}

	@Test
	void ownerFailingBeforeCommitIsRolledBackAndTheTakeoverAnswersOnce() {
		String prefix = "failover-" + UUID.randomUUID();
		String sessionId = UUID.randomUUID().toString();
		AtomicBoolean armed = new AtomicBoolean();
		// Fails whichever instance owns the session, once, after the reply is forwarded
		// and before the transaction commits
		Consumer<@Nullable String> failOnce = key -> {
			if (sessionId.equals(key) && armed.compareAndSet(true, false)) {
				throw new IllegalStateException("Injected failure between forward and commit");
			}
		};
		try (KafkaSessionRepository first = start(prefix, "first", failOnce);
				KafkaSessionRepository second = start(prefix, "second", failOnce)) {
			first.save(Session.builder().id(sessionId).build());
			armed.set(true);

			boolean appended = first.appendEvents(sessionId,
					List.of(KafkaSessionTestSupport.userEvent(sessionId, "one"),
							KafkaSessionTestSupport.userEvent(sessionId, "two"),
							KafkaSessionTestSupport.userEvent(sessionId, "three")),
					0);

			// The failure happened, and the attempt it aborted left nothing behind: had
			// its writes survived, the retry would have found version 1 and said false
			assertThat(armed).isFalse();
			assertThat(appended).isTrue();
			// The aborted attempt's reply reached the broker but no caller: committed
			// readers see the save and the batch, uncommitted ones the aborted reply too
			String replies = prefix + ".replies";
			assertThat(repliesFor(replies, sessionId, "read_committed")).isEqualTo(2);
			assertThat(repliesFor(replies, sessionId, "read_uncommitted")).isEqualTo(3);
			// The instance that failed says so; the other one carries on
			assertThat(first.isHealthy() ^ second.isHealthy()).isTrue();
			assertThat(second.getEventVersion(sessionId)).isEqualTo(1);
			assertThat(second.findEvents(sessionId, EventFilter.all())).extracting(e -> e.getMessage().getText())
				.containsExactly("one", "two", "three");
		}
	}

	@Test
	void instanceWhoseEngineStoppedReportsUnhealthy() {
		String prefix = "stopped-" + UUID.randomUUID();
		AtomicBoolean armed = new AtomicBoolean();
		KafkaSessionRepository repository = KafkaSessionTestSupport
			.repository(this.broker, prefix, this.stateDirectory.resolve("only"))
			.operationTimeout(Duration.ofSeconds(5))
			// A handler that would keep the failed request's writes: the store's own
			// setting wins, so the task still fails and rolls back
			.kafkaProperties(Map.of(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG,
					LogAndContinueProcessingExceptionHandler.class.getName()))
			.afterForward(key -> {
				if ("s".equals(key) && armed.compareAndSet(true, false)) {
					throw new IllegalStateException("Injected failure");
				}
			})
			.build();
		try (repository) {
			repository.start();
			repository.save(Session.builder().id("s").build());
			assertThat(repository.isHealthy()).isTrue();
			armed.set(true);

			// One instance only: nobody takes the partition over, so the call is not
			// answered, and the instance says it needs a restart
			assertThatIllegalStateException()
				.isThrownBy(() -> repository.appendEvent(KafkaSessionTestSupport.userEvent("s", "lost")))
				.withMessageContaining("outcome is unknown");
			assertThat(repository.isHealthy()).isFalse();
		}
	}

	@Test
	void unreadableRequestsAreSkippedAndTheStoreKeepsServing() {
		String prefix = "unreadable-" + UUID.randomUUID();
		try (KafkaSessionRepository repository = start(prefix, "only")) {
			repository.save(Session.builder().id("s").build());
			// Not JSON, the JSON null, and valid JSON that lacks what the operation needs
			List<String> unreadable = List.of("not a request", "null", "{\"op\":\"SAVE\"}",
					"{\"op\":\"FIND_EVENTS\",\"sessionId\":\"s\"}", "{\"op\":\"APPEND\",\"event\":{\"id\":\"e\"}}",
					"{\"op\":\"NO_SUCH_OPERATION\"}");
			try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(
					Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.broker.getBrokersAsString()),
					new StringSerializer(), new ByteArraySerializer())) {
				for (String request : unreadable) {
					producer.send(new ProducerRecord<>(prefix + ".requests", KafkaSessionTestSupport.partitionOf("s"),
							"s", request.getBytes(StandardCharsets.UTF_8)));
				}
			}

			// Queued behind them on the same partition: answered
			// only if the task skipped every one
			repository.appendEvent(KafkaSessionTestSupport.userEvent("s", "after"));

			assertThat(repository.findEvents("s", EventFilter.all())).extracting(e -> e.getMessage().getText())
				.containsExactly("after");
			assertThat(repository.isHealthy()).isTrue();
		}
	}

	@Test
	void producerPartitionerSettingsCannotSplitASession() {
		String prefix = "partitioner-" + UUID.randomUUID();
		// A round-robin partitioner would spread one session's calls over every task
		KafkaSessionRepository repository = KafkaSessionTestSupport
			.repository(this.broker, prefix, this.stateDirectory.resolve("only"))
			.kafkaProperties(Map.of(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName(),
					ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG, true))
			.build();
		try (repository) {
			repository.start();
			assertThat(repository.saveIfAbsent(Session.builder().id("s").build())).isTrue();
			for (int i = 0; i < KafkaSessionTestSupport.PARTITIONS * 2; i++) {
				assertThat(repository.saveIfAbsent(Session.builder().id("s").build())).isFalse();
				repository.appendEvent(KafkaSessionTestSupport.userEvent("s", "message " + i));
				assertThat(repository.getEventVersion("s")).isEqualTo(i + 1);
			}
		}
	}

	@Test
	void failedStartLeavesNothingRunning() {
		String prefix = "failed-start-" + UUID.randomUUID();
		// Fail the start once the reply listener and Streams are both running
		KafkaSessionRepository repository = KafkaSessionTestSupport
			.repository(this.broker, prefix, this.stateDirectory.resolve("failing"))
			.afterComponentsStarted(() -> {
				throw new IllegalStateException("Injected start failure");
			})
			.build();
		try (repository) {
			assertThatIllegalStateException().isThrownBy(repository::start).withMessage("Injected start failure");
			assertThat(repository.isRunning()).isFalse();
			assertThat(repository.anyComponentRunning()).isFalse();
		}
		// Nothing holds the partitions: a new instance gets them all and answers
		try (KafkaSessionRepository next = start(prefix, "next")) {
			next.save(Session.builder().id("s").build());
			assertThat(next.findById("s")).isNotNull();
		}
	}

	@Test
	void existingChangelogThatDeletesStopsStartup() throws Exception {
		String prefix = "changelog-policy-" + UUID.randomUUID();
		String changelog = prefix + "-" + SessionProcessor.EVENTS + "-changelog";
		try (Admin admin = Admin
			.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.broker.getBrokersAsString()))) {
			admin.createTopics(List.of(new NewTopic(changelog, KafkaSessionTestSupport.PARTITIONS, (short) 1)
				// Right in every way but the cleanup policy
				.configs(Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE,
						TopicConfig.MAX_MESSAGE_BYTES_CONFIG, Integer.toString(8 * 1024 * 1024)))))
				.all()
				.get(30, TimeUnit.SECONDS);
		}
		KafkaSessionRepository repository = KafkaSessionTestSupport
			.repository(this.broker, prefix, this.stateDirectory.resolve("only"))
			.build();
		try (repository) {
			assertThatIllegalStateException().isThrownBy(repository::start)
				.withMessageContaining(changelog)
				.withMessageContaining("cleanup.policy=delete");
			assertThat(repository.anyComponentRunning()).isFalse();
		}
	}

	@Test
	void changelogsCompactWhateverTheClientSettings() throws Exception {
		String prefix = "changelog-settings-" + UUID.randomUUID();
		// A client setting for every internal topic that would let Kafka delete stored
		// sessions; a first start has no changelog yet for the startup check to catch
		KafkaSessionRepository repository = KafkaSessionTestSupport
			.repository(this.broker, prefix, this.stateDirectory.resolve("only"))
			.kafkaProperties(Map.of(StreamsConfig.topicPrefix(TopicConfig.CLEANUP_POLICY_CONFIG),
					TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE))
			.build();
		try (repository) {
			repository.start();
			repository.save(Session.builder().id("s").build());
		}
		try (Admin admin = Admin
			.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.broker.getBrokersAsString()))) {
			for (String store : List.of(SessionProcessor.SESSIONS, SessionProcessor.EVENTS,
					SessionProcessor.EVENT_IDS)) {
				ConfigResource changelog = new ConfigResource(ConfigResource.Type.TOPIC,
						prefix + "-" + store + "-changelog");
				Config config = admin.describeConfigs(List.of(changelog))
					.all()
					.get(30, TimeUnit.SECONDS)
					.get(changelog);
				assertThat(config.get(TopicConfig.CLEANUP_POLICY_CONFIG).value()).as(changelog.name())
					.isEqualTo(TopicConfig.CLEANUP_POLICY_COMPACT);
			}
		}
	}

	@Test
	void existingTopicWithASmallerRecordLimitStopsStartup() throws Exception {
		String prefix = "record-limit-" + UUID.randomUUID();
		try (Admin admin = Admin
			.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.broker.getBrokersAsString()))) {
			admin.createTopics(List.of(new NewTopic(prefix + ".replies", KafkaSessionTestSupport.PARTITIONS, (short) 1)
				.configs(Map.of(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "65536")))).all().get(30, TimeUnit.SECONDS);
		}
		KafkaSessionRepository repository = KafkaSessionTestSupport
			.repository(this.broker, prefix, this.stateDirectory.resolve("only"))
			.build();
		try (repository) {
			assertThatIllegalStateException().isThrownBy(repository::start)
				.withMessageContaining(prefix + ".replies")
				.withMessageContaining("65536");
		}
	}

	@Test
	void topicsWithAnotherPartitionCountStopStartup() {
		String prefix = "partitions-" + UUID.randomUUID();
		try (KafkaSessionRepository existing = start(prefix, "existing")) {
			try (KafkaSessionRepository other = KafkaSessionTestSupport
				.repository(this.broker, prefix, this.stateDirectory.resolve("other"))
				.partitions(KafkaSessionTestSupport.PARTITIONS + 2)
				.build()) {
				assertThatIllegalStateException().isThrownBy(other::start)
					.withMessageContaining("has " + KafkaSessionTestSupport.PARTITIONS + " partitions");
			}
		}
	}

	private KafkaSessionRepository start(String prefix, String instance) {
		return start(prefix, instance, key -> {
		});
	}

	private KafkaSessionRepository start(String prefix, String instance, Consumer<@Nullable String> afterForward) {
		KafkaSessionRepository repository = KafkaSessionTestSupport
			.repository(this.broker, prefix, this.stateDirectory.resolve(instance))
			// Notice a failed instance within the call timeout. The restore setting would
			// bring back the writes of an aborted transaction on a takeover; the store's
			// own setting must win
			.kafkaProperties(Map.of("session.timeout.ms", 6000, "heartbeat.interval.ms", 1000,
					StreamsConfig.restoreConsumerPrefix(ConsumerConfig.ISOLATION_LEVEL_CONFIG), "read_uncommitted"))
			.afterForward(afterForward)
			.build();
		repository.start();
		return repository;
	}

	private long repliesFor(String topic, String sessionId, String isolationLevel) {
		return KafkaSessionTestSupport.readAll(this.broker, topic, isolationLevel)
			.stream()
			.filter(record -> sessionId.equals(record.key()))
			.count();
	}

}
