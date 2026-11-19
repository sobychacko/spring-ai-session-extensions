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

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.session.SessionEvent;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Repository defaults that fit the single-node {@code @EmbeddedKafka} broker of the
 * tests, which the test classes configure for exactly-once, and helpers to read a topic
 * directly.
 *
 * @author Soby Chacko
 */
final class KafkaSessionTestSupport {

	static final int PARTITIONS = 4;

	private KafkaSessionTestSupport() {
	}

	static KafkaSessionRepository.Builder repository(EmbeddedKafkaBroker broker, String topicPrefix,
			Path stateDirectory) {
		return KafkaSessionRepository.builder()
			.bootstrapServers(broker.getBrokersAsString())
			.topicPrefix(topicPrefix)
			.partitions(PARTITIONS)
			.replicationFactor((short) 1)
			.standbyReplicas(0)
			.stateDirectory(stateDirectory)
			.operationTimeout(Duration.ofSeconds(30));
	}

	/** A user message event of the session. */
	static SessionEvent userEvent(String sessionId, String text) {
		return SessionEvent.builder().sessionId(sessionId).message(new UserMessage(text)).build();
	}

	/** The partition the repository sends this session's requests to. */
	static int partitionOf(String sessionId) {
		return KafkaSessionRepository.partitionFor(sessionId, PARTITIONS);
	}

	/** A new session id that lands on the same partition as the given one. */
	static String sessionOnSamePartition(String sessionId) {
		return sessionOnPartition(partitionOf(sessionId));
	}

	/** A new session id that lands on the given partition. */
	static String sessionOnPartition(int partition) {
		while (true) {
			String candidate = UUID.randomUUID().toString();
			if (partitionOf(candidate) == partition) {
				return candidate;
			}
		}
	}

	/**
	 * Every record of the topic, from the beginning up to at least its end when the read
	 * starts, read with the given isolation level. Fails if it cannot read that far.
	 */
	static List<ConsumerRecord<String, byte[]>> readAll(EmbeddedKafkaBroker broker, String topic,
			String isolationLevel) {
		Map<String, Object> properties = Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
				ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties, new StringDeserializer(),
				new ByteArrayDeserializer())) {
			List<TopicPartition> partitions = consumer.partitionsFor(topic)
				.stream()
				.map(info -> new TopicPartition(topic, info.partition()))
				.toList();
			consumer.assign(partitions);
			consumer.seekToBeginning(partitions);
			Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
			List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
			long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
			while (partitions.stream().anyMatch(p -> consumer.position(p) < end.get(p))
					&& System.nanoTime() < deadline) {
				consumer.poll(Duration.ofMillis(200)).forEach(records::add);
			}
			// A read that stopped early proves nothing about what the topic holds
			for (TopicPartition partition : partitions) {
				assertThat(consumer.position(partition)).as("the read of %s reached its end", partition)
					.isGreaterThanOrEqualTo(end.get(partition));
			}
			return records;
		}
	}

}
