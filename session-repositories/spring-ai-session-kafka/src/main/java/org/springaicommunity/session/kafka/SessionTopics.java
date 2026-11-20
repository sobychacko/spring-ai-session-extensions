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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.util.Assert;

/**
 * The topics of a session store, created and checked at startup. Kafka never changes an
 * existing topic when asked to create it, so a topic left by an earlier start, or made by
 * hand, may not fit the store. A wrong partition count would move sessions away from the
 * tasks that hold their state, and a wrong record limit or cleanup policy would lose
 * data, so each of them stops startup.
 *
 * @author Soby Chacko
 * @since 0.11.0
 */
final class SessionTopics {

	private static final Logger logger = LoggerFactory.getLogger(SessionTopics.class);

	private final Map<String, Object> adminProperties;

	private final String requestsTopic;

	private final String repliesTopic;

	private final String applicationId;

	private final int partitions;

	private final short replicationFactor;

	private final Duration contentRetention;

	private final int maxMessageBytes;

	private final Duration operationTimeout;

	SessionTopics(Map<String, Object> adminProperties, String requestsTopic, String repliesTopic, String applicationId,
			int partitions, short replicationFactor, Duration contentRetention, int maxMessageBytes,
			Duration operationTimeout) {
		this.adminProperties = adminProperties;
		this.requestsTopic = requestsTopic;
		this.repliesTopic = repliesTopic;
		this.applicationId = applicationId;
		this.partitions = partitions;
		this.replicationFactor = replicationFactor;
		this.contentRetention = contentRetention;
		this.maxMessageBytes = maxMessageBytes;
		this.operationTimeout = operationTimeout;
	}

	/**
	 * Creates the requests and replies topics if they are missing, then checks them, and
	 * the changelogs a previous start created, against what the store needs.
	 * @throws IllegalStateException if a topic does not fit the store
	 */
	void ensure() {
		Map<String, String> configs = Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE,
				TopicConfig.RETENTION_MS_CONFIG, Long.toString(this.contentRetention.toMillis()),
				TopicConfig.SEGMENT_MS_CONFIG, Long.toString(this.contentRetention.toMillis()),
				TopicConfig.MAX_MESSAGE_BYTES_CONFIG, Integer.toString(this.maxMessageBytes));
		List<String> names = List.of(this.requestsTopic, this.repliesTopic);
		try (Admin admin = Admin.create(this.adminProperties)) {
			for (String name : names) {
				try {
					admin
						.createTopics(
								List.of(new NewTopic(name, this.partitions, this.replicationFactor).configs(configs)))
						.all()
						.get(this.operationTimeout.toMillis(), TimeUnit.MILLISECONDS);
				}
				catch (ExecutionException ex) {
					if (!(ex.getCause() instanceof TopicExistsException)) {
						throw ex;
					}
				}
			}
			// Never change the partition count of an existing topic: sessions would move
			// away from the tasks that hold their state
			Map<String, TopicDescription> descriptions = describe(admin, names);
			for (String name : names) {
				int actual = Objects.requireNonNull(descriptions.get(name)).partitions().size();
				Assert.state(actual == this.partitions, () -> "Topic '" + name + "' has " + actual
						+ " partitions, the session store is configured for " + this.partitions);
			}
			checkTopicConfigs(admin, names);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted while checking the session store topics", ex);
		}
		catch (ExecutionException | TimeoutException ex) {
			throw new IllegalStateException("Could not create or check the session store topics", ex);
		}
	}

	/**
	 * Checks the settings of the topics, including those that existed before this start:
	 * creating a topic never changes one. A record limit below the store's would let a
	 * reply pass the store's own check and then fail the task at the broker, so it stops
	 * startup, as a policy other than delete on the requests and replies topics, or other
	 * than compact on a changelog, does. Retention, segment and compaction intervals
	 * longer than the content retention only lengthen how long content stays, so they are
	 * logged. Changelogs are checked once a previous start has created them.
	 */
	private void checkTopicConfigs(Admin admin, List<String> names)
			throws InterruptedException, ExecutionException, TimeoutException {
		Set<String> existing = admin.listTopics().names().get(this.operationTimeout.toMillis(), TimeUnit.MILLISECONDS);
		List<String> changelogs = List
			.of(SessionProcessor.SESSIONS, SessionProcessor.EVENTS, SessionProcessor.EVENT_IDS)
			.stream()
			.map(store -> this.applicationId + "-" + store + "-changelog")
			.filter(existing::contains)
			.toList();
		List<ConfigResource> resources = new ArrayList<>();
		names.forEach(name -> resources.add(new ConfigResource(ConfigResource.Type.TOPIC, name)));
		changelogs.forEach(name -> resources.add(new ConfigResource(ConfigResource.Type.TOPIC, name)));
		Map<ConfigResource, Config> configs = admin.describeConfigs(resources)
			.all()
			.get(this.operationTimeout.toMillis(), TimeUnit.MILLISECONDS);
		long retention = this.contentRetention.toMillis();
		for (Map.Entry<ConfigResource, Config> entry : configs.entrySet()) {
			String topic = entry.getKey().name();
			Config config = entry.getValue();
			long topicLimit = Long.parseLong(value(config, TopicConfig.MAX_MESSAGE_BYTES_CONFIG));
			Assert.state(topicLimit >= this.maxMessageBytes,
					() -> "Topic '" + topic + "' takes records of at most " + topicLimit
							+ " bytes, the session store is configured for " + this.maxMessageBytes
							+ "; raise the topic's max.message.bytes or lower maxMessageBytes");
			if (names.contains(topic)) {
				String policy = value(config, TopicConfig.CLEANUP_POLICY_CONFIG);
				Assert.state(TopicConfig.CLEANUP_POLICY_DELETE.equals(policy), () -> "Topic '" + topic
						+ "' has cleanup.policy=" + policy + ", the session store needs delete");
				warnIfLonger(topic, config, TopicConfig.RETENTION_MS_CONFIG, retention);
			}
			else {
				// A changelog is the store's only durable copy:
				// deleting from it loses sessions on the next restore
				String policy = value(config, TopicConfig.CLEANUP_POLICY_CONFIG);
				Assert.state(TopicConfig.CLEANUP_POLICY_COMPACT.equals(policy), () -> "Changelog '" + topic
						+ "' has cleanup.policy=" + policy + ", the session store needs compact only");
				warnIfLonger(topic, config, TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, retention);
			}
			warnIfLonger(topic, config, TopicConfig.SEGMENT_MS_CONFIG, retention);
		}
	}

	private static String value(Config config, String name) {
		return Objects.requireNonNull(config.get(name), name).value();
	}

	private static void warnIfLonger(String topic, Config config, String name, long contentRetention) {
		long value = Long.parseLong(value(config, name));
		if (value < 0 || value > contentRetention) {
			logger.warn("Topic '{}' has {}={}, longer than the content retention of {} ms: deleted content can stay "
					+ "in Kafka that long", topic, name, value, contentRetention);
		}
	}

	/**
	 * Describes the topics, waiting for a topic that was just created to reach the
	 * broker's metadata.
	 */
	private Map<String, TopicDescription> describe(Admin admin, List<String> names)
			throws InterruptedException, ExecutionException, TimeoutException {
		long deadline = System.nanoTime() + this.operationTimeout.toNanos();
		while (true) {
			try {
				return admin.describeTopics(names)
					.allTopicNames()
					.get(this.operationTimeout.toMillis(), TimeUnit.MILLISECONDS);
			}
			catch (ExecutionException ex) {
				if (!(ex.getCause() instanceof UnknownTopicOrPartitionException) || System.nanoTime() > deadline) {
					throw ex;
				}
				Thread.sleep(100);
			}
		}
	}

}
