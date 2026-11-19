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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

import org.springframework.ai.session.SessionRepository;
import org.springframework.ai.test.session.AbstractSessionRepositoryContractTests;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

/**
 * The shared repository contract on one instance over several partitions. Every test
 * makes its own sessions, so one started repository serves them all.
 *
 * @author Soby Chacko
 */
@EmbeddedKafka(brokerProperties = { "auto.create.topics.enable=false", "transaction.state.log.replication.factor=1",
		"transaction.state.log.min.isr=1" })
class KafkaSessionRepositoryTests extends AbstractSessionRepositoryContractTests {

	@TempDir
	static Path stateDirectory;

	private static KafkaSessionRepository shared;

	@BeforeAll
	static void startRepository(EmbeddedKafkaBroker broker) {
		shared = KafkaSessionTestSupport.repository(broker, "contract", stateDirectory).build();
		shared.start();
	}

	@AfterAll
	static void stopRepository() {
		if (shared != null) {
			shared.close();
		}
	}

	@Override
	protected SessionRepository createRepository() {
		return shared;
	}

}
