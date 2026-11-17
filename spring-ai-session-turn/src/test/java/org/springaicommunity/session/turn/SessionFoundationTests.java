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

package org.springaicommunity.session.turn;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.session.CreateSessionRequest;
import org.springframework.ai.session.DefaultSessionService;
import org.springframework.ai.session.EventFilter;
import org.springframework.ai.session.EventType;
import org.springframework.ai.session.InMemorySessionRepository;
import org.springframework.ai.session.SessionEvent;
import org.springframework.ai.session.SessionService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks the parts of the Spring AI session that this module builds on: an event type of
 * its own, a read by type, and an append that is checked against the log's version.
 *
 * @author Christian Tzolov
 */
class SessionFoundationTests {

	private static final String SESSION_ID = "s1";

	private static final EventType STATE = EventType.of("TEST.STATE");

	private final SessionService sessionService = DefaultSessionService.builder()
		.sessionRepository(InMemorySessionRepository.builder().build())
		.build();

	@BeforeEach
	void createSession() {
		this.sessionService.create(CreateSessionRequest.builder().id(SESSION_ID).build());
	}

	@Test
	void eventOfOwnTypeIsReadByType() {
		this.sessionService.appendMessage(SESSION_ID, new UserMessage("Hello"));
		this.sessionService.appendEvent(state("running"));
		this.sessionService.appendEvent(state("idle"));

		List<SessionEvent> newest = this.sessionService.getEvents(SESSION_ID,
				EventFilter.builder().eventTypes(Set.of(STATE)).lastN(1).build());

		assertThat(newest).singleElement()
			.satisfies(event -> assertThat(event.getPayload()).containsEntry("status", "idle"));
	}

	@Test
	void eventOfOwnTypeIsNotSentToTheModel() {
		this.sessionService.appendMessage(SESSION_ID, new UserMessage("Hello"));
		this.sessionService.appendEvent(state("running"));

		assertThat(this.sessionService.getActiveMessages(SESSION_ID)).singleElement()
			.satisfies(message -> assertThat(message.getText()).isEqualTo("Hello"));
	}

	@Test
	void appendWithStaleVersionIsRejected() {
		long version = this.sessionService.getEventVersion(SESSION_ID);

		assertThat(this.sessionService.appendEvents(SESSION_ID, List.of(state("running")), version)).isTrue();
		assertThat(this.sessionService.appendEvents(SESSION_ID, List.of(state("running")), version)).isFalse();

		assertThat(this.sessionService.getEvents(SESSION_ID)).hasSize(1);
	}

	private static SessionEvent state(String status) {
		return SessionEvent.builder().sessionId(SESSION_ID).type(STATE).payload("status", status).build();
	}

}
