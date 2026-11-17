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

package org.springaicommunity.session.turn.advisor;

import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.ai.chat.client.advisor.ToolCallingAdvisor;
import org.springframework.ai.session.advisor.SessionContexts;
import org.springframework.ai.session.advisor.SessionMemoryAdvisor;
import org.springframework.ai.session.advisor.SessionTurnAdvisor;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks the parts of the Spring AI session advisors that this module builds on: their
 * order around the tool-calling loop, and the session id of a call.
 *
 * @author Christian Tzolov
 */
class SessionAdvisorFoundationTests {

	@Test
	void turnAdvisorIsOutsideTheToolLoopAndMemoryAdvisorInside() {
		assertThat(SessionTurnAdvisor.DEFAULT_ORDER).isLessThan(ToolCallingAdvisor.DEFAULT_ORDER);
		assertThat(ToolCallingAdvisor.DEFAULT_ORDER).isLessThan(SessionMemoryAdvisor.DEFAULT_ORDER);
	}

	@Test
	void sessionIdIsReadFromTheAdvisorContext() {
		Map<String, Object> context = Map.of(SessionMemoryAdvisor.SESSION_ID_CONTEXT_KEY, "s1");

		assertThat(SessionContexts.sessionId(context)).isEqualTo("s1");
	}

}
