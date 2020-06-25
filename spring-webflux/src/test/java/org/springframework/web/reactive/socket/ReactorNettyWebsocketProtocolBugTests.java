/*
 * Copyright 2002-2020 the original author or authors.
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

package org.springframework.web.reactive.socket;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseCookie;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.client.JettyWebSocketClient;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.TomcatWebSocketClient;
import org.springframework.web.reactive.socket.client.UndertowWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import org.springframework.web.server.WebFilter;
import org.springframework.web.testfixture.http.server.reactive.bootstrap.HttpServer;
import org.springframework.web.testfixture.http.server.reactive.bootstrap.TomcatHttpServer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.netty.http.server.WebsocketServerSpec;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests with server-side {@link WebSocketHandler}s.
 *
 * @author Rossen Stoyanchev
 * @author Sam Brannen
 * @author Brian Clozel
 */
class ReactorNettyWebsocketProtocolBugTests extends AbstractWebSocketIntegrationTests {

	private static final Duration TIMEOUT = Duration.ofMillis(5000);

	@Override
	protected Class<?> getWebConfigClass() {
		return WebConfig.class;
	}

	/**
	 * testEcho times out, because the request is dropped in
	 * {@link reactor.netty.http.server.HttpServerOperations#withWebsocketSupport(String, WebsocketServerSpec, BiFunction)}  }
	 */
	@ParameterizedWebSocketTest
	void multipleClientsWithDifferentProtocols(WebSocketClient client, HttpServer server, Class<?> serverConfigClass) throws Exception {
		startServer(client, server, serverConfigClass);

		String protocol = "echo-v1";
		AtomicReference<HandshakeInfo> infoRef = new AtomicReference<>();
		MonoProcessor<Object> output = MonoProcessor.create();

		this.client.execute(getUrl("/sub-protocol"),
				new WebSocketHandler() {
					@Override
					public List<String> getSubProtocols() {
						return Collections.singletonList(protocol);
					}

					@Override
					public Mono<Void> handle(WebSocketSession session) {
						infoRef.set(session.getHandshakeInfo());
						return session.receive()
								.map(WebSocketMessage::getPayloadAsText)
								.subscribeWith(output)
								.then();
					}
				})
				.block();


		// creating a new client, because the protocol from the previous request seems to be persisted in the client as well
		this.client = newClient(this.client);

		if (server instanceof TomcatHttpServer) {
			Mono.fromRunnable(this::testEcho)
					.retryWhen(Retry.max(3).filter(ex -> ex instanceof IllegalStateException))
					.block(TIMEOUT);
		} else {
			testEcho();
		}
	}


	private WebSocketClient newClient(WebSocketClient client) throws java.io.IOException {
		WebSocketClient newClient = null;
		if (client instanceof org.springframework.web.reactive.socket.client.TomcatWebSocketClient) {
			newClient = new TomcatWebSocketClient();
		} else if (client instanceof JettyWebSocketClient) {
			newClient = new JettyWebSocketClient();
			((JettyWebSocketClient) newClient).start();
		} else if (client instanceof ReactorNettyWebSocketClient) {
			newClient = new ReactorNettyWebSocketClient();
		} else if (client instanceof UndertowWebSocketClient) {
			newClient = new UndertowWebSocketClient(org.xnio.Xnio.getInstance().createWorker(org.xnio.OptionMap.EMPTY));
		}
		return newClient;
	}


	//	Copied from WebSocketIntegrationTests

	private void testEcho() {
		int count = 100;
		reactor.core.publisher.Flux<String> input = reactor.core.publisher.Flux.range(1, count).map(index -> "msg-" + index);
		reactor.core.publisher.ReplayProcessor<Object> output = reactor.core.publisher.ReplayProcessor.create(count);
		this.client.execute(getUrl("/echo"), session -> session
				.send(input.map(session::textMessage))
				.thenMany(session.receive().take(count).map(org.springframework.web.reactive.socket.WebSocketMessage::getPayloadAsText))
				.subscribeWith(output)
				.then())
				.block(TIMEOUT);
		assertThat(output.isTerminated()).isTrue();
		assertThat(output.collectList().block()).isEqualTo(input.collectList().block());
	}


	@Configuration
	static class WebConfig {

		@Bean
		public HandlerMapping handlerMapping() {
			Map<String, WebSocketHandler> map = new HashMap<>();
			map.put("/echo", new EchoWebSocketHandler());
			map.put("/sub-protocol", new SubProtocolWebSocketHandler());
			map.put("/custom-header", new CustomHeaderHandler());
			map.put("/close", new SessionClosingHandler());
			map.put("/cookie", new CookieHandler());
			return new SimpleUrlHandlerMapping(map);
		}

		@Bean
		public WebFilter cookieWebFilter() {
			return (exchange, chain) -> {
				if (exchange.getRequest().getPath().value().startsWith("/cookie")) {
					exchange.getResponse().addCookie(ResponseCookie.from("project", "spring").build());
				}
				return chain.filter(exchange);
			};
		}
	}


	private static class EchoWebSocketHandler implements WebSocketHandler {

		@Override
		public Mono<Void> handle(WebSocketSession session) {
			// Use retain() for Reactor Netty
			return session.send(session.receive().doOnNext(WebSocketMessage::retain));
		}
	}


	private static class SubProtocolWebSocketHandler implements WebSocketHandler {

		@Override
		public List<String> getSubProtocols() {
			return Collections.singletonList("echo-v1");
		}

		@Override
		public Mono<Void> handle(WebSocketSession session) {
			String protocol = session.getHandshakeInfo().getSubProtocol();
			WebSocketMessage message = session.textMessage(protocol != null ? protocol : "none");
			return session.send(Mono.just(message));
		}
	}


	private static class CustomHeaderHandler implements WebSocketHandler {

		@Override
		public Mono<Void> handle(WebSocketSession session) {
			HttpHeaders headers = session.getHandshakeInfo().getHeaders();
			String payload = "my-header:" + headers.getFirst("my-header");
			WebSocketMessage message = session.textMessage(payload);
			return session.send(Mono.just(message));
		}
	}


	private static class SessionClosingHandler implements WebSocketHandler {

		@Override
		public Mono<Void> handle(WebSocketSession session) {
			return session.send(Flux
					.error(new Throwable())
					.onErrorResume(ex -> session.close(CloseStatus.GOING_AWAY)) // SPR-17306 (nested close)
					.cast(WebSocketMessage.class));
		}
	}

	private static class CookieHandler implements WebSocketHandler {

		@Override
		public Mono<Void> handle(WebSocketSession session) {
			WebSocketMessage message = session.textMessage("cookie");
			return session.send(Mono.just(message));
		}
	}

}
