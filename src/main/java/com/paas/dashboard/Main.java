/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.paas.dashboard;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

@SpringBootApplication
public class Main {

    @Component
    public class CustomWebFilter implements WebFilter {
        @Override
        public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
            if (exchange.getRequest().getURI().getPath().equals("/")) {
                return chain.filter(exchange.mutate().request(exchange.getRequest().mutate().path("/index.html").build()).build());
            }

            return chain.filter(exchange);
        }
    }

    @Bean
    RouterFunction<ServerResponse> staticResourceRouter(){
        return RouterFunctions.resources("/**", new FileSystemResource("/Users/teenie/code/other/paas-dashboard-incubator/dist/static/"));
    }

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

}
