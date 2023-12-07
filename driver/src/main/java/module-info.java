/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * The Neo4j Java Driver module.
 */
@SuppressWarnings({"requires-automatic", "requires-transitive-automatic"})
module org.neo4j.driver {
    exports org.neo4j.driver;
    exports org.neo4j.driver.async;
    exports org.neo4j.driver.reactive;
    exports org.neo4j.driver.reactivestreams;
    exports org.neo4j.driver.types;
    exports org.neo4j.driver.summary;
    exports org.neo4j.driver.net;
    exports org.neo4j.driver.util;
    exports org.neo4j.driver.exceptions;
    exports org.neo4j.driver.exceptions.value;

    requires reactor.core;
    requires io.netty.common;
    requires io.netty.handler;
    requires io.netty.transport;
    requires io.netty.buffer;
    requires io.netty.codec;
    requires io.netty.resolver;
    requires transitive java.logging;
    requires transitive org.reactivestreams;
    requires transitive io.opentelemetry.api;
    requires static micrometer.core;
    requires static org.graalvm.nativeimage;
    requires static org.slf4j;
    requires static java.management;
    requires static reactor.blockhound;
}
