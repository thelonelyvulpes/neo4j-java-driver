/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.driver.internal.async.connection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.async.inbound.ChannelErrorHandler;
import org.neo4j.driver.internal.async.inbound.ChunkDecoder;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.async.inbound.InboundMessageHandler;
import org.neo4j.driver.internal.async.inbound.MessageDecoder;
import org.neo4j.driver.internal.async.outbound.OutboundMessageHandler;
import org.neo4j.driver.internal.messaging.v3.MessageFormatV3;

class ChannelPipelineBuilderImplTest {
    @Test
    void shouldBuildPipeline() {
        var channel = new EmbeddedChannel();
        ChannelAttributes.setMessageDispatcher(channel, new InboundMessageDispatcher(channel, DEV_NULL_LOGGING));

        new ChannelPipelineBuilderImpl().build(new MessageFormatV3(), channel.pipeline(), DEV_NULL_LOGGING);

        var iterator = channel.pipeline().iterator();
        assertThat(iterator.next().getValue(), instanceOf(ChunkDecoder.class));
        assertThat(iterator.next().getValue(), instanceOf(MessageDecoder.class));
        assertThat(iterator.next().getValue(), instanceOf(InboundMessageHandler.class));

        assertThat(iterator.next().getValue(), instanceOf(OutboundMessageHandler.class));

        assertThat(iterator.next().getValue(), instanceOf(ChannelErrorHandler.class));

        assertFalse(iterator.hasNext());
    }
}
