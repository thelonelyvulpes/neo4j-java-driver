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
package neo4j.org.testkit.backend.channel.handler;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import neo4j.org.testkit.backend.ResponseQueueHanlder;
import neo4j.org.testkit.backend.messages.TestkitModule;
import neo4j.org.testkit.backend.messages.requests.TestkitRequest;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;

public class TestkitRequestResponseMapperHandler extends ChannelDuplexHandler {
    private final Logger log;
    private final ObjectMapper objectMapper = newObjectMapper();
    private final ResponseQueueHanlder responseQueueHanlder;

    public TestkitRequestResponseMapperHandler(Logging logging, ResponseQueueHanlder responseQueueHanlder) {
        log = logging.getLog(getClass());
        this.responseQueueHanlder = responseQueueHanlder;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        var testkitMessage = (String) msg;
        log.debug("Inbound Testkit message '%s'", testkitMessage.trim());
        responseQueueHanlder.increaseRequestCountAndDispatchFirstResponse();
        var testkitRequest = objectMapper.readValue(testkitMessage, TestkitRequest.class);
        ctx.fireChannelRead(testkitRequest);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        var testkitResponse = (TestkitResponse) msg;
        var responseStr = objectMapper.writeValueAsString(testkitResponse);
        log.debug("Outbound Testkit message '%s'", responseStr.trim());
        ctx.writeAndFlush(responseStr, promise);
    }

    public static ObjectMapper newObjectMapper() {
        var objectMapper = new ObjectMapper();
        var testkitModule = new TestkitModule();
        objectMapper.registerModule(testkitModule);
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return objectMapper;
    }
}
