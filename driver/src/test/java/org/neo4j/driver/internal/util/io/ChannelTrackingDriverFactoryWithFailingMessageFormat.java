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
package org.neo4j.driver.internal.util.io;

import java.time.Clock;
import org.neo4j.driver.Config;
import org.neo4j.driver.internal.BoltAgentUtil;
import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.DefaultDomainNameResolver;
import org.neo4j.driver.internal.async.connection.ChannelConnector;
import org.neo4j.driver.internal.async.connection.ChannelConnectorImpl;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.util.FailingMessageFormat;

public class ChannelTrackingDriverFactoryWithFailingMessageFormat extends ChannelTrackingDriverFactory {
    private final ChannelPipelineBuilderWithFailingMessageFormat pipelineBuilder =
            new ChannelPipelineBuilderWithFailingMessageFormat();

    public ChannelTrackingDriverFactoryWithFailingMessageFormat(Clock clock) {
        super(clock);
    }

    @Override
    protected ChannelConnector createRealConnector(
            ConnectionSettings settings,
            SecurityPlan securityPlan,
            Config config,
            Clock clock,
            RoutingContext routingContext) {
        return new ChannelConnectorImpl(
                settings,
                securityPlan,
                pipelineBuilder,
                config.logging(),
                clock,
                routingContext,
                DefaultDomainNameResolver.getInstance(),
                null,
                BoltAgentUtil.VALUE);
    }

    public FailingMessageFormat getFailingMessageFormat() {
        return pipelineBuilder.getFailingMessageFormat();
    }
}
