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
package neo4j.org.testkit.backend.messages.responses.serializer;

import static neo4j.org.testkit.backend.messages.responses.serializer.GenUtils.cypherObject;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.io.Serial;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.types.InternalTypeSystem;

public class TestkitValueSerializer extends StdSerializer<Value> {
    @Serial
    private static final long serialVersionUID = 6125358719511809365L;

    public TestkitValueSerializer() {
        super(Value.class);
    }

    @Override
    public void serialize(Value value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        if (InternalTypeSystem.TYPE_SYSTEM.BOOLEAN().isTypeOf(value)) {
            cypherObject(gen, "CypherBool", value.asBoolean());
        } else if (InternalTypeSystem.TYPE_SYSTEM.NULL().isTypeOf(value)) {
            cypherObject(gen, "CypherNull", () -> gen.writeNullField("value"));
        } else if (InternalTypeSystem.TYPE_SYSTEM.INTEGER().isTypeOf(value)) {
            cypherObject(gen, "CypherInt", value.asLong());
        } else if (InternalTypeSystem.TYPE_SYSTEM.FLOAT().isTypeOf(value)) {
            cypherObject(gen, "CypherFloat", value.asDouble());
        } else if (InternalTypeSystem.TYPE_SYSTEM.STRING().isTypeOf(value)) {
            cypherObject(gen, "CypherString", value.asString());
        }
    }
}
