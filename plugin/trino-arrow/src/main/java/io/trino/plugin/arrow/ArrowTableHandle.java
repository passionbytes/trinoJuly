/*
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
package io.trino.plugin.arrow;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;

import static java.util.Objects.requireNonNull;

public class ArrowTableHandle
        implements ConnectorTableHandle
{
    private final String tableName;

    @JsonCreator
    public ArrowTableHandle(String tableName)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }
}
