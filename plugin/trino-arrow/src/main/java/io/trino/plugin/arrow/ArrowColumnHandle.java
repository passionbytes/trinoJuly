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


import io.airlift.log.Logger;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;
import static com.google.common.base.MoreObjects.toStringHelper;

public class ArrowColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final Type columnType;
    private static final Logger LOGGER = Logger.get(ArrowColumnHandle.class);

    @JsonCreator
    public ArrowColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "type is null");
    }

    @JsonProperty
    public String getColumnName()
    {
    	LOGGER.error("NAME: "+ columnName);
    	
        return columnName;
    }

    @JsonProperty
    public Type getColumnType()
    {
    	LOGGER.error("TYPE: "+ columnType);
        return columnType;
    }

    /*public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(columnName, columnType);
    }*/
    ColumnMetadata getColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(columnName)
                .setType(columnType)
                .setHidden(false)
                .build();
    }
    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("columnType", columnType)
                .toString();
    }
}
