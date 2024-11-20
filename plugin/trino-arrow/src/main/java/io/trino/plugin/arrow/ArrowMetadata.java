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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.statistics.ComputedStatistics;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.arrow.ArrowErrorCode.ARROW_ERROR;
import static io.trino.plugin.arrow.TypeUtils.fromArrowType;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static org.apache.arrow.flight.Criteria.ALL;

public class ArrowMetadata implements ConnectorMetadata {
    private static final Logger LOGGER = Logger.get(ArrowMetadata.class);

    private final FlightClientSupplier flightClientSupplier;

    @Inject
    public ArrowMetadata(FlightClientSupplier flightClientSupplier) {
        this.flightClientSupplier = requireNonNull(flightClientSupplier, "flightClientSupplier is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return ImmutableList.of("default");
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        ImmutableList.Builder<SchemaTableName> tableNameBuilder = ImmutableList.builder();
        if (schemaName.orElse("default").equals("default")) {
            for (FlightInfo flightInfo : flightClientSupplier.getClient(session).listFlights(ALL)) {
                if (flightInfo.getDescriptor().getPath().size() == 1) {
                    tableNameBuilder
                            .add(new SchemaTableName("default", getOnlyElement(flightInfo.getDescriptor().getPath())));
                }
            }
        }
        return tableNameBuilder.build();
    }

    @jakarta.annotation.Nullable
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion) {
        try (FlightClient client = flightClientSupplier.getClient(session)) {
            return new ArrowTableHandle(
                    client.getInfo(FlightDescriptor.path(tableName.getTableName())).getDescriptor().getPath().get(0));
        } catch (Exception e) {
            LOGGER.error("Unable to fetch table details", e);
            return null;
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
        ArrowTableHandle tableHandle = (ArrowTableHandle) table;
        return getColumnHandles(session, tableHandle.getTableName()).stream()
                .collect(toMap(ArrowColumnHandle::getColumnName, column -> column));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        ArrowTableHandle tableHandle = (ArrowTableHandle) table;
        return new ConnectorTableMetadata(new SchemaTableName("default", tableHandle.getTableName()),
                getColumnHandles(session, tableHandle.getTableName()).stream().map(ArrowColumnHandle::getColumnMetadata)
                        .collect(toImmutableList()));
    }

//    @Override
//	public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata,
//			Optional<ConnectorTableLayout> layout) {
//		// TODO Auto-generated method stub
//    	return new ArrowOutputTableHandle(new ArrowTableHandle(tableMetadata.getTable().getTableName()), tableMetadata.getColumns().stream().map(me -> new ArrowColumnHandle(me.getName(), me.getType())).collect(Collectors.toList()));
//	}

//	@Override
//    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
//    {
//        return new ArrowOutputTableHandle(new ArrowTableHandle(tableMetadata.getTable().getTableName()), tableMetadata.getColumns().stream().map(me -> new ArrowColumnHandle(me.getName(), me.getType())).collect(Collectors.toList()));
//    }


    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace) {
        return new ArrowOutputTableHandle(new ArrowTableHandle(tableMetadata.getTable().getTableName()),
                tableMetadata.getColumns().stream().map(me -> new ArrowColumnHandle(me.getName(), me.getType()))
                        .collect(Collectors.toList()));
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table) {
        return new ConnectorTableProperties();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
                                            ColumnHandle columnHandle) {
        return ((ArrowColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session,
                                                               ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments,
                                                               Collection<ComputedStatistics> computedStatistics) {
        return Optional.empty();
    }

//    @Override
//    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
//    {
//        ArrowTableHandle table = (ArrowTableHandle) tableHandle;
//        List<ArrowColumnHandle> columnHandles = getColumnHandles(session, ((ArrowTableHandle) tableHandle).getTableName());
//        return new ArrowInsertTableHandle(table, columnHandles);
//    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle,
                                                  List<ColumnHandle> columns, RetryMode retryMode) {
        // TODO Auto-generated method stub
        ArrowTableHandle table = (ArrowTableHandle) tableHandle;
        List<ArrowColumnHandle> columnHandles = getColumnHandles(session,
                ((ArrowTableHandle) tableHandle).getTableName());
        return new ArrowInsertTableHandle(table, columnHandles);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
                                                                       SchemaTablePrefix prefix) {
        requireNonNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tableNames;
        if (prefix.getTable().isEmpty()) {
            tableNames = listTables(session, prefix.getSchema());
        } else {
            tableNames = ImmutableList.of(prefix.toSchemaTableName());
        }

        for (SchemaTableName tableName : tableNames) {
            try {
                columns.put(tableName,
                        getTableMetadata(session, new ArrowTableHandle(tableName.getTableName())).getColumns());
            } catch (TableNotFoundException e) {
                // information_schema table or a system table
            }
        }
        return columns.buildOrThrow();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, List<ConnectorTableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics) {
        return Optional.empty();
    }

    private List<ArrowColumnHandle> getColumnHandles(ConnectorSession session, String tableName) {
        try (FlightClient client = flightClientSupplier.getClient(session)) {
            FlightInfo flightInfo = client.getInfo(FlightDescriptor.path(tableName));
            LOGGER.error("SCHEMA: " + flightInfo.getSchema().getFields());

            List<ArrowColumnHandle> x = flightInfo.getSchema().getFields().stream()
                    .map(field -> new ArrowColumnHandle(field.getName(), fromArrowType(field.getType())))
                    .collect(toImmutableList());
            LOGGER.error("HERE:  " + x);
            return x;
        } catch (Exception e) {
            throw new TrinoException(ARROW_ERROR, "Unable to fetch column details", e);
        }
    }
}
