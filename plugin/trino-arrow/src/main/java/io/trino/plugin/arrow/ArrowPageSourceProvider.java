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

import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getLast;
import static java.util.Objects.requireNonNull;

public class ArrowPageSourceProvider implements ConnectorPageSourceProvider {
	private final FlightClientSupplier flightClientSupplier;
	private final BufferAllocator allocator;

	@Inject
	public ArrowPageSourceProvider(FlightClientSupplier flightClientSupplier, BufferAllocator allocator) {
		this.flightClientSupplier = requireNonNull(flightClientSupplier, "flightClientSupplier is null");
		this.allocator = allocator;
	}

	@Override
	public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session,
			ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter) {
		ArrowSplit arrowSplit = (ArrowSplit) split;
		FlightClient flightClient = flightClientSupplier.getClient(getLast(arrowSplit.getUri()));
		FlightStream stream = flightClient.getStream(new Ticket(arrowSplit.getTicket()));
		return new ArrowPageSource(stream,
				columns.stream().map(ArrowColumnHandle.class::cast).collect(Collectors.toList()), () -> {
					try {
						flightClient.close();
					} catch (InterruptedException e) {
					}
				});
	}
}
