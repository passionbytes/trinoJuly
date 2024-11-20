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
import io.trino.plugin.arrow.reader.ColumnReader;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import org.apache.arrow.flight.FlightStream;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static io.trino.plugin.arrow.reader.ColumnReaders.getColumnReader;
import static java.util.Objects.requireNonNull;

public class ArrowPageSource
        implements ConnectorPageSource
{
    private static final int MAX_BATCH_SIZE = 1024;
    private final FlightStream flightStream;
    private final List<ArrowColumnHandle> columnHandleList;
    private List<ColumnReader> columnReaders;
    private boolean finished;
    private int offset;
    private int rowCount;
    private final Runnable onClose;

    public ArrowPageSource(FlightStream flightStream, List<ArrowColumnHandle> columnHandles, Runnable onClose)
    {
        this.flightStream = requireNonNull(flightStream, "flightStream is null");
        this.columnHandleList = ImmutableList.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
        this.onClose = requireNonNull(onClose, "onClose is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        if (offset >= rowCount) {
            finished = advanceNextStream();
            if (finished) {
                return null;
            }
        }
        int batchSize = Math.min(MAX_BATCH_SIZE, rowCount - offset);
        Block[] blocks = new Block[columnReaders.size()];
        for (int i = 0; i < columnReaders.size(); i++) {
            blocks[i] = columnReaders.get(i).readBlock(batchSize);
        }
        offset = offset + batchSize;
        return new Page(batchSize, blocks);
    }

    private boolean advanceNextStream()
    {
        offset = 0;
        flightStream.getRoot().clear();

        boolean hasNext = flightStream.next();
        if (hasNext) {
            rowCount = flightStream.getRoot().getRowCount();
            columnReaders = columnHandleList.stream().map(column -> getColumnReader(column, flightStream.getRoot())).collect(Collectors.toList());
            return false;
        }
        return true;
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            flightStream.close();
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        onClose.run();
    }

	@Override
	public long getMemoryUsage() {
		// TODO Auto-generated method stub
		return 0;
	}
}
