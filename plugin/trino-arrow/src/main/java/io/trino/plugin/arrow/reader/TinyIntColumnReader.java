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
package io.trino.plugin.arrow.reader;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ByteArrayBlockBuilder;
import org.apache.arrow.vector.TinyIntVector;

import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

public class TinyIntColumnReader
        implements ColumnReader
{
    private final TinyIntVector tinyIntVector;
    private int offset;

    public TinyIntColumnReader(TinyIntVector tinyIntVector)
    {
        this.tinyIntVector = requireNonNull(tinyIntVector, "tinyIntVector is null");
    }

    @Override
    public Block readBlock(int batchSize)
    {
        ByteArrayBlockBuilder blockBuilder = new ByteArrayBlockBuilder(null, batchSize);
        for (int i = 0; i < batchSize; i++) {
            if (tinyIntVector.isNull(offset + i)) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeByte(tinyIntVector.get(offset + i));
            }
        }
        offset += batchSize;
        return blockBuilder.build();
    }
}
