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
import io.trino.spi.block.ShortArrayBlockBuilder;
import org.apache.arrow.vector.SmallIntVector;
import io.trino.spi.type.Type;

import org.apache.arrow.vector.FieldVector;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static java.util.Objects.requireNonNull;

public class SmallIntColumnReader
        implements ColumnReader
{
    private final SmallIntVector smallIntVector;
    private int offset;

    public SmallIntColumnReader(SmallIntVector smallIntVector)
    {
        this.smallIntVector = requireNonNull(smallIntVector, "smallIntVector is null");
    }

    @Override
    public Block readBlock(int batchSize)
    {
        ShortArrayBlockBuilder blockBuilder = new ShortArrayBlockBuilder(null, batchSize);
        for (int i = 0; i < batchSize; i++) {
            if (smallIntVector.isNull(offset + i)) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeShort(smallIntVector.get(offset + i));
            }
        }
        offset += batchSize;
        return blockBuilder.build();
    }
}
