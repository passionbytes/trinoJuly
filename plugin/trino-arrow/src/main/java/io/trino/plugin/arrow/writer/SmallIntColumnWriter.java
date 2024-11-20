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
package io.trino.plugin.arrow.writer;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import org.apache.arrow.vector.SmallIntVector;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.FieldVector;
import io.trino.spi.type.Type;

import static io.trino.spi.type.SmallintType.SMALLINT;
import static java.util.Objects.requireNonNull;

public class SmallIntColumnWriter
        implements ColumnWriter
{
    private final Type type;
    private final SmallIntVector smallIntVector;

    public SmallIntColumnWriter(Type type, SmallIntVector smallIntVector)
    {
        this.type = requireNonNull(type, "type is null");
        this.smallIntVector = requireNonNull(smallIntVector, "smallIntVector is null");
    }

    @Override
    public void writeBlock(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
            	smallIntVector.setNull(i);
            }
            else {
            	smallIntVector.setSafe(i, (int) type.getLong(block, i));
            }
        }
    }

    @Override
    public FieldVector getValueVector()
    {
        return smallIntVector;
    }
}
