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
import org.apache.arrow.vector.Float8Vector;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.FieldVector;

import static io.trino.spi.type.DoubleType.DOUBLE;
import io.trino.spi.type.DoubleType;

import static java.util.Objects.requireNonNull;



public class DoubleColumnWriter
        implements ColumnWriter
{
    private final Type type;
    private final Float8Vector doubleVector;

    public DoubleColumnWriter(Type type, Float8Vector doubleVector)
    {
        this.type = requireNonNull(type, "type is null");
        this.doubleVector = requireNonNull(doubleVector, "doubleVector is null");
    }

    @Override
    public void writeBlock(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
            	doubleVector.setNull(i);
            }
            else {
            	doubleVector.setSafe(i, (double) type.getDouble(block, i));
            }
        }
    }

    @Override
    public FieldVector getValueVector()
    {
        return doubleVector;
    }
}