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

import io.trino.plugin.arrow.ArrowColumnHandle;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.StandardTypes.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.DoubleType.DOUBLE;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.VarcharType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.IntegerType;

public class ColumnWriters
{
    private ColumnWriters()
    {
    }

    public static ColumnWriter getColumnWriter(ArrowColumnHandle arrowColumnHandle, BufferAllocator bufferAllocator)
    {
        if (arrowColumnHandle.getColumnType().equals(BIGINT)) {
            return new LongColumnWriter(
                    arrowColumnHandle.getColumnType(),
                    new BigIntVector(arrowColumnHandle.getColumnName(), bufferAllocator));
        }
        if (arrowColumnHandle.getColumnType().equals(INTEGER)) {
            return new IntegerColumnWriter(
                    arrowColumnHandle.getColumnType(),
                    new IntVector(arrowColumnHandle.getColumnName(), bufferAllocator));
        }
       
        if (arrowColumnHandle.getColumnType() instanceof DecimalType) {
            return new DecimalColumnWriter(
                    arrowColumnHandle.getColumnType(),
                    new DecimalVector(arrowColumnHandle.getColumnName(), bufferAllocator, ((DecimalType)arrowColumnHandle.getColumnType()).getPrecision() , ((DecimalType)arrowColumnHandle.getColumnType()).getScale()));
        }
        if (arrowColumnHandle.getColumnType().equals(REAL)) {
            return new RealColumnWriter(
                    arrowColumnHandle.getColumnType(),
                    new Float4Vector(arrowColumnHandle.getColumnName(), bufferAllocator));
        }
        if (arrowColumnHandle.getColumnType().equals(DOUBLE)) {
            return new DoubleColumnWriter(
                    arrowColumnHandle.getColumnType(),
                    new Float8Vector(arrowColumnHandle.getColumnName(), bufferAllocator));
        }
        if (arrowColumnHandle.getColumnType() instanceof VarcharType) {
            return new VarcharColumnWriter(
                    arrowColumnHandle.getColumnType(),
                    new VarCharVector(arrowColumnHandle.getColumnName(), bufferAllocator));
        }
        throw new UnsupportedOperationException("Not yet supported for writing type " + arrowColumnHandle.getColumnType() + ":"+ arrowColumnHandle.getColumnType().getClass() + ":"+ arrowColumnHandle.getColumnName());
    }
}
