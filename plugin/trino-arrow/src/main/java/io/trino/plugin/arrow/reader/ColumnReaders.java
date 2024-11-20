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

import io.trino.plugin.arrow.ArrowColumnHandle;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;

import org.apache.arrow.vector.VectorSchemaRoot;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.StandardTypes.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.DoubleType.DOUBLE;
import io.trino.spi.type.DecimalType;


public class ColumnReaders
{
    private ColumnReaders()
    {
    }

    public static ColumnReader getColumnReader(ArrowColumnHandle arrowColumnHandle, VectorSchemaRoot root)
    {
        if (arrowColumnHandle.getColumnType().equals(BIGINT)) {
            return new BigIntColumnReader((BigIntVector) root.getVector(arrowColumnHandle.getColumnName()));
        }
        if (arrowColumnHandle.getColumnType().equals(INTEGER)) {
            return new IntegerColumnReader((IntVector) root.getVector(arrowColumnHandle.getColumnName()));
        }

        if (arrowColumnHandle.getColumnType().equals(TINYINT)) {
            return new TinyIntColumnReader((TinyIntVector) root.getVector(arrowColumnHandle.getColumnName()));
        }
        if (arrowColumnHandle.getColumnType().equals(VARCHAR)) {
            return new VarcharColumnReader((VarCharVector) root.getVector(arrowColumnHandle.getColumnName()));
        }
        if (arrowColumnHandle.getColumnType() instanceof DecimalType) {
            return new DecimalColumnReader((DecimalVector) root.getVector(arrowColumnHandle.getColumnName()));
        }
        if (arrowColumnHandle.getColumnType().equals(REAL)) {
            return new RealColumnReader((Float4Vector) root.getVector(arrowColumnHandle.getColumnName()));
        }
        if (arrowColumnHandle.getColumnType().equals(DOUBLE)) {
            return new DoubleColumnReader((Float8Vector) root.getVector(arrowColumnHandle.getColumnName()));
        }
        throw new UnsupportedOperationException("Not yet read supported for type " + arrowColumnHandle.getColumnType() +":" + arrowColumnHandle.getColumnName());
    }
}
