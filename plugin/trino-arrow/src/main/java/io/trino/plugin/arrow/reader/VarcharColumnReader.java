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
import org.apache.arrow.vector.VarCharVector;

import io.trino.spi.type.VarcharType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class VarcharColumnReader
        implements ColumnReader
{
    private final VarCharVector varCharVector;
    private int offset;

    public VarcharColumnReader(VarCharVector varCharVector)
    {
        this.varCharVector = requireNonNull(varCharVector, "varChartVector is null");
    }

    @Override
    public Block readBlock(int batchSize)
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, batchSize);
        
        for (int i = 0; i < batchSize; i++) {
            if (varCharVector.isNull(offset + i)) {
            	VarcharType.createUnboundedVarcharType().writeString(blockBuilder, null);
            }
            else {
            	VarcharType.createUnboundedVarcharType().writeString(blockBuilder, varCharVector.getObject(offset + i).toString());
            }
        }
        offset += batchSize;
        return blockBuilder.build();
    }
}
