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
import org.apache.arrow.vector.Float4Vector;


import static io.trino.spi.type.RealType.REAL;

import io.trino.spi.type.RealType;

import static java.util.Objects.requireNonNull;



public class RealColumnReader
        implements ColumnReader
{
    private final Float4Vector floatVector;
    private int offset;

    public RealColumnReader(Float4Vector floatVector)
    {
        this.floatVector = requireNonNull(floatVector, "floatVector is null");
    }

    @Override
    public Block readBlock(int batchSize)
    {
    	 BlockBuilder blockBuilder = REAL.createBlockBuilder(null, batchSize);
         for (int i = 0; i < batchSize; i++) {
             if (floatVector.isNull(offset + i)) {
                 blockBuilder.appendNull();
             }
             else {
            	 REAL.writeDouble(blockBuilder,floatVector.get(offset + i));
             }
         }
         offset += batchSize;
         return blockBuilder.build();
     
    }
}
