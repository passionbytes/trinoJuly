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
import io.trino.spi.block.LongArrayBlockBuilder;
import org.apache.arrow.vector.DecimalVector;

import io.trino.spi.type.DecimalType;

import static java.util.Objects.requireNonNull;

import java.math.BigDecimal;

public class DecimalColumnReader
        implements ColumnReader {
    private final DecimalVector decimalVector;
    private int offset;

    public DecimalColumnReader(DecimalVector decimalVector) {
        this.decimalVector = requireNonNull(decimalVector, "decimalVector is null");
    }

    @Override
    public Block readBlock(int batchSize) {
        DecimalType decType = DecimalType.createDecimalType(decimalVector.getPrecision(), decimalVector.getScale());
        LongArrayBlockBuilder blockBuilder = (LongArrayBlockBuilder) decType.createBlockBuilder(null, batchSize);
        for (int i = 0; i < batchSize; i++) {
            if (decimalVector.isNull(offset + i)) {
                blockBuilder.appendNull();
            } else {
                blockBuilder.writeLong(decimalVector.getObject(offset + i).longValue());
            }
        }
        offset += batchSize;
        return blockBuilder.build();
    }
}
