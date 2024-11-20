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


import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;


import static java.util.Objects.requireNonNull;

public class VarcharColumnWriter
        implements ColumnWriter
{
    private final Type type;
    private final VarCharVector varCharVector;

    public VarcharColumnWriter(Type type, VarCharVector varCharVector)
    {
        this.type = requireNonNull(type, "type is null");
        this.varCharVector = requireNonNull(varCharVector, "varCharVector is null");
    }

    @Override
    public void writeBlock(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
            	varCharVector.setNull(i);
            }
            else {
            	varCharVector.setSafe(i, new Text(((Slice)type.getObject(block, i)).toStringUtf8()));
            }
        }
    }

    @Override
    public FieldVector getValueVector()
    {
        return varCharVector;
    }
}

