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
package io.trino.plugin.lance;

import com.google.common.io.Resources;
import io.trino.plugin.lance.internal.LanceReader;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URL;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestLanceDatasetPageSource
{
    private static final SchemaTableName TEST_TABLE_1 = new SchemaTableName("default", "test_table1");

    private LanceMetadata metadata;

    @BeforeEach
    public void setUp()
            throws Exception
    {
        URL lanceDbURL = Resources.getResource(LanceReader.class, "/example_db");
        assertThat(lanceDbURL)
                .describedAs("example db is null")
                .isNotNull();
        LanceConfig lanceConfig = new LanceConfig().setLanceDbUri(lanceDbURL.toString());
        LanceReader lanceReader = new LanceReader(lanceConfig);
        this.metadata = new LanceMetadata(lanceReader, lanceConfig);
    }

    @Test
    public void testTableScan()
    {
        ConnectorTableHandle tableHandle = metadata.getTableHandle(null, TEST_TABLE_1, Optional.empty(), Optional.empty());
        try (LanceDatasetPageSource pageSource = new LanceDatasetPageSource(metadata.getLanceReader(), (LanceTableHandle) tableHandle, metadata.getLanceConfig().getFetchRetryCount())) {
            Page page = pageSource.getNextPage();
            // assert row/column count
            assertThat(page.getChannelCount()).isEqualTo(4);
            assertThat(page.getPositionCount()).isEqualTo(2);
            // assert block content
            Block block = page.getBlock(0);
            assertThat(BIGINT.getLong(block, 0)).isEqualTo(0L);
            block = page.getBlock(1);
            assertThat(BIGINT.getLong(block, 1)).isEqualTo(2L);
            // assert 2nd page
            page = pageSource.getNextPage();
            assertThat(page.getChannelCount()).isEqualTo(4);
            assertThat(page.getPositionCount()).isEqualTo(2);
            // assert no more pages
            page = pageSource.getNextPage();
            assertThat(page).isNull();
            // assert that page is now finish
            assertThat(pageSource.isFinished()).isTrue();
        }
    }
}