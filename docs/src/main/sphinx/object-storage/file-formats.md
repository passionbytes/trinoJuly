# Object storage file formats

Object storage connectors support one or more file formats specified by the
underlying data source.

(hive-orc-configuration)=
## ORC format configuration properties

The following properties are used to configure the read and write operations
with ORC files performed by supported object storage connectors:

:::{list-table} ORC format configuration properties
:widths: 30, 50, 20
:header-rows: 1

* - Property Name
  - Description
  - Default
* - `hive.orc.time-zone`
  - Sets the default time zone for legacy ORC files that did not declare a time
    zone.
  - JVM default
* - `hive.orc.use-column-names`
  - Access ORC columns by name. By default, columns in ORC files are accessed by
    their ordinal position in the Hive table definition. The equivalent catalog
    session property is `orc_use_column_names`.
  - `false`
* - `hive.orc.bloom-filters.enabled`
  - Enable bloom filters for predicate pushdown.
  - `false`
* - `hive.orc.read-legacy-short-zone-id`
  - Allow reads on ORC files with short zone ID in the stripe footer.
  - `false`
:::

(hive-parquet-configuration)=
## Parquet format configuration properties

The following properties are used to configure the read and write operations
with Parquet files performed by supported object storage connectors:

:::{list-table} Parquet format configuration properties
:widths: 30, 50, 20
:header-rows: 1

* - Property Name
  - Description
  - Default
* - `hive.parquet.time-zone`
  - Adjusts timestamp values to a specific time zone. For Hive 3.1+, set this to
    UTC.
  - JVM default
* - `hive.parquet.use-column-names`
  - Access Parquet columns by name by default. Set this property to `false` to
    access columns by their ordinal position in the Hive table definition. The
    equivalent catalog session property is `parquet_use_column_names`.
  - `true`
* - `parquet.writer.validation-percentage`
  - Percentage of parquet files to validate after write by re-reading the whole
    file. The equivalent catalog session property is
    `parquet_optimized_writer_validation_percentage`. Validation can be turned
    off by setting this property to `0`.
  - `5`
* - `parquet.writer.page-size`
  - Maximum size of pages written by Parquet writer.
  - `1 MB`
* - `parquet.writer.page-value-count`
  - Maximum values count of pages written by Parquet writer.
  - `80000`
* - `parquet.writer.block-size`
  - Maximum size of row groups written by Parquet writer.
  - `128 MB`
* - `parquet.writer.batch-size`
  - Maximum number of rows processed by the parquet writer in a batch.
  - `10000`
* - `parquet.use-bloom-filter`
  - Whether bloom filters are used for predicate pushdown when reading Parquet
    files. Set this property to `false` to disable the usage of bloom filters by
    default. The equivalent catalog session property is
    `parquet_use_bloom_filter`.
  - `true`
* - `parquet.use-column-index`
  - Skip reading Parquet pages by using Parquet column indices. The equivalent
    catalog session property is `parquet_use_column_index`. Only supported by
    the Delta Lake and Hive connectors.
  - `true`
* - `parquet.ignore-statistics`
  - Ignore statistics from Parquet to allow querying files with corrupted or
    incorrect statistics. The equivalent catalog session property is
    `parquet_ignore_statistics`.
  - `false`
* - `parquet.max-read-block-row-count`
  - Sets the maximum number of rows read in a batch. The equivalent catalog
    session property is named `parquet_max_read_block_row_count` and supported
    by the Delta Lake, Hive, and Iceberg connectors.
  - `8192`
* - `parquet.small-file-threshold`
  - [Data size](prop-type-data-size) below which a Parquet file is read
    entirely. The equivalent catalog session property is named
    `parquet_small_file_threshold`.
  - `3MB`
* - `parquet.experimental.vectorized-decoding.enabled`
  - Enable using Java Vector API (SIMD) for faster decoding of parquet files.
    The equivalent catalog session property is
    `parquet_vectorized_decoding_enabled`.
  - `true`
:::