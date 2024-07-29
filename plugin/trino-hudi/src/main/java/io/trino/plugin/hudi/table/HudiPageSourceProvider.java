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
package io.trino.plugin.hudi.table;

/*import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.ReaderColumns;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreUtil;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import io.trino.plugin.hudi.HudiPageSource;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.files.HudiFile;
import io.trino.plugin.hudi.query.HudiRealTimeRecordCursor;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.predicateMatches;
import static io.trino.plugin.hive.HivePageSourceProvider.projectBaseColumns;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.ParquetReaderProvider;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.getColumnIndexStore;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.getParquetMessageType;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.getParquetTupleDomain;
import static io.trino.plugin.hive.util.HiveUtil.makePartName;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_BAD_DATA;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CURSOR_ERROR;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_INVALID_PARTITION_VALUE;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_UNSUPPORTED_TABLE_TYPE;
import static io.trino.plugin.hudi.HudiParquetPageSources.createParquetPageSource;
import static io.trino.plugin.hudi.HudiSessionProperties.shouldUseParquetColumnNames;
import static io.trino.plugin.hudi.model.HudiTableType.COPY_ON_WRITE;
import static io.trino.plugin.hudi.model.HudiTableType.MERGE_ON_READ;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.BOOLEAN;
import static io.trino.spi.type.StandardTypes.DATE;
import static io.trino.spi.type.StandardTypes.DECIMAL;
import static io.trino.spi.type.StandardTypes.DOUBLE;
import static io.trino.spi.type.StandardTypes.INTEGER;
import static io.trino.spi.type.StandardTypes.REAL;
import static io.trino.spi.type.StandardTypes.SMALLINT;
import static io.trino.spi.type.StandardTypes.TIMESTAMP;
import static io.trino.spi.type.StandardTypes.TINYINT;
import static io.trino.spi.type.StandardTypes.VARBINARY;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.parseFloat;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableList;

public class HudiPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final FileFormatDataSourceStats dataSourceStats;
    private final ParquetReaderOptions options;
    private final DateTimeZone timeZone;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private static final int DOMAIN_COMPACTION_THRESHOLD = 1000;
    private final BiFunction<ConnectorIdentity, HiveTransactionHandle, HiveMetastore> metastoreProvider;

    @Inject
    public HudiPageSourceProvider(
            TrinoFileSystemFactory fileSystemFactory,
            FileFormatDataSourceStats dataSourceStats,
            ParquetReaderConfig parquetReaderConfig,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            BiFunction<ConnectorIdentity, HiveTransactionHandle, HiveMetastore> metastoreProvider)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.dataSourceStats = requireNonNull(dataSourceStats, "dataSourceStats is null");
        this.options = requireNonNull(parquetReaderConfig, "parquetReaderConfig is null").toParquetReaderOptions();
        this.timeZone = DateTimeZone.forID(TimeZone.getDefault().getID());
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.metastoreProvider = requireNonNull(metastoreProvider, "metastoreProvider is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle connectorTable,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        HudiSplit hudiSplit = (HudiSplit) connectorSplit;
        HudiTableHandle tableHandle = (HudiTableHandle) connectorTable;

        HiveMetastore metastore = metastoreProvider.apply(session.getIdentity(), (HiveTransactionHandle) transaction);

        Table table = metastore.getTable(tableHandle.getSchemaName(), tableHandle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(schemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName())));

        List<HiveColumnHandle> hiveColumns = columns.stream()
                .map(HiveColumnHandle.class::cast)
                .collect(toList());
        // just send regular columns to create parquet page source
        // for partition columns, separate blocks will be created
        List<HiveColumnHandle> regularColumns = hiveColumns.stream()
                .filter(columnHandle -> !columnHandle.isPartitionKey() && !columnHandle.isHidden())
                .collect(Collectors.toList());

        final ConnectorPageSource dataColumnPageSource;

        if (tableHandle.getTableType().equals(COPY_ON_WRITE)) {
            HudiFile baseFile = hudiSplit.getBaseFile().orElseThrow(() ->
                    new TrinoException(HUDI_CANNOT_OPEN_SPLIT, "Split without base file is invalid"));

            Path path = new Path(baseFile.getPath());
            Configuration configuration = hdfsEnvironment.getConfiguration(new HdfsContext(session), path);

            dataColumnPageSource = createParquetPageSource(
                    typeManager,
                    hdfsEnvironment,
                    session,
                    configuration,
                    path,
                    baseFile.getStart(),
                    baseFile.getLength(),
                    regularColumns,
                    TupleDomain.all(), // TODO: predicates
                    dataSourceStats);
        }
        else if (tableHandle.getTableType().equals(MERGE_ON_READ)) {
            Properties schema = MetastoreUtil.getHiveSchema(table);
            RecordCursor recordCursor = HudiRealTimeRecordCursor.createRealtimeRecordCursor(
                    hdfsEnvironment,
                    session,
                    schema,
                    hudiSplit,
                    hiveColumns,
                    ZoneId.of("UTC"), // TODO configurable
                    typeManager);
            List<Type> types = hiveColumns.stream()
                    .map(column -> column.getHiveType().getType(typeManager))
                    .collect(toImmutableList());
            dataColumnPageSource = new RecordPageSource(types, recordCursor);
        }
        else {
            throw new TrinoException(HUDI_UNSUPPORTED_TABLE_TYPE, "Could not create page source for table type " + tableHandle.getTableType());
        }
        return new HudiPageSource(
                hiveColumns,
                hudiSplit.getPartition().getKeyValues(),
                dataColumnPageSource,
                session.getTimeZoneKey(),
                typeManager);
    }
}*/
