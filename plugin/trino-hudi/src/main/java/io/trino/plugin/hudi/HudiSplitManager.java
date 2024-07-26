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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitSource;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hudi.partition.HudiPartition;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.PreDestroy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.hive.HivePartitionManager.extractPartitionValues;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static io.trino.plugin.hive.util.HiveUtil.getPartitionKeyColumnHandles;
import static io.trino.plugin.hive.util.HiveUtil.getRegularColumnHandles;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_INVALID_METADATA;
import static io.trino.plugin.hudi.HudiMetadata.fromDataColumns;
import static io.trino.plugin.hudi.HudiSessionProperties.getMaxOutstandingSplits;
import static io.trino.plugin.hudi.HudiSessionProperties.getMaxSplitsPerSecond;
import static io.trino.plugin.hudi.HudiUtil.getFileSystem;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static org.apache.hudi.common.table.view.FileSystemViewManager.createInMemoryFileSystemViewWithTimeline;

public class HudiSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(HudiSplitManager.class);
    private final HdfsEnvironment hdfsEnvironment;
    private final HudiTransactionManager transactionManager;
    private final TypeManager typeManager;
    private final HudiPartitionManager partitionManager;
    private final BiFunction<ConnectorIdentity, HiveTransactionHandle, HiveMetastore> metastoreProvider;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final ExecutorService executor;
    private final ScheduledExecutorService splitLoaderExecutorService;

    @Inject
    public HudiSplitManager(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            HudiTransactionManager transactionManager,
            HudiPartitionManager partitionManager,
            BiFunction<ConnectorIdentity, HiveTransactionHandle, HiveMetastore> metastoreProvider,
            @ForHudiSplitManager ExecutorService executor,
            TrinoFileSystemFactory fileSystemFactory,
            @ForHudiSplitSource ScheduledExecutorService splitLoaderExecutorService)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.metastoreProvider = requireNonNull(metastoreProvider, "metastoreProvider is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.splitLoaderExecutorService = requireNonNull(splitLoaderExecutorService, "splitLoaderExecutorService is null");
    }

    @PreDestroy
    public void destroy()
    {
        this.executor.shutdown();
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        HudiTableHandle hudiTableHandle = (HudiTableHandle) tableHandle;
        //HudiMetadata hudiMetadata = transactionManager.get(transaction, session.getIdentity());
        HiveMetastore metastore = metastoreProvider.apply(session.getIdentity(), (HiveTransactionHandle) transaction);

        Table table = metastore.getTable(hudiTableHandle.getSchemaName(), hudiTableHandle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(schemaTableName(hudiTableHandle.getSchemaName(), hudiTableHandle.getTableName())));

        List<HiveColumnHandle> partitionColumns = getPartitionKeyColumnHandles(table, typeManager);

        Map<String, HiveColumnHandle> partitionColumnHandles = partitionColumns.stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, identity()));

        // Retrieve and prune partitions
        HoodieTimer timer = new HoodieTimer().startTimer();
        List<String> partitions = partitionManager.getEffectivePartitions(session, metastore, hudiTableHandle.getSchemaTableName(), constraint.getSummary(), hudiTableHandle); //getPartitions(metastore, hudiTableHandle, partitionColumns);
        log.debug("Took %d ms to get %d partitions", timer.endTimer(), partitions.size());

        if (partitions.isEmpty()) {
            return new FixedSplitSource(ImmutableList.of());
        }

        // Load Hudi metadata
        FileSystem fs = getFileSystem(session, hudiTableHandle, hdfsEnvironment);
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(HudiSessionProperties.isHudiMetadataEnabled(session)).build();
        Configuration conf = fs.getConf();
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(conf).setBasePath(hudiTableHandle.getBasePath()).build();
        HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
        String timestamp = timeline.lastInstant().map(HoodieInstant::getTimestamp).orElse(null);
        if (timestamp == null) {
            // no completed instant for current table
            return new FixedSplitSource(ImmutableList.of());
        }
        HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(conf);
        HoodieTableFileSystemView fsView = createInMemoryFileSystemViewWithTimeline(engineContext, metaClient, metadataConfig, timeline);

        HudiSplitSource splitSource = new HudiSplitSource(
                session,
                metastore,
                hudiTableHandle,
                fsView,
                partitions,
                timestamp,
                executor,
                splitLoaderExecutorService,
                getMaxSplitsPerSecond(session),
                getMaxOutstandingSplits(session),
                typeManager);
        return new ClassLoaderSafeConnectorSplitSource(splitSource, HudiSplitManager.class.getClassLoader());
    }

    public static HudiPartition getHudiPartition(HiveMetastore metastore, HudiTableHandle tableHandle, String partitionName, TypeManager typeManager, ConnectorSession session)
    {
        String databaseName = tableHandle.getSchemaName();
        String tableName = tableHandle.getTableName();
        List<HiveColumnHandle> partitionColumns = tableHandle.getPartitionColumns();

        if (partitionColumns.isEmpty()) {
            // non-partitioned tableLayout
            Table table = metastore.getTable(databaseName, tableName)
                    .orElseThrow(() -> new TrinoException(HUDI_INVALID_METADATA, format("Table %s.%s expected but not found", databaseName, tableName)));

            return new HudiPartition(partitionName, ImmutableList.of(), ImmutableMap.of(), table.getStorage(), getRegularColumnHandles(table, typeManager, getTimestampPrecision(session)));
        }
        else {
            // The method extracts the partition values from the partitionName. This method splits the partitionName into individual partition values
            List<String> partitionValues = extractPartitionValues(partitionName);

            //It checks if the number of partition columns matches the number of partition values. If they do not match, it throws an IllegalArgumentException.
            checkArgument(partitionColumns.size() == partitionValues.size(),
                    format("Invalid partition name %s for partition columns %s", partitionName, partitionColumns));

            Table table = metastore.getTable(databaseName, tableName)
                    .orElseThrow(() -> new TrinoException(HUDI_INVALID_METADATA, format("Table %s.%s expected but not found", databaseName, tableName)));

            // The method retrieves the partition metadata from the metastore
            Partition partition = metastore.getPartition(table, partitionValues)
                    .orElseThrow(() -> new TrinoException(HUDI_INVALID_METADATA, format("Partition %s expected but not found", partitionName)));

            //It constructs a map of partition column names to their corresponding values using the zipPartitionKeyValues method.
            Map<String, String> keyValues = zipPartitionKeyValues(partitionColumns, partitionValues);
            return new HudiPartition(partitionName, partitionValues, keyValues, partition.getStorage(), fromDataColumns(partition.getColumns(), typeManager, table));
        }
    }

    private static Map<String, String> zipPartitionKeyValues(List<HiveColumnHandle> partitionColumns, List<String> partitionValues)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        Streams.forEachPair(partitionColumns.stream(), partitionValues.stream(),
                (column, value) -> builder.put(column.getName(), value));
        return builder.buildOrThrow();
    }
}
