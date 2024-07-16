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
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitSource;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hudi.partition.HudiPartition;
import io.trino.plugin.hudi.split.HudiColumnHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.PreDestroy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.hive.metastore.MetastoreUtil.computePartitionKeyFilter;
import static io.trino.plugin.hive.util.HiveUtil.getPartitionKeyColumnHandles;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_FILESYSTEM_ERROR;
import static io.trino.plugin.hudi.HudiSessionProperties.*;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static org.apache.hudi.common.table.view.FileSystemViewManager.createInMemoryFileSystemViewWithTimeline;

public class HudiSplitManager
        implements ConnectorSplitManager
{
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
        HiveMetastore metastore = metastoreProvider.apply(session.getIdentity(), (HiveTransactionHandle) transaction);
        Table table = metastore.getTable(hudiTableHandle.getSchemaName(), hudiTableHandle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(schemaTableName(hudiTableHandle.getSchemaName(), hudiTableHandle.getTableName())));

        List<HiveColumnHandle> partitionColumns = getPartitionKeyColumnHandles(table, typeManager);

        Map<String, HiveColumnHandle> partitionColumnHandles = partitionColumns.stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, identity()));

        List<String> partitions = getPartitions(metastore, hudiTableHandle, partitionColumns);

        // Load Hudi metadata
        FileSystem fs = getFileSystem(session, hudiTableHandle);
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
                getMaxOutstandingSplits(session));
        return new ClassLoaderSafeConnectorSplitSource(splitSource, HudiSplitManager.class.getClassLoader());
    }
    private static List<String> getPartitions(HiveMetastore metastore, HudiTableHandle table, List<HiveColumnHandle> partitionColumns)
    {
        if (partitionColumns.isEmpty()) {
            return ImmutableList.of("");
        }

        return metastore.getPartitionNamesByFilter(
                        table.getSchemaName(),
                        table.getTableName(),
                        partitionColumns.stream().map(HiveColumnHandle::getName).collect(Collectors.toList()),
                        computePartitionKeyFilter(partitionColumns, table.getPartitionPredicates()))
                .orElseThrow(() -> new TableNotFoundException(table.getSchemaTableName()));
    }

    private FileSystem getFileSystem(ConnectorSession session, HudiTableHandle table)
    {
        HdfsContext hdfsContext = new HdfsContext(
                session,
                table.getSchemaName(),
                table.getTableName(),
                table.getBasePath(),
                false);
        try {
            return hdfsEnvironment.getFileSystem(hdfsContext, new Path(table.getBasePath()));
        }
        catch (IOException e) {
            throw new TrinoException(HUDI_FILESYSTEM_ERROR, "Could not open file system for " + table, e);
        }
    }

    public static HudiPartition getHudiPartition(HiveMetastore metastore, HudiTableHandle tableHandle, String partitionName)
    {
        String databaseName = tableHandle.getSchemaName();
        String tableName = tableHandle.getTableName();
        List<HiveColumnHandle> partitionColumns = tableHandle.getPartitionColumns();

        if (partitionColumns.isEmpty()) {
            // non-partitioned tableLayout
            Table table = metastore.getTable(databaseName, tableName)
                    .orElseThrow(() -> new TrinoException(HudiErrorCode.HUDI_INVALID_METADATA, format("Table %s.%s expected but not found", databaseName, tableName)));
            return new HudiPartition(partitionName, ImmutableList.of(), ImmutableMap.of(), table.getStorage(), tableHandle.getDataColumns());
        }
        else {
            // partitioned tableLayout
            List<String> partitionValues = extractPartitionValues(partitionName);
            checkArgument(partitionColumns.size() == partitionValues.size(),
                    format("Invalid partition name %s for partition columns %s", partitionName, partitionColumns));
            Partition partition = metastore.getPartition(context, databaseName, tableName, partitionValues)
                    .orElseThrow(() -> new PrestoException(HUDI_INVALID_METADATA, format("Partition %s expected but not found", partitionName)));
            Map<String, String> keyValues = zipPartitionKeyValues(partitionColumns, partitionValues);
            return new HudiPartition(partitionName, partitionValues, keyValues, partition.getStorage(), fromDataColumns(partition.getColumns()));
        }
    }
}
