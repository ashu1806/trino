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

import com.google.common.util.concurrent.Futures;
import io.airlift.concurrent.BoundedExecutor;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hive.util.ThrottledAsyncQueue;
import io.trino.plugin.hudi.split.HudiBackgroundSplitLoader;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.type.TypeManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.trino.plugin.hudi.HudiSessionProperties.getSplitGeneratorParallelism;

public class HudiSplitSource
        implements ConnectorSplitSource
{
    private final AsyncQueue<ConnectorSplit> queue;
    private final ScheduledFuture splitLoaderFuture;
    private final AtomicReference<TrinoException> trinoException = new AtomicReference<>();
    private final HudiBackgroundSplitLoader splitLoader;

    public HudiSplitSource(
            ConnectorSession session,
            HiveMetastore metastore,
            HudiTableHandle tableHandle,
            HoodieTableFileSystemView fsView,
            List<String> partitions,
            String latestInstant,
            ExecutorService executor,
            ScheduledExecutorService splitLoaderExecutorService,
            int maxSplitsPerSecond,
            int maxOutstandingSplits,
            TypeManager typeManager)
    {
        /*HudiTableMetaClient metaClient = buildTableMetaClient(fileSystemFactory.create(session), tableHandle.getBasePath());
        List<HiveColumnHandle> partitionColumnHandles = table.getPartitionColumns().stream()
                .map(column -> partitionColumnHandleMap.get(column.getName())).collect(toList());

        HudiDirectoryLister hudiDirectoryLister = new HudiReadOptimizedDirectoryLister(
                tableHandle,
                metaClient,
                metastore,
                table,
                partitionColumnHandles,
                partitions);*/

        this.queue = new ThrottledAsyncQueue<>(maxSplitsPerSecond, maxOutstandingSplits, executor);
        //this.queue = new AsyncQueue<>(maxOutstandingSplits, executor);
        this.splitLoader = new HudiBackgroundSplitLoader(
                session,
                metastore,
                new BoundedExecutor(executor, getSplitGeneratorParallelism(session)),
                tableHandle,
                fsView,
                queue,
                partitions,
                latestInstant,
                typeManager);
        this.splitLoaderFuture = splitLoaderExecutorService.schedule(splitLoader, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        boolean noMoreSplits = isFinished();
        Throwable throwable = trinoException.get();
        if (throwable != null) {
            return CompletableFuture.failedFuture(throwable);
        }

        return toCompletableFuture(Futures.transform(
                queue.getBatchAsync(maxSize),
                splits -> new ConnectorSplitBatch(splits, noMoreSplits),
                directExecutor()));
    }

    @Override
    public void close()
    {
        queue.finish();
    }

    @Override
    public boolean isFinished()
    {
        return splitLoaderFuture.isDone() && queue.isFinished();
    }
}
