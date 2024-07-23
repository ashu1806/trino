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
package io.trino.plugin.hudi.split;

import com.google.common.util.concurrent.Futures;
import io.airlift.log.Logger;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.partition.HudiPartitionSplitGenerator;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.type.TypeManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hudi.HudiSessionProperties.getSplitGeneratorParallelism;
import static java.util.Objects.requireNonNull;

public class HudiBackgroundSplitLoader
        implements Runnable
{
    private static final Logger log = Logger.get(HudiBackgroundSplitLoader.class);
    private final ConnectorSession session;
    private final AsyncQueue<ConnectorSplit> asyncQueue;
    private final Executor splitGeneratorExecutor;
    private final int splitGeneratorNumThreads;
    private final List<String> partitions;
    private final String latestInstant;
    private final HiveMetastore metastore;
    private final HudiTableHandle tableHandle;
    private final HoodieTableFileSystemView fsView;
    private final TypeManager typeManager;

    public HudiBackgroundSplitLoader(
            ConnectorSession session,
            HiveMetastore metastore,
            Executor splitGeneratorExecutor,
            HudiTableHandle tableHandle,
            HoodieTableFileSystemView fsView,
            AsyncQueue<ConnectorSplit> asyncQueue,
            List<String> partitions,
            String latestInstant,
            TypeManager typeManager)
    {
        this.session = requireNonNull(session, "session is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.fsView = requireNonNull(fsView, "fsView is null");
        this.asyncQueue = requireNonNull(asyncQueue, "asyncQueue is null");
        this.partitions = requireNonNull(partitions, "partitions is null");
        this.latestInstant = requireNonNull(latestInstant, "latestInstant is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.splitGeneratorExecutor = requireNonNull(splitGeneratorExecutor, "splitGeneratorExecutorService is null");
        this.splitGeneratorNumThreads = getSplitGeneratorParallelism(session);
    }

    @Override
    public void run()
    {
        HoodieTimer timer = new HoodieTimer().startTimer();
        Deque<String> partitionQueue = new ConcurrentLinkedDeque<>(partitions);
        List<HudiPartitionSplitGenerator> splitGeneratorList = new ArrayList<>();
        List<Future> splitGeneratorFutures = new ArrayList<>();

        // Start a number of partition split generators to generate the splits in parallel
        for (int i = 0; i < splitGeneratorNumThreads; i++) {
            //HudiPartitionInfoLoader generator = new HudiPartitionInfoLoader(hudiDirectoryLister, hudiSplitFactory, asyncQueue, partitionQueue, commitTime, tableHandle);
            HudiPartitionSplitGenerator generator = new HudiPartitionSplitGenerator(
                    session, metastore, tableHandle, fsView, asyncQueue, partitionQueue, latestInstant, typeManager);
            splitGeneratorList.add(generator);
            splitGeneratorFutures.add(Futures.submit(generator, splitGeneratorExecutor));
        }

        /*for (HudiPartitionSplitGenerator generator : splitGeneratorList) {
            // Let the split generator stop once the partition queue is empty
            generator.stopRunning();
        }*/

        // Wait for all split generators to finish
        for (Future future : splitGeneratorFutures) {
            try {
                future.get();
            }
            catch (InterruptedException | ExecutionException e) {
                throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, "Error generating Hudi split", e);
            }
        }
        asyncQueue.finish();
        log.debug("Finished getting all splits in %d ms", timer.endTimer());
    }
}
