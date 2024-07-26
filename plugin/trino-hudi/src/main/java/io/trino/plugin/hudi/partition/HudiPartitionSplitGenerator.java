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

package io.trino.plugin.hudi.partition;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.files.HudiFile;
import io.trino.plugin.hudi.model.HudiTableType;
import io.trino.plugin.hudi.split.HudiSplitWeightProvider;
import io.trino.plugin.hudi.split.SizeBasedSplitWeightProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;

import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hudi.HudiSessionProperties.getMinimumAssignedSplitWeight;
import static io.trino.plugin.hudi.HudiSessionProperties.getStandardSplitWeightSize;
import static io.trino.plugin.hudi.HudiSessionProperties.isSizeBasedSplitWeightsEnabled;
import static io.trino.plugin.hudi.HudiSplitManager.getHudiPartition;
import static java.util.Objects.requireNonNull;

/** A runnable to take partition names from a queue of partitions to process, generate Hudi splits from the
 * partition, and add the splits to the async ConnectorSplit queue.*/
public class HudiPartitionSplitGenerator
        implements Runnable
{
    private static final Logger log = Logger.get(HudiPartitionSplitGenerator.class);
    private final HiveMetastore metastore;
    private final HudiTableHandle tableHandle;
    //private final HudiTableHandle table;
    private final Path tablePath;
    private final HoodieTableFileSystemView fsView;
    private final AsyncQueue<ConnectorSplit> asyncQueue;
    private final Deque<String> concurrentPartitionQueue;
    //private final Queue<String> concurrentPartitionQueue;
    private final String latestInstant;
    private final HudiSplitWeightProvider splitWeightProvider;
    private final TypeManager typeManager;
    private final ConnectorSession session;

    public HudiPartitionSplitGenerator(
            ConnectorSession session,
            HiveMetastore metastore,
            HudiTableHandle tableHandle,
            HoodieTableFileSystemView fsView,
            AsyncQueue<ConnectorSplit> asyncQueue,
            Deque<String> concurrentPartitionQueue,
            //Queue<String> concurrentPartitionQueue,
            String latestInstant,
            TypeManager typeManager)
    {
        this.session = requireNonNull(session, "session is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.tableHandle = requireNonNull(tableHandle, "layout is null");
        this.tablePath = new Path(tableHandle.getBasePath());
        this.fsView = requireNonNull(fsView, "fsView is null");
        this.asyncQueue = requireNonNull(asyncQueue, "asyncQueue is null");
        this.concurrentPartitionQueue = requireNonNull(concurrentPartitionQueue, "concurrentPartitionQueue is null");
        this.latestInstant = requireNonNull(latestInstant, "latestInstant is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.splitWeightProvider = createSplitWeightProvider(requireNonNull(session, "session is null"));
    }

    @Override
    public void run()
    {
        HoodieTimer timer = new HoodieTimer().startTimer();
        while (!concurrentPartitionQueue.isEmpty()) {
            String partitionName = concurrentPartitionQueue.poll();
            if (partitionName != null) {
                generateSplitsFromPartition(partitionName);
            }
        }
        log.debug("Partition split generator finished in %d ms", timer.endTimer());
    }

    private void generateSplitsFromPartition(String partitionName)
    {
        HudiPartition hudiPartition = getHudiPartition(metastore, tableHandle, partitionName, typeManager, session);
        Path partitionPath = new Path(hudiPartition.getStorage().getLocation());
        String relativePartitionPath = FSUtils.getRelativePartitionPath(tablePath, partitionPath);

        Stream<FileSlice> fileSlices = HudiTableType.MERGE_ON_READ.equals(tableHandle.getTableType()) ?
                fsView.getLatestMergedFileSlicesBeforeOrOn(relativePartitionPath, latestInstant) :
                fsView.getLatestFileSlicesBeforeOrOn(relativePartitionPath, latestInstant, false);

        fileSlices.map(fileSlice -> createHudiSplit(tableHandle, fileSlice, latestInstant, hudiPartition, splitWeightProvider))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(asyncQueue::offer);
    }

    private Optional<HudiSplit> createHudiSplit(
            HudiTableHandle tableHandle,
            FileSlice slice,
            String timestamp,
            HudiPartition partition,
            HudiSplitWeightProvider splitWeightProvider)
    {
        HudiFile baseFile = slice.getBaseFile().map(f -> new HudiFile(f.getPath(), 0, f.getFileLen())).orElse(null);
        if (null == baseFile && tableHandle.getTableType() == HudiTableType.COPY_ON_WRITE) {
            return Optional.empty();
        }
        List<HudiFile> logFiles = slice.getLogFiles()
                .map(logFile -> new HudiFile(logFile.getPath().toString(), 0, logFile.getFileSize()))
                .collect(toImmutableList());
        long logFilesSize = logFiles.size() > 0 ? logFiles.stream().map(HudiFile::getLength).reduce(0L, Long::sum) : 0L;
        long sizeInBytes = baseFile != null ? baseFile.getLength() + logFilesSize : logFilesSize;

        return Optional.of(new HudiSplit(
                tableHandle,
                timestamp,
                partition,
                Optional.ofNullable(baseFile),
                logFiles,
                ImmutableList.of(),
                splitWeightProvider.calculateSplitWeight(sizeInBytes)));
    }

    private static HudiSplitWeightProvider createSplitWeightProvider(ConnectorSession session)
    {
        if (isSizeBasedSplitWeightsEnabled(session)) {
            DataSize standardSplitWeightSize = getStandardSplitWeightSize(session);
            double minimumAssignedSplitWeight = getMinimumAssignedSplitWeight(session);
            return new SizeBasedSplitWeightProvider(minimumAssignedSplitWeight, standardSplitWeightSize);
        }
        return HudiSplitWeightProvider.uniformStandardWeightProvider();
    }
}
