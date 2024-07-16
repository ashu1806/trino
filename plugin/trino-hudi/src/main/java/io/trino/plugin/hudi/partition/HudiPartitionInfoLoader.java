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

import io.airlift.concurrent.MoreFutures;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.files.FileSlice;
import io.trino.plugin.hudi.model.HudiTableType;
import io.trino.plugin.hudi.query.HudiDirectoryLister;
import io.trino.plugin.hudi.split.HudiSplitFactory;
import io.trino.spi.connector.ConnectorSplit;

import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class HudiPartitionInfoLoader
        implements Runnable
{
    private final HudiDirectoryLister hudiDirectoryLister;
    private final HudiSplitFactory hudiSplitFactory;
    private final AsyncQueue<ConnectorSplit> asyncQueue;
    private final Deque<String> partitionQueue;
    private final String commitTime;

    private boolean isRunning;

    private final HudiTableHandle tableHandle;

    public  HudiPartitionInfoLoader(
            HudiDirectoryLister hudiDirectoryLister,
            HudiSplitFactory hudiSplitFactory,
            AsyncQueue<ConnectorSplit> asyncQueue,
            Deque<String> partitionQueue,
            String commitTime,
            HudiTableHandle tableHandle)
    {
        this.hudiDirectoryLister = hudiDirectoryLister;
        this.hudiSplitFactory = hudiSplitFactory;
        this.asyncQueue = asyncQueue;
        this.partitionQueue = partitionQueue;
        this.commitTime = commitTime;
        this.isRunning = true;
        this.tableHandle = tableHandle;
    }

    @Override
    public void run()
    {
        while (isRunning || !partitionQueue.isEmpty()) {
            String partitionName = partitionQueue.poll();

            if (partitionName != null) {
                generateSplitsFromPartition(partitionName);
            }
        }
    }

    private void generateSplitsFromPartition(String partitionName)
    {
        Optional<HudiPartitionInfo> partitionInfo = hudiDirectoryLister.getPartitionInfo(partitionName);
        partitionInfo.ifPresent(hudiPartitionInfo -> {
            if (hudiPartitionInfo.doesMatchPredicates() || partitionName.equals("")) {
                List<HivePartitionKey> partitionKeys = hudiPartitionInfo.getHivePartitionKeys();

                org.apache.hudi.common.table.view.AbstractTableFileSystemView
                Stream<org.apache.hudi.common.model.FileSlice> fileSlices = HudiTableType.MOR.equals(table.getTableType()) ?
                        fsView.getLatestMergedFileSlicesBeforeOrOn(relativePartitionPath, latestInstant) :
                        fsView.getLatestFileSlicesBeforeOrOn(relativePartitionPath, latestInstant, false);


                List<FileSlice> fileSlices = hudiDirectoryLister.listFileSlicesBeforeOn(hudiPartitionInfo, commitTime);

                HudiTableType.MERGE_ON_READ.equals(tableHandle.getTableType()) ?

                fileSlices.stream()
                        .flatMap(fileSlice -> hudiSplitFactory.createSplits(partitionKeys, fileSlice, commitTime).stream())
                        .map(asyncQueue::offer)
                        .forEachOrdered(MoreFutures::getFutureValue);

                import org.apache.hudi.common.table.view.HoodieTableFileSystemView
            }
        });

        HudiPartition hudiPartition = getHudiPartition(metastore, metastoreContext, layout, partitionName);
        Path partitionPath = new Path(hudiPartition.getStorage().getLocation());
        String relativePartitionPath = FSUtils.getRelativePartitionPath(tablePath, partitionPath);
        Stream<FileSlice> fileSlices = HudiTableType.MOR.equals(table.getTableType()) ?
                fsView.getLatestMergedFileSlicesBeforeOrOn(relativePartitionPath, latestInstant) :
                fsView.getLatestFileSlicesBeforeOrOn(relativePartitionPath, latestInstant, false);
        fileSlices.map(fileSlice -> createHudiSplit(table, fileSlice, latestInstant, hudiPartition, splitWeightProvider))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(asyncQueue::offer);
    }

    public void stopRunning()
    {
        this.isRunning = false;
    }
}
