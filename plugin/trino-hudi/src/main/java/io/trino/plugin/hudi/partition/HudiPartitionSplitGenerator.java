package io.trino.plugin.hudi.partition;

import io.airlift.log.Logger;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.util.AsyncQueue;
import com.facebook.presto.hudi.HudiFile;
import com.facebook.presto.hudi.HudiPartition;
import com.facebook.presto.hudi.HudiSplit;
import com.facebook.presto.hudi.HudiTableHandle;
import com.facebook.presto.hudi.HudiTableLayoutHandle;
import com.facebook.presto.hudi.HudiTableType;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.google.common.collect.ImmutableList;

import io.airlift.units.DataSize;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hudi.HudiTableHandle;

import io.trino.plugin.hudi.split.HudiSplitWeightProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;

import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Stream;

import static com.facebook.presto.hudi.HudiSessionProperties.getMinimumAssignedSplitWeight;
import static com.facebook.presto.hudi.HudiSessionProperties.getStandardSplitWeightSize;
import static com.facebook.presto.hudi.HudiSessionProperties.isSizeBasedSplitWeightsEnabled;
import static com.facebook.presto.hudi.HudiSplitManager.getHudiPartition;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.util.Objects.requireNonNull;

/** A runnable to take partition names from a queue of partitions to process, generate Hudi splits from the
 * partition, and add the splits to the async ConnectorSplit queue.*/
public class HudiPartitionSplitGenerator implements Runnable {
    private static final Logger log = Logger.get(HudiPartitionSplitGenerator.class);
    private final HiveMetastore metastore;
    private final HudiTableHandle tableHandle;
    //private final HudiTableHandle table;
    private final Path tablePath;
    private final HoodieTableFileSystemView fsView;
    private final AsyncQueue<ConnectorSplit> asyncQueue;
    private final Deque<String> concurrentPartitionQueue;
    private final String latestInstant;
    private final HudiSplitWeightProvider splitWeightProvider;

    public HudiPartitionSplitGenerator(
            ConnectorSession session,
            HiveMetastore metastore,
            HudiTableHandle tableHandle,
            HoodieTableFileSystemView fsView,
            AsyncQueue<ConnectorSplit> asyncQueue,
            Deque<String> concurrentPartitionQueue,
            String latestInstant)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.tableHandle = requireNonNull(tableHandle, "layout is null");
        this.tablePath = new Path(tableHandle.getBasePath());
        this.fsView = requireNonNull(fsView, "fsView is null");
        this.asyncQueue = requireNonNull(asyncQueue, "asyncQueue is null");
        this.concurrentPartitionQueue = requireNonNull(concurrentPartitionQueue, "concurrentPartitionQueue is null");
        this.latestInstant = requireNonNull(latestInstant, "latestInstant is null");
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

    private Optional<HudiSplit> createHudiSplit(
            HudiTableHandle table,
            FileSlice slice,
            String timestamp,
            HudiPartition partition,
            HudiSplitWeightProvider splitWeightProvider)
    {
        HudiFile baseFile = slice.getBaseFile().map(f -> new HudiFile(f.getPath(), 0, f.getFileLen())).orElse(null);
        if (null == baseFile && table.getTableType() == HudiTableType.COW) {
            return Optional.empty();
        }
        List<HudiFile> logFiles = slice.getLogFiles()
                .map(logFile -> new HudiFile(logFile.getPath().toString(), 0, logFile.getFileSize()))
                .collect(toImmutableList());
        long logFilesSize = logFiles.size() > 0 ? logFiles.stream().map(HudiFile::getLength).reduce(0L, Long::sum) : 0L;
        long sizeInBytes = baseFile != null ? baseFile.getLength() + logFilesSize : logFilesSize;

        return Optional.of(new HudiSplit(
                table,
                timestamp,
                partition,
                Optional.ofNullable(baseFile),
                logFiles,
                ImmutableList.of(),
                NodeSelectionStrategy.NO_PREFERENCE,
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
