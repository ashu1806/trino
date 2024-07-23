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

import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.Utils;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HivePartitionKey.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static io.trino.plugin.hive.aws.athena.projection.ProjectionType.INTEGER;
import static io.trino.plugin.hive.type.PrimitiveCategory.BOOLEAN;
import static io.trino.plugin.hive.type.PrimitiveCategory.DATE;
import static io.trino.plugin.hive.type.PrimitiveCategory.DOUBLE;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CURSOR_ERROR;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_INVALID_PARTITION_VALUE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.StandardTypes.BIGINT;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HudiPageSource
        implements ConnectorPageSource
{
    private final boolean hasPrefilledBlocks;
    private final Block[] prefilledBlocks;
    private final int[] delegateIndexes;
    private final ConnectorPageSource delegate;

    public HudiPageSource(
            List<HiveColumnHandle> columns,
            Map<String, String> partitionKeys,
            ConnectorPageSource delegate,
            TimeZoneKey timeZoneKey,
            TypeManager typeManager)
    {
        int size = requireNonNull(columns, "columns is null").size();
        requireNonNull(partitionKeys, "partitionKeys is null");
        this.delegate = requireNonNull(delegate, "delegate is null");

        prefilledBlocks = new Block[size];
        delegateIndexes = new int[size];

        int outputIndex = 0;
        int delegateIndex = 0;
        boolean hasPrefilledBlocks = false;
        for (HiveColumnHandle column : columns) {
            if (partitionKeys.containsKey(column.getName())) {
                String partitionValue = partitionKeys.get(column.getName());
                Type type = column.getHiveType().getType(typeManager);
                Object prefilledValue = deserializePartitionValue(type, partitionValue, column.getName(), timeZoneKey);
                prefilledBlocks[outputIndex] = Utils.nativeValueToBlock(type, prefilledValue);
                delegateIndexes[outputIndex] = -1;
                hasPrefilledBlocks = true;
            }
            else {
                delegateIndexes[outputIndex] = delegateIndex;
                delegateIndex++;
            }
            outputIndex++;
        }
        this.hasPrefilledBlocks = hasPrefilledBlocks;
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return delegate.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        try {
            Page dataPage = delegate.getNextPage();
            if (dataPage == null) {
                return null;
            }
            if (!hasPrefilledBlocks) {
                return dataPage;
            }
            int batchSize = dataPage.getPositionCount();
            Block[] blocks = new Block[prefilledBlocks.length];
            for (int i = 0; i < prefilledBlocks.length; i++) {
                if (prefilledBlocks[i] != null) {
                    blocks[i] = RunLengthEncodedBlock.create(blocks[i], batchSize);
                }
                else {
                    blocks[i] = dataPage.getBlock(delegateIndexes[i]);
                }
            }
            return new Page(batchSize, blocks);
        }
        catch (RuntimeException e) {
            closeWithSuppression(e);
            throwIfInstanceOf(e, TrinoException.class);
            throw new TrinoException(HUDI_CURSOR_ERROR, e);
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        try {
            delegate.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return ConnectorPageSource.super.isBlocked();
    }

    @Override
    public Metrics getMetrics()
    {
        return ConnectorPageSource.super.getMetrics();
    }

    @Override
    public String toString()
    {
        return delegate.toString();
    }

    protected void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            // Self-suppression not permitted
            if (throwable != e) {
                throwable.addSuppressed(e);
            }
        }
    }

    static Object deserializePartitionValue(Type type, String valueString, String name, TimeZoneKey timeZoneKey)
    {
        if (valueString == null || HIVE_DEFAULT_DYNAMIC_PARTITION.equals(valueString)) {
            return null;
        }

        try {
            if (type.equals(BOOLEAN)) {
                if (valueString.equalsIgnoreCase("true")) {
                    return true;
                }
                if (valueString.equalsIgnoreCase("false")) {
                    return false;
                }
                throw new IllegalArgumentException();
            }
            if (type.equals(INTEGER)) {
                return (long) parseInt(valueString);
            }
            if (type.equals(BIGINT)) {
                return parseLong(valueString);
            }
            if (type.equals(REAL)) {
                return (long) floatToRawIntBits(parseFloat(valueString));
            }
            if (type.equals(DOUBLE)) {
                return parseDouble(valueString);
            }
            if (type.equals(DATE)) {
                return LocalDate.parse(valueString, DateTimeFormatter.ISO_LOCAL_DATE).toEpochDay();
            }
            if (type instanceof VarcharType) {
                return utf8Slice(valueString);
            }
            if (type.equals(VarbinaryType.VARBINARY)) {
                return utf8Slice(valueString);
            }
            if (type instanceof DecimalType) {
                DecimalType decimalType = (DecimalType) type;
                BigDecimal decimal = new BigDecimal(valueString);
                decimal = decimal.setScale(decimalType.getScale(), BigDecimal.ROUND_UNNECESSARY);
                if (decimal.precision() > decimalType.getPrecision()) {
                    throw new IllegalArgumentException();
                }
                BigInteger unscaledValue = decimal.unscaledValue();
                if (decimal.precision() <= MAX_SHORT_PRECISION) {
                    return unscaledValue.longValue();
                }
                else {
                    return Decimals.encodeScaledValue(decimal, decimalType.getScale());
                }
            }
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(HUDI_INVALID_PARTITION_VALUE, format(
                    "Invalid partition value '%s' for %s partition key: %s",
                    valueString,
                    type.getDisplayName(),
                    name));
        }
        // Hudi tables don't partition by non-primitive-type columns.
        throw new TrinoException(NOT_SUPPORTED, "Invalid partition type " + type);
    }
}
