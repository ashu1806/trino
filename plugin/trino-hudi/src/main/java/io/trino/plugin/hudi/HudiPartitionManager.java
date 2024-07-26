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
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.plugin.hive.HivePartitionManager.extractPartitionValues;
import static io.trino.plugin.hive.metastore.MetastoreUtil.computePartitionKeyFilter;
import static io.trino.plugin.hive.util.HiveUtil.getPartitionKeyColumnHandles;
import static io.trino.plugin.hive.util.HiveUtil.parsePartitionValue;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HudiPartitionManager
{
    private final TypeManager typeManager;

    @Inject
    public HudiPartitionManager(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    /*public List<String> getEffectivePartitions(HudiTableHandle tableHandle, HiveMetastore metastore)
    {
        Table table = metastore.getTable(tableHandle.getSchemaName(), tableHandle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableHandle.getSchemaTableName()));
        List<Column> partitionColumns = table.getPartitionColumns();
        if (partitionColumns.isEmpty()) {
            return ImmutableList.of("");
        }

        List<HiveColumnHandle> partitionColumnHandles = getPartitionKeyColumnHandles(table, typeManager);
        //---
        String schemaName = tableHandle.getSchemaName();
        String tableName = tableHandle.getTableName();
        List<String> partitionColumn = partitionColumnHandles.stream().map(HiveColumnHandle::getName).collect(Collectors.toList());
        TupleDomain<String> partitionPredicate = computePartitionKeyFilter(partitionColumnHandles, tableHandle.getPartitionPredicates());
        Optional<List<String>> partitionNamesByFilter = metastore.getPartitionNamesByFilter(schemaName, tableName, partitionColumn, partitionPredicate);
        return partitionNamesByFilter.orElseThrow(() -> new TableNotFoundException(table.getSchemaTableName()));

        *//*return metastore
                .getPartitionNamesByFilter(
                        tableHandle.getSchemaName(),
                        tableHandle.getTableName(),
                        partitionColumns.stream().map(Column::getName).collect(Collectors.toList()),
                        computePartitionKeyFilter(partitionColumnHandles, tableHandle.getPartitionPredicates()))

                .orElseThrow(() -> new TableNotFoundException(tableHandle.getSchemaTableName()));*//*
    }*/
    public List<String> getEffectivePartitions(
            ConnectorSession connectorSession,
            HiveMetastore metastore,
            SchemaTableName schemaTableName,
            TupleDomain<ColumnHandle> constraintSummary,
            HudiTableHandle tableHandle)
    {
        Table table = metastore.getTable(tableHandle.getSchemaName(), tableHandle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableHandle.getSchemaTableName()));

        List<Column> partitionColumns = table.getPartitionColumns();

        if (partitionColumns.isEmpty()) {
            return ImmutableList.of("");
        }
        List<HiveColumnHandle> partitionColumnHandles = getPartitionKeyColumnHandles(table, typeManager);
        List<String> partitionColumn = partitionColumnHandles.stream().map(HiveColumnHandle::getName).collect(Collectors.toList());
        TupleDomain<String> partitionPredicate = computePartitionKeyFilter(partitionColumnHandles, tableHandle.getPartitionPredicates());
        Optional<List<String>> partitionNames = metastore.getPartitionNamesByFilter(tableHandle.getSchemaName(), tableHandle.getTableName(), partitionColumn, partitionPredicate);

        List<Type> partitionTypes = partitionColumns.stream()
                .map(column -> typeManager.getType(column.getType().getTypeSignature()))
                .collect(toList());

        // Apply the filtering operation
        return partitionNames
                        .map(partitionList -> partitionList.stream()
                                .filter(partitionName -> parseValuesAndFilterPartition(
                                        partitionName,
                                        partitionColumnHandles,
                                        partitionTypes,
                                        constraintSummary))
                                .collect(Collectors.toList()))
                        .orElseGet(List::of); // Return an empty list if the Optional is empty
    }

    private boolean parseValuesAndFilterPartition(
            String partitionName,
            List<HiveColumnHandle> partitionColumns,
            List<Type> partitionColumnTypes,
            TupleDomain<ColumnHandle> constraintSummary)
    {
        if (constraintSummary.isNone()) {
            return false;
        }

        Map<ColumnHandle, Domain> domains = constraintSummary.getDomains().orElseGet(ImmutableMap::of);
        Map<HiveColumnHandle, NullableValue> partitionValues = parsePartition(partitionName, partitionColumns, partitionColumnTypes);
        for (HiveColumnHandle column : partitionColumns) {
            NullableValue value = partitionValues.get(column);
            Domain allowedDomain = domains.get(column);
            if (allowedDomain != null && !allowedDomain.includesNullableValue(value.getValue())) {
                return false;
            }
        }

        return true;
    }

    private static Map<HiveColumnHandle, NullableValue> parsePartition(
            String partitionName,
            List<HiveColumnHandle> partitionColumns,
            List<Type> partitionColumnTypes)
    {
        /*List<String> partitionColumnNames = partitionColumns.stream()
                .map(HiveColumnHandle::getName)
                .collect(Collectors.toList());*/
        List<String> partitionValues = extractPartitionValues(partitionName);
        ImmutableMap.Builder<HiveColumnHandle, NullableValue> builder = ImmutableMap.builder();
        for (int i = 0; i < partitionColumns.size(); i++) {
            HiveColumnHandle column = partitionColumns.get(i);
            NullableValue parsedValue = parsePartitionValue(partitionName, partitionValues.get(i), partitionColumnTypes.get(i));
            builder.put(column, parsedValue);
        }
        return builder.buildOrThrow();
    }
}
