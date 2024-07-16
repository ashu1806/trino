package io.trino.plugin.hudi.split;

import io.trino.spi.type.TypeManager;
import io.trino.plugin.hive.HiveType;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class HudiColumnHandle implements ColumnHandle {
    public enum ColumnType
    {
        PARTITION_KEY,
        REGULAR,
    }

    private final int id;
    private final String name;
    private final HiveType hiveType;
    private final Optional<String> comment;
    private final ColumnType columnType;

    @JsonCreator
    public HudiColumnHandle(
            @JsonProperty("id") int id,
            @JsonProperty("name") String name,
            @JsonProperty("hiveType") HiveType hiveType,
            @JsonProperty("comment") Optional<String> comment,
            @JsonProperty("columnType") ColumnType columnType)
    {
        this.id = id;
        this.name = requireNonNull(name, "name is null");
        this.hiveType = requireNonNull(hiveType, "hiveType is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
    }

    @JsonProperty
    public int getId()
    {
        return id;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HudiColumnHandle that = (HudiColumnHandle) o;
        return id == that.id &&
                Objects.equals(name, that.name) &&
                Objects.equals(hiveType, that.hiveType) &&
                Objects.equals(comment, that.comment) &&
                columnType == that.columnType;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, name, hiveType, comment, columnType);
    }

    @JsonProperty
    public HiveType getHiveType()
    {
        return hiveType;
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    @JsonProperty
    public ColumnType getColumnType()
    {
        return columnType;
    }

    public boolean isRegularColumn()
    {
        return columnType == ColumnType.REGULAR;
    }

    public ColumnMetadata toColumnMetadata(TypeManager typeManager)
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(hiveType.getType(typeManager))
                .setExtraInfo(getExtraInfo())
                .build();
    }

    @Override
    public String toString()
    {
        return id + ":" + name + ":" + hiveType + ":" + columnType;
    }

    private Optional<String> getExtraInfo()
    {
        return columnType == ColumnType.PARTITION_KEY ? Optional.of("partition key") : Optional.empty();
    }
}
