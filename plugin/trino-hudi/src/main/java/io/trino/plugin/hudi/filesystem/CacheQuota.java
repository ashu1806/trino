package io.trino.plugin.hudi.filesystem;

import io.airlift.units.DataSize;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.hash.Hashing.md5;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
public class CacheQuota {
    public static final CacheQuota NO_CACHE_CONSTRAINTS = new CacheQuota("NO_IDENTITY", Optional.empty());

    private final String identity;
    private final long identifier;
    private final Optional<DataSize> quota;

    public CacheQuota(String identity, Optional<DataSize> quota)
    {
        this.identity = requireNonNull(identity, "identity is null");
        this.identifier = md5().hashString(identity, UTF_8).asLong();
        this.quota = requireNonNull(quota, "quota is null");
    }

    public String getIdentity()
    {
        return identity;
    }

    public long getIdentifier()
    {
        return identifier;
    }

    public Optional<DataSize> getQuota()
    {
        return quota;
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
        CacheQuota that = (CacheQuota) o;
        return identity.equals(that.identity) && identifier == that.identifier && Objects.equals(quota, that.quota);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(identity, identifier, quota);
    }
}
