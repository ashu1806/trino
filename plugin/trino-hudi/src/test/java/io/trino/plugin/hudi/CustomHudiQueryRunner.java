package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import io.trino.plugin.hudi.HudiQueryRunner;
import io.trino.plugin.hudi.testing.HudiTablesInitializer;
import io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer;
import io.trino.testing.DistributedQueryRunner;

import java.io.File;
import java.nio.file.Path;
import java.util.Optional;

import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class CustomHudiQueryRunner {

    private static final String SCHEMA_NAME = "default";

    private CustomHudiQueryRunner() {

    }

    public static void main(String[] args) throws Exception {

        DistributedQueryRunner queryRunner = DistributedQueryRunner
                .builder(createSession())
                .setExtraProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .build();

        queryRunner.installPlugin(new HudiPlugin());

        queryRunner.createCatalog(
                "hudi",
                "hudi",
                ImmutableMap.of("hive.metastore.uri", "thrift://localhost:9083")
        );

        queryRunner.execute("SELECT * FROM hudi.default.my_table");
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("hudi")
                .setSchema(SCHEMA_NAME)
                .build();
    }

}
