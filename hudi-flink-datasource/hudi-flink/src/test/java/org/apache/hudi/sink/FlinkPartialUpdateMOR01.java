package org.apache.hudi.sink;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.marker.SimpleTransactionDirectMarkerBasedEarlyConflictDetectionStrategy;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public final class FlinkPartialUpdateMOR01 {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkPartialUpdateMOR01.class);

  private static final String sourceTable1 = "source_1";
  private static final String sinkAliasTable1 = "sink_1";

  private static final String dbName = "hudi_test";
  private final static String targetTable = "hudi_partial_updata_05";
  private static final String warehouse = "hdfs://127.0.0.1:9000/hudi/hudi_db";
  private static final String basePath = warehouse + "/" + dbName + "/" + targetTable;
  private static final String metastoreUrl = "thrift://localhost:9083";

  private FlinkPartialUpdateMOR01() {
  }

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    env.setParallelism(1);
    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);

    Configuration configuration = tableEnv.getConfig().getConfiguration();
    configuration.setString("table.dynamic-table-options.enabled", "true");

    DataStream<Tuple3<String, String, Long>> dataStream1 =
      env.addSource(new StudentDataFunction1(1, 20000));

    Table inputTable1 = tableEnv.fromDataStream(dataStream1, "uuid, name, ts");

    tableEnv.createTemporaryView(sourceTable1, inputTable1);

    LOG.info("sinkTableDDL1 ddl: {}", sinkTableDDL1());
    tableEnv.executeSql(sinkTableDDL1());

    StatementSet statementSet = tableEnv.createStatementSet();
    statementSet.addInsertSql(String.format("insert into %s(uuid, name, _ts1)\n "
        + "select uuid, name, ts as _ts1 from %s \n",
      sinkAliasTable1, sourceTable1));
//        statementSet.addInsertSql(String.format("insert into %s(uuid, name, _ts1)\n "
//                + "select uuid, name, TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd HH:mm:ss')) as _ts1 from %s \n",
//            sinkAliasTable1, sourceTable1));

    statementSet.execute();
  }

  public static String sinkTableDDL1() {
    return String.format("create table %s(\n"
      + "  uuid STRING,\n"
      + "  name STRING,\n"
      + "  age int,\n"
//            + "  _ts1 timestamp(3),\n"
//            + "  _ts2 timestamp(3),\n"
      + "  _ts1 bigint,\n"
      + "  _ts2 bigint,\n"
      + "  PRIMARY KEY(uuid) NOT ENFORCED"
      + ")\n"
      + " PARTITIONED BY (_ts1)\n"
      + " with (\n"
      + "  'connector' = 'hudi',\n"
      + "  'path' = '%s', -- 替换成的绝对路径\n"
      + "  'table.type' = 'MERGE_ON_READ',\n"
      + "  'write.bucket_assign.tasks' = '6',\n"
      + "  'write.tasks' = '3',\n"
      + "  'write.partition.format' = 'yyyyMMdd',\n"
      + "  'write.partition.timestamp.type' = 'EPOCHMILLISECONDS',\n"
      + "  'hoodie.bucket.index.num.buckets' = '5',\n"
      + "  'changelog.enabled' = 'true',\n"
      + "  'index.type' = 'BUCKET',\n"
      + "  'hoodie.bucket.index.num.buckets' = '5',\n"
      + String.format("  '%s' = '%s',\n", FlinkOptions.PRECOMBINE_FIELD.key(), "_ts1:name;_ts2:age")
      + "  'write.payload.class' = '" + PartialUpdateAvroPayload.class.getName() + "',\n"
      + "  'hoodie.write.log.suffix' = 'job1',\n"
      + "  'hoodie.write.concurrency.mode' = 'optimistic_concurrency_control',\n"
      + "  'hoodie.write.lock.provider' = 'org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider',\n"
      + "  'hoodie.cleaner.policy.failed.writes' = 'LAZY',\n"
      + "  'hoodie.cleaner.policy' = 'KEEP_LATEST_BY_HOURS',\n"
      + "  'hoodie.consistency.check.enabled' = 'false',\n"
      + "  'hoodie.write.lock.early.conflict.detection.enable' = 'true',\n"
      + "  'hoodie.write.lock.early.conflict.detection.strategy' = '"
      + SimpleTransactionDirectMarkerBasedEarlyConflictDetectionStrategy.class.getName() + "',\n"
//            + "  'hoodie.logfile.data.block.max.size' = '40',\n"
      + "  'hoodie.keep.min.commits' = '1440',\n"
      + "  'hoodie.keep.max.commits' = '2880',\n"
      + "  'compaction.schedule.enabled'='false',\n"
      + "  'compaction.async.enabled'='false',\n"
      + "  'compaction.trigger.strategy'='num_or_time',\n"
      + "  'compaction.delta_commits' ='5',\n"
      + "  'compaction.delta_seconds' ='180',\n"
      + "  'compaction.max_memory' = '3096',\n"
      + "  'clean.async.enabled' ='false',\n"
      + "  'hoodie.metrics.on' = 'false',\n"
      + "  'hive_sync.enable' = 'false',\n"
      + "  'hive_sync.mode' = 'hms',\n"
      + "  'hive_sync.db' = '%s',\n"
      + "  'hive_sync.table' = '%s',\n"
      + "  'hive_sync.metastore.uris' = '%s'\n"
      + ")", sinkAliasTable1, basePath, dbName, targetTable, metastoreUrl);
  }

  public static class StudentDataFunction1
    extends RichSourceFunction<Tuple3<String, String, Long>> {
    private volatile boolean cancelled;
    RandomDataGenerator generator = new RandomDataGenerator();
    private int idStart;
    private final int idEnd;

    private StudentDataFunction1() {
      this.idStart = 0;
      this.idEnd = Integer.MAX_VALUE;
    }

    private StudentDataFunction1(int idStart, int idEnd) {
      this.idStart = idStart;
      this.idEnd = idEnd;
    }

    @Override
    public void run(SourceContext<Tuple3<String, String, Long>> sourceContext) throws InterruptedException {
      while (!cancelled) {
        if (idStart <= idEnd) {
          String uuid = String.valueOf(idStart);
          String name = generator.nextHexString(10);
          Long ts = System.currentTimeMillis();

          Tuple3<String, String, Long> row = Tuple3.of(uuid, name, ts);
          sourceContext.collect(row);

          idStart++;

          Thread.sleep(1);
        }
      }
    }

    @Override
    public void cancel() {
      cancelled = true;
    }
  }
}
