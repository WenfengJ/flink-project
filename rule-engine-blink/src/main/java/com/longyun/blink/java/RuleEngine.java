package com.longyun.blink.java;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.sinks.csv.CsvTableSink;

/**
 * @author yuanxiaolong
 * @ClassName com.longyun.blink.RuleEngine
 * @Description TODO
 * @Date 2019/2/19 15:12
 * @Version 1.0
 **/
public class RuleEngine {

    final static String brokers = "res-spark-0001:9092,res-spark-0002:9092,res-spark-0003:9092";

    final static String dstPath1 = "flink-sinks/flat-json-1.csv";
    final static String dstPath2 = "flink-sinks/flat-json-2.csv";


    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //启用checkpoint
//        env.enableCheckpointing(5000);
        final StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        final ConnectorDescriptor connect = new Kafka()
                .version("0.11")
                .topic("iot-src")
                .property("bootstrap.servers", brokers)
                .property("group.id", "blink")
//                .property("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//                .property("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                // isn't a known config
//                .property("transaction.max.timeout.ms", "3600000")
//                .property("flink.partition-discovery.interval-millis", "30000")
                //default: read_uncommitted
//                .property("isolation.level", "read_committed")
                .startFromLatest()
//                .startFromEarliest()
                .sinkPartitionerFixed();         // each Flink partition ends up in at-most one Kafka partition (default)

        tableEnv.connect(connect)
                .withFormat(new Json()
                        .failOnMissingField(false)
                        .deriveSchema())
                .withSchema(new Schema()
                        .field("clienttoken", Types.STRING())
                        .field("timestamp", Types.SQL_TIMESTAMP()))
                .inAppendMode()
                .registerTableSource("tb_raw");

        Table table = tableEnv.sqlQuery("select * from tb_raw");

        System.out.println("-------------------------");
        table.printSchema();

        CsvTableSink sink1 = new CsvTableSink(dstPath1,                             // output path
                "|",                 // optional: delimit files by '|'
                1,                     // optional: write to a single file
                FileSystem.WriteMode.OVERWRITE);  // optional: override existing files

        tableEnv.registerTableSink("csv1",
                new String[]{"clienttoken", "timestamp"},
                new DataType[]{DataTypes.STRING, DataTypes.TIMESTAMP},
                sink1);

        table.insertInto("csv1");

        try {
            tableEnv.execute("Flink Rule Engine Application");
//            env.execute();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
