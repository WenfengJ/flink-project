package com.lynn.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sinks.PrintTableSink;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;


/**
 * @author lynn
 * @ClassName com.lynn.flink.JsonEngine
 * @Description TODO
 * @Date 19-3-26 上午8:53
 * @Version 1.0
 **/
public class JsonEngine {

    private static final Logger LOG = LoggerFactory.getLogger(JsonEngine.class);

    static final String brokers = "res-spark-0001:9092,res-spark-0002:9092,res-spark-0003:9092";

//    static final String sourceTopic = "iot-src"; //sh-res
    static final String sourceTopic = "tp-arr";

    /**
     *
     * @param args
     */
    public static void main(String[] args) throws Exception{
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        Kafka kafka = new Kafka().version("0.11")
                .topic(sourceTopic)
                .startFromEarliest()
//                .startFromLatest()
                .property("bootstrap.servers", brokers)
                .property("group.id", "res")
                .property("session.timeout.ms", "30000")
                .sinkPartitionerFixed();

        tableEnv.connect(kafka)
                .withFormat(new Json()
                        .failOnMissingField(false)
                        .deriveSchema())
                .withSchema(new Schema()
                        .field("search_time", Types.LONG())
                        .field("code", Types.INT())
                        .field("results", Types.ROW(
                                new String[]{"id", "items"},
                                new TypeInformation[]{
                                        Types.STRING(),
                                        ObjectArrayTypeInfo.getInfoFor(Array.newInstance(Row.class, 10).getClass(),
                                                Types.ROW(
                                                        new String[]{"id", "name", "title", "url", "publish_time", "score"},
                                                        new TypeInformation[]{Types.STRING(),Types.STRING(),Types.STRING(),Types.STRING(),Types.LONG(),Types.FLOAT()}
                                                        ))})
                        )).inAppendMode().registerTableSource("tb_json");
        /*
        String sql1 = "select * from tb_json";
        Table table = tableEnv.sqlQuery(sql1);

        LOG.info("------------------print schema------------------");
        table.printSchema();

        tableEnv.registerTableSink("console",
                new String[]{"f0", "f1", "f2"},
        new TypeInformation[]{Types.LONG(),Types.INT(), Types.ROW(Types.STRING(),
                ObjectArrayTypeInfo.getInfoFor(Types.ROW(Types.STRING(),Types.STRING(),Types.STRING(),Types.STRING(),Types.LONG(),Types.FLOAT())))},
        new PrintTableSink());

        table.insertInto("console");

        //item[1] item[10] 数组下标从1开始
        String sql2 = "select search_time, code, id as result_id, items[10] as item_0\n"
                + "from tb_json";

        Table table2 = tableEnv.sqlQuery(sql2);
        tableEnv.registerTable("tb_item_10", table2);
        LOG.info("------------------print {} schema------------------", "tb_item_10");
        table2.printSchema();
        tableEnv.registerTableSink("console2",
                new String[]{"f0", "f1", "f2", "f3"},
                new TypeInformation[]{
                        Types.LONG(),Types.INT(),
                        Types.STRING(),
                        Types.ROW(Types.STRING(),Types.STRING(),Types.STRING(),Types.STRING(),Types.LONG(),Types.FLOAT())
                },
                new PrintTableSink());

        table2.insertInto("console2");

        String sql3 = "select search_time, code, result_id, title, id, name, publish_time, url, score\n" +
                "from tb_item_10";
        Table table3 = tableEnv.sqlQuery(sql3);

        LOG.info("------------------print {} schema------------------", "tb_item");
        table3.printSchema();

        tableEnv.registerTableSink("console3",
                new String[]{"search_time", "code", "result_id", "title", "id", "name", "publish_time", "url", "score"},
                new TypeInformation[]{Types.LONG(),Types.INT(),
                        Types.STRING(),
                        Types.STRING(),
                        Types.STRING(),
                        Types.STRING(),
                        Types.LONG(),
                        Types.STRING(),
                        Types.FLOAT()
                },
                new PrintTableSink());

        table3.insertInto("console3");
        */

        String sql4 = "select search_time, code, results.id as result_id, items[1].name as item_1_name, items[2].id as item_2_id\n"
                + "from tb_json";

        Table table4 = tableEnv.sqlQuery(sql4);
        tableEnv.registerTable("tb_item_2", table4);
        LOG.info("------------------print {} schema------------------", "tb_item_2");
        table4.printSchema();
        tableEnv.registerTableSink("console4",
                new String[]{"f0", "f1", "f2", "f3", "f4"},
                new TypeInformation[]{
                        Types.LONG(),Types.INT(),
                        Types.STRING(),
                        Types.STRING(),
                        Types.STRING()
//                        Types.ROW(Types.STRING(),Types.STRING(),Types.STRING(),Types.STRING(),Types.LONG(),Types.FLOAT()),
//                        Types.ROW(Types.STRING(),Types.STRING(),Types.STRING(),Types.STRING(),Types.LONG(),Types.FLOAT())
                },
                new PrintTableSink());

        table4.insertInto("console4");

        // execute program
        env.execute("Flink Table Json Engine");
    }
}
