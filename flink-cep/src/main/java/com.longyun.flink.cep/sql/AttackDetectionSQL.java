package com.longyun.flink.cep.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;

import java.util.Arrays;

import com.longyun.flink.cep.LoginEvent;
import org.apache.flink.table.sinks.PrintTableSink;

/**
 * @author lynn
 * @ClassName com.longyun.flink.cep.AttackDetection
 * @Description TODO
 * @Date 19-3-12 下午5:47
 * @Version 1.0
 **/
public class AttackDetectionSQL {

    /**
    private static void batchSQL() throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        DataSet<LoginEvent> input = env.fromElements(
                new LoginEvent("1","192.168.0.1","fail", new Date(Instant.EPOCH.getEpochSecond())),
                new LoginEvent("1","192.168.0.2","fail", new Date(Instant.EPOCH.getEpochSecond())),
                new LoginEvent("1","192.168.0.3","fail", new Date(Instant.EPOCH.getEpochSecond())),
                new LoginEvent("2","192.168.10.10","success", new Date(Instant.EPOCH.getEpochSecond()))
        );

        // register the DataSet as table "t"
        tEnv.registerDataSet("events", input, "userId, ip, type");

        String sql = "SELECT T.A_userId, T.B_userId\n" +
                "FROM events\n" +
                "MATCH_RECOGNIZE(\n" +
                "   PARTITION BY userId\n" +
//                "   ORDER BY proctime\n" +
                "   MEASURES\n" +
                "       A.userId as A_userId,\n" +
                "       B.userId as B_userId\n" +
                "   PATTERN (A B)\n" +
                "   DEFINE\n" +
                "   A AS type='fail',\n" +
                "   B AS type='fail'\n" +
                ")AS T\n";

        Table result = tEnv.sqlQuery(sql);

        result.printSchema();

        CsvTableSink sink = new CsvTableSink("/tmp/flink-sinks/cep-attack.csv",
                "|",
                1,
                FileSystem.WriteMode.OVERWRITE);

        tEnv.registerTableSink("attack",
//                new String[]{"A_userId", "A_ip", "A_type", "B_userId", "B_ip", "B_type"},
                new String[]{"A_userId", "B_userId"},
                new TypeInformation[]{Types.STRING(), Types.STRING()},
                sink);
        result.insertInto("attack");

        env.execute("Flink CEP Attack Detection");
    }

    */

    /**
     *
     * @param args
     */
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

/*        DataStream<LoginEvent> loginEventStream = env.fromCollection(Arrays.asList(
                new LoginEvent("1","192.168.0.1","fail", new Date(Instant.EPOCH.getEpochSecond())),
                new LoginEvent("1","192.168.0.2","fail", new Date(Instant.EPOCH.getEpochSecond())),
                new LoginEvent("1","192.168.0.3","fail", new Date(Instant.EPOCH.getEpochSecond())),
                new LoginEvent("2","192.168.10.10","success", new Date(Instant.EPOCH.getEpochSecond()))
        ));*/

        DataStream<LoginEvent> loginEventStream = env.fromCollection(Arrays.asList(
                new LoginEvent("1","192.168.0.1","fail"),
                new LoginEvent("1","192.168.0.2","fail"),
                new LoginEvent("1","192.168.0.3","fail"),
                new LoginEvent("2","192.168.10.10","success")
        ));

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        Table table = tableEnv.fromDataStream(loginEventStream, "userId, ip, type, proctime.proctime");
        table.printSchema();
        tableEnv.registerTable("events", table);

        String sql = "SELECT T.A_userId, T.A_ip, T.B_userId, T.B_ip\n" +
                "FROM events\n" +
                "MATCH_RECOGNIZE(\n" +
                "   PARTITION BY userId\n" +
                "   ORDER BY proctime\n" +
                "   MEASURES\n" +
                "       A.userId as A_userId,\n" +
                "       A.ip as A_ip,\n" +
                "       B.userId as B_userId,\n" +
                "       B.ip as B_ip\n" +
                "   PATTERN (A B)\n" +
                "   DEFINE\n" +
                "   A AS type='fail',\n" +
                "   B AS type='fail'\n" +
                ")AS T\n";

        Table result = tableEnv.sqlQuery(sql);

        result.printSchema();

        CsvTableSink sink = new CsvTableSink("/tmp/flink-sinks/cep-attack.csv",
                "|",
                1,
                FileSystem.WriteMode.OVERWRITE);

        tableEnv.registerTableSink("attack",
//                new String[]{"A_userId", "A_ip", "A_type", "B_userId", "B_ip", "B_type"},
                new String[]{"A_userId", "A_ip", "B_userId", "B_ip"},
                new TypeInformation[]{Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()},
                sink);
        result.insertInto("attack");

        result.writeToSink(new PrintTableSink());

        env.execute("Flink CEP Attack Detection");
    }
}
