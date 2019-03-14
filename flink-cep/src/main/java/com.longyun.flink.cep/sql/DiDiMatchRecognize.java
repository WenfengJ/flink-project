package com.longyun.flink.cep.sql;

import com.sun.xml.internal.fastinfoset.tools.PrintTable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;

/**
 * @author lynn
 * @ClassName com.longyun.flink.cep.sql.DiDiMatchRecognize
 * @Description TODO
 * @Date 19-3-13 上午9:04
 * @Version 1.0
 **/
public class DiDiMatchRecognize {

    /**
     *
     * @param args
     */
    public static void main(String[] args) throws Exception{

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);


        DataSet<WordCount> input = env.fromElements(
                new WordCount("1", 1),
                new WordCount("2", 2),
                new WordCount("1", 3),
                new WordCount("2", 3),
                new WordCount("2", 2),
                new WordCount("1", 1));

        // register the DataSet as table "t"
        tEnv.registerDataSet("t", input, "word, frequency");

        final String sql = "select * \n"
                + "  from t match_recognize \n"
                + "  (\n"
                + "       measures STRT.word as  start_word ,"
                + "    FINAL LAST(A.frequency) as A_id \n"
                + "    pattern (STRT A+) \n"
                + "    define \n"
                + "      A AS A.word = '1' \n"
                + "  ) mr";
//     run a SQL query on the Table and retrieve the result as a new Table
//    Table result = tEnv.sqlQuery(
//       "SELECT word, SUM(frequency) as frequency FROM t GROUP BY word");
//    tEnv.sqlUpdate(sql);
        Table result = tEnv.sqlQuery(sql);

        result.printSchema();

        CsvTableSink sink = new CsvTableSink("/tmp/flink-sinks/cepdidi.csv",
                "|",
                1,
                FileSystem.WriteMode.OVERWRITE);

        tEnv.registerTableSink("didi",
                new String[]{"start_word", "A_id"},
                new TypeInformation[]{Types.STRING(), Types.INT()},
                sink);
        result.insertInto("didi");

        env.execute();
    }
}
