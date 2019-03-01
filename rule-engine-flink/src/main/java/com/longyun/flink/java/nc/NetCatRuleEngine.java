package com.longyun.flink.java.nc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.longyun.calcite.json.JsonSchemaFactory;
import com.longyun.flink.java.res.Raw;
import com.longyun.flink.java.res.Rule;
import com.longyun.flink.java.res.RuleRaw;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;

/**
 * @author yuanxiaolong
 * @ClassName com.longyun.blink.java.KeyedBroadcastRuleEngine
 * @Description TODO
 * @Date 2019/2/26 11:37
 * @Version 1.0
 **/
public class NetCatRuleEngine {

    final static String hostname = "localhost";

    final static int port = 9092;


    final static String templateRow = "{\"clienttoken\":\"token1\",\"state\":{\"status\":\"online\", \"value\":12},\"timestamp\"：1551422889}";

    /**
     *
     * @param args
     */
    public static void main(String[] args) throws Exception{
        System.out.println(templateRow);
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);



        final String sql = "SELECT clienttoken, state__status, state__value, `timestamp`"
                + " from tb_raw";

        System.out.println(sql);

        DataStreamSource<Rule> ruleStream = env.fromElements(
                    new Rule()
                        .withId("1")
                        .withTopicPattern("1")
                        .withRuleSQL(sql+ " WHERE v > 10")
                        .withTemplateRow(templateRow),

                    new Rule()
                        .withId("2")
                        .withTopicPattern("2")
                        .withTemplateRow(templateRow)
                        .withRuleSQL(sql+ " where v > 20")
        );
        //定义广播规则描述
        // a map descriptor to store the name of the rule (string) and the rule itself.
        MapStateDescriptor<String, Rule> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Rule>() {}));
        // broadcast the rules and create the broadcast state
        BroadcastStream<Rule> ruleBroadcastStream = ruleStream
                .broadcast(ruleStateDescriptor);

        //add source
        DataStream<String> stream = env.socketTextStream(hostname, port, "\n");

        KeyedStream<Raw, String> keyedStream = stream.filter(raw ->  !"".equals(raw) && !raw.contains("："))
                .map(raw -> {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode jsonNode = mapper.readTree(raw);
                    if(null != jsonNode.get("clienttoken")){
                        String key = jsonNode.get("clienttoken").asText();
                        String id = key.substring("token".length());
                        return Raw.of(id, raw);
                    }else{
                        return Raw.of("", raw);
                    }
                }).filter(t -> !"".equals(t.getKey()))
//                .returns(new TypeHint<Raw>(){})
//                .addSink(new PrintSinkFunction<Raw>());
                   .keyBy(raw -> raw.getKey());
//
//        keyedStream.addSink(new PrintSinkFunction<>());

        DataStream<RuleRaw> rrDataStream = keyedStream.connect(ruleBroadcastStream)
               .process(new KeyedBroadcastProcessFunction<String, Raw, Rule, RuleRaw>() {

                   @Override
                   public void processBroadcastElement(Rule value, Context ctx, Collector<RuleRaw> out) throws Exception {
                       //更新配置的规则
                        BroadcastState<String, Rule> state = ctx.getBroadcastState(ruleStateDescriptor);
                        state.put(value.getId(), value);
                   }


                   @Override
                   public void processElement(Raw value, ReadOnlyContext ctx, Collector<RuleRaw> out) throws Exception {
//                       this.getRuntimeContext().getMap
                       ReadOnlyBroadcastState<String, Rule> state = ctx.getBroadcastState(ruleStateDescriptor);
                       state.immutableEntries().forEach(entry -> {
                           Rule rule = entry.getValue();
                           //匹配规则
                           if(value.getKey().equals(rule.getTopicPattern())){
//                               out.collect(new RuleRaw().withRule(rule.getRuleSQL()).withRaw(value.getRaw()));

                               try {
                                   Class.forName("org.apache.calcite.jdbc.Driver");
                                   Properties info = new Properties();
                                   Connection connection =
                                           DriverManager.getConnection("jdbc:calcite:caseSensitive=false;lex=MYSQL", info);
                                   CalciteConnection calciteConnection =
                                           connection.unwrap(CalciteConnection.class);
                                   SchemaPlus rootSchema = calciteConnection.getRootSchema();

                                   Map<String, Object> operand = Maps.newHashMap();

                                   Queue<String> source = new LinkedList<>();
                                   source.offer(value.getRaw());

                                   operand.putIfAbsent("source", source);
                                   operand.putIfAbsent("name", "SENSOR");
                                   operand.putIfAbsent("flavor", null);

                                   Schema schema = JsonSchemaFactory.INSTANCE.create(rootSchema, "JSON", operand);
                                   rootSchema.add("JSON", schema);

                                   Statement statement = calciteConnection.createStatement();
                                   ResultSet resultSet = statement.executeQuery("SELECT * FROM JSON.SENSOR");

                                   if(resultSet != null){
                                       final StringBuilder buf = new StringBuilder();
                                       while (resultSet.next()) {
                                           int n = resultSet.getMetaData().getColumnCount();
                                           for (int i = 1; i <= n; i++) {
                                               buf.append(i > 1 ? "; " : "")
                                                       .append(resultSet.getMetaData().getColumnLabel(i))
                                                       .append("\t")
                                                       .append(resultSet.getObject(i));
                                           }
                                           System.out.println(buf.toString());
                                           out.collect(new RuleRaw().withRule(rule.getId()).withRaw(buf.toString()));
                                           buf.setLength(0);
                                       }
                                       resultSet.close();
                                   }else {
                                       System.err.println("resultSet is null!");
                                   }

                                   statement.close();
                                   connection.close();


                               }catch (Exception e){
                                   e.printStackTrace();
                               }
                           }
                       });
                   }
               });

        rrDataStream.addSink(new PrintSinkFunction<>());
        /*
        tableEnv.registerDataStream("tb_rule_raw", rrDataStream, "rule, raw");

        Table table = tableEnv.sqlQuery("select * from tb_rule_raw");

        CsvTableSink sink = new CsvTableSink(
                dstPath,
                "|",
                1,
                FileSystem.WriteMode.OVERWRITE);

        tableEnv.registerTableSink("csvOutputTable",
            new String[]{"f0", "f1"},
            new TypeInformation[]{Types.STRING(), Types.STRING()},
            sink
        );

        table.insertInto("csvOutputTable");
*/


        JobExecutionResult result = env.execute("blink-broadcast-table");

        System.out.println(result.getAllAccumulatorResults());
    }
}