package com.longyun.flink.java.res;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.longyun.calcite.json.JsonSchemaFactory;
import com.longyun.calcite.json.MemorySource;
import com.longyun.flink.java.nc.NetCatRuleEngine;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author yuanxiaolong
 * @ClassName com.longyun.blink.java.KeyedBroadcastRuleEngine
 * @Description TODO
 * @Date 2019/2/26 11:37
 * @Version 1.0
 **/
public class KeyedBroadcastRuleEngine {

    final static String  dstPath = "/tmp/flink-sinks/flat-json.csv";

    final static String brokers = "res-spark-0001:9092,res-spark-0002:9092,res-spark-0003:9092";


    final static String templateRow = "{\"clienttoken\":\"token1\",\"state\":{\"status\":\"online\", \"value\":12},\"timestamp\":1551422889}";

    /**
     *
     * @param args
     */
    public static void main(String[] args) throws Exception{
        if(null == args || args.length==0){
            System.err.println("Please input parameters...");
            System.exit(-1);
        }
        ParameterTool tool = ParameterTool.fromArgs(args);

        Properties paraProps = tool.getProperties();

//        System.out.println(templateRow);
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(1);

        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokers);
        properties.setProperty("group.id", paraProps.getProperty("group.id"));
        properties.setProperty("session.timeout.ms", "30000");

        final String sql = "SELECT clienttoken, state__status, state__value, `timestamp`"
                + " from JSON.sensor";

//        System.out.println(sql);

        List<Rule> rules = new ArrayList<>();
        for (int i = 0; i < Integer.parseInt(paraProps.getProperty("rules.num")); i++) {
            rules.add(new Rule()
                    .withId(""+i)
                    .withTopicPattern(""+i)
                    .withRuleSQL(sql+ " WHERE state__value > "+i)
                    .withTemplateRow(templateRow));
        }
        DataStreamSource<Rule> ruleStream = env.fromCollection(rules);
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
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>("iot-src", new SimpleStringSchema(), properties);
//        consumer.setStartFromEarliest();
        consumer.setStartFromLatest();

        DataStreamSource<String> stream = env.addSource(consumer);

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
               .process(new RuleProcessFunction());

        rrDataStream.addSink(new PrintSinkFunction<>());

        rrDataStream.addSink(StreamingFileSink.forRowFormat(new Path(paraProps.getProperty("fs.sink.path")), new SimpleStringEncoder()).build());


        JobExecutionResult result = env.execute("blink-broadcast-table");

        System.out.println("AllAccumulatorResults" + result.getAllAccumulatorResults());
    }

    static class CalciteSchema{
        CalciteConnection connection;
        MemorySource<String> source;

        protected CalciteSchema(CalciteConnection connection, MemorySource<String> source) {
            this.connection = connection;
            this.source = source;
        }
    }

    static class RuleProcessFunction extends KeyedBroadcastProcessFunction<String, Raw, Rule, RuleRaw>{

        //rule.id -> connection
        private final Map<String, CalciteSchema> calciteSchemaMap;
        private static final MapStateDescriptor<String, Rule> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Rule>() {}));
        public RuleProcessFunction() {
            this(Maps.newHashMap());
        }

        public RuleProcessFunction(Map<String, CalciteSchema> calciteSchemaMap) {
            this.calciteSchemaMap = calciteSchemaMap;
        }

        @Override
        public void processBroadcastElement(Rule value, Context ctx, Collector<RuleRaw> out) throws Exception {
            //更新配置的规则
            BroadcastState<String, Rule> state = ctx.getBroadcastState(ruleStateDescriptor);
            state.put(value.getId(), value);
        }

        @Override
        public void processElement(Raw value, ReadOnlyContext ctx, Collector<RuleRaw> out) throws Exception {
//            System.out.println(value.getRaw());
            ReadOnlyBroadcastState<String, Rule> state = ctx.getBroadcastState(ruleStateDescriptor);
            state.immutableEntries().forEach(entry -> {
                Rule rule = entry.getValue();
                //匹配规则
                if(value.getKey().equals(rule.getTopicPattern())){
                    CalciteSchema schema = getCalciteSchema(rule);
                    schema.source.offer("sensor", value.getRaw());
                    try{
                        Statement statement = schema.connection.createStatement();
                        ResultSet resultSet = statement.executeQuery(rule.getRuleSQL());

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
                                out.collect(new RuleRaw().withRule(rule.getId()).withRaw(buf.toString()));
                                buf.setLength(0);
                            }
                            resultSet.close();
                        }else {
                            System.err.println("resultSet is null!");
                        }

                        statement.close();
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
            });
        }

        /**
         *
         * @param
         * @return
         */
        private CalciteSchema getCalciteSchema(Rule rule){
//            System.out.println("------------getCalciteSchema------------");
            if(this.calciteSchemaMap.containsKey(rule.getId())){
                return calciteSchemaMap.get(rule.getId());
            }else{
//                System.out.println("------------create net connection------------");
                try {
                    Class.forName("org.apache.calcite.jdbc.Driver");
                    Properties info = new Properties();
                    Connection connection =
                            DriverManager.getConnection("jdbc:calcite:caseSensitive=false;lex=MYSQL", info);
                    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

                    SchemaPlus rootSchema = calciteConnection.getRootSchema();

                    Map<String, Object> operand = Maps.newHashMap();

                    MemorySource<String> source = new MemorySource<>();

                    Map<String, String> tblTplMap = Maps.newHashMap();
                    tblTplMap.put("sensor", rule.getTemplateRow());

                    operand.putIfAbsent("source", source);
                    operand.putIfAbsent("tbl-tpl", tblTplMap);

                    Schema schema = JsonSchemaFactory.INSTANCE.create(rootSchema, "JSON", operand);
                    rootSchema.add("JSON", schema);

                    CalciteSchema calciteSchema = new CalciteSchema(calciteConnection, source);
                    this.calciteSchemaMap.put(rule.getId(), calciteSchema);

                    return calciteSchema;
                }catch (Exception e){
                    e.printStackTrace();
                }

                return null;
            }
        }
    }
}