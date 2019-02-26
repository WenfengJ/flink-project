package com.longyun.blink.java.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.longyun.blink.java.udf.JsonRow;
import groovy.lang.Tuple;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.sinks.PrintTableSink;
import org.apache.flink.util.Collector;
import scala.collection.immutable.Seq;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

/**
 * @author yuanxiaolong
 * @ClassName com.longyun.blink.java.kafka.KeyedBroadcastRuleEngine
 * @Description TODO
 * @Date 2019/2/26 11:37
 * @Version 1.0
 **/
public class KeyedBroadcastRuleEngine {

    final static String brokers = "res-spark-0001:9092,res-spark-0002:9092,res-spark-0003:9092";

    /**
     *
     * @param args
     */
    public static void main(String[] args) throws Exception{
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        final StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokers);
        properties.setProperty("group.id", "flink");
        properties.setProperty("session.timeout.ms", "30000");

        final String sql = "SELECT `ts`,ct,ss,v"
                + " from tb_raw"
                + " LEFT JOIN LATERAL TABLE(ly_json_row(raw, 'timestamp', 'clienttoken', 'state.status', 'state.value')) as T1(`ts`,ct,ss, v) ON TRUE";

        DataStreamSource<Rule> ruleStream = env.fromElements(
                    new Rule()
                        .withId("1")
                        .withTopicPattern("1")
                        .withRuleSQL(sql+ " WHERE v > 10")
                        .addFieldTypes("clienttoken", DataTypes.STRING)
                        .addFieldTypes("timestamp", DataTypes.LONG)
                            .addFieldTypes("state.status", DataTypes.STRING)
                            .addFieldTypes("state.value", DataTypes.FLOAT),
                new Rule()
                        .withId("2")
                        .withTopicPattern("2")
                        .withRuleSQL(sql+ " where v > 20")
                        .addFieldTypes("clienttoken", DataTypes.STRING)
                        .addFieldTypes("timestamp", DataTypes.LONG)
                        .addFieldTypes("state.status", DataTypes.STRING)
                        .addFieldTypes("state.value", DataTypes.FLOAT)
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
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>("iot-src", new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();

        DataStreamSource<String> stream = env.addSource(consumer);

       KeyedStream<Raw, String> keyedStream = stream.filter(raw ->  !"".equals(raw) && raw.length() >= 93 &&  raw.length() <=  94)
                .map(raw -> {
                    JSONObject jsonObject = JSON.parseObject(raw);
                    if(jsonObject.containsKey("clienttoken")){
                        String key = jsonObject.getString("clienttoken");
                        String id = key.substring("token".length());
                        return Raw.of(id, raw);
                    }else{
                        return Raw.of("", raw);
                    }
                }).filter(t -> !"".equals(t.getKey()))
//                .returns(new TypeHint<Raw>(){})
//                .addSink(new PrintSinkFunction<Raw>());
                   .keyBy(raw -> raw.getKey());

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
                               out.collect(new RuleRaw().withRule(rule.getRuleSQL()).withRaw(value.getRaw()));
                           }
                       });
                   }
               });

        tableEnv.registerDataStream("tb_rule_raw", rrDataStream, "rule, raw");

        tableEnv.sqlQuery("select rule, raw from tb_rule_raw")
                .writeToSink(new PrintTableSink(TimeZone.getDefault()));

//        Table rules = tableEnv.sqlQuery("select distinct rule from tb_rule_raw");
//        rules.writeToSink(new PrintTableSink(TimeZone.getDefault()));

        Map<String, DataType> fieldTypes = Maps.newTreeMap();
        fieldTypes.putIfAbsent("clienttoken", DataTypes.STRING);
        fieldTypes.putIfAbsent("timestamp", DataTypes.LONG);
        fieldTypes.putIfAbsent("state.status", DataTypes.STRING);
        fieldTypes.putIfAbsent("state.value", DataTypes.FLOAT);

        tableEnv.registerFunction("ly_json_row", new JsonRow(fieldTypes));

        env.execute("blink-broadcast-table");
    }
}