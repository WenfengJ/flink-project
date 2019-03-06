package com.longyun.flink.java.res;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;

/**
 * @author lynn
 * @ClassName com.longyun.flink.java.res.BroadcastRuleEngine
 * @Description TODO
 * @Date 19-3-6 上午11:24
 * @Version 1.0
 **/
public class BroadcastRuleEngine {
    final static String hostname = "localhost";

    final static int port = 9092;

    final static String brokers = "res-spark-0001:9092,res-spark-0002:9092,res-spark-0003:9092";

    final static String templateRow = "{\"clienttoken\":\"token1\",\"state\":{\"status\":\"online\", \"value\":12},\"timestamp\":1551422889}";

    final static String sql = "SELECT clienttoken, state__status, state__value, `timestamp`"
            + " from JSON.sensor";


    private static DataStreamSource<String> addKafkaSource(StreamExecutionEnvironment env, Properties props){
        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokers);
        properties.setProperty("group.id", props.getProperty("group.id"));
        properties.setProperty("session.timeout.ms", "30000");

        //add source
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>("iot-src", new SimpleStringSchema(), properties);
//        consumer.setStartFromEarliest();
        consumer.setStartFromLatest();

        return env.addSource(consumer);
    }

    private static Rule<String> initRule(int i){
        return new Rule()
                .withId(""+i)
                .withTopicPattern(""+i)
                .withRuleSQL(sql+ " WHERE state__value > "+i)
                .withTemplateRow(templateRow)
                .withOutputTag(new OutputTag<String>("tag"+i){});
    }

    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        if(null == args || args.length==0){
            System.err.println("Please input parameters...");
            System.exit(-1);
        }
        ParameterTool tool = ParameterTool.fromArgs(args);

        Properties props = tool.getProperties();

//        System.out.println(templateRow);
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Rule<String>> ruleStream = env.socketTextStream(hostname, port, "\n")
                .map(idx -> initRule(Integer.parseInt(idx)))
                .returns(new TypeHint<Rule<String>>(){});
        MapStateDescriptor<String, Rule> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Rule>() {}));
        // broadcast the rules and create the broadcast state
        BroadcastStream<Rule<String>> ruleBroadcastStream = ruleStream
                .broadcast(ruleStateDescriptor);


        SingleOutputStreamOperator<Raw> stream = addKafkaSource(env, props).filter(raw ->  !"".equals(raw) && !raw.contains("："))
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
                }).filter(t -> !"".equals(t.getKey()));

        SingleOutputStreamOperator<RuleRaw> ruledStream = stream.connect(ruleBroadcastStream)
                .process(new BroadcastProcessFunction<Raw, Rule<String>, RuleRaw>() {
            @Override
            public void processElement(Raw value, ReadOnlyContext ctx, Collector<RuleRaw> out) throws Exception {
                ReadOnlyBroadcastState<String, Rule> state = ctx.getBroadcastState(ruleStateDescriptor);
                state.immutableEntries().forEach(entry -> {
                    Rule rule = entry.getValue();
                    //匹配规则
                    if(value.getKey().equals(rule.getTopicPattern())){
                        System.out.println("match rule raw " + value);
                        out.collect(new RuleRaw().withRule(rule.toString()).withRaw(value.toString()));
                    }
                });
            }

            @Override
            public void processBroadcastElement(Rule<String> value, Context ctx, Collector<RuleRaw> out) throws Exception {
                //更新配置的规则
                BroadcastState<String, Rule> state = ctx.getBroadcastState(ruleStateDescriptor);
                state.put(value.getId(), value);
            }
        });

        ruledStream.addSink(new RuledSinks<>());
    }

    static class RuledSinks<RuleRaw> extends RichSinkFunction{

    }
}
