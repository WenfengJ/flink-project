package com.longyun.flink.java.res;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.longyun.flink.java.res.event.AddEvent;
import com.longyun.flink.java.res.event.Event;
import com.longyun.flink.java.res.event.EventListener;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.Properties;

/**
 * @author lynn
 * @ClassName com.longyun.flink.java.res.EventDrivedRuleEngine
 * @Description TODO
 * @Date 19-3-5 下午6:50
 * @Version 1.0
 **/
public class EventDrivedRuleEngine {

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

    private static RuleContainer<String, Void> initRuleContainer(){
        RuleContainer<String, Void> container = new RuleContainer<>();
        return container;
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
    public static void main(String[] args) throws Exception{

        if(null == args || args.length==0){
            System.err.println("Please input parameters...");
            System.exit(-1);
        }
        ParameterTool tool = ParameterTool.fromArgs(args);

        Properties props = tool.getProperties();

//        System.out.println(templateRow);
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(1);

        KeyedStream<Raw, String> stream = addKafkaSource(env, props)
                .filter(raw ->  !"".equals(raw) && !raw.contains("："))
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
                .keyBy(raw -> raw.getKey());

        //add source
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

        final OutputTag<Rule<String>> tag = new OutputTag<Rule<String>>("rule-tag"){};

        SingleOutputStreamOperator<String> ruledStream =
            stream.connect(ruleBroadcastStream).process(new KeyedBroadcastProcessFunction<String, Raw, Rule<String>, String>() {
                @Override
                public void processElement(Raw value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                    ReadOnlyBroadcastState<String, Rule> state = ctx.getBroadcastState(ruleStateDescriptor);
                    state.immutableEntries().forEach(entry -> {
                        Rule rule = entry.getValue();
                        //匹配规则
                        if(value.getKey().equals(rule.getTopicPattern())){
                            System.out.println("match rule raw " + value);
                            ctx.output(rule.getOutputTag(), new RuleRaw()
                                    .withRule(rule.getId())
                                    .withRaw(value.toString()).toString());
                        }
                    });
                }

                @Override
                public void processBroadcastElement(Rule<String> value, Context ctx, Collector<String> out) throws Exception {
                    //更新配置的规则
                    BroadcastState<String, Rule> state = ctx.getBroadcastState(ruleStateDescriptor);
                    System.out.println("processBroadcastElement rule.id : " + value.getId());
                    state.put(value.getId(), value);
                    ctx.output(tag, value);
                }
            });

        RuleContainer<String, Void> container = new RuleContainer<>();
        container.addListener(new RuleChangedListener<Void>(ruledStream));

        //Caused by: java.io.NotSerializableException: org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
        ruledStream.getSideOutput(tag).process(new RuleHandler(container));

        env.execute("rule engine");
    }

    static class RuleHandler extends ProcessFunction<Rule<String>, Void> implements Serializable{

        final RuleContainer<String, Void> container;

        public RuleHandler(RuleContainer<String, Void> container) {
            this.container = container;
        }

        @Override
        public void processElement(Rule<String> value, Context ctx, Collector<Void> out) throws Exception {
            System.out.println(value);
            container.add(value);
        }
    }
    static class RuleChangedListener<Void> implements EventListener, Serializable{
        final SingleOutputStreamOperator<String> streamOperator;

        public RuleChangedListener(SingleOutputStreamOperator<String> streamOperator) {
            this.streamOperator = streamOperator;
        }

        @Override
        public Void handle(Event event) {
            Rule<RuleRaw> rule = (Rule<RuleRaw>)event.getElem();
            System.out.println("trigger event handler" + rule.getId());
            if(event instanceof AddEvent){
                System.out.println("rule added" + rule.getId());
                streamOperator.getSideOutput(rule.getOutputTag()).addSink(new PrintSinkFunction<>());
            }else{

            }

            return null;
        }
    }
}
