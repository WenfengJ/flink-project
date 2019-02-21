/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.longyun.flink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Properties;
import java.util.Set;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class RuleEngine {

    final static String brokers = "res-spark-0001:9092,res-spark-0002:9092,res-spark-0003:9092";

    /**
     *
     * @param args
     * @throws Exception
     */
	public static void main(String[] args) throws Exception {
	   //1. set kafka consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokers);
        properties.setProperty("group.id", "flink");
        properties.setProperty("session.timeout.ms", "30000");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //Partition discovery
        properties.setProperty("flink.partition-discovery.interval-millis", "30000");

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //启用checkpoint
        env.enableCheckpointing(5000);

        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer010<String>("iot-src", new SimpleStringSchema(), properties));

        String template = "{\"state\": {\"reported\": {\"temperature\": 73.09}},\"clientToken\": \"client-27\",\"timeStamp\": 1533709399480}";

        JSONObject json = JSONObject.parseObject(template);
        Set<String>  keys = json.keySet();
        String[] fieldNames = new String[keys.size()];
        keys.toArray(fieldNames);

        // get StreamTableEnvironment
        // registration of a DataSet in a BatchTableEnvironment is equivalent
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);


        // register the DataStream as Table "myTable" with fields
        tableEnv.registerDataStream("sensor", stream);

        Table table = tableEnv.sqlQuery("select * from sensor");
//        Table table = tableEnv.fromDataStream(stream);
//        TableSchema schema = table.getSchema();
        System.out.println("--------------------table schema--------------------");
        table.printSchema();



		// execute program
		env.execute("Flink Rule Engine");
	}
}
