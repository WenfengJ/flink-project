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

package com.inspur.cloud.res

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment

import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSONObject

/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object RuleEngineOld {

  val brokers = "res-spark-0001:9092,res-spark-0002:9092,res-spark-0003:9092"

  val sourceTopic = "iot-src"

  /**
    *
    * @param args
    */
  def main(args: Array[String]) {

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", brokers)
    properties.setProperty("group.id", "flink")
    properties.setProperty("session.timeout.ms", "30000")
//    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    //Partition discovery
    properties.put("flink.partition-discovery.interval-millis", "30000")

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //启用checkpoint
    env.enableCheckpointing(5000)

//    val template: String = "{\"state\": {\"reported\": {\"temperature\": 73.09}},\"clientToken\": \"client-27\",\"timeStamp\": 1533709399480}"

    val template: String = "{\"state\": \"online\",\"clientToken\": \"client-27\",\"timestamp\": 1533709399480, \"ruleId\":\"0\"}"

    val option = scala.util.parsing.json.JSON.parseFull(template)
    println(option.get.getClass)
//    scala.util.parsing.json.JSONObject.apply((Map)(option.getOr))

    val json = com.alibaba.fastjson.JSON.parseObject(template)

    val keys = json.keySet()
    val fields = new Array[String](keys.size())
    keys.toArray(fields)

    // get StreamTableEnvironment
    // registration of a DataSet in a BatchTableEnvironment is equivalent
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
    val stream: DataStream[String] = env.addSource(
      new FlinkKafkaConsumer010[String](sourceTopic,
        new SimpleStringSchema(), properties)).map(msg => {
      val json = com.alibaba.fastjson.JSON.parseObject(msg)
      json.toJSONString
    })

    val s2 = stream.map(msg => {
      val json = com.alibaba.fastjson.JSON.parseObject(msg)
      val values = new ArrayBuffer[Any](fields.length)
      for(i<- 0 until fields.length){
        values(i) = json.get(fields(i))
      }
      values
    })

    // register the DataStream as Table "sensor" with fields
    tableEnv.registerDataStream("sensor", s2)

    val table = tableEnv.sqlQuery("select * from sensor")


    println("--------------------table schema--------------------")
    table.printSchema()

    // execute program
    env.execute("Flink Rule Engine Application")
  }
}
