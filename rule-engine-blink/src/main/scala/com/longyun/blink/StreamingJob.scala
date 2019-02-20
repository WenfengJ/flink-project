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

package com.longyun.blink

import com.longyun.blink.scala.udf.JsonRow
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.sinks.csv.CsvTableSink
import org.apache.flink.table.sources.csv.CsvTableSource

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
object StreamingJob {

  val fieldTypes:Map[String, DataType] = Map("clienttoken" -> DataTypes.STRING,
    "timestamp" -> DataTypes.LONG,
    "state.status" -> DataTypes.STRING,
    "state.value" -> DataTypes.FLOAT)

  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    // execute program
//    env.execute("Flink Streaming Scala API Skeleton")

    val tableEnv = TableEnvironment.getTableEnvironment(env)
    /*
    val csvSource: TableSource = new CsvTableSource("data/msg.json",
      Array("msg"),
      Array(DataTypes.STRING),
      Array(false),
      "\t",
      "\n",
      '\'',
      false,
      "#",
      false,
      "UTF-8",
      false
    )
    */
    val csvSource = CsvTableSource.builder()
      .path("data/msg.json")
      .field("msg", DataTypes.STRING)
      .fieldDelimiter("\t")
      .lineDelimiter("\n")
      .commentPrefix("#")
      .ignoreParseErrors()
      .charset("utf-8")
        .build()


    tableEnv.registerTableSource("view_msg", csvSource)


    tableEnv.registerFunction("ly_json_row", new JsonRow(fieldTypes))
    //    tableEnv.registerFunction("ly_json_row", new JsonRow)
//    import scala.collection.JavaConversions._
//    tableEnv.registerFunction("ly_json_row", new com.longyun.flink.java.udf.JsonRow(fieldTypes))

    val result = tableEnv.sqlQuery("SELECT `ts`,ct,ss,v " +
      "FROM view_msg " +
      "LEFT JOIN LATERAL TABLE(ly_json_row(msg, 'timestamp', 'clienttoken', 'state.status', 'state.value')) as T1(`ts`,ct,ss, v) ON TRUE " +
      "WHERE v > 50 ")

    val csvSink = new CsvTableSink("data/sink/g_"+System.currentTimeMillis()+".csv")

    /*
    tableEnv.registerTableSink("CsvSinkTable",
      Array("msg"),
      Array(DataTypes.STRING),
      csvSink)
    */
    result.writeToSink(csvSink)

    val result2 = tableEnv.sqlQuery("SELECT `ts`,ct,ss,v " +
      "FROM view_msg " +
      "LEFT JOIN LATERAL TABLE(ly_json_row(msg, 'timestamp', 'clienttoken', 'state.status', 'state.value')) as T1(`ts`,ct,ss, v) ON TRUE " +
      "WHERE v < 50 ")

    val csvSink2 = new CsvTableSink("data/sink/l_"+System.currentTimeMillis()+".csv")

    result2.writeToSink(csvSink2)
    tableEnv.execute()
  }
}
