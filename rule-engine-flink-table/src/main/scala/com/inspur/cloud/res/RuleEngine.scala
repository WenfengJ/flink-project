package com.inspur.cloud.res

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.descriptors.{Csv, Json, Kafka, Schema}
import org.apache.flink.table.sinks.{CsvTableSink, PrintTableSink}

import scala.collection.mutable.ArrayBuffer

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
object RuleEngine {

  val brokers = "res-spark-0001:9092,res-spark-0002:9092,res-spark-0003:9092"

  val sourceTopic = "iot-src"

  /**
    *
    * @param args
    */
  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //启用checkpoint
//    env.enableCheckpointing(5000)

//    val template: String = "{\"state\": \"online\",\"clienttoken\": \"client-27\",\"timestamp\": 1533709399480}"
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val descriptor = new Kafka().version("0.11")
        .topic(sourceTopic)
        .startFromEarliest()

//        .startFromLatest()
        .property("bootstrap.servers", brokers)
        .property("group.id", "res")
        .property("session.timeout.ms", "30000")
        .sinkPartitionerFixed()

    val tableDescriptor = tableEnv.connect(descriptor)
      .withFormat(new Json()
        .failOnMissingField(false)
        .deriveSchema())
      .withSchema(new Schema()
        .field("clienttoken", Types.STRING)
        .field("timestamp", Types.LONG)
        .field("state", Types.ROW(Array[String]("status", "value"), Array[TypeInformation[_]](Types.STRING, Types.INT)))
//          .field("state", Types.STRING)
      )
      .inAppendMode().registerTableSource("sensor")
    val table = tableEnv.sqlQuery("select clienttoken, `timestamp`, state from sensor where `value` is not null")

    println("--------------------table sensor schema--------------------")
    table.printSchema()


    tableEnv.registerTableSink("console",
      Array[String]("f0", "f1", "f2"),
      Array[TypeInformation[_]](Types.STRING,Types.LONG, Types.ROW(Types.STRING, Types.INT)),
      new PrintTableSink
    )

    table.insertInto("console")

    // execute program
    env.execute("Flink Rule Engine Application")
  }
}
