package com.inspur.cloud.res

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.descriptors.{Csv, Json, Kafka, Schema}
import org.apache.flink.table.sinks.CsvTableSink

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

  val dstPath = "/tmp/flink-sinks/flat-json.csv"
  val dstPath2 = "/tmp/flink-sinks/flat-json2.csv"

  /**
    *
    * @param args
    */
  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //启用checkpoint
    env.enableCheckpointing(5000)

//    val template: String = "{\"state\": {\"reported\": {\"temperature\": 73.09}},\"clientToken\": \"client-27\",\"timestamp\": 1533709399480}"
    val template: String = "{\"state\": \"online\",\"clientToken\": \"client-27\",\"timestamp\": 1533709399480}"

//    val option = scala.util.parsing.json.JSON.parseFull(template)
//    println(option.get.getClass)
    //    scala.util.parsing.json.JSONObject.apply((Map)(option.getOr))

    val json = com.alibaba.fastjson.JSON.parseObject(template)

    val keys = json.keySet()
    val fields = new Array[String](keys.size())
    keys.toArray(fields)

    // get StreamTableEnvironment
    // registration of a DataSet in a BatchTableEnvironment is equivalent
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val descriptor = new Kafka().version("0.10")
        .topic(sourceTopic)
//        .startFromEarliest()
        .startFromLatest()
        .property("bootstrap.servers", brokers)
        .property("group.id", "flink")
        .property("session.timeout.ms", "30000")
        .property("flink.partition-discovery.interval-millis", "30000") //Partition discovery

    val typeInformations = Array[TypeInformation[_]](Types.STRING, Types.STRING, Types.SQL_TIMESTAMP)

    val schema = new Schema()
    for(i<-0 until fields.length){
      schema.field(fields(i), typeInformations(i))
    }

    val tableDescriptor = tableEnv.connect(descriptor)
      .withFormat(new Json().deriveSchema())
      .withSchema(schema)
      .inAppendMode().registerTableSource("sensor")
    val table = tableEnv.sqlQuery("select * from sensor")

    println("--------------------table sensor schema--------------------")
    table.printSchema()

    val sink: CsvTableSink = new CsvTableSink(
      dstPath,                             // output path
      fieldDelim = "|",                 // optional: delimit files by '|'
      numFiles = 1,                     // optional: write to a single file
      writeMode = WriteMode.OVERWRITE)  // optional: override existing files

    tableEnv.registerTableSink("csvOutputTable",
      Array[String]("f0", "f1", "f2"),
      Array[TypeInformation[_]](Types.STRING, Types.STRING, Types.SQL_TIMESTAMP),
      sink
    )

    tableEnv.registerTableSink("csvOutputTable2",
      Array[String]("f0", "f1"),
      Array[TypeInformation[_]](Types.STRING, Types.STRING),
      new CsvTableSink(
        dstPath2,                             // output path
        fieldDelim = "|",                 // optional: delimit files by '|'
        numFiles = 1,                     // optional: write to a single file
        writeMode = WriteMode.OVERWRITE)  // optional: override existing files
    )

    table.insertInto("csvOutputTable")

    val table2 = tableEnv.sqlQuery("select clientToken, state from sensor")
    table2.insertInto("csvOutputTable2")
    // execute program
    env.execute("Flink Rule Engine Application")
  }
}
