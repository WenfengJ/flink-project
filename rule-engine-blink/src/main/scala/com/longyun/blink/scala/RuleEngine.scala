package com.longyun.blink.scala

import java.util.TimeZone

import com.longyun.blink.scala.udf.JsonRow
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.descriptors._
import org.apache.flink.table.sinks.PrintTableSink
import org.apache.flink.table.sinks.csv.CsvTableSink

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

  val dstPath = "flink-sinks/"

  val ruleNum = 2

  val fieldTypes:Map[String, DataType] = Map("clienttoken" -> DataTypes.STRING,
    "timestamp" -> DataTypes.LONG,
    "state.status" -> DataTypes.STRING,
    "state.value" -> DataTypes.FLOAT)

  /**
    *
    * @param args
    */
  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //启用checkpoint
//    env.enableCheckpointing(5000)
    env.setParallelism(1)
//    val template: String = "{\"state\": {\"reported\": {\"temperature\": 73.09}},\"clientToken\": \"client-27\",\"timestamp\": 1533709399480}"
    val template: String = "{\"state\": \"online\",\"clienttoken\": \"client-27\",\"timestamp\": 1533709399480}"

    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val descriptor = new Kafka().version("0.11")
        .topic(sourceTopic)
        .startFromEarliest()
//        .startFromLatest()
        .property("bootstrap.servers", brokers)
        .property("group.id", "blink")
        .property("session.timeout.ms", "30000")
//        .property("flink.partition-discovery.interval-millis", "30000") //Partition discovery

    val tableDescriptor = tableEnv.connect(descriptor)
//      .withFormat(new Json()
//        .failOnMissingField(false)
//        .deriveSchema())
//      .withSchema(new Schema()
//        .field("clienttoken", Types.STRING)
//        .field("status", Types.STRING)
//        .field("timestamp", Types.SQL_TIMESTAMP))
      .withFormat(new RawString()
        .characterEncoding("UTF-8")
        .deriveSchema())
      .withSchema(new Schema().field("raw", Types.STRING))
      .inAppendMode().registerTableSource("tb_raw")

    val table = tableEnv.sqlQuery("select raw from tb_raw where raw is not null and raw <>''")
    /*
    tableEnv.registerFunction("ly_json_row", new JsonRow(fieldTypes))
    val table = tableEnv.sqlQuery("SELECT `ts`,ct,ss,v " +
      "FROM tb_raw " +
      "LEFT JOIN LATERAL TABLE(ly_json_row(raw, 'timestamp', 'clienttoken', 'state.status', 'state.value')) as T1(`ts`,ct,ss, v) ON TRUE " +
      "WHERE v > 50 ")
*/
    tableEnv.registerTable(s"tb_filtered", table)
    for(i <- 1 to ruleNum){
      tableEnv.registerFunction(s"ly_json_row_$i", new JsonRow(fieldTypes))

      val sql = "SELECT raw, ct " +
        "FROM tb_filtered " +
        s"LEFT JOIN LATERAL TABLE(ly_json_row_$i(raw, 'clienttoken')) as T1(ct) ON TRUE " +
       s"WHERE ct='token$i'"
      println(sql)
      val table = tableEnv.sqlQuery(sql)

      tableEnv.registerTable(s"tb_mid_$i", table)
//      table.insertInto(s"tb_mid_$i")

      val rule = "SELECT `ts`,json.ct,ss,v " +
        s"FROM tb_mid_$i t " +
        s"LEFT JOIN LATERAL TABLE(ly_json_row_$i(raw, 'timestamp', 'clienttoken', 'state.status', 'state.value')) as json(`ts`,ct,ss, v) ON TRUE " +
      s"WHERE v <> $i"

      val _table = tableEnv.sqlQuery(rule)
      println(s"--------------------table rule $i schema--------------------")
      _table.printSchema()

//      _table.writeToSink(new PrintTableSink(TimeZone.getDefault))

      _table.writeToSink(new CsvTableSink(s"$dstPath/rule-$i.csv",
        fieldDelim = "|",
        numFiles = 1,
        writeMode = WriteMode.OVERWRITE))
    }

//    println("--------------------table tb_raw schema--------------------")
//    table.printSchema()
//    table.writeToSink(new PrintTableSink(TimeZone.getDefault))

    /*
    val csvTableSink = new CsvTableSink(dstPath,
      fieldDelim = "|",
      numFiles = 1,
      writeMode = WriteMode.OVERWRITE)
    table.writeToSink(csvTableSink)
    */
    /*
    val printTblSink = new PrintTableSink(TimeZone.getDefault)
    tableEnv.registerTableSink("csvOutputTable",
//      Array[String]("f0", "f1", "f2", "f3"),
//      Array[DataType](DataTypes.LONG, DataTypes.STRING, DataTypes.STRING, DataTypes.FLOAT),
      Array[String]("f0"),
      Array[DataType](DataTypes.STRING),
      printTblSink
    )
    table.insertInto("csvOutputTable")*/

    // execute program
    env.execute("Blink Rule Engine Application")
  }
}
