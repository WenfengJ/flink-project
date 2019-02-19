package com.longyun.flink.udf.scala

import java.sql.{Date, Timestamp, Time}

import org.apache.flink.table.api.functions.TableFunction
import org.apache.flink.table.api.types.{DataType, DataTypes, RowType}
import org.apache.flink.table.runtime.functions.utils.JsonUtils
import org.apache.flink.types.Row

import scala.annotation.varargs
import scala.collection.mutable.ArrayBuffer

class JsonRow(val fieldTypeMap:Map[String, DataType]) extends TableFunction[Row] {

  @varargs
  def eval(jsonStr: String, paths: String*): Unit = {
    println("jsonStr="+jsonStr)
    val row = new Row(paths.length)
    for (i <- 0 until paths.length) {
      val value = JsonUtils.getInstance.getJsonObject(jsonStr, "$."+paths(i))
      val typeName = fieldTypeMap.getOrElse(paths(i), DataTypes.STRING)
      typeName match {
        case DataTypes.FLOAT => row.setField(i, java.lang.Float.parseFloat(value))
        case DataTypes.INT => row.setField(i, java.lang.Integer.parseInt(value))
        case DataTypes.DOUBLE => row.setField(i, java.lang.Double.parseDouble(value))
        case DataTypes.LONG => row.setField(i, java.lang.Long.parseLong(value))
        case DataTypes.DATE => {
          val v = java.lang.Long.parseLong(value)
          row.setField(i, new Date(v))
        }
        case DataTypes.TIMESTAMP => {
          val v = java.lang.Long.parseLong(value)
          row.setField(i, new Timestamp(v))
        }
        case DataTypes.TIME => {
          val v = java.lang.Long.parseLong(value)
          row.setField(i, new Time(v))
        }
        case _ => row.setField(i, value)
      }
    }

    collect(row)
  }

  override def getResultType(arguments: Array[AnyRef], argTypes: Array[Class[_]]): DataType = {
    import util.control.Breaks._
    val fields = new ArrayBuffer[String]()
    val types = new ArrayBuffer[DataType]()
    for(i <- 0 until argTypes.length){
      breakable{
        if(arguments(i) == null) {
          break
        }
        fields += (arguments(i).toString)
        types += fieldTypeMap.getOrElse((arguments(i).toString), DataTypes.STRING)
      }
    }

    new RowType(types.toArray, fields.toArray)
  }
}