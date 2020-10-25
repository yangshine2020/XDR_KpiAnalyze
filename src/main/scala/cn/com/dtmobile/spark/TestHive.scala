package cn.com.dtmobile.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class TestHive
object TestHive {

  def main(args: Array[String]): Unit = {
    // Hive数据库
    val DDB:String = args(0)

    // 设置spark日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)


    val conf: SparkConf = new SparkConf()
      //      .setMaster("local[*]")
      .setAppName("testhive")

    val session: SparkSession = SparkSession.builder().config(conf)
      .enableHiveSupport()
      .getOrCreate()

    session.sql(
      s"""
        |create database if not exists ${DDB}
      """.stripMargin)
  }

}
