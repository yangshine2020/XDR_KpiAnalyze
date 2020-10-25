package cn.com.dtmobile.kpi

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

class S1mmeKpiDayAnalyze
object S1mmeKpiDayAnalyze {

  def main(args: Array[String]): Unit = {
    // PG库IP
    val pg_ip:String = args(0)
    // PG库端口
    val pg_port:Int = args(1).toInt
//    // Hive数据库
//    val DDB:String = args(2)
//
//    // 城市分区
//    val p_city:String = args(3)
//
//    // 小时分区
//    val p_hour:String = args(4)

    // 设置spark日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val conf: SparkConf = new SparkConf()
      //      .setMaster("local[*]")
      .setAppName("kpiAnalyze")
    // 设置shuffle产生的分区为1
//    conf.set("spark.sql.shuffle.partitions","1")

    val session: SparkSession = SparkSession.builder().config(conf)
      //      .enableHiveSupport()
      .getOrCreate()

    val prop = new Properties()
    // 用户名
    prop.setProperty("user","postgres")
    // 密码
    prop.setProperty("password","postgres")

    // 连接JDBC读取pg库
    val test: DataFrame = session.read.jdbc(s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e",s"public.xdr_4g_enb_cell_access_h",prop)

    test.createOrReplaceTempView("xdr_4g_enb_cell_access_h_tmp")

    session.sql(
      """
        |select
        |cgi,
        |null as mme_ems_id,
        |cast('2020-07-14 00:00:00' as timestamp) as time,
        |SUM(kpi_0100_001) kpi_0100_001,
        |SUM(kpi_0100_002) kpi_0100_002,
        |SUM(kpi_0100_003) kpi_0100_003,
        |SUM(kpi_0100_004) kpi_0100_004,
        |SUM(kpi_0100_005) kpi_0100_005,
        |SUM(kpi_0100_006) kpi_0100_006,
        |SUM(kpi_0100_007) kpi_0100_007,
        |SUM(kpi_0101_001) kpi_0101_001,
        |SUM(kpi_0101_002) kpi_0101_002,
        |SUM(kpi_0101_003) kpi_0101_003,
        |SUM(kpi_0102_001) kpi_0102_001,
        |SUM(kpi_0102_002) kpi_0102_002,
        |SUM(kpi_0102_003) kpi_0102_003,
        |from xdr_4g_enb_cell_access_h_tmp
        |group by
        |cgi
      """.stripMargin).write.mode("append").format("jdbc")
      .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
      .option("dbtable",s"public.xdr_4g_enb_cell_access_d")
      .option("user", "postgres")
      .option("password", "postgres")
      .save()
    println(s"write to public.xdr_4g_enb_cell_access_d finish")

  }
}
