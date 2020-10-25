package cn.com.dtmobile.kpi

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

class S1uKpiDayAnalyze
object S1uKpiDayAnalyze {

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
    val test: DataFrame = session.read.jdbc(s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e",s"public.xdr_4g_enb_cell_s1u_h",prop)

    test.createOrReplaceTempView("xdr_4g_enb_cell_s1u_h_tmp")


    session.sql(
      """select
        |cgi,
        |null as equip_ems_id,
        |null as qci,
        |sgw_ip,
        |cast('2020-07-14 00:00:00' as timestamp) as time,
        |SUM(kpi_0200_001) kpi_0200_001,
        |SUM(kpi_0200_002) kpi_0200_002,
        |SUM(kpi_0200_003) kpi_0200_003,
        |SUM(kpi_0200_004) kpi_0200_004,
        |SUM(kpi_0201_001) kpi_0201_001,
        |SUM(kpi_0201_002) kpi_0201_002,
        |SUM(kpi_0201_003) kpi_0201_003,
        |SUM(kpi_0201_004) kpi_0201_004,
        |SUM(kpi_0202_001) kpi_0202_001,
        |SUM(kpi_0202_002) kpi_0202_002,
        |SUM(kpi_0202_003) kpi_0202_003,
        |round(AVG(kpi_0203_001)) kpi_0203_001,
        |SUM(kpi_0203_002) kpi_0203_002,
        |SUM(kpi_0203_003) kpi_0203_003,
        |SUM(kpi_0203_004) kpi_0203_004,
        |SUM(kpi_0203_005) kpi_0203_005,
        |SUM(kpi_0204_001) kpi_0204_001,
        |SUM(kpi_0204_002) kpi_0204_002,
        |SUM(kpi_0204_003) kpi_0204_003,
        |SUM(kpi_0204_004) kpi_0204_004,
        |SUM(kpi_0204_005) kpi_0204_005,
        |SUM(kpi_0205_001) kpi_0205_001,
        |SUM(kpi_0205_002) kpi_0205_002,
        |SUM(kpi_0205_003) kpi_0205_003,
        |SUM(kpi_0205_004) kpi_0205_004,
        |SUM(kpi_0205_005) kpi_0205_005,
        |SUM(kpi_0206_001) kpi_0206_001,
        |SUM(kpi_0206_002) kpi_0206_002,
        |SUM(kpi_0206_003) kpi_0206_003,
        |SUM(kpi_0206_004) kpi_0206_004,
        |SUM(kpi_0206_005) kpi_0206_005,
        |SUM(kpi_0206_006) kpi_0206_006,
        |SUM(kpi_0207_001) kpi_0207_001,
        |SUM(kpi_0207_002) kpi_0207_002,
        |SUM(kpi_0207_003) kpi_0207_003,
        |SUM(kpi_0207_004) kpi_0207_004,
        |SUM(kpi_0207_005) kpi_0207_005,
        |SUM(kpi_0208_001) kpi_0208_001,
        |SUM(kpi_0208_002) kpi_0208_002,
        |SUM(kpi_0208_003) kpi_0208_003,
        |SUM(kpi_0208_004) kpi_0208_004,
        |round(AVG(kpi_0209_001)) kpi_0209_001,
        |SUM(kpi_0209_002) kpi_0209_002,
        |SUM(kpi_0209_003) kpi_0209_003,
        |SUM(kpi_0209_004) kpi_0209_004,
        |SUM(kpi_0209_005) kpi_0209_005,
        |SUM(kpi_0209_006) kpi_0209_006,
        |SUM(kpi_0209_007) kpi_0209_007,
        |SUM(kpi_0209_008) kpi_0209_008,
        |SUM(kpi_0209_009) kpi_0209_009,
        |SUM(kpi_0209_010) kpi_0209_010,
        |SUM(kpi_0209_011) kpi_0209_011,
        |SUM(kpi_0209_012) kpi_0209_012,
        |SUM(kpi_0209_013) kpi_0209_013,
        |SUM(kpi_0209_014) kpi_0209_014,
        |SUM(kpi_0209_015) kpi_0209_015,
        |SUM(kpi_0209_016) kpi_0209_016,
        |SUM(kpi_0209_017) kpi_0209_017,
        |SUM(kpi_0209_018) kpi_0209_018,
        |round(AVG(kpi_0210_001)) kpi_0210_001,
        |SUM(kpi_0210_002) kpi_0210_002,
        |SUM(kpi_0210_003) kpi_0210_003,
        |SUM(kpi_0210_004) kpi_0210_004,
        |SUM(kpi_0210_005) kpi_0210_005,
        |SUM(kpi_0210_006) kpi_0210_006,
        |SUM(kpi_0210_007) kpi_0210_007,
        |SUM(kpi_0210_008) kpi_0210_008,
        |SUM(kpi_0210_009) kpi_0210_009,
        |MAX(kpi_0210_010) kpi_0210_010,
        |MIN(kpi_0210_011) kpi_0210_011,
        |MAX(kpi_0210_012) kpi_0210_012,
        |MIN(kpi_0210_013) kpi_0210_013,
        |SUM(kpi_0210_014) kpi_0210_014,
        |SUM(kpi_0210_015) kpi_0210_015,
        |SUM(kpi_0210_016) kpi_0210_016,
        |SUM(kpi_0210_017) kpi_0210_017,
        |SUM(kpi_0210_018) kpi_0210_018,
        |SUM(kpi_0210_019) kpi_0210_019,
        |SUM(kpi_0210_020) kpi_0210_020,
        |SUM(kpi_0210_021) kpi_0210_021,
        |SUM(kpi_0210_022) kpi_0210_022,
        |SUM(kpi_0210_023) kpi_0210_023,
        |SUM(kpi_0210_024) kpi_0210_024,
        |SUM(kpi_0210_025) kpi_0210_025,
        |SUM(kpi_0210_026) kpi_0210_026,
        |SUM(kpi_0210_027) kpi_0210_027,
        |round(AVG(kpi_0238_001)) kpi_0238_001,
        |SUM(kpi_0238_002) kpi_0238_002,
        |SUM(kpi_0238_003) kpi_0238_003,
        |SUM(kpi_0238_004) kpi_0238_004,
        |SUM(kpi_0238_005) kpi_0238_005,
        |SUM(kpi_0238_006) kpi_0238_006,
        |SUM(kpi_0238_007) kpi_0238_007,
        |SUM(kpi_0238_008) kpi_0238_008,
        |SUM(kpi_0238_009) kpi_0238_009,
        |SUM(kpi_0238_010) kpi_0238_010,
        |SUM(kpi_0238_011) kpi_0238_011,
        |SUM(kpi_0238_012) kpi_0238_012,
        |SUM(kpi_0238_013) kpi_0238_013,
        |SUM(kpi_0238_014) kpi_0238_014,
        |SUM(kpi_0238_015) kpi_0238_015,
        |SUM(kpi_0238_016) kpi_0238_016,
        |SUM(kpi_0238_017) kpi_0238_017,
        |SUM(kpi_0238_018) kpi_0238_018,
        |SUM(kpi_0238_019) kpi_0238_019,
        |SUM(kpi_0238_020) kpi_0238_020,
        |SUM(kpi_0238_021) kpi_0238_021,
        |SUM(kpi_0238_022) kpi_0238_022,
        |SUM(kpi_0238_023) kpi_0238_023,
        |SUM(kpi_0238_024) kpi_0238_024,
        |SUM(kpi_0238_025) kpi_0238_025,
        |SUM(kpi_0238_026) kpi_0238_026,
        |round(AVG(kpi_0239_001)) kpi_0239_001,
        |SUM(kpi_0239_002) kpi_0239_002,
        |SUM(kpi_0239_003) kpi_0239_003,
        |SUM(kpi_0239_004) kpi_0239_004,
        |SUM(kpi_0239_005) kpi_0239_005,
        |SUM(kpi_0239_006) kpi_0239_006,
        |SUM(kpi_0239_007) kpi_0239_007,
        |SUM(kpi_0239_008) kpi_0239_008,
        |SUM(kpi_0239_009) kpi_0239_009,
        |SUM(kpi_0239_010) kpi_0239_010,
        |SUM(kpi_0239_011) kpi_0239_011,
        |SUM(kpi_0239_012) kpi_0239_012,
        |SUM(kpi_0239_013) kpi_0239_013,
        |SUM(kpi_0239_014) kpi_0239_014,
        |SUM(kpi_0239_015) kpi_0239_015,
        |from xdr_4g_enb_cell_s1u_h_tmp
        |group by
        |sgw_ip,cgi
      """.stripMargin).write.mode("append").format("jdbc")
      .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
      .option("dbtable",s"public.xdr_4g_enb_cell_s1u_d")
      .option("user", "postgres")
      .option("password", "postgres")
      .save()
    println(s"write to public.xdr_4g_enb_cell_s1u_d finish")




  }


}
