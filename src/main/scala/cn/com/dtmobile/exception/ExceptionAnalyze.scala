package cn.com.dtmobile.exception

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import cn.com.dtmobile.constants.Constant
import org.apache.spark.rdd.RDD



class ExceptionAnalyze
object ExceptionAnalyze {

  def main(args: Array[String]): Unit = {

    // 城市分区
    val p_city:String = args(0)

    // 小时分区
    val p_hour:String = args(1)

    // 设置spark日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)


    val conf: SparkConf = new SparkConf()
      //      .setMaster("local[*]")
      .setAppName("exceptionAnalyze")

    val prop = new Properties()
    // 用户名
    prop.setProperty("user","postgres")
    // 密码
    prop.setProperty("password","postgres")

    val session: SparkSession = SparkSession.builder().config(conf)
      .enableHiveSupport()
      .getOrCreate()

    // 连接JDBC读取pg库
    val s1: DataFrame = session.read.jdbc("jdbc:postgresql://172.30.100.182:5432/netop_e2e","omc_4g_enb_cell_access_h",prop)
    s1.createOrReplaceTempView("omc_4g_enb_cell_access_h_tmp")

    val s2: DataFrame = session.read.jdbc("jdbc:postgresql://172.30.100.182:5432/netop_e2e","omc_4g_enb_cell_res_h",prop)
    s2.createOrReplaceTempView("omc_4g_enb_cell_res_h_tmp")

    val s3: DataFrame = session.read.jdbc("jdbc:postgresql://172.30.100.182:5432/netop_e2e","omc_4g_enb_cell_flow_h",prop)
    s3.createOrReplaceTempView("omc_4g_enb_cell_flow_h_tmp")


    def httpServiceFailureException( p_hour: String, p_city: String): Unit ={

      // 使用Hive数据库xdr_kpi_ods
      session.sql("use xdr_kpi_ods")
      // 判定XDR为HTTP业务失败异常XDR(HTTP_CODE >= 400)
      val df: DataFrame = session.sql(s"select * from tb_xdr_s1u_http_par_dwd where p_city=${p_city} and p_hour=${p_hour} and http_code >= 400")
      df.createOrReplaceTempView("tb_xdr_s1u_http_par_dwd_tmp")


      session.sql(
        s"""
           |select
           |a.kpi1 as MR_LteScRSRP,
           |a.kpi3 as MR_LteScRSRQ,
           |a.kpi8 as MR_LteScSinrUL,
           |min(abs(floor(meatime/1000) - unix_timestamp(end_t)))
           |from
           |(select * from ${Constant.LTE_MRO_SOURCE}) a
           |inner join
           |(select * from tb_xdr_s1u_http_par_dwd_tmp) b
           |on a.cellid = b.eci
           |where floor(meatime/1000) >= (unix_timestamp(b.end_t) - 300) and floor(meatime/1000) <= (unix_timestamp(b.end_t) + 300)
           |group by kpi1,kpi3,kpi8
        """.stripMargin).show()


      //    // 注册UDF函数
      //    session.udf.register("end_t_range",(f:String) => f.length)

      //    session.sql(
      //      s"""
      //        |select
      //        |a.kpi1 as MR_LteScRSRP,
      //        |a.kpi3 as MR_LteScRSRQ,
      //        |a.kpi8 as MR_LteScSinrUL
      //        |from
      //        |(select * from ${Constant.LTE_MRO_SOURCE} where p_city=571 and p_hour=2020041200) a
      //        |left join
      //        |(select * from tb_xdr_s1u_http_par_dwd_tmp) b
      //        |on a.cellid = b.eci and a.kpi70 = b.imsi
      //      """.stripMargin).show()
      //
      //
      //    session.sql(
      //      """
      //        |select
      //        |(case when HTTP_CODE like '4%' then (case when cast(HTTP_CODE as int) in (401,402,403,413,414,415,416) then 3 else 'a' end)
      //        |      when HTTP_CODE like '5%' then 4
      //        |      when HTTP_CODE like '8%' then 6 else 'b' end
      //        |) as cellRegion
      //        |from tb_xdr_s1u_http_par_dwd_tmp
      //      """.stripMargin).show()

      //    session.sql(
      //      """
      //        |select
      //        |b.KPI_0001_005,
      //        |b.KPI_0001_006,
      //        |b.KPI_0002_001,
      //        |b.KPI_0057_001
      //        |from
      //        |(select * from tb_xdr_s1u_http_par_dwd_tmp) a
      //        |right join
      //        |(select * from omc_4g_enb_cell_access_h_tmp) b
      //        |on a.cgi = b.cgi
      //      """.stripMargin).show()
      //
      //
      //    session.sql(
      //      """
      //        |select
      //        |b.KPI_0016_005,
      //        |b.KPI_0016_007,
      //        |b.KPI_0016_008
      //        |from
      //        |(select * from tb_xdr_s1u_http_par_dwd_tmp) a
      //        |right join
      //        |(select * from omc_4g_enb_cell_res_h_tmp) b
      //        |on a.cgi = b.cgi
      //      """.stripMargin).show()
      //
      //    session.sql(
      //      """
      //        |select
      //        |b.KPI_0008_001,
      //        |b.KPI_0008_002
      //        |from
      //        |(select * from tb_xdr_s1u_http_par_dwd_tmp) a
      //        |right join
      //        |(select * from omc_4g_enb_cell_flow_h_tmp) b
      //        |on a.cgi = b.cgi
      //      """.stripMargin).show()



      //    session.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      //    session.sql(
      //      s"""
      //        |insert into table 5gop.tb_exception_event partition(p_city,p_hour)
      //        |select
      //        |0 as xdrid,
      //        |0 as u_type,
      //        |0 as starttime,
      //        |0 as endtime,
      //        |0 as imsi,
      //        |0 as imei,
      //        |0 as msisdn,
      //        |0 as uetac,
      //        |0 as rat,
      //        |0 as ip_sgw,
      //        |0 as tac,
      //        |0 as cgi,
      //        |0 as apn,
      //        |0 as host,
      //        |0 as ip_serv,
      //        |0 as servname,
      //        |0 as app_status,
      //        |0 as menthod,
      //        |0 as http_code,
      //        |0 as tcp_win,
      //        |0 as tcp_mss,
      //        |0 as dur_tcp1st,
      //        |0 as dur_tcp2nd,
      //        |0 as rtt_ul,
      //        |0 as rtt_dl,
      //        |0 as rtt_dlnum,
      //        |0 as rtt_ulnum,
      //        |0 as avg_ul_jitter,
      //        |0 as avg_dl_jitter,
      //        |0 as ul_rtt_gt_th2_num,
      //        |0 as dl_rtt_gt_th4_num,
      //        |0 as dl_traff,
      //        |0 as videotime,
      //        |0 as videoblocknum,
      //        |a.kpi1 as MR_LteScRSRP,
      //        |a.kpi3 as MR_LteScRSRQ,
      //        |a.kpi8 as MR_LteScSinrUL,
      //        |0 as KPI_0057_001,
      //        |0 as KPI_0001_006,
      //        |0 as KPI_0017_001,
      //        |0 as flowPerERAB,
      //        |0 as KPI_0001_005,
      //        |0 as KPI_0016_005,
      //        |0 as KPI_0008_001,
      //        |0 as KPI_0016_007,
      //        |0 as KPI_0016_008,
      //        |0 as KPI_0008_002,
      //        |0 as etype,
      //        |0 as cellRegion,
      //        |0 as radioCause,
      //        |a.p_city as p_city,
      //        |a.p_hour as p_hour
      //        |from
      //        |(select * from ${Constant.LTE_MRO_SOURCE} where p_city=571 and p_hour=2020041200) a
      //        |left join
      //        |(select * from tb_xdr_s1u_http_par_dwd_tmp) b
      //        |on a.cellid = b.eci and a.kpi70 = b.imsi
      //      """.stripMargin)


    }









  }

}
