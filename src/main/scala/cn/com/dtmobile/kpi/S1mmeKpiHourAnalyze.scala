package cn.com.dtmobile.kpi


import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}


class S1mmeKpiHourAnalyze
object S1mmeKpiHourAnalyze {

  def main(args: Array[String]): Unit = {

    // PG库IP
    val pg_ip:String = args(0)
    // PG库端口
    val pg_port:Int = args(1).toInt

    // Hive数据库
    val DDB:String = args(2)

    // 城市分区
    val p_city:String = args(3)

    // 小时分区
    val p_hour:String = args(4)



    // 设置spark日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)


    val conf: SparkConf = new SparkConf()
      //      .setMaster("local[*]")
      .setAppName("kpiAnalyze")
    // 设置shuffle产生的分区为1
    conf.set("spark.sql.shuffle.partitions","1")

    val session: SparkSession = SparkSession.builder().config(conf)
      .enableHiveSupport()
      .getOrCreate()

    // xdr s1mme指标统计写入hive表
    def writeToHive(pg_ip: String, pg_port: Int, DDB:String, p_city:String, p_hour:String): Unit = {
      // 使用Hive数据库
      session.sql(s"use ${DDB}")

      // 写入Hive表 xdr_4g_enb_cell_access_h
      // 创建 xdr_4g_enb_cell_access_h表
      session.sql(
        s"""
           |CREATE TABLE if not exists xdr_4g_enb_cell_access_h (
           |mme_ems_id varchar(255),
           |cgi varchar(255),
           |kpi_0100_001  DECIMAL,
           |kpi_0100_002  DECIMAL,
           |kpi_0100_003  DECIMAL,
           |kpi_0100_004  DECIMAL,
           |kpi_0100_005  DECIMAL,
           |kpi_0100_006  Float,
           |kpi_0100_007  Float,
           |kpi_0101_001  DECIMAL,
           |kpi_0101_002  DECIMAL,
           |kpi_0101_003  Float,
           |kpi_0102_001  DECIMAL,
           |kpi_0102_002  DECIMAL,
           |kpi_0102_003  Float,
           |time timestamp  )
        """.stripMargin)

      // S1-MME 附着（41）
      session.sql(
        s"""
          |insert overwrite table xdr_4g_enb_cell_access_h
          |select
          |null as mme_ems_id,
          |cgi,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and sdrtype=41 and srvstat=0 then 1 else 0 end) as KPI_0100_001,
          |sum(case when interface=31 and access_type=6  and cdrstat=0 and sdrtype=41 then 1 else 0 end) as KPI_0100_002,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and sdrtype=41 and srvtype_req=2 then 1 else 0 end)as KPI_0100_003,
          |sum(case when interface=31 and access_type=6 and  cdrstat=0 and sdrtype=41 and srvtype_req=2 and srvtype_rsp=2 then 1 else 0 end)as KPI_0100_004,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and sdrtype=41 and srvtype_req=2 and srvtype_rsp=1 then 1 else 0 end)as KPI_0100_005,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and sdrtype=41 and srvstat=0 then unix_timestamp(split(accept_time,'\\.')[0])*1000000+cast(split(accept_time,'\\.')[1] as int)-unix_timestamp(split(starttime,'\\.')[0])*1000000-cast(split(starttime,'\\.')[1] as int) else 0 end)as KPI_0100_006,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and sdrtype=41 and srvstat=0 and srvtype_req=2 then unix_timestamp(split(accept_time,'\\.')[0])*1000000+cast(split(accept_time,'\\.')[1] as int)-unix_timestamp(split(starttime,'\\.')[0])*1000000-cast(split(starttime,'\\.')[1] as int) else 0 end) as KPI_0100_007,
          |0 as kpi_0101_001,
          |0 as kpi_0101_002,
          |0 as kpi_0101_003,
          |0 as kpi_0102_001,
          |0 as kpi_0102_002,
          |0 as kpi_0102_003,
          |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
          |from db_ben3.tb_xdr_s1mme_41_par
          |where p_city=${p_city} and p_hour='${p_hour}'
          |group by
          |cgi
        """.stripMargin)

      // S1-MME 寻呼和业务请求（24）
      session.sql(
        s"""
          |insert into table xdr_4g_enb_cell_access_h
          |select
          |null as mme_ems_id,
          |cgi,
          |0 as kpi_0100_001,
          |0 as kpi_0100_002,
          |0 as kpi_0100_003,
          |0 as kpi_0100_004,
          |0 as kpi_0100_005,
          |0 as kpi_0100_006,
          |0 as kpi_0100_007,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and sdrtype=24 and srvstat=0 and direction=0 and srvorig=1 then 1 else 0 end) as KPI_0101_001,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and sdrtype=24 and direction=0 and srvorig=1 then 1 else 0 end) as KPI_0101_002,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and sdrtype=24 and srvstat=0 and direction=0 and srvorig=1 then unix_timestamp(split(req_time,'\\.')[0])*1000000+cast(split(req_time,'\\.')[1] as int)-unix_timestamp(split(starttime,'\\.')[0])*1000000-cast(split(starttime,'\\.')[1] as int) else 0 end) as KPI_0101_003,
          |0 as kpi_0102_001,
          |0 as kpi_0102_002,
          |0 as kpi_0102_003,
          |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
          |from db_ben3.tb_xdr_s1mme_24_par
          |where p_city=${p_city} and p_hour='${p_hour}'
          |group by
          |cgi
        """.stripMargin)


      // S1-MME 附着（41）、S1-MME 寻呼和业务请求（24）、S1-MME 跟踪区更新（43）
      session.sql(
        s"""
          |insert into table xdr_4g_enb_cell_access_h
          |select
          |null as mme_ems_id,
          |cgi,
          |0 as kpi_0100_001,
          |0 as kpi_0100_002,
          |0 as kpi_0100_003,
          |0 as kpi_0100_004,
          |0 as kpi_0100_005,
          |0 as kpi_0100_006,
          |0 as kpi_0100_007,
          |0 as kpi_0101_001,
          |0 as kpi_0101_002,
          |0 as kpi_0101_003,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and auth_time is not null and auth_end_time is not null and cause is null then 1 else 0 end) as KPI_0102_001,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and auth_time is not null then 1 else 0 end) as KPI_0102_002,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and auth_time is not null and auth_end_time is not null and cause is null then unix_timestamp(split(auth_end_time,'\\.')[0])*1000000+cast(split(auth_end_time,'\\.')[1] as int)-unix_timestamp(split(auth_time,'\\.')[0])*1000000-cast(split(auth_time,'\\.')[1] as int) else 0 end) as KPI_0102_003,
          |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
          |from db_ben3.tb_xdr_s1mme_41_par
          |where p_city=${p_city} and p_hour='${p_hour}'
          |group by
          |cgi
        """.stripMargin)

      session.sql(
        s"""
          |insert into table xdr_4g_enb_cell_access_h
          |select
          |null as mme_ems_id,
          |cgi,
          |0 as kpi_0100_001,
          |0 as kpi_0100_002,
          |0 as kpi_0100_003,
          |0 as kpi_0100_004,
          |0 as kpi_0100_005,
          |0 as kpi_0100_006,
          |0 as kpi_0100_007,
          |0 as kpi_0101_001,
          |0 as kpi_0101_002,
          |0 as kpi_0101_003,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and auth_time is not null and auth_end_time is not null and cause is null then 1 else 0 end) as KPI_0102_001,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and auth_time is not null then 1 else 0 end) as KPI_0102_002,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and auth_time is not null and auth_end_time is not null and cause is null then unix_timestamp(split(auth_end_time,'\\.')[0])*1000000+cast(split(auth_end_time,'\\.')[1] as int)-unix_timestamp(split(auth_time,'\\.')[0])*1000000-cast(split(auth_time,'\\.')[1] as int) else 0 end) as KPI_0102_003,
          |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
          |from db_ben3.tb_xdr_s1mme_24_par
          |where p_city=${p_city} and p_hour='${p_hour}'
          |group by
          |cgi
        """.stripMargin)

      session.sql(
        s"""
          |insert into table xdr_4g_enb_cell_access_h
          |select
          |null as mme_ems_id,
          |cgi,
          |0 as kpi_0100_001,
          |0 as kpi_0100_002,
          |0 as kpi_0100_003,
          |0 as kpi_0100_004,
          |0 as kpi_0100_005,
          |0 as kpi_0100_006,
          |0 as kpi_0100_007,
          |0 as kpi_0101_001,
          |0 as kpi_0101_002,
          |0 as kpi_0101_003,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and auth_time is not null and auth_end_time is not null and cause is null then 1 else 0 end) as KPI_0102_001,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and auth_time is not null then 1 else 0 end) as KPI_0102_002,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and auth_time is not null and auth_end_time is not null and cause is null then unix_timestamp(split(auth_end_time,'\\.')[0])*1000000+cast(split(auth_end_time,'\\.')[1] as int)-unix_timestamp(split(auth_time,'\\.')[0])*1000000-cast(split(auth_time,'\\.')[1] as int) else 0 end) as KPI_0102_003,
          |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
          |from db_ben3.tb_xdr_s1mme_43_par
          |where p_city=${p_city} and p_hour='${p_hour}'
          |group by
          |cgi
        """.stripMargin)


      // 写入Hive表 xdr_4g_enb_cell_ho_h
      // 创建xdr_4g_enb_cell_ho_h表
      session.sql(
        s"""
           |CREATE TABLE if not exists xdr_4g_enb_cell_ho_h(
           |mme_ems_id varchar(255),
           |cgi varchar(255),
           |kpi_0103_001 DECIMAL,
           |kpi_0103_002 DECIMAL,
           |kpi_0103_003 DECIMAL,
           |kpi_0103_004 DECIMAL,
           |kpi_0103_005 DECIMAL,
           |kpi_0103_006 Float,
           |kpi_0103_007 Float,
           |kpi_0104_001 DECIMAL,
           |kpi_0104_002 DECIMAL,
           |kpi_0104_003 DECIMAL,
           |kpi_0104_004 DECIMAL,
           |kpi_0104_005 Float,
           |kpi_0104_006 Float,
           |kpi_0104_007 DECIMAL,
           |kpi_0104_008 DECIMAL,
           |kpi_0104_009 DECIMAL,
           |kpi_0104_010 DECIMAL,
           |kpi_0104_011 Float,
           |kpi_0104_012 Float,
           |time timestamp)
        """.stripMargin)

      // S1-MME 跟踪区更新（43）
      session.sql(
        s"""
          |insert overwrite table xdr_4g_enb_cell_ho_h
          |select
          |null as mme_ems_id,
          |cgi,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and srvstat=0 then 1 else 0 end) as KPI_0103_001,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 then 1 else 0 end) as KPI_0103_002,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and (srvtype_req=1 or srvtype_req=2) then 1 else 0 end) as KPI_0103_003,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and (srvtype_req=1 or srvtype_req=2) and srvtype_rsp=1 then 1 else 0 end) as KPI_0103_004,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and (srvtype_req=1 or srvtype_req=2) and srvtype_rsp=0 then 1 else 0 end) as KPI_0103_005,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and srvstat=0 then unix_timestamp(split(acpt_time,'\\.')[0])*1000000+cast(split(acpt_time,'\\.')[1] as int)-unix_timestamp(split(starttime,'\\.')[0])*1000000-cast(split(starttime,'\\.')[1] as int) else 0 end) as KPI_0103_006,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and (srvtype_req=1 or srvtype_req=2) then unix_timestamp(split(acpt_time,'\\.')[0])*1000000+cast(split(acpt_time,'\\.')[1] as int)-unix_timestamp(split(starttime,'\\.')[0])*1000000-cast(split(starttime,'\\.')[1] as int) else 0 end) as KPI_0103_007,
          |0 as kpi_0104_001,
          |0 as kpi_0104_002,
          |0 as kpi_0104_003,
          |0 as kpi_0104_004,
          |0 as kpi_0104_005,
          |0 as kpi_0104_006,
          |0 as kpi_0104_007,
          |0 as kpi_0104_008,
          |0 as kpi_0104_009,
          |0 as kpi_0104_010,
          |0 as kpi_0104_011,
          |0 as kpi_0104_012,
          |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
          |from db_ben3.tb_xdr_s1mme_43_par
          |where p_city=${p_city} and p_hour='${p_hour}'
          |group by
          |cgi
        """.stripMargin)


      // 写入Hive表 xdr_4g_enb_cell_spt_h
      // 创建xdr_4g_enb_cell_spt_h表
      session.sql(
        s"""
           |CREATE TABLE if not exists xdr_4g_enb_cell_spt_h (
           |mme_ems_id varchar(255),
           |cgi varchar(255),
           |kpi_0105_001  DECIMAL,
           |kpi_0105_002  DECIMAL,
           |kpi_0105_003  Float,
           |time timestamp  )
        """.stripMargin)

      // S1-MME 寻呼和业务请求(24)
      session.sql(
        s"""
          |insert overwrite table xdr_4g_enb_cell_spt_h
          |select
          |null as mme_ems_id,
          |cgi,
          |sum(case when interface=31 and cdrstat=0 and req_time is not null and srvtype is null and rsp_time is not null then 1 else 0 end)as KPI_0105_001,
          |sum(case when interface=31 and cdrstat=0 and req_time is not null and srvtype is null then 1 else 0 end) as KPI_0105_002,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and srvstat=0 and direction=0 and srvorig=1  then unix_timestamp(split(req_time,'\\.')[0])*1000000+cast(split(req_time,'\\.')[1] as int)-unix_timestamp(split(starttime,'\\.')[0])*1000000-cast(split(starttime,'\\.')[1] as int) else 0 end)as KPI_0105_003,
          |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
          |from tb_xdr_s1mme_24_par
          |where p_city=${p_city} and p_hour='${p_hour}'
          |group by
          |cgi
        """.stripMargin)

      // 写入Hive表 xdr_4g_enb_cell_bearer_h
      // 创建xdr_4g_enb_cell_bearer_h表
      session.sql(
        s"""
           |CREATE TABLE if not exists xdr_4g_enb_cell_bearer_h(
           |mme_ems_id varchar(255),
           |cgi varchar(255),
           |kpi_0107_001  DECIMAL,
           |kpi_0107_002  DECIMAL,
           |kpi_0107_003  Float,
           |kpi_0107_004  DECIMAL,
           |kpi_0107_005  DECIMAL,
           |kpi_0107_006  Float,
           |kpi_0107_007  DECIMAL,
           |kpi_0107_008  DECIMAL,
           |time timestamp  )
        """.stripMargin)

      // S1-MME PDN连接(21)
      session.sql(
        s"""
          |insert overwrite table xdr_4g_enb_cell_bearer_h
          |select
          |null as mme_ems_id,
          |cgi,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and srvstat=0 then 1 else 0 end) as KPI_0107_001,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 then 1 else 0 end) as KPI_0107_002,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and srvstat=0 then unix_timestamp(split(eear_accept_time,'\\.')[0])*1000000+cast(split(eear_accept_time,'\\.')[1] as int)-unix_timestamp(split(bear_act_time,'\\.')[0])*1000000-cast(split(bear_act_time,'\\.')[1] as int) else 0 end) as KPI_0107_003,
          |0 as kpi_0107_004,
          |0 as kpi_0107_005,
          |0 as kpi_0107_006,
          |0 as kpi_0107_007,
          |0 as kpi_0107_008,
          |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
          |from tb_xdr_s1mme_21_par
          |where p_city=${p_city} and p_hour='${p_hour}'
          |group by
          |cgi
        """.stripMargin)

      // S1-MME 网络发起EPS承载上下文激活(27)
      session.sql(
        s"""
          |insert into table xdr_4g_enb_cell_bearer_h
          |select
          |null as mme_ems_id,
          |cgi,
          |0 as kpi_0107_001,
          |0 as kpi_0107_002,
          |0 as kpi_0107_003,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and srvstat=0 then 1 else 0 end)as KPI_0107_004,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 then 1 else 0 end) as KPI_0107_005,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and srvstat=0 then unix_timestamp(split(rsp_time,'\\.')[0])*1000000+cast(split(rsp_time,'\\.')[1] as int)-unix_timestamp(split(req_time,'\\.')[0])*1000000+cast(split(req_time,'\\.')[1] as int) else 0 end)as KPI_0107_006,
          |0 as kpi_0107_007,
          |0 as kpi_0107_008,
          |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
          |from tb_xdr_s1mme_27_par
          |where p_city=${p_city} and p_hour='${p_hour}'
          |group by
          |cgi
        """.stripMargin)

      // S1-MME 网络发起EPS承载上下文修改(29)
      session.sql(
        s"""
          |insert into table xdr_4g_enb_cell_bearer_h
          |select
          |null as mme_ems_id,
          |cgi,
          |0 as kpi_0107_001,
          |0 as kpi_0107_002,
          |0 as kpi_0107_003,
          |0 as kpi_0107_004,
          |0 as kpi_0107_005,
          |0 as kpi_0107_006,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 and srvstat=0 then 1 else 0 end)as KPI_0107_007,
          |sum(case when interface=31 and access_type=6 and cdrstat=0 then 1 else 0 end) as KPI_0107_008,
          |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
          |from tb_xdr_s1mme_29_par
          |where p_city=${p_city} and p_hour='${p_hour}'
          |group by
          |cgi
        """.stripMargin)

    }

    def hiveWriteToPG(pg_ip: String, pg_port: Int, DDB:String, p_city:String, p_hour:String): Unit={
      session.sql(s"use ${DDB}")


      // 写入xdr_4g_enb_cell_access_h
      session.sql(
        s"""
           |select * from xdr_4g_enb_cell_access_h
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable",s"public.xdr_4g_enb_cell_access_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_access_h finish")


      // 写入xdr_4g_enb_cell_ho_h
      session.sql(
        s"""
           |select * from xdr_4g_enb_cell_ho_h
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable",s"public.xdr_4g_enb_cell_ho_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_ho_h")


      // 写入xdr_4g_enb_cell_spt_h
      session.sql(
        s"""
           |select * from xdr_4g_enb_cell_spt_h
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable",s"public.xdr_4g_enb_cell_spt_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_spt_h")

      // 写入xdr_4g_enb_cell_bearer_h
      session.sql(
        s"""
           |select * from xdr_4g_enb_cell_bearer_h
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable",s"public.xdr_4g_enb_cell_bearer_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_bearer_h")



    }





    // 查询统计指标
    // 写入PG库
    def writeToPg(pg_ip: String, pg_port: Int, DDB:String, p_city:String, p_hour:String): Unit = {

      // 使用hive数据库
      session.sql(
        s"""
          |use ${DDB}
        """.stripMargin)

      // 4G信令面指标统计
      // 写入public.xdr_4g_enb_cell_access_h
      // S1-MME 附着（41）
      session.sql(
        s"""
           |select
           |mme_ip,
           |cgi,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and sdrtype=41 and srvstat=0 then 1 else 0 end) as KPI_0100_001,
           |sum(case when interface=31 and access_type=6  and cdrstat=0 and sdrtype=41 then 1 else 0 end) as KPI_0100_002,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and sdrtype=41 and srvtype_req=2 then 1 else 0 end)as KPI_0100_003,
           |sum(case when interface=31 and access_type=6 and  cdrstat=0 and sdrtype=41 and srvtype_req=2 and srvtype_rsp=2 then 1 else 0 end)as KPI_0100_004,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and sdrtype=41 and srvtype_req=2 and srvtype_rsp=1 then 1 else 0 end)as KPI_0100_005,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and sdrtype=41 and srvstat=0 then unix_timestamp(split(accept_time,'\\.')[0])*1000000+cast(split(accept_time,'\\.')[1] as int)-unix_timestamp(split(starttime,'\\.')[0])*1000000-cast(split(starttime,'\\.')[1] as int) else 0 end)as KPI_0100_006,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and sdrtype=41 and srvstat=0 and srvtype_req=2 then unix_timestamp(split(accept_time,'\\.')[0])*1000000+cast(split(accept_time,'\\.')[1] as int)-unix_timestamp(split(starttime,'\\.')[0])*1000000-cast(split(starttime,'\\.')[1] as int) else 0 end) as KPI_0100_007,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from tb_xdr_s1mme_41_par
           |where p_city=${p_city} and p_hour='${p_hour}'
           |group by
           |mme_ip,cgi
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable",s"public.xdr_4g_enb_cell_access_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_access_h")


      // S1-MME 寻呼和业务请求（24）
      session.sql(
        s"""
           |select
           |mme_ip,
           |cgi,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and sdrtype=24 and srvstat=0 and direction=0 and srvorig=1 then 1 else 0 end) as KPI_0101_001,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and sdrtype=24 and direction=0 and srvorig=1 then 1 else 0 end) as KPI_0101_002,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and sdrtype=24 and srvstat=0 and direction=0 and srvorig=1 then unix_timestamp(split(req_time,'\\.')[0])*1000000+cast(split(req_time,'\\.')[1] as int)-unix_timestamp(split(starttime,'\\.')[0])*1000000-cast(split(starttime,'\\.')[1] as int) else 0 end) as KPI_0101_003,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from tb_xdr_s1mme_24_par
           |where p_city=${p_city} and p_hour='${p_hour}'
           |group by
           |mme_ip,cgi
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable",s"public.xdr_4g_enb_cell_access_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_access_h")

      // S1-MME 附着（41）、S1-MME 寻呼和业务请求（24）、S1-MME 跟踪区更新（43）
      session.sql(
        s"""
           |select
           |mme_ip,
           |cgi,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and auth_time is not null and auth_end_time is not null and cause is null then 1 else 0 end) as KPI_0102_001,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and auth_time is not null then 1 else 0 end) as KPI_0102_002,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and auth_time is not null and auth_end_time is not null and cause is null then unix_timestamp(split(auth_end_time,'\\.')[0])*1000000+cast(split(auth_end_time,'\\.')[1] as int)-unix_timestamp(split(auth_time,'\\.')[0])*1000000-cast(split(auth_time,'\\.')[1] as int) else 0 end) as KPI_0102_003,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from tb_xdr_s1mme_41_par
           |where p_city=${p_city} and p_hour='${p_hour}'
           |group by
           |mme_ip,cgi
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable",s"public.xdr_4g_enb_cell_access_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_access_h")

      session.sql(
        s"""
           |select
           |mme_ip,
           |cgi,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and auth_time is not null and auth_end_time is not null and cause is null then 1 else 0 end) as KPI_0102_001,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and auth_time is not null then 1 else 0 end) as KPI_0102_002,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and auth_time is not null and auth_end_time is not null and cause is null then unix_timestamp(split(auth_end_time,'\\.')[0])*1000000+cast(split(auth_end_time,'\\.')[1] as int)-unix_timestamp(split(auth_time,'\\.')[0])*1000000-cast(split(auth_time,'\\.')[1] as int) else 0 end) as KPI_0102_003,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from tb_xdr_s1mme_24_par
           |where p_city=${p_city} and p_hour='${p_hour}'
           |group by
           |mme_ip,cgi
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable",s"public.xdr_4g_enb_cell_access_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_access_h")

      session.sql(
        s"""
           |select
           |mme_ip,
           |cgi,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and auth_time is not null and auth_end_time is not null and cause is null then 1 else 0 end) as KPI_0102_001,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and auth_time is not null then 1 else 0 end) as KPI_0102_002,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and auth_time is not null and auth_end_time is not null and cause is null then unix_timestamp(split(auth_end_time,'\\.')[0])*1000000+cast(split(auth_end_time,'\\.')[1] as int)-unix_timestamp(split(auth_time,'\\.')[0])*1000000-cast(split(auth_time,'\\.')[1] as int) else 0 end) as KPI_0102_003,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from tb_xdr_s1mme_43_par
           |where p_city=${p_city} and p_hour='${p_hour}'
           |group by
           |mme_ip,cgi
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable",s"public.xdr_4g_enb_cell_access_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_access_h")



      // 写入PG库表"public"."xdr_4g_enb_cell_ho_h"
      // // S1-MME 跟踪区更新（43）
      session.sql(
        s"""
           |select
           |mme_ip,
           |cgi,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and srvstat=0 then 1 else 0 end) as KPI_0103_001,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 then 1 else 0 end) as KPI_0103_002,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and (srvtype_req=1 or srvtype_req=2) then 1 else 0 end) as KPI_0103_003,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and (srvtype_req=1 or srvtype_req=2) and srvtype_rsp=1 then 1 else 0 end) as KPI_0103_004,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and (srvtype_req=1 or srvtype_req=2) and srvtype_rsp=0 then 1 else 0 end) as KPI_0103_005,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and srvstat=0 then unix_timestamp(split(acpt_time,'\\.')[0])*1000000+cast(split(acpt_time,'\\.')[1] as int)-unix_timestamp(split(starttime,'\\.')[0])*1000000-cast(split(starttime,'\\.')[1] as int) else 0 end) as KPI_0103_006,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and (srvtype_req=1 or srvtype_req=2) then unix_timestamp(split(acpt_time,'\\.')[0])*1000000+cast(split(acpt_time,'\\.')[1] as int)-unix_timestamp(split(starttime,'\\.')[0])*1000000-cast(split(starttime,'\\.')[1] as int) else 0 end) as KPI_0103_007,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from tb_xdr_s1mme_43_par
           |where p_city=${p_city} and p_hour='${p_hour}'
           |group by
           |mme_ip,cgi
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable",s"public.xdr_4g_enb_cell_ho_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_ho_h")



      // 写入PG库表"public"."xdr_4g_enb_cell_spt_h"
      // S1-MME 寻呼和业务请求(24)
      session.sql(
        s"""
           |select
           |mme_ip,
           |cgi,
           |sum(case when interface=31 and cdrstat=0 and req_time is not null and srvtype is null and rsp_time is not null then 1 else 0 end)as KPI_0105_001,
           |sum(case when interface=31 and cdrstat=0 and req_time is not null and srvtype is null then 1 else 0 end) as KPI_0105_002,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and srvstat=0 and direction=0 and srvorig=1  then unix_timestamp(split(req_time,'\\.')[0])*1000000+cast(split(req_time,'\\.')[1] as int)-unix_timestamp(split(starttime,'\\.')[0])*1000000-cast(split(starttime,'\\.')[1] as int) else 0 end)as KPI_0105_003,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from tb_xdr_s1mme_24_par
           |where p_city=${p_city} and p_hour='${p_hour}'
           |group by
           |mme_ip,cgi
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable",s"public.xdr_4g_enb_cell_spt_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_spt_h")


      /// 写入PG库表""public"."xdr_4g_enb_cell_bearer_h"
      // S1-MME PDN连接(21)
      session.sql(
        s"""
           |select
           |mme_ip,
           |cgi,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and srvstat=0 then 1 else 0 end) as KPI_0107_001,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 then 1 else 0 end) as KPI_0107_002,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and srvstat=0 then unix_timestamp(split(eear_accept_time,'\\.')[0])*1000000+cast(split(eear_accept_time,'\\.')[1] as int)-unix_timestamp(split(bear_act_time,'\\.')[0])*1000000-cast(split(bear_act_time,'\\.')[1] as int) else 0 end) as KPI_0107_003,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from tb_xdr_s1mme_21_par
           |where p_city=${p_city} and p_hour='${p_hour}'
           |group by
           |mme_ip,cgi
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable",s"public.xdr_4g_enb_cell_bearer_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_bearer_h")

      // S1-MME 网络发起EPS承载上下文激活(27)
      session.sql(
        s"""
           |select
           |mme_ip,
           |cgi,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and srvstat=0 then 1 else 0 end)as KPI_0107_004,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 then 1 else 0 end) as KPI_0107_005,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and srvstat=0 then unix_timestamp(split(rsp_time,'\\.')[0])*1000000+cast(split(rsp_time,'\\.')[1] as int)-unix_timestamp(split(req_time,'\\.')[0])*1000000+cast(split(req_time,'\\.')[1] as int) else 0 end)as KPI_0107_006,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from tb_xdr_s1mme_27_par
           |where p_city=${p_city} and p_hour='${p_hour}'
           |group by
           |mme_ip,cgi
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable",s"public.xdr_4g_enb_cell_bearer_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_bearer_h")

      // S1-MME 网络发起EPS承载上下文修改(29)
      session.sql(
        s"""
           |select
           |mme_ip,
           |cgi,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 and srvstat=0 then 1 else 0 end)as KPI_0107_007,
           |sum(case when interface=31 and access_type=6 and cdrstat=0 then 1 else 0 end) as KPI_0107_008,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from tb_xdr_s1mme_29_par
           |where p_city=${p_city} and p_hour='${p_hour}'
           |group by
           |mme_ip,cgi
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable",s"public.xdr_4g_enb_cell_bearer_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_bearer_h")



    }

//    writeToPg(pg_ip,pg_port,DDB,p_city,p_hour)
    writeToHive(pg_ip,pg_port,DDB,p_city,p_hour)
    hiveWriteToPG(pg_ip,pg_port,DDB,p_city,p_hour)



  }

}


