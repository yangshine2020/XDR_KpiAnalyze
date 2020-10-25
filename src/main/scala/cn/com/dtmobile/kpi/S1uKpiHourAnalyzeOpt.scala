package cn.com.dtmobile.kpi

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

class  S1uKpiHourAnalyzeOpt
object S1uKpiHourAnalyzeOpt {

  def main(args: Array[String]): Unit = {

    // PG库IP
    val pg_ip: String = args(0)
    // PG库端口
    val pg_port: Int = args(1).toInt
    // Hive数据库
    val DDB: String = args(2)

    // 城市分区
    val p_city: String = args(3)

    // 小时分区
    val p_hour: String = args(4)

    // 设置spark日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)


    val conf: SparkConf = new SparkConf()
      //      .setMaster("local[*]")
      .setAppName("kpiAnalyze")
    // 设置shuffle产生的分区为1
    conf.set("spark.sql.shuffle.partitions", "1")

    val prop = new Properties()
    // 用户名
    prop.setProperty("user","postgres")
    // 密码
    prop.setProperty("password","postgres")

    val session: SparkSession = SparkSession.builder().config(conf)
      .enableHiveSupport()
      .getOrCreate()


    // 连接JDBC读取pg库
    val df: DataFrame = session.read.jdbc(s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e",s"public.t_epc_ip_table",prop)

    df.createOrReplaceTempView("t_epc_ip_table_tmp")

    // 写入Hive指标统计表
    def writeToHive(pg_ip: String, pg_port: Int, DDB: String, p_city: String, p_hour: String): Unit = {
      // 使用Hive数据库
      session.sql(s"use ${DDB}")

      // 创建 xdr_4g_enb_cell_s1u_h，写入s1u指标统计数据
      session.sql(
        s"""
           |CREATE TABLE if not exists xdr_4g_enb_cell_s1u_h (
           |sgw_ip varchar(255),
           |equip_ems_id varchar(255),
           |cgi varchar(255),
           |qci varchar(255),
           |kpi_0200_001  DECIMAL,
           |kpi_0200_002  DECIMAL,
           |kpi_0200_003  DECIMAL,
           |kpi_0200_004  DECIMAL,
           |kpi_0201_001  DECIMAL,
           |kpi_0201_002  DECIMAL,
           |kpi_0201_003  DECIMAL,
           |kpi_0201_004  DECIMAL,
           |kpi_0202_001  DECIMAL,
           |kpi_0202_002  DECIMAL,
           |kpi_0202_003  DECIMAL,
           |kpi_0203_001  DECIMAL,
           |kpi_0203_002   Float,
           |kpi_0203_003   Float,
           |kpi_0203_004  DECIMAL,
           |kpi_0203_005  DECIMAL,
           |kpi_0204_001  DECIMAL,
           |kpi_0204_002  DECIMAL,
           |kpi_0204_003  DECIMAL,
           |kpi_0204_004  DECIMAL,
           |kpi_0204_005  DECIMAL,
           |kpi_0205_001  DECIMAL,
           |kpi_0205_002  DECIMAL,
           |kpi_0205_003  DECIMAL,
           |kpi_0205_004  DECIMAL,
           |kpi_0205_005  DECIMAL,
           |kpi_0206_001  DECIMAL,
           |kpi_0206_002  DECIMAL,
           |kpi_0206_003  DECIMAL,
           |kpi_0206_004  DECIMAL,
           |kpi_0206_005  DECIMAL,
           |kpi_0206_006  DECIMAL,
           |kpi_0207_001  DECIMAL,
           |kpi_0207_002  DECIMAL,
           |kpi_0207_003  DECIMAL,
           |kpi_0207_004  DECIMAL,
           |kpi_0207_005  DECIMAL,
           |kpi_0208_001  DECIMAL,
           |kpi_0208_002  DECIMAL,
           |kpi_0208_003  DECIMAL,
           |kpi_0208_004  DECIMAL,
           |kpi_0209_001  DECIMAL,
           |kpi_0209_002   Float,
           |kpi_0209_003   Float,
           |kpi_0209_004  DECIMAL,
           |kpi_0209_005  DECIMAL,
           |kpi_0209_006  DECIMAL,
           |kpi_0209_007  DECIMAL,
           |kpi_0209_008  DECIMAL,
           |kpi_0209_009  DECIMAL,
           |kpi_0209_010  DECIMAL,
           |kpi_0209_011  DECIMAL,
           |kpi_0209_012  DECIMAL,
           |kpi_0209_013  DECIMAL,
           |kpi_0209_014  DECIMAL,
           |kpi_0209_015  DECIMAL,
           |kpi_0209_016  DECIMAL,
           |kpi_0209_017  DECIMAL,
           |kpi_0209_018  DECIMAL,
           |kpi_0210_001  DECIMAL,
           |kpi_0210_002   Float,
           |kpi_0210_003   Float,
           |kpi_0210_004  DECIMAL,
           |kpi_0210_005  DECIMAL,
           |kpi_0210_006  DECIMAL,
           |kpi_0210_007  DECIMAL,
           |kpi_0210_008  DECIMAL,
           |kpi_0210_009  DECIMAL,
           |kpi_0210_010  DECIMAL,
           |kpi_0210_011  DECIMAL,
           |kpi_0210_012  DECIMAL,
           |kpi_0210_013  DECIMAL,
           |kpi_0210_014  DECIMAL,
           |kpi_0210_015  DECIMAL,
           |kpi_0210_016  DECIMAL,
           |kpi_0210_017  DECIMAL,
           |kpi_0210_018  DECIMAL,
           |kpi_0210_019  DECIMAL,
           |kpi_0210_020  DECIMAL,
           |kpi_0210_021  DECIMAL,
           |kpi_0210_022  DECIMAL,
           |kpi_0210_023  DECIMAL,
           |kpi_0210_024  DECIMAL,
           |kpi_0210_025  DECIMAL,
           |kpi_0210_026  DECIMAL,
           |kpi_0210_027  DECIMAL,
           |kpi_0238_001  DECIMAL,
           |kpi_0238_002   Float,
           |kpi_0238_003   Float,
           |kpi_0238_004  DECIMAL,
           |kpi_0238_005  DECIMAL,
           |kpi_0238_006  DECIMAL,
           |kpi_0238_007  DECIMAL,
           |kpi_0238_008  DECIMAL,
           |kpi_0238_009  DECIMAL,
           |kpi_0238_010  DECIMAL,
           |kpi_0238_011  DECIMAL,
           |kpi_0238_012  DECIMAL,
           |kpi_0238_013  DECIMAL,
           |kpi_0238_014  DECIMAL,
           |kpi_0238_015  DECIMAL,
           |kpi_0238_016  DECIMAL,
           |kpi_0238_017  DECIMAL,
           |kpi_0238_018  DECIMAL,
           |kpi_0238_019  DECIMAL,
           |kpi_0238_020  DECIMAL,
           |kpi_0238_021  DECIMAL,
           |kpi_0238_022  DECIMAL,
           |kpi_0238_023  DECIMAL,
           |kpi_0238_024  DECIMAL,
           |kpi_0238_025  DECIMAL,
           |kpi_0238_026  DECIMAL,
           |kpi_0239_001  DECIMAL,
           |kpi_0239_002   Float,
           |kpi_0239_003   Float,
           |kpi_0239_004  DECIMAL,
           |kpi_0239_005  DECIMAL,
           |kpi_0239_006  DECIMAL,
           |kpi_0239_007  DECIMAL,
           |kpi_0239_008  DECIMAL,
           |kpi_0239_009  DECIMAL,
           |kpi_0239_010  DECIMAL,
           |kpi_0239_011  DECIMAL,
           |kpi_0239_012  DECIMAL,
           |kpi_0239_013  DECIMAL,
           |kpi_0239_014  DECIMAL,
           |kpi_0239_015  DECIMAL,
           |time timestamp )
        """.stripMargin)

      session.sql(
        s"""
           |insert overwrite table xdr_4g_enb_cell_s1u_h
           |select
           |ip_sgw as sgw_ip,
           |ems_id as equip_ems_id,
           |cgi,
           |null as qci,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0200_001,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and l4_type = 0 then 1 else 0 end) as KPI_0200_002,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP1ST else 0 end) as KPI_0200_003,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP2ND else 0 end) as KPI_0200_004,
           |0 as KPI_0201_001,
           |0 as KPI_0201_002,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then TCP_ULRETR_NUM else 0 end) as KPI_0201_003,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then TCP_DLRETR_NUM else 0 end) as KPI_0201_004,
           |0 as kpi_0202_001,
           |0 as kpi_0202_002,
           |0 as kpi_0202_003,
           |count(distinct(case when (u_type = 1 or u_type = 2) and rat = 6 then IMSI end)) as KPI_0203_001,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then UL_TRAFF else 0 end) as KPI_0203_002,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DL_TRAFF else 0 end) as KPI_0203_003,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then 1 else 0 end) as KPI_0203_004,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DURITION else 0 end) as KPI_0203_005,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'text%' and cast(HTTP_CODE as int) in (99,400) then 1 else 0 end) as KPI_0204_001,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'text%' then 1 else 0 end) as KPI_0204_002,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'text%' and cast(HTTP_CODE as int) in (99,400) then L7_Delay else 0 end) as KPI_0204_003,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'text%' and cast(HTTP_CODE as int) in (99,400) and (HTTP_LASTACK_DELAY is not null or HTTP_LASTACK_DELAY != 0) then 1 else 0 end) as KPI_0204_004,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'text%' and cast(HTTP_CODE as int) in (99,400) then HTTP_LASTACK_DELAY else 0 end) as KPI_0204_005,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'image%' and cast(HTTP_CODE as int) in (99,400) then 1 else 0 end) as KPI_0205_001,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'image%' then 1 else 0 end) as KPI_0205_002,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'image%' and cast(HTTP_CODE as int) in (99,400) then L7_Delay else 0 end) as KPI_0205_003,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'image%' and cast(HTTP_CODE as int) in (99,400) and (HTTP_LASTACK_DELAY is not null or HTTP_LASTACK_DELAY != 0) then 1 else 0 end) as KPI_0205_004,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'image%' and cast(HTTP_CODE as int) in (99,400) then HTTP_LASTACK_DELAY else 0 end) as KPI_0205_005,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'video%' and cast(HTTP_CODE as int) in (99,400) then 1 else 0 end) as KPI_0206_001,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'video%' then 1 else 0 end) as KPI_0206_002,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'video%' and cast(HTTP_CODE as int) in (99,400) then L7_Delay else 0 end) as KPI_0206_003,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'video%' and cast(HTTP_CODE as int) in (99,400) and (HTTP_LASTACK_DELAY is not null or HTTP_LASTACK_DELAY != 0) then 1 else 0 end) as KPI_0206_004,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'video%' and cast(HTTP_CODE as int) in (99,400) then HTTP_LASTACK_DELAY else 0 end) as KPI_0206_005,
           |0 as kpi_0206_006,
           |0 as kpi_0207_001,
           |0 as kpi_0207_002,
           |0 as kpi_0207_003,
           |0 as kpi_0207_004,
           |0 as kpi_0207_005,
           |0 as kpi_0208_001,
           |0 as kpi_0208_002,
           |0 as kpi_0208_003,
           |0 as kpi_0208_004,
           |0 as kpi_0209_001,
           |0 as kpi_0209_002,
           |0 as kpi_0209_003,
           |0 as kpi_0209_004,
           |0 as kpi_0209_005,
           |0 as kpi_0209_006,
           |0 as kpi_0209_007,
           |0 as kpi_0209_008,
           |0 as kpi_0209_009,
           |0 as kpi_0209_010,
           |0 as kpi_0209_011,
           |0 as kpi_0209_012,
           |0 as kpi_0209_013,
           |0 as kpi_0209_014,
           |0 as kpi_0209_015,
           |0 as kpi_0209_016,
           |0 as kpi_0209_017,
           |0 as kpi_0209_018,
           |0 as kpi_0210_001,
           |0 as kpi_0210_002,
           |0 as kpi_0210_003,
           |0 as kpi_0210_004,
           |0 as kpi_0210_005,
           |0 as kpi_0210_006,
           |0 as kpi_0210_007,
           |0 as kpi_0210_008,
           |0 as kpi_0210_009,
           |0 as kpi_0210_010,
           |0 as kpi_0210_011,
           |0 as kpi_0210_012,
           |0 as kpi_0210_013,
           |0 as kpi_0210_014,
           |0 as kpi_0210_015,
           |0 as kpi_0210_016,
           |0 as kpi_0210_017,
           |0 as kpi_0210_018,
           |0 as kpi_0210_019,
           |0 as kpi_0210_020,
           |0 as kpi_0210_021,
           |0 as kpi_0210_022,
           |0 as kpi_0210_023,
           |0 as kpi_0210_024,
           |0 as kpi_0210_025,
           |0 as kpi_0210_026,
           |0 as kpi_0210_027,
           |0 as kpi_0238_001,
           |0 as kpi_0238_002,
           |0 as kpi_0238_003,
           |0 as kpi_0238_004,
           |0 as kpi_0238_005,
           |0 as kpi_0238_006,
           |0 as kpi_0238_007,
           |0 as kpi_0238_008,
           |0 as kpi_0238_009,
           |0 as kpi_0238_010,
           |0 as kpi_0238_011,
           |0 as kpi_0238_012,
           |0 as kpi_0238_013,
           |0 as kpi_0238_014,
           |0 as kpi_0238_015,
           |0 as kpi_0238_016,
           |0 as kpi_0238_017,
           |0 as kpi_0238_018,
           |0 as kpi_0238_019,
           |0 as kpi_0238_020,
           |0 as kpi_0238_021,
           |0 as kpi_0238_022,
           |0 as kpi_0238_023,
           |0 as kpi_0238_024,
           |0 as kpi_0238_025,
           |0 as kpi_0238_026,
           |0 as kpi_0239_001,
           |0 as kpi_0239_002,
           |0 as kpi_0239_003,
           |0 as kpi_0239_004,
           |0 as kpi_0239_005,
           |0 as kpi_0239_006,
           |0 as kpi_0239_007,
           |0 as kpi_0239_008,
           |0 as kpi_0239_009,
           |0 as kpi_0239_010,
           |0 as kpi_0239_011,
           |0 as kpi_0239_012,
           |0 as kpi_0239_013,
           |0 as kpi_0239_014,
           |0 as kpi_0239_015,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from t_epc_ip_table_tmp where ne_type = 1) a
           |inner join
           |(select * from tb_xdr_s1u_http_par_dwd where p_city=${p_city} and p_hour=${p_hour}) b
           |on a.local_addr = b.ip_sgw and a.city_id = b.p_city
           |group by
           |cgi,ip_sgw,ems_id
        """.stripMargin)


      session.sql(
        s"""
           |insert into table xdr_4g_enb_cell_s1u_h
           |select
           |ip_sgw as sgw_ip,
           |ems_id as equip_ems_id,
           |cgi,
           |null as qci,
           |0 as kpi_0200_001,
           |0 as kpi_0200_002,
           |0 as kpi_0200_003,
           |0 as kpi_0200_004,
           |0 as kpi_0201_001,
           |0 as kpi_0201_002,
           |0 as kpi_0201_003,
           |0 as kpi_0201_004,
           |0 as kpi_0202_001,
           |0 as kpi_0202_002,
           |0 as kpi_0202_003,
           |0 as kpi_0203_001,
           |0 as kpi_0203_002,
           |0 as kpi_0203_003,
           |0 as kpi_0203_004,
           |0 as kpi_0203_005,
           |0 as kpi_0204_001,
           |0 as kpi_0204_002,
           |0 as kpi_0204_003,
           |0 as kpi_0204_004,
           |0 as kpi_0204_005,
           |0 as kpi_0205_001,
           |0 as kpi_0205_002,
           |0 as kpi_0205_003,
           |0 as kpi_0205_004,
           |0 as kpi_0205_005,
           |0 as kpi_0206_001,
           |0 as kpi_0206_002,
           |0 as kpi_0206_003,
           |0 as kpi_0206_004,
           |0 as kpi_0206_005,
           |0 as kpi_0206_006,
           |0 as kpi_0207_001,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and cast(a.SERVNAME as int) not in (652,658,2000066,2000064,2000067,800034,2000065,2000068) then TCP_SYNNUM else 0 end) as KPI_0207_002,
           |0 as kpi_0207_003,
           |0 as kpi_0207_004,
           |0 as kpi_0207_005,
           |0 as kpi_0208_001,
           |0 as kpi_0208_002,
           |0 as kpi_0208_003,
           |0 as kpi_0208_004,
           |0 as kpi_0209_001,
           |0 as kpi_0209_002,
           |0 as kpi_0209_003,
           |0 as kpi_0209_004,
           |0 as kpi_0209_005,
           |0 as kpi_0209_006,
           |0 as kpi_0209_007,
           |0 as kpi_0209_008,
           |0 as kpi_0209_009,
           |0 as kpi_0209_010,
           |0 as kpi_0209_011,
           |0 as kpi_0209_012,
           |0 as kpi_0209_013,
           |0 as kpi_0209_014,
           |0 as kpi_0209_015,
           |0 as kpi_0209_016,
           |0 as kpi_0209_017,
           |0 as kpi_0209_018,
           |0 as kpi_0210_001,
           |0 as kpi_0210_002,
           |0 as kpi_0210_003,
           |0 as kpi_0210_004,
           |0 as kpi_0210_005,
           |0 as kpi_0210_006,
           |0 as kpi_0210_007,
           |0 as kpi_0210_008,
           |0 as kpi_0210_009,
           |0 as kpi_0210_010,
           |0 as kpi_0210_011,
           |0 as kpi_0210_012,
           |0 as kpi_0210_013,
           |0 as kpi_0210_014,
           |0 as kpi_0210_015,
           |0 as kpi_0210_016,
           |0 as kpi_0210_017,
           |0 as kpi_0210_018,
           |0 as kpi_0210_019,
           |0 as kpi_0210_020,
           |0 as kpi_0210_021,
           |0 as kpi_0210_022,
           |0 as kpi_0210_023,
           |0 as kpi_0210_024,
           |0 as kpi_0210_025,
           |0 as kpi_0210_026,
           |0 as kpi_0210_027,
           |0 as kpi_0238_001,
           |0 as kpi_0238_002,
           |0 as kpi_0238_003,
           |0 as kpi_0238_004,
           |0 as kpi_0238_005,
           |0 as kpi_0238_006,
           |0 as kpi_0238_007,
           |0 as kpi_0238_008,
           |0 as kpi_0238_009,
           |0 as kpi_0238_010,
           |0 as kpi_0238_011,
           |0 as kpi_0238_012,
           |0 as kpi_0238_013,
           |0 as kpi_0238_014,
           |0 as kpi_0238_015,
           |0 as kpi_0238_016,
           |0 as kpi_0238_017,
           |0 as kpi_0238_018,
           |0 as kpi_0238_019,
           |0 as kpi_0238_020,
           |0 as kpi_0238_021,
           |0 as kpi_0238_022,
           |0 as kpi_0238_023,
           |0 as kpi_0238_024,
           |0 as kpi_0238_025,
           |0 as kpi_0238_026,
           |0 as kpi_0239_001,
           |0 as kpi_0239_002,
           |0 as kpi_0239_003,
           |0 as kpi_0239_004,
           |0 as kpi_0239_005,
           |0 as kpi_0239_006,
           |0 as kpi_0239_007,
           |0 as kpi_0239_008,
           |0 as kpi_0239_009,
           |0 as kpi_0239_010,
           |0 as kpi_0239_011,
           |0 as kpi_0239_012,
           |0 as kpi_0239_013,
           |0 as kpi_0239_014,
           |0 as kpi_0239_015,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from tb_xdr_s1u_http_par_dwd where p_city=${p_city} and p_hour='${p_hour}') a
           |inner join
           |(select * from servname_traffic_type where traffic_name = '手机游戏') b
           |on a.servname = b.servname
           |inner join
           |(select * from t_epc_ip_table_tmp where ne_type = 1) c
           |on a.ip_sgw = c.local_addr and a.p_city = c.city_id
           |group by
           |cgi,ip_sgw,ems_id
        """.stripMargin)


      session.sql(
        s"""
           |insert into table xdr_4g_enb_cell_s1u_h
           |select
           |sgw_ggsn_ip as sgw_ip,
           |ems_id as equip_ems_id,
           |null as cgi,
           |null as qci,
           |0 as kpi_0200_001,
           |0 as kpi_0200_002,
           |0 as kpi_0200_003,
           |0 as kpi_0200_004,
           |0 as kpi_0201_001,
           |0 as kpi_0201_002,
           |0 as kpi_0201_003,
           |0 as kpi_0201_004,
           |sum(case when rat = 6 and RESPONSE_CODE = 0 then RESPONSE_NUMBER else 0 end) as KPI_0202_001,
           |sum(case when rat = 6 then REQUEST_TIMES else 0 end) as KPI_0202_002,
           |sum(case when rat = 6 and RESPONSE_CODE = 0 then unix_timestamp(split(endtime,'\\.')[0])*1000000+cast(split(endtime,'\\.')[1] as int)-unix_timestamp(split(starttime,'\\.')[0])*1000000-cast(split(starttime,'\\.')[1] as int) else 0 end) as KPI_0202_003,
           |0 as kpi_0203_001,
           |0 as kpi_0203_002,
           |0 as kpi_0203_003,
           |0 as kpi_0203_004,
           |0 as kpi_0203_005,
           |0 as kpi_0204_001,
           |0 as kpi_0204_002,
           |0 as kpi_0204_003,
           |0 as kpi_0204_004,
           |0 as kpi_0204_005,
           |0 as kpi_0205_001,
           |0 as kpi_0205_002,
           |0 as kpi_0205_003,
           |0 as kpi_0205_004,
           |0 as kpi_0205_005,
           |0 as kpi_0206_001,
           |0 as kpi_0206_002,
           |0 as kpi_0206_003,
           |0 as kpi_0206_004,
           |0 as kpi_0206_005,
           |0 as kpi_0206_006,
           |0 as kpi_0207_001,
           |0 as KPI_0207_002,
           |0 as kpi_0207_003,
           |0 as kpi_0207_004,
           |0 as kpi_0207_005,
           |0 as kpi_0208_001,
           |0 as kpi_0208_002,
           |0 as kpi_0208_003,
           |0 as kpi_0208_004,
           |0 as kpi_0209_001,
           |0 as kpi_0209_002,
           |0 as kpi_0209_003,
           |0 as kpi_0209_004,
           |0 as kpi_0209_005,
           |0 as kpi_0209_006,
           |0 as kpi_0209_007,
           |0 as kpi_0209_008,
           |0 as kpi_0209_009,
           |0 as kpi_0209_010,
           |0 as kpi_0209_011,
           |0 as kpi_0209_012,
           |0 as kpi_0209_013,
           |0 as kpi_0209_014,
           |0 as kpi_0209_015,
           |0 as kpi_0209_016,
           |0 as kpi_0209_017,
           |0 as kpi_0209_018,
           |0 as kpi_0210_001,
           |0 as kpi_0210_002,
           |0 as kpi_0210_003,
           |0 as kpi_0210_004,
           |0 as kpi_0210_005,
           |0 as kpi_0210_006,
           |0 as kpi_0210_007,
           |0 as kpi_0210_008,
           |0 as kpi_0210_009,
           |0 as kpi_0210_010,
           |0 as kpi_0210_011,
           |0 as kpi_0210_012,
           |0 as kpi_0210_013,
           |0 as kpi_0210_014,
           |0 as kpi_0210_015,
           |0 as kpi_0210_016,
           |0 as kpi_0210_017,
           |0 as kpi_0210_018,
           |0 as kpi_0210_019,
           |0 as kpi_0210_020,
           |0 as kpi_0210_021,
           |0 as kpi_0210_022,
           |0 as kpi_0210_023,
           |0 as kpi_0210_024,
           |0 as kpi_0210_025,
           |0 as kpi_0210_026,
           |0 as kpi_0210_027,
           |0 as kpi_0238_001,
           |0 as kpi_0238_002,
           |0 as kpi_0238_003,
           |0 as kpi_0238_004,
           |0 as kpi_0238_005,
           |0 as kpi_0238_006,
           |0 as kpi_0238_007,
           |0 as kpi_0238_008,
           |0 as kpi_0238_009,
           |0 as kpi_0238_010,
           |0 as kpi_0238_011,
           |0 as kpi_0238_012,
           |0 as kpi_0238_013,
           |0 as kpi_0238_014,
           |0 as kpi_0238_015,
           |0 as kpi_0238_016,
           |0 as kpi_0238_017,
           |0 as kpi_0238_018,
           |0 as kpi_0238_019,
           |0 as kpi_0238_020,
           |0 as kpi_0238_021,
           |0 as kpi_0238_022,
           |0 as kpi_0238_023,
           |0 as kpi_0238_024,
           |0 as kpi_0238_025,
           |0 as kpi_0238_026,
           |0 as kpi_0239_001,
           |0 as kpi_0239_002,
           |0 as kpi_0239_003,
           |0 as kpi_0239_004,
           |0 as kpi_0239_005,
           |0 as kpi_0239_006,
           |0 as kpi_0239_007,
           |0 as kpi_0239_008,
           |0 as kpi_0239_009,
           |0 as kpi_0239_010,
           |0 as kpi_0239_011,
           |0 as kpi_0239_012,
           |0 as kpi_0239_013,
           |0 as kpi_0239_014,
           |0 as kpi_0239_015,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from t_epc_ip_table_tmp where ne_type = 1) a
           |inner join
           |(select * from tb_xdr_s1u_dns_par_dwd where p_city=${p_city} and p_hour=${p_hour}) b
           |on a.local_addr = b.sgw_ggsn_ip and a.city_id = b.p_city
           |group by
           |sgw_ggsn_ip,ems_id
        """.stripMargin)


      session.sql(
        s"""
           |insert into table xdr_4g_enb_cell_s1u_h
           |select
           |ip_sgw as sgw_ip,
           |ems_id as equip_ems_id,
           |cgi,
           |null as qci,
           |0 as kpi_0200_001,
           |0 as kpi_0200_002,
           |0 as kpi_0200_003,
           |0 as kpi_0200_004,
           |0 as kpi_0201_001,
           |0 as kpi_0201_002,
           |0 as kpi_0201_003,
           |0 as kpi_0201_004,
           |0 as kpi_0202_001,
           |0 as kpi_0202_002,
           |0 as kpi_0202_003,
           |0 as kpi_0203_001,
           |0 as kpi_0203_002,
           |0 as kpi_0203_003,
           |0 as kpi_0203_004,
           |0 as kpi_0203_005,
           |0 as kpi_0204_001,
           |0 as kpi_0204_002,
           |0 as kpi_0204_003,
           |0 as kpi_0204_004,
           |0 as kpi_0204_005,
           |0 as kpi_0205_001,
           |0 as kpi_0205_002,
           |0 as kpi_0205_003,
           |0 as kpi_0205_004,
           |0 as kpi_0205_005,
           |0 as kpi_0206_001,
           |0 as kpi_0206_002,
           |0 as kpi_0206_003,
           |0 as kpi_0206_004,
           |0 as kpi_0206_005,
           |0 as kpi_0206_006,
           |0 as kpi_0207_001,
           |0 as KPI_0207_002,
           |0 as kpi_0207_003,
           |0 as kpi_0207_004,
           |0 as kpi_0207_005,
           |0 as kpi_0208_001,
           |0 as kpi_0208_002,
           |0 as kpi_0208_003,
           |0 as kpi_0208_004,
           |count(distinct(case when (u_type = 1 or u_type = 2) and rat = 6 then IMSI  end)) as KPI_0209_001,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then UL_TRAFF else 0 end) as KPI_0209_002,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DL_TRAFF else 0 end) as KPI_0209_003,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then 1 else 0 end) as KPI_0209_004,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DURITION else 0 end) as KPI_0209_005,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then UL_DURARION else 0 end) as KPI_0209_006,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DL_DURARION else 0 end) as KPI_0209_007,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_ULNUM else 0 end) as KPI_0209_008,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_UL else 0 end) as KPI_0209_009,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_DLNUM else 0 end) as KPI_0209_010,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_DL else 0 end) as KPI_0209_011,
           |0 as kpi_0209_012,
           |0 as kpi_0209_013,
           |0 as kpi_0209_014,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0209_015,
           |sum(case when rat = 6 and l4_type = 0 then 1 else 0 end) as KPI_0209_016,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP1ST else 0 end) as KPI_0209_017,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP2ND else 0 end) as KPI_0209_018,
           |0 as kpi_0210_001,
           |0 as kpi_0210_002,
           |0 as kpi_0210_003,
           |0 as kpi_0210_004,
           |0 as kpi_0210_005,
           |0 as kpi_0210_006,
           |0 as kpi_0210_007,
           |0 as kpi_0210_008,
           |0 as kpi_0210_009,
           |0 as kpi_0210_010,
           |0 as kpi_0210_011,
           |0 as kpi_0210_012,
           |0 as kpi_0210_013,
           |0 as kpi_0210_014,
           |0 as kpi_0210_015,
           |0 as kpi_0210_016,
           |0 as kpi_0210_017,
           |0 as kpi_0210_018,
           |0 as kpi_0210_019,
           |0 as kpi_0210_020,
           |0 as kpi_0210_021,
           |0 as kpi_0210_022,
           |0 as kpi_0210_023,
           |0 as kpi_0210_024,
           |0 as kpi_0210_025,
           |0 as kpi_0210_026,
           |0 as kpi_0210_027,
           |0 as kpi_0238_001,
           |0 as kpi_0238_002,
           |0 as kpi_0238_003,
           |0 as kpi_0238_004,
           |0 as kpi_0238_005,
           |0 as kpi_0238_006,
           |0 as kpi_0238_007,
           |0 as kpi_0238_008,
           |0 as kpi_0238_009,
           |0 as kpi_0238_010,
           |0 as kpi_0238_011,
           |0 as kpi_0238_012,
           |0 as kpi_0238_013,
           |0 as kpi_0238_014,
           |0 as kpi_0238_015,
           |0 as kpi_0238_016,
           |0 as kpi_0238_017,
           |0 as kpi_0238_018,
           |0 as kpi_0238_019,
           |0 as kpi_0238_020,
           |0 as kpi_0238_021,
           |0 as kpi_0238_022,
           |0 as kpi_0238_023,
           |0 as kpi_0238_024,
           |0 as kpi_0238_025,
           |0 as kpi_0238_026,
           |0 as kpi_0239_001,
           |0 as kpi_0239_002,
           |0 as kpi_0239_003,
           |0 as kpi_0239_004,
           |0 as kpi_0239_005,
           |0 as kpi_0239_006,
           |0 as kpi_0239_007,
           |0 as kpi_0239_008,
           |0 as kpi_0239_009,
           |0 as kpi_0239_010,
           |0 as kpi_0239_011,
           |0 as kpi_0239_012,
           |0 as kpi_0239_013,
           |0 as kpi_0239_014,
           |0 as kpi_0239_015,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from tb_xdr_s1u_http_par_dwd where p_city=${p_city} and p_hour='${p_hour}') a
           |inner join
           |(select * from servname_traffic_type where traffic_name = '视频') b
           |on a.servname = b.servname
           |inner join
           |(select * from t_epc_ip_table_tmp where ne_type = 1) c
           |on a.ip_sgw = c.local_addr and a.p_city = c.city_id
           |group by
           |cgi,ip_sgw,ems_id
        """.stripMargin)


      session.sql(
        s"""
           |insert into table xdr_4g_enb_cell_s1u_h
           |select
           |ip_sgw as sgw_ip,
           |ems_id as equip_ems_id,
           |cgi,
           |null as qci,
           |0 as kpi_0200_001,
           |0 as kpi_0200_002,
           |0 as kpi_0200_003,
           |0 as kpi_0200_004,
           |0 as kpi_0201_001,
           |0 as kpi_0201_002,
           |0 as kpi_0201_003,
           |0 as kpi_0201_004,
           |0 as KPI_0202_001,
           |0 as KPI_0202_002,
           |0 as KPI_0202_003,
           |0 as kpi_0203_001,
           |0 as kpi_0203_002,
           |0 as kpi_0203_003,
           |0 as kpi_0203_004,
           |0 as kpi_0203_005,
           |0 as kpi_0204_001,
           |0 as kpi_0204_002,
           |0 as kpi_0204_003,
           |0 as kpi_0204_004,
           |0 as kpi_0204_005,
           |0 as kpi_0205_001,
           |0 as kpi_0205_002,
           |0 as kpi_0205_003,
           |0 as kpi_0205_004,
           |0 as kpi_0205_005,
           |0 as kpi_0206_001,
           |0 as kpi_0206_002,
           |0 as kpi_0206_003,
           |0 as kpi_0206_004,
           |0 as kpi_0206_005,
           |0 as kpi_0206_006,
           |0 as kpi_0207_001,
           |0 as KPI_0207_002,
           |0 as kpi_0207_003,
           |0 as kpi_0207_004,
           |0 as kpi_0207_005,
           |0 as kpi_0208_001,
           |0 as kpi_0208_002,
           |0 as kpi_0208_003,
           |0 as kpi_0208_004,
           |0 as kpi_0209_001,
           |0 as kpi_0209_002,
           |0 as kpi_0209_003,
           |0 as kpi_0209_004,
           |0 as kpi_0209_005,
           |0 as kpi_0209_006,
           |0 as kpi_0209_007,
           |0 as kpi_0209_008,
           |0 as kpi_0209_009,
           |0 as kpi_0209_010,
           |0 as kpi_0209_011,
           |sum(case when rat = 6 and VIDEOFAILCAUSE is not null then 1 else 0 end) as KPI_0209_012,
           |sum(case when rat = 6 then VIDEOBLOCKNUM else 0 end) as KPI_0209_013,
           |sum(case when rat = 6 then VIDEOBLOCK_URARION else 0 end) as KPI_0209_014,
           |0 as kpi_0209_015,
           |0 as kpi_0209_016,
           |0 as kpi_0209_017,
           |0 as kpi_0209_018,
           |0 as kpi_0210_001,
           |0 as kpi_0210_002,
           |0 as kpi_0210_003,
           |0 as kpi_0210_004,
           |0 as kpi_0210_005,
           |0 as kpi_0210_006,
           |0 as kpi_0210_007,
           |0 as kpi_0210_008,
           |0 as kpi_0210_009,
           |0 as kpi_0210_010,
           |0 as kpi_0210_011,
           |0 as kpi_0210_012,
           |0 as kpi_0210_013,
           |0 as kpi_0210_014,
           |0 as kpi_0210_015,
           |0 as kpi_0210_016,
           |0 as kpi_0210_017,
           |0 as kpi_0210_018,
           |0 as kpi_0210_019,
           |0 as kpi_0210_020,
           |0 as kpi_0210_021,
           |0 as kpi_0210_022,
           |0 as kpi_0210_023,
           |0 as kpi_0210_024,
           |0 as kpi_0210_025,
           |0 as kpi_0210_026,
           |0 as kpi_0210_027,
           |0 as kpi_0238_001,
           |0 as kpi_0238_002,
           |0 as kpi_0238_003,
           |0 as kpi_0238_004,
           |0 as kpi_0238_005,
           |0 as kpi_0238_006,
           |0 as kpi_0238_007,
           |0 as kpi_0238_008,
           |0 as kpi_0238_009,
           |0 as kpi_0238_010,
           |0 as kpi_0238_011,
           |0 as kpi_0238_012,
           |0 as kpi_0238_013,
           |0 as kpi_0238_014,
           |0 as kpi_0238_015,
           |0 as kpi_0238_016,
           |0 as kpi_0238_017,
           |0 as kpi_0238_018,
           |0 as kpi_0238_019,
           |0 as kpi_0238_020,
           |0 as kpi_0238_021,
           |0 as kpi_0238_022,
           |0 as kpi_0238_023,
           |0 as kpi_0238_024,
           |0 as kpi_0238_025,
           |0 as kpi_0238_026,
           |0 as kpi_0239_001,
           |0 as kpi_0239_002,
           |0 as kpi_0239_003,
           |0 as kpi_0239_004,
           |0 as kpi_0239_005,
           |0 as kpi_0239_006,
           |0 as kpi_0239_007,
           |0 as kpi_0239_008,
           |0 as kpi_0239_009,
           |0 as kpi_0239_010,
           |0 as kpi_0239_011,
           |0 as kpi_0239_012,
           |0 as kpi_0239_013,
           |0 as kpi_0239_014,
           |0 as kpi_0239_015,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from tb_xdr_s1u_streaming_par_dwd where p_city=${p_city} and p_hour='${p_hour}') a
           |inner join
           |(select * from servname_traffic_type where traffic_name = '视频') b
           |on a.servname = b.servname
           |inner join
           |(select * from t_epc_ip_table_tmp where ne_type = 1) c
           |on a.ip_sgw = c.local_addr and a.p_city = c.city_id
           |group by
           |cgi,ip_sgw,ems_id
        """.stripMargin)


      session.sql(
        s"""
           |insert into table xdr_4g_enb_cell_s1u_h
           |select
           |ip_sgw as sgw_ip,
           |ems_id as equip_ems_id,
           |cgi,
           |null as qci,
           |0 as kpi_0200_001,
           |0 as kpi_0200_002,
           |0 as kpi_0200_003,
           |0 as kpi_0200_004,
           |0 as kpi_0201_001,
           |0 as kpi_0201_002,
           |0 as kpi_0201_003,
           |0 as kpi_0201_004,
           |0 as KPI_0202_001,
           |0 as KPI_0202_002,
           |0 as KPI_0202_003,
           |0 as kpi_0203_001,
           |0 as kpi_0203_002,
           |0 as kpi_0203_003,
           |0 as kpi_0203_004,
           |0 as kpi_0203_005,
           |0 as kpi_0204_001,
           |0 as kpi_0204_002,
           |0 as kpi_0204_003,
           |0 as kpi_0204_004,
           |0 as kpi_0204_005,
           |0 as kpi_0205_001,
           |0 as kpi_0205_002,
           |0 as kpi_0205_003,
           |0 as kpi_0205_004,
           |0 as kpi_0205_005,
           |0 as kpi_0206_001,
           |0 as kpi_0206_002,
           |0 as kpi_0206_003,
           |0 as kpi_0206_004,
           |0 as kpi_0206_005,
           |0 as kpi_0206_006,
           |0 as kpi_0207_001,
           |0 as KPI_0207_002,
           |0 as kpi_0207_003,
           |0 as kpi_0207_004,
           |0 as kpi_0207_005,
           |0 as kpi_0208_001,
           |0 as kpi_0208_002,
           |0 as kpi_0208_003,
           |0 as kpi_0208_004,
           |0 as kpi_0209_001,
           |0 as kpi_0209_002,
           |0 as kpi_0209_003,
           |0 as kpi_0209_004,
           |0 as kpi_0209_005,
           |0 as kpi_0209_006,
           |0 as kpi_0209_007,
           |0 as kpi_0209_008,
           |0 as kpi_0209_009,
           |0 as kpi_0209_010,
           |0 as kpi_0209_011,
           |0 as KPI_0209_012,
           |0 as KPI_0209_013,
           |0 as KPI_0209_014,
           |0 as kpi_0209_015,
           |0 as kpi_0209_016,
           |0 as kpi_0209_017,
           |0 as kpi_0209_018,
           |count(distinct(case when (u_type = 1 or u_type = 2) and rat = 6 then IMSI  end)) as KPI_0210_001,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then UL_TRAFF else 0 end) as KPI_0210_002,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DL_TRAFF else 0 end) as KPI_0210_003,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then 1 else 0 end) as KPI_0210_004,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DURITION else 0 end) as KPI_0210_005,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_ULNUM else 0 end) as KPI_0210_006,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_UL else 0 end) as KPI_0210_007,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_DLNUM else 0 end) as KPI_0210_008,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_DL else 0 end) as KPI_0210_009,
           |0 as kpi_0210_010,
           |0 as kpi_0210_011,
           |0 as kpi_0210_012,
           |0 as kpi_0210_013,
           |0 as kpi_0210_014,
           |0 as kpi_0210_015,
           |0 as kpi_0210_016,
           |0 as kpi_0210_017,
           |0 as kpi_0210_018,
           |0 as kpi_0210_019,
           |0 as kpi_0210_020,
           |0 as kpi_0210_021,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then UL_DURARION else 0 end) as KPI_0210_022,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DL_DURARION else 0 end) as KPI_0210_023,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0210_024,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0210_025,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP1ST else 0 end) as KPI_0210_026,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP2ND else 0 end) as KPI_0210_027,
           |0 as kpi_0238_001,
           |0 as kpi_0238_002,
           |0 as kpi_0238_003,
           |0 as kpi_0238_004,
           |0 as kpi_0238_005,
           |0 as kpi_0238_006,
           |0 as kpi_0238_007,
           |0 as kpi_0238_008,
           |0 as kpi_0238_009,
           |0 as kpi_0238_010,
           |0 as kpi_0238_011,
           |0 as kpi_0238_012,
           |0 as kpi_0238_013,
           |0 as kpi_0238_014,
           |0 as kpi_0238_015,
           |0 as kpi_0238_016,
           |0 as kpi_0238_017,
           |0 as kpi_0238_018,
           |0 as kpi_0238_019,
           |0 as kpi_0238_020,
           |0 as kpi_0238_021,
           |0 as kpi_0238_022,
           |0 as kpi_0238_023,
           |0 as kpi_0238_024,
           |0 as kpi_0238_025,
           |0 as kpi_0238_026,
           |0 as kpi_0239_001,
           |0 as kpi_0239_002,
           |0 as kpi_0239_003,
           |0 as kpi_0239_004,
           |0 as kpi_0239_005,
           |0 as kpi_0239_006,
           |0 as kpi_0239_007,
           |0 as kpi_0239_008,
           |0 as kpi_0239_009,
           |0 as kpi_0239_010,
           |0 as kpi_0239_011,
           |0 as kpi_0239_012,
           |0 as kpi_0239_013,
           |0 as kpi_0239_014,
           |0 as kpi_0239_015,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from tb_xdr_s1u_http_par_dwd where p_city=${p_city} and p_hour='${p_hour}') a
           |inner join
           |(select * from servname_traffic_type where traffic_name = '手机游戏') b
           |on a.servname = b.servname
           |inner join
           |(select * from t_epc_ip_table_tmp where ne_type = 1) c
           |on a.ip_sgw = c.local_addr and a.p_city = c.city_id
           |group by
           |cgi,ip_sgw,ems_id
        """.stripMargin)


      session.sql(
        s"""
           |insert into table xdr_4g_enb_cell_s1u_h
           |select
           |ip_sgw as sgw_ip,
           |ems_id as equip_ems_id,
           |cgi,
           |null as qci,
           |0 as kpi_0200_001,
           |0 as kpi_0200_002,
           |0 as kpi_0200_003,
           |0 as kpi_0200_004,
           |0 as kpi_0201_001,
           |0 as kpi_0201_002,
           |0 as kpi_0201_003,
           |0 as kpi_0201_004,
           |0 as KPI_0202_001,
           |0 as KPI_0202_002,
           |0 as KPI_0202_003,
           |0 as kpi_0203_001,
           |0 as kpi_0203_002,
           |0 as kpi_0203_003,
           |0 as kpi_0203_004,
           |0 as kpi_0203_005,
           |0 as kpi_0204_001,
           |0 as kpi_0204_002,
           |0 as kpi_0204_003,
           |0 as kpi_0204_004,
           |0 as kpi_0204_005,
           |0 as kpi_0205_001,
           |0 as kpi_0205_002,
           |0 as kpi_0205_003,
           |0 as kpi_0205_004,
           |0 as kpi_0205_005,
           |0 as kpi_0206_001,
           |0 as kpi_0206_002,
           |0 as kpi_0206_003,
           |0 as kpi_0206_004,
           |0 as kpi_0206_005,
           |0 as kpi_0206_006,
           |0 as kpi_0207_001,
           |0 as KPI_0207_002,
           |0 as kpi_0207_003,
           |0 as kpi_0207_004,
           |0 as kpi_0207_005,
           |0 as kpi_0208_001,
           |0 as kpi_0208_002,
           |0 as kpi_0208_003,
           |0 as kpi_0208_004,
           |0 as kpi_0209_001,
           |0 as kpi_0209_002,
           |0 as kpi_0209_003,
           |0 as kpi_0209_004,
           |0 as kpi_0209_005,
           |0 as kpi_0209_006,
           |0 as kpi_0209_007,
           |0 as kpi_0209_008,
           |0 as kpi_0209_009,
           |0 as kpi_0209_010,
           |0 as kpi_0209_011,
           |0 as KPI_0209_012,
           |0 as KPI_0209_013,
           |0 as KPI_0209_014,
           |0 as kpi_0209_015,
           |0 as kpi_0209_016,
           |0 as kpi_0209_017,
           |0 as kpi_0209_018,
           |0 as kpi_0210_001,
           |0 as kpi_0210_002,
           |0 as kpi_0210_003,
           |0 as kpi_0210_004,
           |0 as kpi_0210_005,
           |0 as kpi_0210_006,
           |0 as kpi_0210_007,
           |0 as kpi_0210_008,
           |0 as kpi_0210_009,
           |max(case when rat = 6 then MAX_UL_JITTER else 0 end) as KPI_0210_010,
           |min(case when rat = 6 then MIN_UL_JITTER else 0 end) as KPI_0210_011,
           |max(case when rat = 6 then MAX_DL_JITTER else 0 end) as KPI_0210_012,
           |min(case when rat = 6 then MIN_DL_JITTER else 0 end) as KPI_0210_013,
           |sum(case when rat = 6 then DL_RTT_LT_TH1_NUM else 0 end) as KPI_0210_014,
           |sum(case when rat = 6 then DL_RTT_TH1_TH2_NUM else 0 end) as KPI_0210_015,
           |sum(case when rat = 6 then DL_RTT_TH2_TH3_NUM else 0 end) as KPI_0210_016,
           |sum(case when rat = 6 then DL_RTT_TH3_TH4_NUM else 0 end) as KPI_0210_017,
           |sum(case when rat = 6 then DL_RTT_GT_TH4_NUM else 0 end) as KPI_0210_018,
           |sum(case when rat = 6 then UL_RTT_LT_TH1_NUM else 0 end) as KPI_0210_019,
           |sum(case when rat = 6 then UL_RTT_TH1_TH2_NUM else 0 end) as KPI_0210_020,
           |sum(case when rat = 6 then UL_RTT_GT_TH2_NUM else 0 end) as KPI_0210_021,
           |0 as kpi_0210_022,
           |0 as kpi_0210_023,
           |0 as kpi_0210_024,
           |0 as kpi_0210_025,
           |0 as kpi_0210_026,
           |0 as kpi_0210_027,
           |0 as kpi_0238_001,
           |0 as kpi_0238_002,
           |0 as kpi_0238_003,
           |0 as kpi_0238_004,
           |0 as kpi_0238_005,
           |0 as kpi_0238_006,
           |0 as kpi_0238_007,
           |0 as kpi_0238_008,
           |0 as kpi_0238_009,
           |0 as kpi_0238_010,
           |0 as kpi_0238_011,
           |0 as kpi_0238_012,
           |0 as kpi_0238_013,
           |0 as kpi_0238_014,
           |0 as kpi_0238_015,
           |0 as kpi_0238_016,
           |0 as kpi_0238_017,
           |0 as kpi_0238_018,
           |0 as kpi_0238_019,
           |0 as kpi_0238_020,
           |0 as kpi_0238_021,
           |0 as kpi_0238_022,
           |0 as kpi_0238_023,
           |0 as kpi_0238_024,
           |0 as kpi_0238_025,
           |0 as kpi_0238_026,
           |0 as kpi_0239_001,
           |0 as kpi_0239_002,
           |0 as kpi_0239_003,
           |0 as kpi_0239_004,
           |0 as kpi_0239_005,
           |0 as kpi_0239_006,
           |0 as kpi_0239_007,
           |0 as kpi_0239_008,
           |0 as kpi_0239_009,
           |0 as kpi_0239_010,
           |0 as kpi_0239_011,
           |0 as kpi_0239_012,
           |0 as kpi_0239_013,
           |0 as kpi_0239_014,
           |0 as kpi_0239_015,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from t_epc_ip_table_tmp where ne_type = 1) a
           |inner join
           |(select * from tb_xdr_s1u_game_par_dwd where p_city=${p_city} and p_hour=${p_hour}) b
           |on a.local_addr = b.ip_sgw and a.city_id = b.p_city
           |group by
           |cgi,ip_sgw,ems_id
        """.stripMargin)

      session.sql(
        s"""
           |insert into table xdr_4g_enb_cell_s1u_h
           |select
           |ip_sgw as sgw_ip,
           |ems_id as equip_ems_id,
           |cgi,
           |null as qci,
           |0 as kpi_0200_001,
           |0 as kpi_0200_002,
           |0 as kpi_0200_003,
           |0 as kpi_0200_004,
           |0 as kpi_0201_001,
           |0 as kpi_0201_002,
           |0 as kpi_0201_003,
           |0 as kpi_0201_004,
           |0 as KPI_0202_001,
           |0 as KPI_0202_002,
           |0 as KPI_0202_003,
           |0 as kpi_0203_001,
           |0 as kpi_0203_002,
           |0 as kpi_0203_003,
           |0 as kpi_0203_004,
           |0 as kpi_0203_005,
           |0 as kpi_0204_001,
           |0 as kpi_0204_002,
           |0 as kpi_0204_003,
           |0 as kpi_0204_004,
           |0 as kpi_0204_005,
           |0 as kpi_0205_001,
           |0 as kpi_0205_002,
           |0 as kpi_0205_003,
           |0 as kpi_0205_004,
           |0 as kpi_0205_005,
           |0 as kpi_0206_001,
           |0 as kpi_0206_002,
           |0 as kpi_0206_003,
           |0 as kpi_0206_004,
           |0 as kpi_0206_005,
           |0 as kpi_0206_006,
           |0 as kpi_0207_001,
           |0 as KPI_0207_002,
           |0 as kpi_0207_003,
           |0 as kpi_0207_004,
           |0 as kpi_0207_005,
           |0 as kpi_0208_001,
           |0 as kpi_0208_002,
           |0 as kpi_0208_003,
           |0 as kpi_0208_004,
           |0 as kpi_0209_001,
           |0 as kpi_0209_002,
           |0 as kpi_0209_003,
           |0 as kpi_0209_004,
           |0 as kpi_0209_005,
           |0 as kpi_0209_006,
           |0 as kpi_0209_007,
           |0 as kpi_0209_008,
           |0 as kpi_0209_009,
           |0 as kpi_0209_010,
           |0 as kpi_0209_011,
           |0 as KPI_0209_012,
           |0 as KPI_0209_013,
           |0 as KPI_0209_014,
           |0 as kpi_0209_015,
           |0 as kpi_0209_016,
           |0 as kpi_0209_017,
           |0 as kpi_0209_018,
           |0 as kpi_0210_001,
           |0 as kpi_0210_002,
           |0 as kpi_0210_003,
           |0 as kpi_0210_004,
           |0 as kpi_0210_005,
           |0 as kpi_0210_006,
           |0 as kpi_0210_007,
           |0 as kpi_0210_008,
           |0 as kpi_0210_009,
           |0 as kpi_0210_010,
           |0 as kpi_0210_011,
           |0 as kpi_0210_012,
           |0 as kpi_0210_013,
           |0 as kpi_0210_014,
           |0 as kpi_0210_015,
           |0 as kpi_0210_016,
           |0 as kpi_0210_017,
           |0 as kpi_0210_018,
           |0 as kpi_0210_019,
           |0 as kpi_0210_020,
           |0 as kpi_0210_021,
           |0 as kpi_0210_022,
           |0 as kpi_0210_023,
           |0 as kpi_0210_024,
           |0 as kpi_0210_025,
           |0 as kpi_0210_026,
           |0 as kpi_0210_027,
           |count(distinct(case when (u_type = 1 or u_type = 2) and rat = 6 then IMSI end)) as KPI_0238_001,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then UL_TRAFF else 0 end) as KPI_0238_002,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DL_TRAFF else 0 end) as KPI_0238_003,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then 1 else 0 end) as KPI_0238_004,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DURITION else 0 end) as KPI_0238_005,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_ULNUM else 0 end) as KPI_0238_006,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_UL else 0 end) as KPI_0238_007,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_DLNUM else 0 end) as KPI_0238_008,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_DL else 0 end) as KPI_0238_009,
           |0 as kpi_0238_010,
           |0 as kpi_0238_011,
           |0 as kpi_0238_012,
           |0 as kpi_0238_013,
           |0 as kpi_0238_014,
           |0 as kpi_0238_015,
           |0 as kpi_0238_016,
           |0 as kpi_0238_017,
           |0 as kpi_0238_018,
           |0 as kpi_0238_019,
           |0 as kpi_0238_020,
           |0 as kpi_0238_021,
           |0 as kpi_0238_022,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0238_023,
           |sum(case when rat = 6 and l4_type = 0 then 1 else 0 end) as KPI_0238_024,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP1ST else 0 end) as KPI_0238_025,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP2ND else 0 end) as KPI_0238_026,
           |0 as kpi_0239_001,
           |0 as kpi_0239_002,
           |0 as kpi_0239_003,
           |0 as kpi_0239_004,
           |0 as kpi_0239_005,
           |0 as kpi_0239_006,
           |0 as kpi_0239_007,
           |0 as kpi_0239_008,
           |0 as kpi_0239_009,
           |0 as kpi_0239_010,
           |0 as kpi_0239_011,
           |0 as kpi_0239_012,
           |0 as kpi_0239_013,
           |0 as kpi_0239_014,
           |0 as kpi_0239_015,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from tb_xdr_s1u_http_par_dwd where p_city=${p_city} and p_hour='${p_hour}') a
           |inner join
           |(select * from servname_traffic_type where traffic_name = '微信') b
           |on a.servname = b.servname
           |inner join
           |(select * from t_epc_ip_table_tmp where ne_type = 1) c
           |on a.ip_sgw = c.local_addr and a.p_city = c.city_id
           |group by
           |cgi,ip_sgw,ems_id
        """.stripMargin)


      session.sql(
        s"""
           |insert into table xdr_4g_enb_cell_s1u_h
           |select
           |ip_sgw as sgw_ip,
           |ems_id as equip_ems_id,
           |cgi,
           |null as qci,
           |0 as kpi_0200_001,
           |0 as kpi_0200_002,
           |0 as kpi_0200_003,
           |0 as kpi_0200_004,
           |0 as kpi_0201_001,
           |0 as kpi_0201_002,
           |0 as kpi_0201_003,
           |0 as kpi_0201_004,
           |0 as KPI_0202_001,
           |0 as KPI_0202_002,
           |0 as KPI_0202_003,
           |0 as kpi_0203_001,
           |0 as kpi_0203_002,
           |0 as kpi_0203_003,
           |0 as kpi_0203_004,
           |0 as kpi_0203_005,
           |0 as kpi_0204_001,
           |0 as kpi_0204_002,
           |0 as kpi_0204_003,
           |0 as kpi_0204_004,
           |0 as kpi_0204_005,
           |0 as kpi_0205_001,
           |0 as kpi_0205_002,
           |0 as kpi_0205_003,
           |0 as kpi_0205_004,
           |0 as kpi_0205_005,
           |0 as kpi_0206_001,
           |0 as kpi_0206_002,
           |0 as kpi_0206_003,
           |0 as kpi_0206_004,
           |0 as kpi_0206_005,
           |0 as kpi_0206_006,
           |0 as kpi_0207_001,
           |0 as KPI_0207_002,
           |0 as kpi_0207_003,
           |0 as kpi_0207_004,
           |0 as kpi_0207_005,
           |0 as kpi_0208_001,
           |0 as kpi_0208_002,
           |0 as kpi_0208_003,
           |0 as kpi_0208_004,
           |0 as kpi_0209_001,
           |0 as kpi_0209_002,
           |0 as kpi_0209_003,
           |0 as kpi_0209_004,
           |0 as kpi_0209_005,
           |0 as kpi_0209_006,
           |0 as kpi_0209_007,
           |0 as kpi_0209_008,
           |0 as kpi_0209_009,
           |0 as kpi_0209_010,
           |0 as kpi_0209_011,
           |0 as KPI_0209_012,
           |0 as KPI_0209_013,
           |0 as KPI_0209_014,
           |0 as kpi_0209_015,
           |0 as kpi_0209_016,
           |0 as kpi_0209_017,
           |0 as kpi_0209_018,
           |0 as kpi_0210_001,
           |0 as kpi_0210_002,
           |0 as kpi_0210_003,
           |0 as kpi_0210_004,
           |0 as kpi_0210_005,
           |0 as kpi_0210_006,
           |0 as kpi_0210_007,
           |0 as kpi_0210_008,
           |0 as kpi_0210_009,
           |0 as kpi_0210_010,
           |0 as kpi_0210_011,
           |0 as kpi_0210_012,
           |0 as kpi_0210_013,
           |0 as kpi_0210_014,
           |0 as kpi_0210_015,
           |0 as kpi_0210_016,
           |0 as kpi_0210_017,
           |0 as kpi_0210_018,
           |0 as kpi_0210_019,
           |0 as kpi_0210_020,
           |0 as kpi_0210_021,
           |0 as kpi_0210_022,
           |0 as kpi_0210_023,
           |0 as kpi_0210_024,
           |0 as kpi_0210_025,
           |0 as kpi_0210_026,
           |0 as kpi_0210_027,
           |0 as kpi_0238_001,
           |0 as kpi_0238_002,
           |0 as kpi_0238_003,
           |0 as kpi_0238_004,
           |0 as kpi_0238_005,
           |0 as kpi_0238_006,
           |0 as kpi_0238_007,
           |0 as kpi_0238_008,
           |0 as kpi_0238_009,
           |0 as kpi_0238_010,
           |0 as kpi_0238_011,
           |0 as kpi_0238_012,
           |0 as kpi_0238_013,
           |0 as kpi_0238_014,
           |0 as kpi_0238_015,
           |0 as kpi_0238_016,
           |0 as kpi_0238_017,
           |0 as kpi_0238_018,
           |0 as kpi_0238_019,
           |0 as kpi_0238_020,
           |0 as kpi_0238_021,
           |0 as kpi_0238_022,
           |0 as kpi_0238_023,
           |0 as kpi_0238_024,
           |0 as kpi_0238_025,
           |0 as kpi_0238_026,
           |count(distinct(case when (u_type = 1 or u_type = 2) and rat = 6 then IMSI end)) as KPI_0239_001,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then UL_TRAFF else 0 end) as KPI_0239_002,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DL_TRAFF else 0 end) as KPI_0239_003,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then 1 else 0 end) as KPI_0239_004,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DURITION else 0 end) as KPI_0239_005,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then UL_DURARION else 0 end) as KPI_0239_006,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DL_DURARION else 0 end) as KPI_0239_007,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_ULNUM else 0 end) as KPI_0239_008,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_UL else 0 end) as KPI_0239_009,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_DLNUM else 0 end) as KPI_0239_010,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_DL else 0 end) as KPI_0239_011,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0239_012,
           |sum(case when rat = 6 and l4_type = 0 then 1 else 0 end) as KPI_0239_013,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP1ST else 0 end) as KPI_0239_014,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP2ND else 0 end) as KPI_0239_015,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from tb_xdr_s1u_http_par_dwd where p_city=${p_city} and p_hour='${p_hour}') a
           |inner join
           |(select * from servname_traffic_type where traffic_name = '微信') b
           |on a.servname = b.servname
           |inner join
           |(select * from t_epc_ip_table_tmp where ne_type = 1) c
           |on a.ip_sgw = c.local_addr and a.p_city = c.city_id
           |group by
           |cgi,ip_sgw,ems_id
        """.stripMargin)


      // HTTP OTHER
      session.sql(
        s"""
           |insert into table xdr_4g_enb_cell_s1u_h
           |select
           |ip_sgw as sgw_ip,
           |ems_id as equip_ems_id,
           |cgi,
           |null as qci,
           |sum(case when u_type = 4 and rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0200_001,
           |sum(case when u_type = 4 and rat = 6 and l4_type = 0 then 1 else 0 end) as KPI_0200_002,
           |sum(case when u_type = 4 and rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP1ST else 0 end) as KPI_0200_003,
           |sum(case when u_type = 4 and rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP2ND else 0 end) as KPI_0200_004,
           |0 as kpi_0201_001,
           |0 as kpi_0201_002,
           |sum(case when u_type = 4 and rat = 6 then TCP_ULRETR_NUM else 0 end) as KPI_0201_003,
           |sum(case when u_type = 4 and rat = 6 then TCP_DLRETR_NUM else 0 end) as KPI_0201_004,
           |0 as KPI_0202_001,
           |0 as KPI_0202_002,
           |0 as KPI_0202_003,
           |0 as kpi_0203_001,
           |0 as kpi_0203_002,
           |0 as kpi_0203_003,
           |0 as kpi_0203_004,
           |0 as kpi_0203_005,
           |0 as kpi_0204_001,
           |0 as kpi_0204_002,
           |0 as kpi_0204_003,
           |0 as kpi_0204_004,
           |0 as kpi_0204_005,
           |0 as kpi_0205_001,
           |0 as kpi_0205_002,
           |0 as kpi_0205_003,
           |0 as kpi_0205_004,
           |0 as kpi_0205_005,
           |0 as kpi_0206_001,
           |0 as kpi_0206_002,
           |0 as kpi_0206_003,
           |0 as kpi_0206_004,
           |0 as kpi_0206_005,
           |0 as kpi_0206_006,
           |0 as kpi_0207_001,
           |0 as kpi_0207_002,
           |0 as kpi_0207_003,
           |0 as kpi_0207_004,
           |0 as kpi_0207_005,
           |0 as kpi_0208_001,
           |0 as kpi_0208_002,
           |0 as kpi_0208_003,
           |0 as kpi_0208_004,
           |0 as kpi_0209_001,
           |0 as kpi_0209_002,
           |0 as kpi_0209_003,
           |0 as kpi_0209_004,
           |0 as kpi_0209_005,
           |0 as kpi_0209_006,
           |0 as kpi_0209_007,
           |0 as kpi_0209_008,
           |0 as kpi_0209_009,
           |0 as kpi_0209_010,
           |0 as kpi_0209_011,
           |0 as kpi_0209_012,
           |0 as kpi_0209_013,
           |0 as kpi_0209_014,
           |0 as kpi_0209_015,
           |0 as kpi_0209_016,
           |0 as kpi_0209_017,
           |0 as kpi_0209_018,
           |0 as kpi_0210_001,
           |0 as kpi_0210_002,
           |0 as kpi_0210_003,
           |0 as kpi_0210_004,
           |0 as kpi_0210_005,
           |0 as kpi_0210_006,
           |0 as kpi_0210_007,
           |0 as kpi_0210_008,
           |0 as kpi_0210_009,
           |0 as kpi_0210_010,
           |0 as kpi_0210_011,
           |0 as kpi_0210_012,
           |0 as kpi_0210_013,
           |0 as kpi_0210_014,
           |0 as kpi_0210_015,
           |0 as kpi_0210_016,
           |0 as kpi_0210_017,
           |0 as kpi_0210_018,
           |0 as kpi_0210_019,
           |0 as kpi_0210_020,
           |0 as kpi_0210_021,
           |0 as kpi_0210_022,
           |0 as kpi_0210_023,
           |0 as kpi_0210_024,
           |0 as kpi_0210_025,
           |0 as kpi_0210_026,
           |0 as kpi_0210_027,
           |0 as kpi_0238_001,
           |0 as kpi_0238_002,
           |0 as kpi_0238_003,
           |0 as kpi_0238_004,
           |0 as kpi_0238_005,
           |0 as kpi_0238_006,
           |0 as kpi_0238_007,
           |0 as kpi_0238_008,
           |0 as kpi_0238_009,
           |0 as kpi_0238_010,
           |0 as kpi_0238_011,
           |0 as kpi_0238_012,
           |0 as kpi_0238_013,
           |0 as kpi_0238_014,
           |0 as kpi_0238_015,
           |0 as kpi_0238_016,
           |0 as kpi_0238_017,
           |0 as kpi_0238_018,
           |0 as kpi_0238_019,
           |0 as kpi_0238_020,
           |0 as kpi_0238_021,
           |0 as kpi_0238_022,
           |0 as kpi_0238_023,
           |0 as kpi_0238_024,
           |0 as kpi_0238_025,
           |0 as kpi_0238_026,
           |0 as kpi_0239_001,
           |0 as kpi_0239_002,
           |0 as kpi_0239_003,
           |0 as kpi_0239_004,
           |0 as kpi_0239_005,
           |0 as kpi_0239_006,
           |0 as kpi_0239_007,
           |0 as kpi_0239_008,
           |0 as kpi_0239_009,
           |0 as kpi_0239_010,
           |0 as kpi_0239_011,
           |0 as kpi_0239_012,
           |0 as kpi_0239_013,
           |0 as kpi_0239_014,
           |0 as kpi_0239_015,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from t_epc_ip_table_tmp where ne_type = 1) a
           |inner join
           |(select * from tb_xdr_s1u_other_par_dwd where p_city=${p_city} and p_hour=${p_hour}) b
           |on a.local_addr = b.ip_sgw and a.city_id = b.p_city
           |group by
           |cgi,ip_sgw,ems_id
        """.stripMargin)

      session.sql(
        s"""
           |insert into table xdr_4g_enb_cell_s1u_h
           |select
           |ip_sgw as sgw_ip,
           |ems_id as equip_ems_id,
           |cgi,
           |null as qci,
           |0 as kpi_0200_001,
           |0 as kpi_0200_002,
           |0 as kpi_0200_003,
           |0 as kpi_0200_004,
           |0 as kpi_0201_001,
           |0 as kpi_0201_002,
           |0 as kpi_0201_003,
           |0 as kpi_0201_004,
           |0 as kpi_0202_001,
           |0 as kpi_0202_002,
           |0 as kpi_0202_003,
           |0 as kpi_0203_001,
           |0 as kpi_0203_002,
           |0 as kpi_0203_003,
           |0 as kpi_0203_004,
           |0 as kpi_0203_005,
           |0 as kpi_0204_001,
           |0 as kpi_0204_002,
           |0 as kpi_0204_003,
           |0 as kpi_0204_004,
           |0 as kpi_0204_005,
           |0 as kpi_0205_001,
           |0 as kpi_0205_002,
           |0 as kpi_0205_003,
           |0 as kpi_0205_004,
           |0 as kpi_0205_005,
           |0 as kpi_0206_001,
           |0 as kpi_0206_002,
           |0 as kpi_0206_003,
           |0 as kpi_0206_004,
           |0 as kpi_0206_005,
           |0 as kpi_0206_006,
           |0 as kpi_0207_001,
           |0 as KPI_0207_002,
           |0 as kpi_0207_003,
           |0 as kpi_0207_004,
           |0 as kpi_0207_005,
           |0 as kpi_0208_001,
           |0 as kpi_0208_002,
           |0 as kpi_0208_003,
           |0 as kpi_0208_004,
           |count(distinct(case when u_type = 4 and rat = 6 then IMSI  end)) as KPI_0209_001,
           |sum(case when u_type = 4 and rat = 6 then UL_TRAFF else 0 end) as KPI_0209_002,
           |sum(case when u_type = 4 and rat = 6 then DL_TRAFF else 0 end) as KPI_0209_003,
           |sum(case when u_type = 4 and rat = 6 then 1 else 0 end) as KPI_0209_004,
           |sum(case when u_type = 4 and rat = 6 then DURITION else 0 end) as KPI_0209_005,
           |sum(case when u_type = 4 and rat = 6 then UL_DURARION else 0 end) as KPI_0209_006,
           |sum(case when u_type = 4 and rat = 6 then DL_DURARION else 0 end) as KPI_0209_007,
           |sum(case when u_type = 4 and rat = 6 then RTT_ULNUM else 0 end) as KPI_0209_008,
           |sum(case when u_type = 4 and rat = 6 then RTT_UL else 0 end) as KPI_0209_009,
           |sum(case when u_type = 4 and rat = 6 then RTT_DLNUM else 0 end) as KPI_0209_010,
           |sum(case when u_type = 4 and rat = 6 then RTT_DL else 0 end) as KPI_0209_011,
           |0 as kpi_0209_012,
           |0 as kpi_0209_013,
           |0 as kpi_0209_014,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0209_015,
           |sum(case when rat = 6 and l4_type = 0 then 1 else 0 end) as KPI_0209_016,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP1ST else 0 end) as KPI_0209_017,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP2ND else 0 end) as KPI_0209_018,
           |0 as kpi_0210_001,
           |0 as kpi_0210_002,
           |0 as kpi_0210_003,
           |0 as kpi_0210_004,
           |0 as kpi_0210_005,
           |0 as kpi_0210_006,
           |0 as kpi_0210_007,
           |0 as kpi_0210_008,
           |0 as kpi_0210_009,
           |0 as kpi_0210_010,
           |0 as kpi_0210_011,
           |0 as kpi_0210_012,
           |0 as kpi_0210_013,
           |0 as kpi_0210_014,
           |0 as kpi_0210_015,
           |0 as kpi_0210_016,
           |0 as kpi_0210_017,
           |0 as kpi_0210_018,
           |0 as kpi_0210_019,
           |0 as kpi_0210_020,
           |0 as kpi_0210_021,
           |0 as kpi_0210_022,
           |0 as kpi_0210_023,
           |0 as kpi_0210_024,
           |0 as kpi_0210_025,
           |0 as kpi_0210_026,
           |0 as kpi_0210_027,
           |0 as kpi_0238_001,
           |0 as kpi_0238_002,
           |0 as kpi_0238_003,
           |0 as kpi_0238_004,
           |0 as kpi_0238_005,
           |0 as kpi_0238_006,
           |0 as kpi_0238_007,
           |0 as kpi_0238_008,
           |0 as kpi_0238_009,
           |0 as kpi_0238_010,
           |0 as kpi_0238_011,
           |0 as kpi_0238_012,
           |0 as kpi_0238_013,
           |0 as kpi_0238_014,
           |0 as kpi_0238_015,
           |0 as kpi_0238_016,
           |0 as kpi_0238_017,
           |0 as kpi_0238_018,
           |0 as kpi_0238_019,
           |0 as kpi_0238_020,
           |0 as kpi_0238_021,
           |0 as kpi_0238_022,
           |0 as kpi_0238_023,
           |0 as kpi_0238_024,
           |0 as kpi_0238_025,
           |0 as kpi_0238_026,
           |0 as kpi_0239_001,
           |0 as kpi_0239_002,
           |0 as kpi_0239_003,
           |0 as kpi_0239_004,
           |0 as kpi_0239_005,
           |0 as kpi_0239_006,
           |0 as kpi_0239_007,
           |0 as kpi_0239_008,
           |0 as kpi_0239_009,
           |0 as kpi_0239_010,
           |0 as kpi_0239_011,
           |0 as kpi_0239_012,
           |0 as kpi_0239_013,
           |0 as kpi_0239_014,
           |0 as kpi_0239_015,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from tb_xdr_s1u_other_par_dwd where p_city=${p_city} and p_hour='${p_hour}') a
           |inner join
           |(select * from servname_traffic_type where traffic_name = '视频') b
           |on a.servname = b.servname
           |inner join
           |(select * from t_epc_ip_table_tmp where ne_type = 1) c
           |on a.ip_sgw = c.local_addr and a.p_city = c.city_id
           |group by
           |cgi,ip_sgw,ems_id
        """.stripMargin)


      session.sql(
        s"""
           |insert into table xdr_4g_enb_cell_s1u_h
           |select
           |ip_sgw as sgw_ip,
           |ems_id as equip_ems_id,
           |cgi,
           |null as qci,
           |0 as kpi_0200_001,
           |0 as kpi_0200_002,
           |0 as kpi_0200_003,
           |0 as kpi_0200_004,
           |0 as kpi_0201_001,
           |0 as kpi_0201_002,
           |0 as kpi_0201_003,
           |0 as kpi_0201_004,
           |0 as KPI_0202_001,
           |0 as KPI_0202_002,
           |0 as KPI_0202_003,
           |0 as kpi_0203_001,
           |0 as kpi_0203_002,
           |0 as kpi_0203_003,
           |0 as kpi_0203_004,
           |0 as kpi_0203_005,
           |0 as kpi_0204_001,
           |0 as kpi_0204_002,
           |0 as kpi_0204_003,
           |0 as kpi_0204_004,
           |0 as kpi_0204_005,
           |0 as kpi_0205_001,
           |0 as kpi_0205_002,
           |0 as kpi_0205_003,
           |0 as kpi_0205_004,
           |0 as kpi_0205_005,
           |0 as kpi_0206_001,
           |0 as kpi_0206_002,
           |0 as kpi_0206_003,
           |0 as kpi_0206_004,
           |0 as kpi_0206_005,
           |0 as kpi_0206_006,
           |0 as kpi_0207_001,
           |0 as KPI_0207_002,
           |0 as kpi_0207_003,
           |0 as kpi_0207_004,
           |0 as kpi_0207_005,
           |0 as kpi_0208_001,
           |0 as kpi_0208_002,
           |0 as kpi_0208_003,
           |0 as kpi_0208_004,
           |0 as kpi_0209_001,
           |0 as kpi_0209_002,
           |0 as kpi_0209_003,
           |0 as kpi_0209_004,
           |0 as kpi_0209_005,
           |0 as kpi_0209_006,
           |0 as kpi_0209_007,
           |0 as kpi_0209_008,
           |0 as kpi_0209_009,
           |0 as kpi_0209_010,
           |0 as kpi_0209_011,
           |0 as KPI_0209_012,
           |0 as KPI_0209_013,
           |0 as KPI_0209_014,
           |0 as kpi_0209_015,
           |0 as kpi_0209_016,
           |0 as kpi_0209_017,
           |0 as kpi_0209_018,
           |count(distinct(case when u_type = 4 and rat = 6 then IMSI  end)) as KPI_0210_001,
           |sum(case when u_type = 4 and rat = 6 then UL_TRAFF else 0 end) as KPI_0210_002,
           |sum(case when u_type = 4 and rat = 6 then DL_TRAFF else 0 end) as KPI_0210_003,
           |sum(case when u_type = 4 and rat = 6 then 1 else 0 end) as KPI_0210_004,
           |sum(case when u_type = 4 and rat = 6 then DURITION else 0 end) as KPI_0210_005,
           |sum(case when u_type = 4 and rat = 6 then RTT_ULNUM else 0 end) as KPI_0210_006,
           |sum(case when u_type = 4 and rat = 6 then RTT_UL else 0 end) as KPI_0210_007,
           |sum(case when u_type = 4 and rat = 6 then RTT_DLNUM else 0 end) as KPI_0210_008,
           |sum(case when u_type = 4 and rat = 6 then RTT_DL else 0 end) as KPI_0210_009,
           |0 as kpi_0210_010,
           |0 as kpi_0210_011,
           |0 as kpi_0210_012,
           |0 as kpi_0210_013,
           |0 as kpi_0210_014,
           |0 as kpi_0210_015,
           |0 as kpi_0210_016,
           |0 as kpi_0210_017,
           |0 as kpi_0210_018,
           |0 as kpi_0210_019,
           |0 as kpi_0210_020,
           |0 as kpi_0210_021,
           |sum(case when u_type = 4 and rat = 6 then UL_DURARION else 0 end) as KPI_0210_022,
           |sum(case when u_type = 4 and rat = 6 then DL_DURARION else 0 end) as KPI_0210_023,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0210_024,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0210_025,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP1ST else 0 end) as KPI_0210_026,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP2ND else 0 end) as KPI_0210_027,
           |0 as kpi_0238_001,
           |0 as kpi_0238_002,
           |0 as kpi_0238_003,
           |0 as kpi_0238_004,
           |0 as kpi_0238_005,
           |0 as kpi_0238_006,
           |0 as kpi_0238_007,
           |0 as kpi_0238_008,
           |0 as kpi_0238_009,
           |0 as kpi_0238_010,
           |0 as kpi_0238_011,
           |0 as kpi_0238_012,
           |0 as kpi_0238_013,
           |0 as kpi_0238_014,
           |0 as kpi_0238_015,
           |0 as kpi_0238_016,
           |0 as kpi_0238_017,
           |0 as kpi_0238_018,
           |0 as kpi_0238_019,
           |0 as kpi_0238_020,
           |0 as kpi_0238_021,
           |0 as kpi_0238_022,
           |0 as kpi_0238_023,
           |0 as kpi_0238_024,
           |0 as kpi_0238_025,
           |0 as kpi_0238_026,
           |0 as kpi_0239_001,
           |0 as kpi_0239_002,
           |0 as kpi_0239_003,
           |0 as kpi_0239_004,
           |0 as kpi_0239_005,
           |0 as kpi_0239_006,
           |0 as kpi_0239_007,
           |0 as kpi_0239_008,
           |0 as kpi_0239_009,
           |0 as kpi_0239_010,
           |0 as kpi_0239_011,
           |0 as kpi_0239_012,
           |0 as kpi_0239_013,
           |0 as kpi_0239_014,
           |0 as kpi_0239_015,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from tb_xdr_s1u_other_par_dwd where p_city=${p_city} and p_hour='${p_hour}') a
           |inner join
           |(select * from servname_traffic_type where traffic_name = '手机游戏') b
           |on a.servname = b.servname
           |inner join
           |(select * from t_epc_ip_table_tmp where ne_type = 1) c
           |on a.ip_sgw = c.local_addr and a.p_city = c.city_id
           |group by
           |cgi,ip_sgw,ems_id
        """.stripMargin)

      session.sql(
        s"""
           |insert into table xdr_4g_enb_cell_s1u_h
           |select
           |ip_sgw as sgw_ip,
           |ems_id as equip_ems_id,
           |cgi,
           |null as qci,
           |0 as kpi_0200_001,
           |0 as kpi_0200_002,
           |0 as kpi_0200_003,
           |0 as kpi_0200_004,
           |0 as kpi_0201_001,
           |0 as kpi_0201_002,
           |0 as kpi_0201_003,
           |0 as kpi_0201_004,
           |0 as KPI_0202_001,
           |0 as KPI_0202_002,
           |0 as KPI_0202_003,
           |0 as kpi_0203_001,
           |0 as kpi_0203_002,
           |0 as kpi_0203_003,
           |0 as kpi_0203_004,
           |0 as kpi_0203_005,
           |0 as kpi_0204_001,
           |0 as kpi_0204_002,
           |0 as kpi_0204_003,
           |0 as kpi_0204_004,
           |0 as kpi_0204_005,
           |0 as kpi_0205_001,
           |0 as kpi_0205_002,
           |0 as kpi_0205_003,
           |0 as kpi_0205_004,
           |0 as kpi_0205_005,
           |0 as kpi_0206_001,
           |0 as kpi_0206_002,
           |0 as kpi_0206_003,
           |0 as kpi_0206_004,
           |0 as kpi_0206_005,
           |0 as kpi_0206_006,
           |0 as kpi_0207_001,
           |0 as KPI_0207_002,
           |0 as kpi_0207_003,
           |0 as kpi_0207_004,
           |0 as kpi_0207_005,
           |0 as kpi_0208_001,
           |0 as kpi_0208_002,
           |0 as kpi_0208_003,
           |0 as kpi_0208_004,
           |0 as kpi_0209_001,
           |0 as kpi_0209_002,
           |0 as kpi_0209_003,
           |0 as kpi_0209_004,
           |0 as kpi_0209_005,
           |0 as kpi_0209_006,
           |0 as kpi_0209_007,
           |0 as kpi_0209_008,
           |0 as kpi_0209_009,
           |0 as kpi_0209_010,
           |0 as kpi_0209_011,
           |0 as KPI_0209_012,
           |0 as KPI_0209_013,
           |0 as KPI_0209_014,
           |0 as kpi_0209_015,
           |0 as kpi_0209_016,
           |0 as kpi_0209_017,
           |0 as kpi_0209_018,
           |0 as kpi_0210_001,
           |0 as kpi_0210_002,
           |0 as kpi_0210_003,
           |0 as kpi_0210_004,
           |0 as kpi_0210_005,
           |0 as kpi_0210_006,
           |0 as kpi_0210_007,
           |0 as kpi_0210_008,
           |0 as kpi_0210_009,
           |0 as kpi_0210_010,
           |0 as kpi_0210_011,
           |0 as kpi_0210_012,
           |0 as kpi_0210_013,
           |0 as kpi_0210_014,
           |0 as kpi_0210_015,
           |0 as kpi_0210_016,
           |0 as kpi_0210_017,
           |0 as kpi_0210_018,
           |0 as kpi_0210_019,
           |0 as kpi_0210_020,
           |0 as kpi_0210_021,
           |0 as kpi_0210_022,
           |0 as kpi_0210_023,
           |0 as kpi_0210_024,
           |0 as kpi_0210_025,
           |0 as kpi_0210_026,
           |0 as kpi_0210_027,
           |count(distinct(case when u_type = 4 and rat = 6 then IMSI end)) as KPI_0238_001,
           |sum(case when u_type = 4 and rat = 6 then UL_TRAFF else 0 end) as KPI_0238_002,
           |sum(case when u_type = 4 and rat = 6 then DL_TRAFF else 0 end) as KPI_0238_003,
           |sum(case when u_type = 4 and rat = 6 then 1 else 0 end) as KPI_0238_004,
           |sum(case when u_type = 4 and rat = 6 then DURITION else 0 end) as KPI_0238_005,
           |sum(case when u_type = 4 and rat = 6 then RTT_ULNUM else 0 end) as KPI_0238_006,
           |sum(case when u_type = 4 and rat = 6 then RTT_UL else 0 end) as KPI_0238_007,
           |sum(case when u_type = 4 and rat = 6 then RTT_DLNUM else 0 end) as KPI_0238_008,
           |sum(case when u_type = 4 and rat = 6 then RTT_DL else 0 end) as KPI_0238_009,
           |0 as kpi_0238_010,
           |0 as kpi_0238_011,
           |0 as kpi_0238_012,
           |0 as kpi_0238_013,
           |0 as kpi_0238_014,
           |0 as kpi_0238_015,
           |0 as kpi_0238_016,
           |0 as kpi_0238_017,
           |0 as kpi_0238_018,
           |0 as kpi_0238_019,
           |0 as kpi_0238_020,
           |0 as kpi_0238_021,
           |0 as kpi_0238_022,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0238_023,
           |sum(case when rat = 6 and l4_type = 0 then 1 else 0 end) as KPI_0238_024,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP1ST else 0 end) as KPI_0238_025,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP2ND else 0 end) as KPI_0238_026,
           |0 as kpi_0239_001,
           |0 as kpi_0239_002,
           |0 as kpi_0239_003,
           |0 as kpi_0239_004,
           |0 as kpi_0239_005,
           |0 as kpi_0239_006,
           |0 as kpi_0239_007,
           |0 as kpi_0239_008,
           |0 as kpi_0239_009,
           |0 as kpi_0239_010,
           |0 as kpi_0239_011,
           |0 as kpi_0239_012,
           |0 as kpi_0239_013,
           |0 as kpi_0239_014,
           |0 as kpi_0239_015,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from tb_xdr_s1u_other_par_dwd where p_city=${p_city} and p_hour='${p_hour}') a
           |inner join
           |(select * from servname_traffic_type where traffic_name = '微信') b
           |on a.servname = b.servname
           |inner join
           |(select * from t_epc_ip_table_tmp where ne_type = 1) c
           |on a.ip_sgw = c.local_addr and a.p_city = c.city_id
           |group by
           |cgi,ip_sgw,ems_id
        """.stripMargin)

      session.sql(
        s"""
           |insert into table xdr_4g_enb_cell_s1u_h
           |select
           |ip_sgw as sgw_ip,
           |ems_id as equip_ems_id,
           |cgi,
           |null as qci,
           |0 as kpi_0200_001,
           |0 as kpi_0200_002,
           |0 as kpi_0200_003,
           |0 as kpi_0200_004,
           |0 as kpi_0201_001,
           |0 as kpi_0201_002,
           |0 as kpi_0201_003,
           |0 as kpi_0201_004,
           |0 as KPI_0202_001,
           |0 as KPI_0202_002,
           |0 as KPI_0202_003,
           |0 as kpi_0203_001,
           |0 as kpi_0203_002,
           |0 as kpi_0203_003,
           |0 as kpi_0203_004,
           |0 as kpi_0203_005,
           |0 as kpi_0204_001,
           |0 as kpi_0204_002,
           |0 as kpi_0204_003,
           |0 as kpi_0204_004,
           |0 as kpi_0204_005,
           |0 as kpi_0205_001,
           |0 as kpi_0205_002,
           |0 as kpi_0205_003,
           |0 as kpi_0205_004,
           |0 as kpi_0205_005,
           |0 as kpi_0206_001,
           |0 as kpi_0206_002,
           |0 as kpi_0206_003,
           |0 as kpi_0206_004,
           |0 as kpi_0206_005,
           |0 as kpi_0206_006,
           |0 as kpi_0207_001,
           |0 as KPI_0207_002,
           |0 as kpi_0207_003,
           |0 as kpi_0207_004,
           |0 as kpi_0207_005,
           |0 as kpi_0208_001,
           |0 as kpi_0208_002,
           |0 as kpi_0208_003,
           |0 as kpi_0208_004,
           |0 as kpi_0209_001,
           |0 as kpi_0209_002,
           |0 as kpi_0209_003,
           |0 as kpi_0209_004,
           |0 as kpi_0209_005,
           |0 as kpi_0209_006,
           |0 as kpi_0209_007,
           |0 as kpi_0209_008,
           |0 as kpi_0209_009,
           |0 as kpi_0209_010,
           |0 as kpi_0209_011,
           |0 as KPI_0209_012,
           |0 as KPI_0209_013,
           |0 as KPI_0209_014,
           |0 as kpi_0209_015,
           |0 as kpi_0209_016,
           |0 as kpi_0209_017,
           |0 as kpi_0209_018,
           |0 as kpi_0210_001,
           |0 as kpi_0210_002,
           |0 as kpi_0210_003,
           |0 as kpi_0210_004,
           |0 as kpi_0210_005,
           |0 as kpi_0210_006,
           |0 as kpi_0210_007,
           |0 as kpi_0210_008,
           |0 as kpi_0210_009,
           |0 as kpi_0210_010,
           |0 as kpi_0210_011,
           |0 as kpi_0210_012,
           |0 as kpi_0210_013,
           |0 as kpi_0210_014,
           |0 as kpi_0210_015,
           |0 as kpi_0210_016,
           |0 as kpi_0210_017,
           |0 as kpi_0210_018,
           |0 as kpi_0210_019,
           |0 as kpi_0210_020,
           |0 as kpi_0210_021,
           |0 as kpi_0210_022,
           |0 as kpi_0210_023,
           |0 as kpi_0210_024,
           |0 as kpi_0210_025,
           |0 as kpi_0210_026,
           |0 as kpi_0210_027,
           |0 as kpi_0238_001,
           |0 as kpi_0238_002,
           |0 as kpi_0238_003,
           |0 as kpi_0238_004,
           |0 as kpi_0238_005,
           |0 as kpi_0238_006,
           |0 as kpi_0238_007,
           |0 as kpi_0238_008,
           |0 as kpi_0238_009,
           |0 as kpi_0238_010,
           |0 as kpi_0238_011,
           |0 as kpi_0238_012,
           |0 as kpi_0238_013,
           |0 as kpi_0238_014,
           |0 as kpi_0238_015,
           |0 as kpi_0238_016,
           |0 as kpi_0238_017,
           |0 as kpi_0238_018,
           |0 as kpi_0238_019,
           |0 as kpi_0238_020,
           |0 as kpi_0238_021,
           |0 as kpi_0238_022,
           |0 as kpi_0238_023,
           |0 as kpi_0238_024,
           |0 as kpi_0238_025,
           |0 as kpi_0238_026,
           |count(distinct(case when u_type = 4 and rat = 6 then IMSI end)) as KPI_0239_001,
           |sum(case when u_type = 4 and rat = 6 then UL_TRAFF else 0 end) as KPI_0239_002,
           |sum(case when u_type = 4 and rat = 6 then DL_TRAFF else 0 end) as KPI_0239_003,
           |sum(case when u_type = 4 and rat = 6 then 1 else 0 end) as KPI_0239_004,
           |sum(case when u_type = 4 and rat = 6 then DURITION else 0 end) as KPI_0239_005,
           |sum(case when u_type = 4 and rat = 6 then UL_DURARION else 0 end) as KPI_0239_006,
           |sum(case when u_type = 4 and rat = 6 then DL_DURARION else 0 end) as KPI_0239_007,
           |sum(case when u_type = 4 and rat = 6 then RTT_ULNUM else 0 end) as KPI_0239_008,
           |sum(case when u_type = 4 and rat = 6 then RTT_UL else 0 end) as KPI_0239_009,
           |sum(case when u_type = 4 and rat = 6 then RTT_DLNUM else 0 end) as KPI_0239_010,
           |sum(case when u_type = 4 and rat = 6 then RTT_DL else 0 end) as KPI_0239_011,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0239_012,
           |sum(case when rat = 6 and l4_type = 0 then 1 else 0 end) as KPI_0239_013,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP1ST else 0 end) as KPI_0239_014,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP2ND else 0 end) as KPI_0239_015,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from tb_xdr_s1u_other_par_dwd where p_city=${p_city} and p_hour='${p_hour}') a
           |inner join
           |(select * from servname_traffic_type where traffic_name = '微信') b
           |on a.servname = b.servname
           |inner join
           |(select * from t_epc_ip_table_tmp where ne_type = 1) c
           |on a.ip_sgw = c.local_addr and a.p_city = c.city_id
           |group by
           |cgi,ip_sgw,ems_id
        """.stripMargin)
    }

    def hiveWriteToPG(pg_ip: String, pg_port: Int, DDB: String, p_city: String, p_hour: String): Unit = {
      session.sql(s"use ${DDB}")

      session.sql(
        s"""
           |select * from xdr_4g_enb_cell_s1u_h
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable", s"public.xdr_4g_enb_cell_s1u_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_s1u_h finish")
    }


    // 查询统计指标
    // 写入PG库
    def writeToPg(pg_ip: String, pg_port: Int, DDB: String, p_city: String, p_hour: String): Unit = {

      session.sql(s"use ${DDB}")


      // 4G业务面指标统计
      // 写入public.xdr_4g_enb_cell_s1u_h
      // S1-U HTTP
      session.sql(
        s"""
            select cgi,
           |ip_sgw as sgw_ip,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0200_001,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and l4_type = 0 then 1 else 0 end) as KPI_0200_002,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP1ST else 0 end) as KPI_0200_003,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP2ND else 0 end) as KPI_0200_004,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then TCP_ULRETR_NUM else 0 end) as KPI_0201_003,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then TCP_DLRETR_NUM else 0 end) as KPI_0201_004,
           |count(distinct(case when (u_type = 1 or u_type = 2) and rat = 6 then IMSI end)) as KPI_0203_001,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then UL_TRAFF else 0 end) as KPI_0203_002,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DL_TRAFF else 0 end) as KPI_0203_003,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then 1 else 0 end) as KPI_0203_004,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DURITION else 0 end) as KPI_0203_005,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'text%' and cast(HTTP_CODE as int) in (99,400) then 1 else 0 end) as KPI_0204_001,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'text%' then 1 else 0 end) as KPI_0204_002,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'text%' and cast(HTTP_CODE as int) in (99,400) then L7_Delay else 0 end) as KPI_0204_003,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'text%' and cast(HTTP_CODE as int) in (99,400) and (HTTP_LASTACK_DELAY is not null or HTTP_LASTACK_DELAY != 0) then 1 else 0 end) as KPI_0204_004,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'text%' and cast(HTTP_CODE as int) in (99,400) then HTTP_LASTACK_DELAY else 0 end) as KPI_0204_005,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'image%' and cast(HTTP_CODE as int) in (99,400) then 1 else 0 end) as KPI_0205_001,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'image%' then 1 else 0 end) as KPI_0205_002,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'image%' and cast(HTTP_CODE as int) in (99,400) then L7_Delay else 0 end) as KPI_0205_003,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'image%' and cast(HTTP_CODE as int) in (99,400) and (HTTP_LASTACK_DELAY is not null or HTTP_LASTACK_DELAY != 0) then 1 else 0 end) as KPI_0205_004,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'image%' and cast(HTTP_CODE as int) in (99,400) then HTTP_LASTACK_DELAY else 0 end) as KPI_0205_005,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'video%' and cast(HTTP_CODE as int) in (99,400) then 1 else 0 end) as KPI_0206_001,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'video%' then 1 else 0 end) as KPI_0206_002,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'video%' and cast(HTTP_CODE as int) in (99,400) then L7_Delay else 0 end) as KPI_0206_003,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'video%' and cast(HTTP_CODE as int) in (99,400) and (HTTP_LASTACK_DELAY is not null or HTTP_LASTACK_DELAY != 0) then 1 else 0 end) as KPI_0206_004,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 and CONTENT_TYPE like 'video%' and cast(HTTP_CODE as int) in (99,400) then HTTP_LASTACK_DELAY else 0 end) as KPI_0206_005,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from tb_xdr_s1u_http_par_dwd
           |where p_city=${p_city} and p_hour='${p_hour}'
           |group by
           |cgi,
           |ip_sgw
      """.stripMargin
      ).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable", s"public.xdr_4g_enb_cell_s1u_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_s1u_h finish")

      session.sql(
        s"""
           |select cgi,
           |sum(case when rat = 6 and RESPONSE_CODE = 0 then RESPONSE_NUMBER else 0 end) as KPI_0202_001,
           |sum(case when rat = 6 then REQUEST_TIMES else 0 end) as KPI_0202_002,
           |sum(case when rat = 6 and RESPONSE_CODE = 0 then unix_timestamp(split(endtime,'\\.')[0])*1000000+cast(split(endtime,'\\.')[1] as int)-unix_timestamp(split(starttime,'\\.')[0])*1000000-cast(split(starttime,'\\.')[1] as int) else 0 end) as KPI_0202_003,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from xdr_kpi_ods.tb_xdr_s1u_dns_par_dwd where p_city=${p_city} and p_hour='${p_hour}'
           |group by
           |eci
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable", s"public.xdr_4g_enb_cell_s1u_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_s1u_h finish")


      session.sql(
        s"""
           |select cgi,
           |ip_sgw as sgw_ip,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then TCP_SYNNUM else 0 end) as KPI_0207_002,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from tb_xdr_s1u_http_par_dwd where p_city=${p_city} and p_hour='${p_hour}') a
           |inner join
           |(select * from servname_traffic_type where traffic_name = '手机游戏') b
           |on a.servname = b.servname
           |group by cgi,ip_sgw
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable", s"public.xdr_4g_enb_cell_s1u_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_s1u_h finish")


      session.sql(
        s"""
           |select cgi,
           |ip_sgw as sgw_ip,
           |count(distinct(case when (u_type = 1 or u_type = 2) and rat = 6 then IMSI  end)) as KPI_0209_001,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then UL_TRAFF else 0 end) as KPI_0209_002,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DL_TRAFF else 0 end) as KPI_0209_003,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then 1 else 0 end) as KPI_0209_004,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DURITION else 0 end) as KPI_0209_005,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then UL_DURARION else 0 end) as KPI_0209_006,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DL_DURARION else 0 end) as KPI_0209_007,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_ULNUM else 0 end) as KPI_0209_008,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_UL else 0 end) as KPI_0209_009,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_DLNUM else 0 end) as KPI_0209_010,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_DL else 0 end) as KPI_0209_011,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0209_015,
           |sum(case when rat = 6 and l4_type = 0 then 1 else 0 end) as KPI_0209_016,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP1ST else 0 end) as KPI_0209_017,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP2ND else 0 end) as KPI_0209_018,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from tb_xdr_s1u_http_par_dwd where p_city=${p_city} and p_hour='${p_hour}') a
           |inner join
           |(select * from servname_traffic_type where traffic_name = '视频') b
           |on a.servname = b.servname
           |group by cgi,ip_sgw
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable", s"public.xdr_4g_enb_cell_s1u_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_s1u_h finish")


      session.sql(
        s"""
           |select cgi,
           |ip_sgw as sgw_ip,
           |sum(case when rat = 6 and VIDEOFAILCAUSE is not null then 1 else 0 end) as KPI_0209_012,
           |sum(case when rat = 6 then VIDEOBLOCKNUM else 0 end) as KPI_0209_013,
           |sum(case when rat = 6 then VIDEOBLOCK_URARION else 0 end) as KPI_0209_014,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from tb_xdr_s1u_streaming_par_dwd where p_city=${p_city} and p_hour='${p_hour}') a
           |inner join
           |(select * from servname_traffic_type where traffic_name = '视频') b
           |on a.servname = b.servname
           |group by cgi,ip_sgw
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable", s"public.xdr_4g_enb_cell_s1u_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_s1u_h finish")

      session.sql(
        s"""
           |select cgi,
           |ip_sgw as sgw_ip,
           |count(distinct(case when (u_type = 1 or u_type = 2) and rat = 6 then IMSI  end)) as KPI_0210_001,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then UL_TRAFF else 0 end) as KPI_0210_002,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DL_TRAFF else 0 end) as KPI_0210_003,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then 1 else 0 end) as KPI_0210_004,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DURITION else 0 end) as KPI_0210_005,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_ULNUM else 0 end) as KPI_0210_006,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_UL else 0 end) as KPI_0210_007,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_DLNUM else 0 end) as KPI_0210_008,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_DL else 0 end) as KPI_0210_009,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then UL_DURARION else 0 end) as KPI_0210_022,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DL_DURARION else 0 end) as KPI_0210_023,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0210_024,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0210_025,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP1ST else 0 end) as KPI_0210_026,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP2ND else 0 end) as KPI_0210_027,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from tb_xdr_s1u_http_par_dwd where p_city=${p_city} and p_hour='${p_hour}') a
           |inner join
           |(select * from servname_traffic_type where traffic_name = '手机游戏') b
           |on a.servname = b.servname
           |group by cgi,ip_sgw
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable", s"public.xdr_4g_enb_cell_s1u_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_s1u_h finish")


      session.sql(
        s"""
           |select cgi,
           |ip_sgw as sgw_ip,
           |max(case when rat = 6 then MAX_UL_JITTER else 0 end) as KPI_0210_010,
           |min(case when rat = 6 then MIN_UL_JITTER else 0 end) as KPI_0210_011,
           |max(case when rat = 6 then MAX_DL_JITTER else 0 end) as KPI_0210_012,
           |min(case when rat = 6 then MIN_DL_JITTER else 0 end) as KPI_0210_013,
           |sum(case when rat = 6 then DL_RTT_LT_TH1_NUM else 0 end) as KPI_0210_014,
           |sum(case when rat = 6 then DL_RTT_TH1_TH2_NUM else 0 end) as KPI_0210_015,
           |sum(case when rat = 6 then DL_RTT_TH2_TH3_NUM else 0 end) as KPI_0210_016,
           |sum(case when rat = 6 then DL_RTT_TH3_TH4_NUM else 0 end) as KPI_0210_017,
           |sum(case when rat = 6 then DL_RTT_GT_TH4_NUM else 0 end) as KPI_0210_018,
           |sum(case when rat = 6 then UL_RTT_LT_TH1_NUM else 0 end) as KPI_0210_019,
           |sum(case when rat = 6 then UL_RTT_TH1_TH2_NUM else 0 end) as KPI_0210_020,
           |sum(case when rat = 6 then UL_RTT_GT_TH2_NUM else 0 end) as KPI_0210_021,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from tb_xdr_s1u_game_par_dwd where p_city=${p_city} and p_hour='${p_hour}'
           |group by cgi,ip_sgw
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable", s"public.xdr_4g_enb_cell_s1u_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_s1u_h finish")


      session.sql(
        s"""
           |select cgi,
           |ip_sgw as sgw_ip,
           |count(distinct(case when (u_type = 1 or u_type = 2) and rat = 6 then IMSI end)) as KPI_0238_001,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then UL_TRAFF else 0 end) as KPI_0238_002,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DL_TRAFF else 0 end) as KPI_0238_003,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then 1 else 0 end) as KPI_0238_004,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DURITION else 0 end) as KPI_0238_005,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_ULNUM else 0 end) as KPI_0238_006,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_UL else 0 end) as KPI_0238_007,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_DLNUM else 0 end) as KPI_0238_008,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_DL else 0 end) as KPI_0238_009,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0238_023,
           |sum(case when rat = 6 and l4_type = 0 then 1 else 0 end) as KPI_0238_024,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP1ST else 0 end) as KPI_0238_025,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP2ND else 0 end) as KPI_0238_026,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from tb_xdr_s1u_http_par_dwd where p_city=${p_city} and p_hour='${p_hour}') a
           |inner join
           |(select * from servname_traffic_type where traffic_name = '微信') b
           |on a.servname = b.servname
           |group by cgi,ip_sgw
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable", s"public.xdr_4g_enb_cell_s1u_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_s1u_h finish")


      session.sql(
        s"""
           |select cgi,
           |ip_sgw as sgw_ip,
           |count(distinct(case when (u_type = 1 or u_type = 2) and rat = 6 then IMSI end)) as KPI_0239_001,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then UL_TRAFF else 0 end) as KPI_0239_002,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DL_TRAFF else 0 end) as KPI_0239_003,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then 1 else 0 end) as KPI_0239_004,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DURITION else 0 end) as KPI_0239_005,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then UL_DURARION else 0 end) as KPI_0239_006,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then DL_DURARION else 0 end) as KPI_0239_007,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_ULNUM else 0 end) as KPI_0239_008,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_UL else 0 end) as KPI_0239_009,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_DLNUM else 0 end) as KPI_0239_010,
           |sum(case when (u_type = 1 or u_type = 2) and rat = 6 then RTT_DL else 0 end) as KPI_0239_011,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0239_012,
           |sum(case when rat = 6 and l4_type = 0 then 1 else 0 end) as KPI_0239_013,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP1ST else 0 end) as KPI_0239_014,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP2ND else 0 end) as KPI_0239_015,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from tb_xdr_s1u_http_par_dwd where p_city=${p_city} and p_hour='${p_hour}') a
           |inner join
           |(select * from servname_traffic_type where traffic_name = '浏览') b
           |on a.servname = b.servname
           |group by cgi,ip_sgw
        """.stripMargin)



      // S1-U Other
      session.sql(
        s"""
           |select cgi,
           |ip_sgw as sgw_ip,
           |sum(case when u_type = 4 and rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0200_001,
           |sum(case when u_type = 4 and rat = 6 and l4_type = 0 then 1 else 0 end) as KPI_0200_002,
           |sum(case when u_type = 4 and rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP1ST else 0 end) as KPI_0200_003,
           |sum(case when u_type = 4 and rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP2ND else 0 end) as KPI_0200_004,
           |sum(case when u_type = 4 and rat = 6 then TCP_ULRETR_NUM else 0 end) as KPI_0201_003,
           |sum(case when u_type = 4 and rat = 6 then TCP_DLRETR_NUM else 0 end) as KPI_0201_004,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from tb_xdr_s1u_other_par_dwd
           |where p_city=${p_city} and p_hour='${p_hour}'
           |group by
           |cgi,ip_sgw
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable", s"public.xdr_4g_enb_cell_s1u_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_s1u_h finish")

      session.sql(
        s"""
           |select cgi,
           |ip_sgw as sgw_ip,
           |count(distinct(case when u_type = 4 and rat = 6 then IMSI  end)) as KPI_0209_001,
           |sum(case when u_type = 4 and rat = 6 then UL_TRAFF else 0 end) as KPI_0209_002,
           |sum(case when u_type = 4 and rat = 6 then DL_TRAFF else 0 end) as KPI_0209_003,
           |sum(case when u_type = 4 and rat = 6 then 1 else 0 end) as KPI_0209_004,
           |sum(case when u_type = 4 and rat = 6 then DURITION else 0 end) as KPI_0209_005,
           |sum(case when u_type = 4 and rat = 6 then UL_DURARION else 0 end) as KPI_0209_006,
           |sum(case when u_type = 4 and rat = 6 then DL_DURARION else 0 end) as KPI_0209_007,
           |sum(case when u_type = 4 and rat = 6 then RTT_ULNUM else 0 end) as KPI_0209_008,
           |sum(case when u_type = 4 and rat = 6 then RTT_UL else 0 end) as KPI_0209_009,
           |sum(case when u_type = 4 and rat = 6 then RTT_DLNUM else 0 end) as KPI_0209_010,
           |sum(case when u_type = 4 and rat = 6 then RTT_DL else 0 end) as KPI_0209_011,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0209_015,
           |sum(case when rat = 6 and l4_type = 0 then 1 else 0 end) as KPI_0209_016,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP1ST else 0 end) as KPI_0209_017,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP2ND else 0 end) as KPI_0209_018,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from tb_xdr_s1u_other_par_dwd where p_city=${p_city} and p_hour='${p_hour}') a
           |inner join
           |(select * from servname_traffic_type where traffic_name = '视频') b
           |on a.servname = b.servname
           |group by
           |cgi,ip_sgw
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable", s"public.xdr_4g_enb_cell_s1u_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_s1u_h finish")


      session.sql(
        s"""
           |select cgi,
           |ip_sgw as sgw_ip,
           |count(distinct(case when u_type = 4 and rat = 6 then IMSI  end)) as KPI_0210_001,
           |sum(case when u_type = 4 and rat = 6 then UL_TRAFF else 0 end) as KPI_0210_002,
           |sum(case when u_type = 4 and rat = 6 then DL_TRAFF else 0 end) as KPI_0210_003,
           |sum(case when u_type = 4 and rat = 6 then 1 else 0 end) as KPI_0210_004,
           |sum(case when u_type = 4 and rat = 6 then DURITION else 0 end) as KPI_0210_005,
           |sum(case when u_type = 4 and rat = 6 then RTT_ULNUM else 0 end) as KPI_0210_006,
           |sum(case when u_type = 4 and rat = 6 then RTT_UL else 0 end) as KPI_0210_007,
           |sum(case when u_type = 4 and rat = 6 then RTT_DLNUM else 0 end) as KPI_0210_008,
           |sum(case when u_type = 4 and rat = 6 then RTT_DL else 0 end) as KPI_0210_009,
           |sum(case when u_type = 4 and rat = 6 then UL_DURARION else 0 end) as KPI_0210_022,
           |sum(case when u_type = 4 and rat = 6 then DL_DURARION else 0 end) as KPI_0210_023,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0210_024,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0210_025,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP1ST else 0 end) as KPI_0210_026,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP2ND else 0 end) as KPI_0210_027,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from tb_xdr_s1u_other_par_dwd where p_city=${p_city} and p_hour='${p_hour}') a
           |inner join
           |(select * from servname_traffic_type where traffic_name = '手机游戏') b
           |on a.servname = b.servname
           |group by
           |cgi,ip_sgw
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable", s"public.xdr_4g_enb_cell_s1u_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_s1u_h finish")


      session.sql(
        s"""
           |select cgi,
           |ip_sgw as sgw_ip,
           |count(distinct(case when u_type = 4 and rat = 6 then IMSI end)) as KPI_0238_001,
           |sum(case when u_type = 4 and rat = 6 then UL_TRAFF else 0 end) as KPI_0238_002,
           |sum(case when u_type = 4 and rat = 6 then DL_TRAFF else 0 end) as KPI_0238_003,
           |sum(case when u_type = 4 and rat = 6 then 1 else 0 end) as KPI_0238_004,
           |sum(case when u_type = 4 and rat = 6 then DURITION else 0 end) as KPI_0238_005,
           |sum(case when u_type = 4 and rat = 6 then RTT_ULNUM else 0 end) as KPI_0238_006,
           |sum(case when u_type = 4 and rat = 6 then RTT_UL else 0 end) as KPI_0238_007,
           |sum(case when u_type = 4 and rat = 6 then RTT_DLNUM else 0 end) as KPI_0238_008,
           |sum(case when u_type = 4 and rat = 6 then RTT_DL else 0 end) as KPI_0238_009,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0238_023,
           |sum(case when rat = 6 and l4_type = 0 then 1 else 0 end) as KPI_0238_024,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP1ST else 0 end) as KPI_0238_025,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP2ND else 0 end) as KPI_0238_026,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from tb_xdr_s1u_other_par_dwd where p_city=${p_city} and p_hour='${p_hour}') a
           |inner join
           |(select * from servname_traffic_type where traffic_name = '微信') b
           |on a.servname = b.servname
           |group by
           |cgi,ip_sgw
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable", s"public.xdr_4g_enb_cell_s1u_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_s1u_h finish")

      session.sql(
        s"""
           |select cgi,
           |ip_sgw as sgw_ip,
           |count(distinct(case when u_type = 4 and rat = 6 then IMSI end)) as KPI_0239_001,
           |sum(case when u_type = 4 and rat = 6 then UL_TRAFF else 0 end) as KPI_0239_002,
           |sum(case when u_type = 4 and rat = 6 then DL_TRAFF else 0 end) as KPI_0239_003,
           |sum(case when u_type = 4 and rat = 6 then 1 else 0 end) as KPI_0239_004,
           |sum(case when u_type = 4 and rat = 6 then DURITION else 0 end) as KPI_0239_005,
           |sum(case when u_type = 4 and rat = 6 then UL_DURARION else 0 end) as KPI_0239_006,
           |sum(case when u_type = 4 and rat = 6 then DL_DURARION else 0 end) as KPI_0239_007,
           |sum(case when u_type = 4 and rat = 6 then RTT_ULNUM else 0 end) as KPI_0239_008,
           |sum(case when u_type = 4 and rat = 6 then RTT_UL else 0 end) as KPI_0239_009,
           |sum(case when u_type = 4 and rat = 6 then RTT_DLNUM else 0 end) as KPI_0239_010,
           |sum(case when u_type = 4 and rat = 6 then RTT_DL else 0 end) as KPI_0239_011,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then 1 else 0 end) as KPI_0239_012,
           |sum(case when rat = 6 and l4_type = 0 then 1 else 0 end) as KPI_0239_013,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP1ST else 0 end) as KPI_0239_014,
           |sum(case when rat = 6 and l4_type = 0 and tcp_status = 0 then DUR_TCP2ND else 0 end) as KPI_0239_015,
           |cast(from_unixtime(unix_timestamp('${p_hour}','yyyyMMddHH')) as timestamp) as time
           |from
           |(select * from tb_xdr_s1u_other_par_dwd where p_city=${p_city} and p_hour='${p_hour}') a
           |inner join
           |(select * from servname_traffic_type where traffic_name = '浏览') b
           |on a.servname = b.servname
           |group by
           |cgi,ip_sgw
        """.stripMargin).write.mode("append").format("jdbc")
        .option("url", s"jdbc:postgresql://${pg_ip}:${pg_port}/netop_e2e")
        .option("dbtable", s"public.xdr_4g_enb_cell_s1u_h")
        .option("user", "postgres")
        .option("password", "postgres")
        .save()
      println(s"write to public.xdr_4g_enb_cell_s1u_h finish")



      //       4G业务面指标统计
      //       写入public.xdr_4g_enb_cell_s1usub_h
      //       S1-U HTTP


    }

    //    writeToPg(pg_ip,pg_port,DDB,p_city,p_hour)
    writeToHive(pg_ip, pg_port, DDB, p_city, p_hour)
    hiveWriteToPG(pg_ip, pg_port, DDB, p_city, p_hour)

  }

}
