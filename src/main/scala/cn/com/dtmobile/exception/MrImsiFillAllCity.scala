//package cn.com.dtmobile.exception
//
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
//
//object MrImsiFillAllCity {
//  var numPartition = 350
//
//  def main(args: Array[String]): Unit = {
//
//    var sign = ""
//    val p_hour = args(0)
//    sign = args(1)
//
//
//    val p_city="570,571,572,573,574,575,576,577,578,579,580"
//    println(s"print+++++++++++++++++++++++++++++++++:$p_city")
//
//    //默认传0 0:只保留能关联mr的。 传1：不管有没有关联都保存
//    if ("1".equals(sign)) {
//      numPartition = 500
//    } else {
//      numPartition = 350
//    }
//
//    //1:用转存s1mme数据  0:用原始数据s1mme
//    val unloading_flag = args(2)
//
//    //spark配置
//    val conf = new SparkConf()
//    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
//
//    // 参数配置
//    envConfig(spark)
//
//    method_one(spark, p_hour, p_city, sign, unloading_flag)
//  }
//
//  def method_one(spark: SparkSession, p_hour: String, p_city: String, sign: String, unloading_flag: String): Unit = {
//    import spark.sql
//    spark.sql(s"use ${Configuration.JC_RC_DATABSE}")
//
//    //mme清洗 一个imsi一分钟一条记录
//    var s1name = "default.d_ens_s1_mme"
//    if ("0".equalsIgnoreCase(unloading_flag)) {
//      s1name = s"${Configuration.DEFAULT_DATABASE}.d_ens_s1_mme"
//    } else {
//      s1name = "unloading_s1mme"
//    }
//
//
//
//    // 过滤掉S1MME的异常数据，将时间转换为秒
//    sql(
//      s"""
//         |select city, mme_ue_s1ap_id, mme_group_id, mme_code, imsi, msisdn
//         |,cast(conv(cell_id,16,10) as int) as cellid
//         |,cast(unix_timestamp(procedure_start_time, "yyyy/MM/dd HH:mm:ss") as decimal(10, 0)) procedure_start_time
//         |from $s1name l
//         |where p_hour = $p_hour and city in ($p_city)
//         |and procedure_type != 39
//         |and imsi is not null
//         |and procedure_start_time is not null
//         |and mme_ue_s1ap_id != 0 and mme_group_id != 0 and mme_code != 0
//         |and mme_ue_s1ap_id != 1 and mme_group_id != 1 and mme_code != 1
//         |and mme_ue_s1ap_id is not null and mme_group_id is not null and mme_code is not null
//       """.stripMargin).createOrReplaceTempView("temp_clean_mme")
//
//
//
//
//    //mme根据key分组，回填上下时间范围为10分钟，对10进行取模，产生一个分组号
//    sql(
//      s"""
//         |select
//         |city, mme_ue_s1ap_id, mme_group_id, mme_code, imsi, msisdn, procedure_start_time,
//         |procedure_start_time - pmod(procedure_start_time, 600) time_stamp_min,
//         |procedure_start_time - pmod(procedure_start_time, 600) + 600 time_stamp_max
//         |from temp_clean_mme
//       """.stripMargin).createOrReplaceTempView("temp_group_mme")
//
//
//    // 对MR使用同样的算法进行编号，加速关联效率
//    sql(
//      s"""
//         |select
//         |a_cgi, z_cgi, distance, adjacent, mmecode, mmegroupid, mmeues1apid,
//         |time_stamp,id, ltescrsrp, ltencrsrp, ltescrsrq,
//         |ltencrsrq, ltescearfcn, ltescpci, ltencearfcn, ltencpci,
//         |ltesctadv, ltescphr, ltescrip, ltescaoa
//         |,'' ltescplrulqci1
//         |,'' ltescplrulqci2
//         |,'' ltescplrulqci3
//         |,'' ltescplrulqci4
//         |,'' ltescplrulqci5
//         |,'' ltescplrulqci6
//         |,'' ltescplrulqci7
//         |,'' ltescplrulqci8
//         |,'' ltescplrulqci9
//         |,'' ltescplrdlqci1
//         |,'' ltescplrdlqci2
//         |,'' ltescplrdlqci3
//         |,'' ltescplrdlqci4
//         |,'' ltescplrdlqci5
//         |,'' ltescplrdlqci6
//         |,'' ltescplrdlqci7
//         |,'' ltescplrdlqci8
//         |,'' ltescplrdlqci9
//         |, ltescsinrul, ltescri1, ltescri2, ltescri4, ltescri8
//         |,'' ltescpuschprbnum
//         |,'' ltescpdschprbnum
//         |,'' ltescbsr
//         |,'' ltescenbrxtxtimediff
//         |,'' gsmncellbcch
//         |,'' gsmncellcarrierrssi
//         |,'' gsmncellncc
//         |,'' gsmncellbcc
//         |,'' tdspccpchrscp
//         |,'' tdsncelluarfcn
//         |,'' tdscellparameterid
//         |, p_city,
//         |if(pmod(cast(unix_timestamp(time_stamp, "yyyy-MM-dd HH:mm:ss") as decimal(10, 0)), 600) >
//         |600 - pmod(cast(unix_timestamp(time_stamp, "yyyy-MM-dd HH:mm:ss") as decimal(10, 0)), 600),
//         |cast(unix_timestamp(time_stamp, "yyyy-MM-dd HH:mm:ss") as decimal(10, 0)) -
//         |pmod(cast(unix_timestamp(time_stamp, "yyyy-MM-dd HH:mm:ss") as decimal(10, 0)), 600) + 600,
//         |cast(unix_timestamp(time_stamp, "yyyy-MM-dd HH:mm:ss") as decimal(10, 0)) -
//         |pmod(cast(unix_timestamp(time_stamp, "yyyy-MM-dd HH:mm:ss") as decimal(10, 0)), 600)) time_stamp_num
//         |,cast((cast(split(a_cgi, '-')[2] as int) * 256 + cast(split(a_cgi, '-')[3] as int)) as int)as objectid
//         |from degault.d_enl_mr_new_h b
//         |where p_hour = $p_hour and p_city in ($p_city)
//         |and ltescrsrp is not null and a_cgi is not null
//         |and mmeues1apid != 0 and mmegroupid != 0 and mmecode != 0
//         |and mmeues1apid != 1 and mmegroupid != 1 and mmecode != 1
//         |and mmeues1apid is not null and mmegroupid is not null and mmecode is not null
//       """.stripMargin).createOrReplaceTempView("temp_clean_mr")
//
//
//    if ("1".equals(sign)) {
//      //mr关联mme 不管有没有回填都算上
//      sql(
//        s"""
//           |select mr.*, mme.imsi, mme.msisdn, mme.procedure_start_time,mme.city
//           |from
//           |temp_clean_mr mr
//           |left join
//           |temp_group_mme mme
//           |on
//           |mr.p_city = mme.city and
//           |mr.mmecode = mme.mme_code
//           |and mr.mmegroupid = mme.mme_group_id and mr.mmeues1apid = mme.mme_ue_s1ap_id
//           |and (mr.time_stamp_num = mme.time_stamp_min or mr.time_stamp_num = mme.time_stamp_max)
//       """.stripMargin).createOrReplaceTempView("temp_mr_all")
//    } else if ("0".equals(sign)) {
//      //mr关联mme 只关联上mr的
//      sql(
//        s"""
//           |select mr.*, mme.imsi, mme.msisdn, mme.procedure_start_time,mme.city
//           |from
//           |temp_clean_mr mr
//           |inner join
//           |temp_group_mme mme
//           |on
//           |mr.p_city = mme.city and
//           |mr.mmecode = mme.mme_code
//           |and mr.mmegroupid = mme.mme_group_id and mr.mmeues1apid = mme.mme_ue_s1ap_id
//           |and (mr.time_stamp_num = mme.time_stamp_min or mr.time_stamp_num = mme.time_stamp_max)
//       """.stripMargin).createOrReplaceTempView("temp_mr_all")
//    }
//
//
//    //保留距mr时间戳最近的imsi,同一分钟只取一条数据
//    sql(
//      s"""
//         |select *
//         |from (
//         |select *, row_number() over(partition by p_city, a_cgi, mmeues1apid, mmegroupid, mmecode, time_stamp, z_cgi, ltencrsrp, ltencrsrq, ltencpci
//         |order by abs(cast(unix_timestamp(time_stamp, "yyyy-MM-dd HH:mm:ss") as decimal(10, 0)) - procedure_start_time) asc) rn
//         |from temp_mr_all) t
//         |where rn = 1
//       """.stripMargin).createOrReplaceTempView("temp_h_imsi")
//
//    //整理成 d_enl_mr_new_h_imsi 格式
//    sql(
//      s"""
//         |select
//         |a_cgi
//         |,z_cgi
//         |,distance
//         |,adjacent
//         |,mmecode
//         |,mmegroupid
//         |,mmeues1apid
//         |,time_stamp
//         |,id
//         |,ltescrsrp - 141 as ltescrsrp
//         |,ltencrsrp - 141 as ltencrsrp
//         |,ltescrsrq * 0.5 - 20 as ltescrsrq
//         |,ltencrsrq * 0.5 - 20 as ltencrsrq
//         |,ltescearfcn
//         |,ltescpci
//         |,ltencearfcn
//         |,ltencpci
//         |,ltesctadv
//         |,ltescphr - 23 as ltescphr
//         |,ltescrip
//         |,ltescaoa * 0.5  as ltescaoa
//         |,ltescplrulqci1
//         |,ltescplrulqci2
//         |,ltescplrulqci3
//         |,ltescplrulqci4
//         |,ltescplrulqci5
//         |,ltescplrulqci6
//         |,ltescplrulqci7
//         |,ltescplrulqci8
//         |,ltescplrulqci9
//         |,ltescplrdlqci1
//         |,ltescplrdlqci2
//         |,ltescplrdlqci3
//         |,ltescplrdlqci4
//         |,ltescplrdlqci5
//         |,ltescplrdlqci6
//         |,ltescplrdlqci7
//         |,ltescplrdlqci8
//         |,ltescplrdlqci9
//         |,ltescsinrul - 11 as ltescsinrul
//         |,ltescri1
//         |,ltescri2
//         |,ltescri4
//         |,ltescri8
//         |,ltescpuschprbnum
//         |,ltescpdschprbnum
//         |,ltescbsr
//         |,ltescenbrxtxtimediff
//         |,gsmncellbcch
//         |,gsmncellcarrierrssi
//         |,gsmncellncc
//         |,gsmncellbcc
//         |,tdspccpchrscp
//         |,tdsncelluarfcn
//         |,tdscellparameterid
//         |,(case when imsi is null then concat_ws('_', mmecode, mmegroupid, mmeues1apid) else imsi end) imsi
//         |, msisdn
//         |,(cast(split(a_cgi, '-')[2] as int) * 256 + cast(split(a_cgi, '-')[3] as int)) as objectid
//         |,(cast(split(z_cgi, '-')[2] as int) * 256 + cast(split(z_cgi, '-')[3] as int)) as nobjectid
//         |,'' as mrtime
//         |,p_city as city
//         |from temp_h_imsi
//       """.stripMargin).repartition(numPartition).createOrReplaceTempView("temp_d_enl_mr_new_h_imsi")
//
//    sql(
//      s"""
//         |insert overwrite table ftp_d_enl_mr_new_h_imsi partition(p_hour = $p_hour, p_city)
//         |select *, city as p_city from temp_d_enl_mr_new_h_imsi
//       """.stripMargin)
//
//
//  }
//
//  def envConfig(spark:SparkSession): Unit ={
//    import spark.sql
//    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
//    sql(s"use rc_hive_db")
//    //hive配置
//    sql("set hive.insert.into.multilevel.dirs = true")
//    sql("set hive.exec.compress.intermediate = true")
//    sql("set hive.exec.compress.output = true")
//    sql("set mapred.output.compression.codec = org.apache.hadoop.io.compress.GzipCodec")
//    sql("set hive.exec.dynamic.partition = true")
//    sql("set hive.exec.dynamic.partition.mode = nonstrict")
//    sql("alter table ftp_d_enl_mr_new_h_imsi set serdeproperties('serialization.null.format' = '')")
//  }
//
//
//
//}
