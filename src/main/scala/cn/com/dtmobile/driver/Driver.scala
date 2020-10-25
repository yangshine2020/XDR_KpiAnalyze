package cn.com.dtmobile.driver

import cn.com.dtmobile.exception.ExceptionAnalyze
import cn.com.dtmobile.kpi._
import cn.com.dtmobile.spark.TestHive
import org.apache.hadoop.util.ProgramDriver

object Driver {

  def main(args: Array[String]): Unit = {
    val driver = new ProgramDriver
    driver.addClass("s1ukpihourAnalyze",classOf[S1uKpiHourAnalyze],"S1U小时指标统计")
    driver.addClass("s1ukpidayAnalyze",classOf[S1uKpiDayAnalyze],"S1U天指标统计")
    driver.addClass("s1mmekpihourAnalyze",classOf[S1mmeKpiHourAnalyze],"S1MME小时指标统计")
    driver.addClass("s1mmekpidayAnalyze",classOf[S1mmeKpiDayAnalyze],"S1MME天指标统计")
    driver.addClass("testhive",classOf[TestHive],"创建数据库")
    driver.addClass("exceptionAnalyze",classOf[ExceptionAnalyze],"异常事件分析")
    driver.addClass("s1ukpihourAnalyzeOpt",classOf[S1uKpiHourAnalyzeOpt],"S1U小时指标统计优化")
    driver.addClass("s1mmekpihourAnalyzeOpt",classOf[S1mmeKpiHourAnalyzeOpt],"S1mme小时指标统计优化")
    driver.run(args)
  }

}
