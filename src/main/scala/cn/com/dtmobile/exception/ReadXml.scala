package cn.com.dtmobile.exception

import scala.xml.{Elem, XML}

object ReadXml {

  def main(args: Array[String]): Unit = {

    val xml: Elem = XML.load(this.getClass.getClassLoader.getResource("FDD-LTE_MRO_NSN_OMC_134927_20200812033000.xml"))

    print(xml.toList)
  }

}
