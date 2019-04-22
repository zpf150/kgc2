package day02

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Teacher1 {
  def main(args: Array[String]): Unit = {
    //new Sparkconf
   val conf = new SparkConf().setAppName("Teacher1")
      .setMaster("local[1]")
    //new SparkContext
    val sc = new SparkContext(conf)
    //读取数据
    val lines: RDD[String] = sc.textFile("E:\\test\\teacher.log")
    //处理数据
  val result1: RDD[((String, String), Int)] =lines.map(tp=>{
   //先获取到host地址
    val host: String = new URL(tp).getHost
    //截取我们需要的数据(科目）
   val subject = host.substring(0,host.indexOf("."))
    //我们要获取到老师的名字
    val teacher = tp.substring(tp.lastIndexOf("/")+1)
    ((subject,teacher),1)
 })
    //聚合
    val result2: RDD[((String, String), Int)] = result1.reduceByKey(_+_)
    //排序
    val result3 = result2.sortBy(- _._2)
    //取前三
    val result4 = result3.take(3)
    //打印
    result4.foreach(println(_))
    //关闭
  sc.stop()
  }

}
//如果我要分组求TOPN