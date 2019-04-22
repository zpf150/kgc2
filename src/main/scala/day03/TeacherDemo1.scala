package day03

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TeacherDemo1 {
  def main(args: Array[String]): Unit = {
    //第一步我要去创建sparkcontext对象的统一入口
    //给对象设置一些参数 sparkConf
    val conf = new SparkConf().setAppName("TeacherDemo1")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    //我们的spark是利用大量的rdd算子去处理数据的
    //读取数据，返回值恰好是我们的RDD
    val lines: RDD[String] = sc.textFile("E:\\test\\teacher.log")
    //处理数据  ((科目，老师),1)
    val result1: RDD[((String, String), Int)] = lines.map(tp=>{
      val host = new URL(tp).getHost
      val subject = host.substring(0,host.indexOf("."))
      val teacher = tp.substring(tp.lastIndexOf("/")+1)
      ((subject,teacher),1)
    })
    //聚合
    val result2: RDD[((String, String), Int)] = result1.reduceByKey(_+_)
    //排序，取值
    val result3 = result2.sortBy(- _._2).take(3)
    //打印结果
    result3.foreach(println(_))
    //关流
    sc.stop()
  }
}
