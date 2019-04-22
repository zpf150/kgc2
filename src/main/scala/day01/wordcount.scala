package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object wordcount {
//main方法
def main(args: Array[String]): Unit = {
  //new conf 这里是sparkContext对象的参数设置
  //local指的是本地模式,*指的是本地所有线程
 val  conf=new SparkConf().setAppName(this.getClass.getSimpleName)
    .setMaster("local[*]")
  //创建sparkContext对象
val sc=  new SparkContext(conf)
  //sc对象去读取文件，返回的是一个RDD数据集
 val lines: RDD[String] =sc.textFile("D:\\wc.txt")
  //处理数据，flatMap
 val result1: RDD[String] = lines.flatMap(_.split(" "))
  //分组，（key,1)
  val result2: RDD[(String, Int)] =result1.map(tp=>(tp,1))
  //聚合 reduceByKey(key,50)
  val result3: RDD[(String, Int)] =result2.reduceByKey(_+_)
  //排序
  val result4: RDD[(String, Int)] =result3.sortBy(- _._2)
  //result3.sortBy(_._2,false)
  //收集、输出打印
result4.foreach(println)
  //关闭对象
  sc.stop()
}
}
