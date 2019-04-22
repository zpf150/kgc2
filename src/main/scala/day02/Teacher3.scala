package day02

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Teacher3 {
  def main(args: Array[String]): Unit = {
    //SparkContext 对象是spark程序的入口，要用到spark框架去分析数据,就必须
    //先要new SparkContext对象，在new对象之前，首先要配置对象的参数，所以
    //我们这里先new SparkConf对象设置参数，之后把参数传递给我的sparkcontext对象
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    //因为我们的Spark需要利用大量的RDD算子去处理数据，所以我们需要吧数据转化为RDD
    //接下来有了SC对象，就可以读取数据了,返回值为rdd
    val lines: RDD[String] = sc.textFile("E:\\test\\teacher.log")
    //拿到数据后，开始处理数据  ((科目，老师),1)
   val result1: RDD[((String, String), Int)] = lines.map(tp=>{
  //我们这里的数据为特殊数据，需要用URL
      val host= new URL(tp).getHost
      //这样子我们就拿到了科目
      val subject = host.substring(0,host.indexOf("."))
      //接下来我们就需要拿到教师了
      val teacher = tp.substring(tp.lastIndexOf("/")+1)
      ((subject,teacher),1)
    })
  //聚合  ((科目，老师),聚合后的次数)
    //这样子的结果是全局的topn,我们想要的结果并不是全局的topn，我们想要的是
    //每个科目下的topn，所以我们还得根据科目分组
    val result2: RDD[((String, String), Int)] = result1.reduceByKey(_+_)
    //根据科目分组 (科目，【迭代器(科目，老师)，次数】)
    val result3: RDD[(String, Iterable[((String, String), Int)])] = result2.groupBy(_._1._1)
    //利用mapvalues方法，取到迭代器，吧迭代器转化为List之后，就可以排序了
    val result4: RDD[(String, List[((String, String), Int)])] = result3.mapValues(_.toList.sortBy(- _._2).take(2))
    //打印结果
    result4.foreach(println(_))
    //关流
    sc.stop()
  }
}
