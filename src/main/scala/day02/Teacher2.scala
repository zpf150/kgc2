package day02
import java.net.URL
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
object Teacher2 {
  def main(args: Array[String]): Unit = {
    //new Sparkconf
    val conf = new SparkConf().setAppName("Teacher2")
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
    //先聚合
    val result2: RDD[((String, String), Int)] = result1.reduceByKey(_+_)
    //聚合完之后，根据 科目 分组 groupBy(科目）
    val result3: RDD[(String, Iterable[((String, String), Int)])] = result2.groupBy(_._1._1)
    //迭代器内部没有封装排序方法，转化为List集合
    val result4: RDD[(String, List[((String, String), Int)])] = result3.mapValues(_.toList.sortBy(- _._2).take(2))
    //dayin
    result4.foreach(println(_))
    sc.stop()
  }
}
//不用tolist的方法  如何解决内存溢出问题？