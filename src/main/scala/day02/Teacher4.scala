package day02
import java.net.URL
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable

object Teacher4 {
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
  val result1: RDD[((String, String), Int)] = lines.map(tp => {
    //我们这里的数据为特殊数据，需要用URL
    val host = new URL(tp).getHost
    //这样子我们就拿到了科目
    val subject = host.substring(0, host.indexOf("."))
    //接下来我们就需要拿到教师了
    val teacher = tp.substring(tp.lastIndexOf("/") + 1)
    ((subject, teacher), 1)
  })
  //这样子的结果是全局的topn,我们想要的结果并不是全局的topn，我们想要的是
  //接下来，我们要考虑的问题是如何不转List，完成需求，并且内存不会溢出
  //我们不再按照他们的规则分区分组，我们自己去自定义分区分组
  //在自定义分区分组之前，我们先拿到我们的所有科目，并去重
  val subjects: Array[String] = result1.map(_._1._1).distinct().collect()
  //接下来我自定义分区分组
  val value = new Partitions(subjects)
  //接下来就可以按照我们自己定义的分区来去聚合，不需要在根据默认的去聚合
  val result2: RDD[((String, String), Int)] = result1.reduceByKey(value,_+_)
  //接下来我就可以对每一个分区的数据来处理了
  result2.foreachPartition(filter=>{
    //分区已经完毕，接下来，在想一想，如何不用List,内存不会溢出
    //大家还记得treeSet这个集合吗
    //这个set怎么排序，无法排序，我们自己去给他定义规则
    var treeset = new mutable.TreeSet[((String, String), Int)]()(new Orders())
    //这下子就万事俱备了
    while (filter.hasNext){
      val tuple = filter.next()
      treeset.add(tuple)
      if(treeset.size>2){
        treeset=treeset.dropRight(1)
      }
    }
   println(treeset)
  })
sc.stop()
    }
}
//自定义的分区类
class Partitions(subjects: Array[String]) extends Partitioner{
  //需要创建一个map
 val map = new mutable.HashMap[String,Int]()
  //咱们在这里是不是需要完成我们的需求，每传进来一个科目，给他们各自分区
  var  count =0
  for(subject <- subjects){
    map += ((subject,count))
    count +=1
  }

  override def numPartitions =subjects.length

  override def getPartition(key: Any) = {
    val tuple = key.asInstanceOf[(String,String)]
    //现在我们的需求变了，现在我们需要每传递进来一个科目，我们就得
    //知道这个科目属于哪个分区里面的，并且把它归到各自的类里面
    val subject2= tuple._1
    //我们想想，可不可以放一个map集合，通过map集合拿到我们需要的分区号
    map(subject2)
  }
}
//自定义一个set排序的类
class Orders extends  Ordering[((String, String), Int)]{
  override def compare(x: ((String, String), Int), y: ((String, String), Int)) = {
   - (x._2 - y._2)
  }
}