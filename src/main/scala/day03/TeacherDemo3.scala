package day03

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object TeacherDemo3 {
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
    //之前不想用List
      //拿到科目,去重
    val subjects: Array[String] = result1.map(_._1._1).distinct().collect()
    //不按照系统默认的方法去分区分组，我们自己自定义分区分组
    val value: Partitions = new  Partitions(subjects)
    //接下来就按照我们的分区来聚合
    val result2: RDD[((String, String), Int)] = result1.reduceByKey(value,_+_)
    //接下来对每一个分区取值
    result2.foreachPartition(filter=>{
      //一个数据能比较
      //自定义一个排序规则
       var treeSet = new mutable.TreeSet[((String, String), Int)]()(new Orders())
      //万事大吉了，只需要吧元素添加到treeset集合中就好了
      while(filter.hasNext){
        val tuple = filter.next()
        treeSet.add(tuple)
        //treeset集合中只能放俩个元素  top 2
        if(treeSet.size >2){
          treeSet = treeSet.dropRight(1)
        }
      }
      println(treeSet)
    })
    sc.stop()
  }
}


//自定义分区类
class Partitions(subjects: Array[String]) extends Partitioner{
  //上面new map
  private val map = new mutable.HashMap[String,Int]()
  //现在需要吧分区规定好
  //需要定义标签记录数据
  var count=0
  for(subject <- subjects){
    map += ((subject,count))
    count += 1
  }
  override def numPartitions = subjects.length

  override def getPartition(key: Any) = {
    //自定义为 我自己想要的key
    val tuple = key.asInstanceOf[(String,String)]
    //我们这里现在的需求
    val subject2 = tuple._1
    //利用一个map集合
    map(subject2)
  }
}
//自定义一个比较方法的类
class Orders extends Ordering[((String, String), Int)]{
  override def compare(x: ((String, String), Int), y: ((String, String), Int)) = {
    y._2 - x._2
    // - （x._2 - y._2)
  }
}