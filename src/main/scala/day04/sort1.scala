package day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object sort1 {
  def main(args: Array[String]): Unit = {
    //自定义排序
    //id   name   age   works
    //第一步我要去创建sparkcontext对象的统一入口
    //给对象设置一些参数 sparkConf
    val conf = new SparkConf().setAppName("sort1")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    //定义数据
    val array=Array("1,xiaozhang,25,200","2,xiaowang,30,250","3,xiaoli,25,300","4,xiaozhao,35,350")
    //需求  先根据老师的年龄排序（升序） 年龄如果相同，根据带课次数排序（降序）
    //自定义排序guize
    val lines: RDD[String] = sc.makeRDD(array,2)
    //处理数据
   val resulte1: RDD[(String, String, Int, Int)] = lines.map(tp=>{
      val splits: Array[String] = tp.split(",")
      val id = splits(0)
      val name = splits(1)
      val age = splits(2).toInt
      val works = splits(3).toInt
      (id,name,age,works)
    })
    //排序
    val result2 = resulte1.sortBy(tp=>orders(tp._1,tp._2,tp._3,tp._4))
    //结果给手机起来
    result2.collect().foreach(println(_))
  }
}
case class orders(id:String,name:String,age:Int,works:Int) extends Ordered[orders]{
  override def compare(that: orders) = {
    //开始自己定义
    if(this.age == that.age){
      that.works - this.works
    }else {
      this.age - that.age
    }
  }
}