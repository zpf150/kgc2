package day01

import java.sql.DriverManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ipaccess {
  def main(args: Array[String]): Unit = {
    //new conf
    val conf= new SparkConf().setAppName("Movice").setMaster("local[1]")
    //new SparkContext
    val sc = new SparkContext(conf)
    //读取D:\ap\ip.txt
    val iplines=sc.textFile("D:\\app\\ip.txt")
    //数据处理 （开始IP，结束IP，省份）
   val result1: RDD[(Long, Long, String)] = iplines.map(tp=>{
     val splits= tp.split("[|]")
      val startip=splits(2).toLong
      val endip=splits(3).toLong
      val province=splits(6)
      (startip,endip,province)
    })
    //广播变量   collect 变成Array
    //取广播变量 .value
   val result2: Array[(Long, Long, String)] = result1.collect()
    val broadresult: Broadcast[Array[(Long, Long, String)]] =sc.broadcast(result2)
   //读取acccess.log
    val accesslines=sc.textFile("D:\\app\\access.log")
    //处理数据
   val ipresult: RDD[(String, Int)] = accesslines.map(tp=>{
      val splits2=tp.split("[|]")
      val ip=splits2(1)
     //ip转化为Long类型
     val ipre=MyUtils.ip2Long(ip)
     //取广播变量
     val result3: Array[(Long, Long, String)] =broadresult.value
     //调用二分查找法
     val ipnum=MyUtils.binarySearch(result3,ipre)
     var provinces=""
     if(ipnum!= -1){
       val tps: (Long, Long, String) =result3(ipnum)
       provinces= tps._3
     }
     (provinces,1)
    })
   //聚合
   val result4: RDD[(String, Int)] = ipresult.reduceByKey(_+_)
    //排序
    val result5=result4.sortBy(_._2)
    //写入mysql
    result5.foreachPartition(filter=>{
     val conn= DriverManager.getConnection(
       "jdbc:mysql://192.168.1.144:3306/test1?serverTimezone=Asia/Shanghai",
       "root","123456")
      filter.foreach(tp=>{
        val ps=conn.prepareStatement("insert into suibian values(?,?)")
        ps.setString(1,tp._1)
        ps.setInt(2,tp._2)
        ps.executeUpdate()
        ps.close()
      })
      conn.close()
    })
    sc.stop()
  }
}
