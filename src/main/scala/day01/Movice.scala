package day01
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/*数据说明：
users.dat ---UserID::Gender::Age::Occupation::Zip-code
movies.dat --- MovieID::Title::Genres
ratings.dat ---UserID::MovieID::Rating::Timestamp
（moveid,(raring,1))
需求：
1：评分（平均分）最高的10部电影
2：18 - 24 岁的男性年轻人 最喜欢看的10部电影
3：女性观看次数最多的10部电影名称及观看次数*/
object Movice {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    //new conf
   val conf= new SparkConf().setAppName("Movice").setMaster("local[*]")
    //new SparkContext
    val sc=new SparkContext(conf)
    //读取文件ratings.dat
    val lines1=sc.textFile("D:\\app\\ratings.dat")
    //处理数据  (UserID,MovieID,Rating)
   val ranting1: RDD[(String, String, Int)] = lines1.map(tp=>{
     val splits: Array[String] = tp.split("::")
      val userid1=splits(0)
      val moveid1=splits(1)
      val rating=splits(2).toInt
      (userid1,moveid1,rating)
    })
    //1：评分（平均分）最高的10部电影
    //这是我们想要的数据结构（moveid,(raring,1))
   val ranting2: RDD[(String, (Int, Int))] = ranting1.map(tp=>(tp._2,(tp._3,1)))
    //分组
   //val group1: RDD[(String, Iterable[(String, (Int, Int))])] = ranting2.groupBy(_._1)
    val group2: RDD[(String, Iterable[(Int, Int)])] =ranting2.groupByKey()
    //聚合(movid,rtingsum,counsum)
   val rantresult1: RDD[(String, Int, Int)] = group2.map(tp=>{
      val rantsum=tp._2.map(tp=>tp._1).sum
      val countsum=tp._2.map(_._2).sum
      (tp._1,rantsum,countsum)
    })
    //取平均值
    val ranresult2=rantresult1.map(tp=>{
      (tp._1,tp._2/tp._3)
    })
    //读取文件，movies.dat
    val movelines=sc.textFile("D:\\app\\movies.dat")
    //处理数据（moveid,电影名字）
   val moveresult1: RDD[(String, String)] = movelines.map(tp=>{
      val splits2=tp.split("::")
      val moveid2=splits2(0)
      val moveiname=splits2(1)
      (moveid2,moveiname)
    })
    //join(moveresult1与ranresult2）(电影ID，（电影评分，电影名字））
    val joinresult: RDD[(String, (Int, String))] =ranresult2.join(moveresult1)
    //sort排序 去前十  输出
    println("-------------------平均分最高的十部电影----------------------")
    val result1=joinresult.sortBy(- _._2._1).take(10)
    result1.foreach(println(_))



    println("---------------男性喜欢看的十部电影---------------------")
    //2：18 - 24 岁的男性年轻人 最喜欢看的10部电影
    //读取users.dat
   val userlines= sc.textFile("D:\\app\\users.dat")
    //处理数据
    val userresult1: RDD[(String, String, Int)] =userlines.map(tp=>{
      val splits3=tp.split("::")
      val userid=splits3(0)
      val gender=splits3(1)
      val age=splits3(2).toInt
      (userid,gender,age)
    })
    //过滤  性别跟年龄
    val userresult2: RDD[(String, String, Int)] =userresult1.filter(tp=>tp._2.contains("M")&& tp._3>=18 && tp._3<=24)

    //(用户id,用户性别)
    val userrunst3: RDD[(String, String)] =userresult2.map(tp=>(tp._1,tp._2))

    userrunst3.cache();


    //(用户id,电影id)
   val ranting3: RDD[(String, String)] = ranting1.map(tp=>(tp._1,tp._2))

    //join(用户id,(用户性别,电影id))
   val result: RDD[(String, (String, String))] = userrunst3.join(ranting3)
    //(电影id,用户性别)
    val result5: RDD[(String, String)] =result.map(tp=>(tp._2._2,tp._2._1)).distinct()
    //（电影id,电影总次数）
    val rantresult3: RDD[(String, Int)] =rantresult1.map(tp=>(tp._1,tp._3))
    val result6: RDD[(String, (String, Int))] =result5.join(rantresult3)
    val result7= result6.sortBy(- _._2._2).take(10)
    result7.foreach(println(_))
    println("-----------------女性观看次数最多的10部电影名称及观看次数---------------------")
    userrunst3.unpersist(true)

    //关闭对象
    sc.stop()
  }
}
