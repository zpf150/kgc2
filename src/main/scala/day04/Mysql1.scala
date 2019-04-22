package day04
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Mysql1 {
  def main(args: Array[String]): Unit = {
    //给对象设置一些参数 sparkConf
    val conf = new SparkConf().setAppName("sort1")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    //接下来要操作的是sql
    //new SQLcongtext对象
    //这种方法过时了
    val sQLContext = new SQLContext(sc)
    //读取数据，拿到数据之后，转化为表
    val lines: RDD[String] = sc.textFile("E:\\test\\emp.csv")
    //想要的是dataframe  第一种方式：用样列类  第二种方式：定义schmec信息
   val lines2: RDD[Array[String]] = lines.map(_.split(","))
    //吧七个字段依次匹配
  val result3: RDD[Emp] = lines2.map(tp=>Emp(tp(0),tp(1),tp(2),tp(3),tp(4).toInt,tp(5).toInt,tp(6)))
    //要写sql语句，就得隐士转化
    import sQLContext.implicits._
    //转化为dataframe
    val result4: DataFrame = result3.toDF()
    //吧dataframe注册成表
    result4.createTempView("test1")
    //就可以写sql语句了
    val result5: DataFrame =sQLContext.sql(
      """
        |select name,sale,money from test1
      """.stripMargin)
    //调用打印的方法  表现打印
    result5.show()
  }
}
/*1.编号 id
2.姓名 name
3.职业 job
4.日期 date
5.工资 sale
6.奖金 money
7.部门编号 number*/
case class  Emp(id:String,name:String,job:String,date:String,sale:Int,money:Int,number:String)