package day01
import java.sql.DriverManager
object MyUtils {
  //IP地址转化为Long类型
  def ip2Long(ip:String):Long ={
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for(i<- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }
//二分查找法
  def binarySearch(lines: Array[(Long,Long,String)],ip: Long):Int ={
    var start =0
    var end =lines.length-1
    while(start <=end){
      val middle =(start+end)/2
      if((ip>=lines(middle)._1) && (ip<=lines(middle)._2))
        return middle
      if(ip < lines(middle)._1)
        end=middle -1
      else{
        start =middle +1
      }
    }
    -1
  }

  /**
    * 将每个分区的数据写入到Mysql数据库
    * @param it
    */
  def Data2MySQL(it:Iterator[(String, Int)]) = {
    //一个迭代器代表一个分区，分区中包含了数据
    val conn = DriverManager.getConnection("jdbc:mysql://bigdata01:3306/bigdata?characterEncoding=UTF-8", "root", "123456")
    val ps = conn.prepareStatement("INSERT INTO access_log values(?,?)")
    //将分区中的数据一条条的写入到mysql
    //    while (it.hasNext)
    it.foreach(tp => {
      ps.setString(1, tp._1)
      ps.setInt(2, tp._2)
      ps.executeUpdate()
    })
    //将分区中的数据全部写完以后，在关闭连接
    if (ps != null) ps.close()
    if (conn != null) conn.close()
  }
}
