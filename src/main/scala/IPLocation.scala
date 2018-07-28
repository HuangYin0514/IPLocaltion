import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable

class IPLocation {


}

object IPLocation {

  /**
    * 二分法查找
    * 因为ip段中，ip是有序的，所以采用二分法排序最快
    */
  def binarySearch(lines: Array[(String, String, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong)) {
        return middle
      }
      if (ip < lines(middle)._1.toLong) {
        high = middle - 1
      } else {
        low = middle + 1
      }
    }
    -1

  }

  /**
    * 第一个数  | 第二个数  |  第三个数 |  第四个数
    * 0000 0000 | 0000 0000 | 0000 0000 | 0000 0000
    * 另外每次左移八位,就相当于扩大256 = 2的8次幂
    */
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def data2MySQL(iterator: Iterator[(String, Int)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "INSERT INTO location_info (location, total_count) VALUES (?, ?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://192.168.25.185:3306/big_data?useUnicode=true&characterEncoding=utf-8", "root", "root")
      iterator.foreach(line => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, line._1.toString)
        ps.setInt(2, line._2)
        ps.execute()
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\工作\\大数据\\hadoop\\软件\\hadoop-2.6.1")
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val conf = new SparkConf().setAppName("IPLocation").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //读文件,取出ip开始、ip结束、省，构建RDD
    //(3755982848,3755985919,北京)
    val ipNumRange2LocationRdd = sc.textFile("C:\\Users\\10713\\IdeaProjects\\IPLocaltion\\src\\main\\resources\\ip.txt")
      .map(_.split("\\|")).map(x => {
      (x(2), x(3), x(6))
    })
    //将ip段信息加载到内存
    val ipNumRang2LocationArray = ipNumRange2LocationRdd.collect()
    //广播ip端变量
    val broadCaseArray = sc.broadcast(ipNumRang2LocationArray)
    //读取日志信息
    val ipLogRDD = sc.textFile("C:\\Users\\10713\\IdeaProjects\\IPLocaltion\\src\\main\\resources\\20090121000132.394251.http.format")
      .map(_.split("\\|"))
    //将日志信息的ip map出来
    val locationAndIp = ipLogRDD.map(_ (1)).mapPartitions(it => {
      val arr = broadCaseArray.value
      it.map(ip => {
        val ipNum = ip2Long(ip)
        //将ip转换为数字
        val index = binarySearch(arr, ipNum) //二分法查找ip地址在那个省份,返回在广播array中的角标
        val t = arr(index)
        (t._3, ip) //返回广播变量和ip
      })
    })
    //统计省份的ip数
    val locationCount: RDD[(String, Int)] = locationAndIp.map(t => (t._1, 1)).reduceByKey(_ + _)
    locationCount.foreach(println)
    locationCount.foreachPartition(data2MySQL)
    sc.stop()
  }
}
