import java.util

import org.apache.spark.sql.SparkSession

/**
 * 100,200 300 400 500 600
 * 200,100 300 400
 * 300,100 200 300 400 500
 * 400,100 200 300
 * 500,100 300
 * 600,100
 */
object Test_1统计共同好友 {
  def main(args: Array[String]): Unit = {
    //数据集
    val spark=SparkSession
      .builder()
      .appName("统计共同好友")
      .master("local[*]")
      .getOrCreate()
    //读取数据集
    val lines=spark.sparkContext.textFile("Data/file.txt")
    //将一列拆分成多列
   val linemap= lines.flatMap(f = line => {
     val fileds = line.split(",")
     val value = fileds(1).split(" ").toList
     //用于存储每一个列的结果 为的是将其
     val value_all_row = value
     for (i <- 0 until value.length) yield (  (fileds(0),value(i)),(value_all_row.diff(List(value(i))))  )
   }).map(row=>{
     if (row._1._1.toInt>row._1._2.toInt){
       ((row._1._2,row._1._1) ,row._2)
     }else{
       ((row._1._1,row._1._2) ,row._2)
     }
   }).reduceByKey((v1,v2)=>{
      //先循环 v1
     v1.intersect(v2)
   }).sortBy(p=>p._1._1,false).foreach(println)


  }
}
