import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object Test2_推荐系统 {
  /**
   * 1 2,3,4,5,6,7,8
   * 2 1,3,4,5,7
   * 3 1,2
   * 4 1,2,6
   * 5 1,2
   * 6 1,4
   * 7 1,2
   * 8 1
   * 项目需求:  用户一与所有人都是好友,所以不需要向这个用户推荐任何人.另一方面,用户3与用户1的共同好友是2,所以我
   * 可以向用户三推荐 4,5,6,7,8
   * 分析一下用户三,先向3用户推荐4 推荐的理由是  (3,1) 共同好友和 (2,3)共同好友   都有 可以将其变成一个这样的格式
   * 3 : 4(2,[1,2])  用户: 推荐(数量,[共同好友中包含4的好友])
   * @param args
   */
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR) //配置日志

    val spark=SparkSession
        .builder()
        .appName("实时用户推荐系统")
        .master("local[*]")
        .getOrCreate()//创建连接
  //  spark.sparkContext.setCheckpointDir("./check")
    //开始读数据
    val line=spark.sparkContext.textFile("Data/friends.txt")

    //根据其找出共同好友
    line.map(row=>{   //执行每一行 3 1,2
    //先设置一个类型
      val stringline=new StringBuffer()
      var list: List[String] = null
      //没循环一行  每一行中的每一个值都要进行匹配 从 (1,2)开始 找到 1,2要推荐的人6,循环中的推荐人,如果这个循环里面没有，
      //就要跳过,反过来,必须要找到  为什么要推荐6 呢 (1,6)和(2,6) 很明显 2中没有6，那么推荐的原因就是 该用户2的好友1里面有好友6
      val fileds=row.split(" ")
      val value=fileds(1).split(",")
      for (v<-value){
        if(fileds(0).toInt<v.toInt) {
          // (1,3) (2,3)
          // (fileds(0),v) 先找到所有的可插入列表
          list = findKey((fileds(0), v))
        }else{
          list= findKey((v,fileds(0)))
        }
//        println("这里"+list)
        for (li <- list){
//          println(value.toList,li,value.toList.contains(li))
          //先判断要推荐的这个人是不是包含
          if(!value.contains(li)) {
            //是不是要进行推荐  假设这里是用户3 要推荐的是 4 3有用户 1和 2
            //将 4 拼接上去
            stringline.append(li+" " )
            //还要判断 (1,4),(2,4)是不是有值
            if (isExcter( (v,li)  )){
              //说明有 所以我要拼接
              stringline.append(v+" ")
            }
            stringline.append(" ")
          }
        }
      }
      (fileds(0),stringline.toString)
    }).map(row=>{
      val map: mutable.HashMap[String,util.ArrayList[String]] = mutable.HashMap()
      val key=row._1
        val value = row._2.split("  ") //[ (3 1),  (5 1 ), (7 1 ), (8 1 ), (3 2) , (5 2),  (7 2 )  ]
        for (v <- value) {
          val filds = v.split(" ")
          if(filds.length==2) {
          if (map.keys.toList.contains(filds(0))) {
          //  (3 2)
            map.get(filds(0)).getOrElse(null).add(filds(1))
          } else {
            val list=new util.ArrayList[String]()
            list.add(filds(1))
            map.put(filds(0),list )
          }
        }
      }
      (key,map)
    }).map(row=>{
      val map: mutable.HashMap[String,util.ArrayList[String]] = mutable.HashMap()
      val list=new util.ArrayList[String]()
      for (i<- row._2.values){
        val sb=new StringBuffer()
        sb.append(i.toArray().length+","+i)
        list.add(sb.toString)
     }
      map.put(row._1,list)

      (row._1,map)
    }).foreach(println)

  }
  //根据键找出一个推荐列表的集合
  def findKey(key: Tuple2[String, String]): List[String] = {
    val spark=SparkSession
      .builder()
      .appName("实时用户推荐系统")
      .master("local[*]")
      .getOrCreate()//创建连接
    val line=spark.sparkContext.textFile("Data/friends.txt")

    val map1: mutable.HashMap[Tuple2[String, String], List[String]] = mutable.HashMap()
    //先将其两个人共同的好友进行展平
    val flatValue=line.flatMap(row=>{
      val fileds=row.split(" ")
      val value=fileds(1).split(",").toList
      //才存储一下这一行的值
      val row_value=value
      for (x <-value) yield ((fileds(0),x),row_value.diff(List(x)))
    }).map( row=>{
      //对两个键进行排序
      if(row._1._1.toInt>row._1._2.toInt){
        ((row._1._2,row._1._1),row._2)
      }else{
        ((row._1._1,row._1._2),row._2)
      }
    }   ).reduceByKey((v1,v2)=>{
      v1.diff(v2)
    }).collect()
    for (v<-flatValue){
      map1.put(v._1,v._2)
    }
  //  println(map1.keys.toList.contains(key))
//    println(key,map1.toString())
//    println( map1.get(key).getOrElse(List(null)) )
    map1.get(key).getOrElse(List(null))
  }
  //找出推荐这个用户的依据
  def isExcter(key: Tuple2[String, String]):Boolean={
    val spark=SparkSession
      .builder()
      .appName("实时用户推荐系统")
      .master("local[*]")
      .getOrCreate()//创建连接
    val line=spark.sparkContext.textFile("Data/friends.txt")
    val map: mutable.HashMap[Tuple2[String, String], List[String]] = mutable.HashMap()
    val flatValue=line.flatMap(row=>{
      val fileds=row.split(" ")
      val value=fileds(1).split(",").toList
      //才存储一下这一行的值
      val row_value=value
      for (x <-value) yield ((fileds(0),x),row_value.diff(List(x)))
    }).collect()
    for (v<- flatValue){
      map.put(v._1,v._2)
    }
    val list=map.keys.toList
   // println(list,key,list.contains(key))
    list.contains(key)
  }

}
