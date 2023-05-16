package com.hoho.spark.spark01_core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {

  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf()
      // [*] 表示当前系统最大可用核数，用于模拟并行的操作
      .setMaster("local[*]")
      .setAppName("RDD")
      .set("spark.driver.bindAddress", "127.0.0.1")

    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD
    // 从内存中创建RDD，将内存中集合的数据作为处理的数据源
    val seq = Seq[Int](1, 2, 3, 4)

    // parallelize : 并行
    //val rdd: RDD[Int] = sc.parallelize(seq)
    // makeRDD方法在底层实现时其实就是调用了rdd对象的parallelize方法。
    val rdd: RDD[Int] = sc.makeRDD(seq)
//    val rdd = sc.makeRDD(seq)

    rdd.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
