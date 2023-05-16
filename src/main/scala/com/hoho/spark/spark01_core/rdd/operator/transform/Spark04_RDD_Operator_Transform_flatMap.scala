package com.hoho.spark.spark01_core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform_flatMap {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator").set("spark.driver.bindAddress", "127.0.0.1")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - mapPartitions
        val rdd: RDD[List[Int]] = sc.makeRDD(List(
            List(1, 2), List(3, 4)
        ))

        // list => list 函数的作用是将 RDD 中的每个集合作为输入，返回一个由该集合中的所有元素组成的RDD。
        // 由于这个函数返回的是集合本身，因此 flatMap 会将所有集合中的元素都取出来，然后将它们组成一个新的 RDD 返回
        val flatRDD: RDD[Int] = rdd.flatMap(list => list)
        flatRDD.collect().foreach(println)



        sc.stop()

    }
}
