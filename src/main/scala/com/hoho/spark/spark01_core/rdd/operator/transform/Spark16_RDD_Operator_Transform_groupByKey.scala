package com.hoho.spark.spark01_core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Operator_Transform_groupByKey {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator").set("spark.driver.bindAddress", "127.0.0.1")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key - Value类型)

        val rdd = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("a", 3), ("b", 4)
        ))

        // groupByKey : 将数据源中的数据，相同key的数据分在一个组中，形成一个对偶元组
        //              元组中的第一个元素就是key，
        //              元组中的第二个元素就是相同key的value的集合

        // spark中，shuffle操作必须落盘处理，不能在内存中数据等待，会导致内存溢出。
        val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
        groupRDD.collect().foreach(println)

        println("========")

        val groupRDD1: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
        groupRDD1.collect().foreach(println)

        sc.stop()

    }
}
