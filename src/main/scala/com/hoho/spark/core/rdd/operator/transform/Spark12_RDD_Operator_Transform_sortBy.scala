package com.hoho.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform_sortBy {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator").set("spark.driver.bindAddress", "127.0.0.1")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - sortBy
        val rdd = sc.makeRDD(List(6,2,4,5,3,1), 2)

        val newRDD: RDD[Int] = rdd.sortBy(num=>num)

        newRDD.saveAsTextFile("output")

        sc.stop()

    }
}
