package com.hoho.spark.spark01_core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform1 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator").set("spark.driver.bindAddress", "127.0.0.1")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - flatMap
        val rdd: RDD[String] = sc.makeRDD(List(
            "Hello Scala", "Hello Spark"
        ))

        val flatRDD: RDD[String] = rdd.flatMap(
            s => {
                s.split(" ")
            }
        )
        flatRDD.collect().foreach(println)



        sc.stop()

    }
}
