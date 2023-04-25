package com.hoho.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object WordCountScala {

  def main(args: Array[String]): Unit = {
    // 1.创建上下文
    var conf = new SparkConf()
    conf.setAppName("WordCountScala")
    conf.setMaster("local")
    conf.set("spark.driver.bindAddress", "127.0.0.1")
    val sc = new SparkContext(conf)

    // 2.加载数据
    var path = "datas";
    if (args.length == 1) {
      path = args(0)
    }
    val linesRDD = sc.textFile(path)
    //    val linesRDD = sc.textFile("/Users/tlc/IdeaProjects/bigdata-test/src/main/resources/hello.txt")

    // 3.数据切割
    val wordsRDD = linesRDD.flatMap(_.split(" "))

    // 4.转化成 （word,1)这种形式
    val pairRDD = wordsRDD.map(word => (word, 1))

    // 5.聚合分组
    val wordCountRDD = pairRDD.reduceByKey(_ + _)

    // 6.打印结果
    wordCountRDD.foreach(wordCount => println(wordCount._1 + "--" + wordCount._2))

    // 7.停止sc
    sc.stop()
  }
}
