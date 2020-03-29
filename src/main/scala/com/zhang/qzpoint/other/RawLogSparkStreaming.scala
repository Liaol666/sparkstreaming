/*
package com.zhang.qzpoint.other

import java.sql.ResultSet
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * 通用原始日志数据落盘到hdfs
  */
object RawLogSparkStreaming {
  private var fs: FileSystem = null
  private var fSOutputStream: FSDataOutputStream = null
  private var writePath: Path = null
  private val hdfsBasicPath = "hdfs://nameservice1/user/atguigu/rawlogdata/"

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val topic = "page_topic" //args(0)
    val groupid = "raw_groupid" //args(1)
    val kafka_broker_list = "hadoop001:9092,hadoop002:9092,hadoop003:9092" //args(2)
    val topicTable = "offset_manager" //args(3)
    val sparkConf = new SparkConf().setAppName("RawLog_SparkStreaming")
      .set("spark.streaming.kafka.maxRatePerPartition", "20")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
//      .setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val sparkContext = ssc.sparkContext
    sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    sparkContext.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    val broker_list = kafka_broker_list
    val kafkaParam = Map(
      "bootstrap.servers" -> broker_list, //用于初始化链接到集群
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> groupid,
      //lastest自动重置偏移量为最新偏移量
      //"auto.offset.reset" -> "latest",earliest
      "auto.offset.reset" -> "earliest",
      //如果是true,则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val sqlProxy = new SqlProxy()
    val client = DataSourceUtil.getConnection()
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    try {
      sqlProxy.executeQuery(client, s"select *from ${topicTable} where groupid=?", Array(topic), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            val model = new TopicPartition(rs.getString(2), rs.getInt(3))
            val offset = rs.getLong(4)
            offsetMap.put(model, offset)
          }
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }
    val dataDStream = if (offsetMap.size == 0) {
      KafkaUtils.createDirectStream[String, String](ssc,
        LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
    } else {
      KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam, offsetMap))
    }

    //将时间戳格式化到天获取完整路径
    def getTotalPath(lastTime: Long): String = {
      val dft = DateTimeFormatter.ofPattern("yyyyMMdd")
      val formatDate = dft.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(lastTime), ZoneId.systemDefault()))
      //val directories = formatDate.split("-")
      val totalPath = hdfsBasicPath + "/" + topic + "/" + formatDate
      totalPath
    }

    val dataValueStream = dataDStream.map(item => (item.key(), item.value()))
    dataValueStream.foreachRDD(rdd => {
      val lastTime = System.currentTimeMillis()
      val writePath = getTotalPath(lastTime)
      val job = new JobConf()
      job.set("mapred.output.compress", "true")
      job.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec")
      rdd.saveAsHadoopFile(writePath,
        classOf[Text], classOf[Text], classOf[RDDMultipleAppendTextOutputFormat], job)
    })
    dataDStream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection()
      try {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (or <- offsetRanges) {
          sqlProxy.executeUpdate(client, s"replace into `${topicTable}` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
            Array(groupid, or.topic, or.partition.toString, or.untilOffset))
        }
      } catch {
        case e: Exception => e.printStackTrace();
      } finally {
        sqlProxy.shutdown(client)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
*/
