package com.zhang.qzpoint.streaming

import org.apache.spark.{SparkConf, SparkFiles}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.lang
import java.sql.{Connection, ResultSet}
import java.text.NumberFormat

import com.alibaba.fastjson.JSONObject
import com.zhang.qzpoint.util.{DataSourceUtil, ParseJsonData, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  *
  * 页面转换率实时统计
  */
object PageStreaming {
  private val groupid = "page_groupid"

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "100") //每秒单个区拉取文件个数
      .set("spark.streaming.backpressure.enabled", "true") //开启背压，动态获取
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    //开启优雅关闭，即使kill掉之后，也会把任务跑完在关闭
    val ssc = new StreamingContext(conf, Seconds(3))
    //设置监控的topic
    val topics: Array[String] = Array("pagetopic")
    //设置kafka的配置
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop101:9092,hadoop102:9092,hadoop103:9092",
      //设置key，value的反序列化
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      //第一次启动不丢数据，从最开始消费
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    //查询mysql是否存在偏移量
    val sqlProxy = new SqlProxy()
    //用来存放偏移量
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val client: Connection = DataSourceUtil.getConnection
    try {
      sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid=?",
        Array(groupid), new QueryCallback {
          override def process(rs: ResultSet): Unit = {
            while (rs.next()) {
              val model = new TopicPartition(rs.getString(2), rs.getInt(3))
              val offset: Long = rs.getLong(4)
              offsetMap.put(model, offset)
            }
          }
        })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }
    //设置kafka消费数据的参数，判断本地是否有偏移量 有则根据偏移量继续消费 无则重新消费
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap)
      )
    } else {
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
    }
    //解析json数据
    val dsStream: DStream[(String, String, String, String, String, String, String)] = stream.map(item => item.value()).mapPartitions(partition => {
      partition.map(item => {
        val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
        //解析json之后，判断key是否存在
        val uid: String = if (jsonObject.containsKey("uid")) jsonObject.getString("uid") else ""
        val app_id: String = if (jsonObject.containsKey("app_id")) jsonObject.getString("app_id") else ""
        val device_id: String = if (jsonObject.containsKey("device_id")) jsonObject.getString("device_id") else ""
        val ip: String = if (jsonObject.containsKey("ip")) jsonObject.getString("ip") else ""
        val last_page_id: String = if (jsonObject.containsKey("last_page_id")) jsonObject.getString("last_page_id") else ""
        val pageid: String = if (jsonObject.containsKey("page_id")) jsonObject.getString("page_id") else ""
        val next_page_id: String = if (jsonObject.containsKey("next_page_id")) jsonObject.getString("next_page_id") else ""
        //封装到元组里，不建议这样，因为识别度低，使用cass class
        (uid, app_id, device_id, ip, last_page_id, pageid, next_page_id)
      })
    })
    //因为需要多次用到，所以缓存下
    dsStream.cache()
    val resultDStream: DStream[(String, Int)] = dsStream.map(item => (item._5 + "-" + item._6 + "-" + item._7, 1)).reduceByKey(_ + _)
    //按照组分好，并且聚合之后，按照分区连接数据源，查询之后并更新
    //计算页面跳转个数
    resultDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        //从分区创建连接
        val sqlProxy = new SqlProxy()
        val client: Connection = DataSourceUtil.getConnection
        try {
          partition.foreach(item => {
            calcPageJumpCount(sqlProxy, item, client)
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })
    })

    /**
      * 计算城市点击并且求top3；先用ip工具解析ip得出城市，先从mysql获得历史数据，早加上现在的数据
      * 同时更新到mysql
      * 计算top数据，同时更新mysql
      */
    //ip解析工具，addFile是分发解析工具，为了让每个任务可以从各自的executor获取
    ssc.sparkContext.addFile("hdfs://mycluster/user/sparkstreaming/ip2region.db")
//    ssc.sparkContext.addFile(this.getClass.getResource("ip2region.db").getPath)
    val ipDStream: DStream[(String, Long)] = dsStream.mapPartitions(partitions => {
      val dbFile: String = SparkFiles.get("ip2region.db")
      val ipSearch = new DbSearcher(new DbConfig(), dbFile)
      partitions.map(item => {
        val ip: String = item._4
        //通过解析工具获取到省份
        val province: String = ipSearch.memorySearch(ip).getRegion().split("\\|")(2)
        (province, 1L) //根据省份统计点击个数
      })
    }).reduceByKey(_ + _)

    ipDStream.foreachRDD(rdd => {
      //查询mysql历史数据 转成rdd
      val ipSqlProxy = new SqlProxy()
      val ipClient: Connection = DataSourceUtil.getConnection
      try {
        //用来储存历史数据
        val history_data = new ArrayBuffer[(String, Long)]()
        ipSqlProxy.executeQuery(ipClient, "select province, num from tmp_city_num_detail", null, new QueryCallback {
          override def process(rs: ResultSet): Unit = {
            while (rs.next()) {
              val tuple: (String, Long) = (rs.getString(1), rs.getLong(2))
              history_data += tuple
            }
          }
        })
        val history_rdd: RDD[(String, Long)] = ssc.sparkContext.makeRDD(history_data)
        //老数据和新数据join之后，数量相加
        val resultRdd: RDD[(String, Long)] = history_rdd.fullOuterJoin(rdd).map(item => {
          val province: String = item._1
          //如果数量一个是null 一个是Long的话，相加会报错，所以使用getOrElse
          val nums: Long = item._2._1.getOrElse(0L) + item._2._2.getOrElse(0L)
          (province, nums)
        })
        //把结果的rdd，存到mysql中
        resultRdd.foreachPartition(partitions => {
          val sqlProxy = new SqlProxy()
          val client: Connection = DataSourceUtil.getConnection
          try {
            partitions.foreach(item => {
              val province: String = item._1
              val num: Long = item._2
              //修改mysql的数据，并重组返回最新结果数据
              sqlProxy.executeUpdate(client, "insert into tmp_city_num_detail(province, num) values(?,?) on duplicate key update num=?",
                Array(province, num, num))
            })
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            sqlProxy.shutdown(client)
          }
        })
        //求出top3
        val top3Rdd: Array[(String, Long)] = resultRdd.sortBy[Long](_._2, false).take(3) //false之后为倒序
        //先清空原先的top表
        sqlProxy.executeUpdate(ipClient, "truncate table top_city_num", null)
        //插入数据
        top3Rdd.foreach(item => {
          sqlProxy.executeUpdate(ipClient, "insert into top_city_num(province,num) values(?,?)", Array(item._1, item._2))
        })
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(ipClient)
      }
    })

    /**
      * 计算转换率
      * 处理完业务逻辑之后 手动提交offset
      */
    stream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client: Connection = DataSourceUtil.getConnection
      try {
        calcJumRate(sqlProxy, client) //计算转化率
        //完成业务后手动提交offset
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (elem <- offsetRanges) {
          sqlProxy.executeUpdate(client, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
            Array(groupid, elem.topic, elem.partition, elem.untilOffset))
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(client)
      }

    })
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 计算页面跳转个数
    */
  def calcPageJumpCount(sqlProxy: SqlProxy, item: (String, Int), client: Connection) = {
    val keys: Array[String] = item._1.split("-")
    //当前页面的点击次数
    var num: Long = item._2
    val page_id: Int = keys(1).toInt
    //获取当前page_id
    val last_page_id: Int = keys(0).toInt
    //获取上一page_id
    val next_page_id: Int = keys(2).toInt //获取下页面page_id
    //获取当前页pageid的历史num个数
    sqlProxy.executeQuery(client, "select num from page_jump_rate where page_id=?", Array(page_id), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          //rs是游标，下标从1开始，查询的返回值按照下标获取
          num += rs.getLong(1)
        }
        rs.close()
      }
    })
    //对num进行修改，并判断当前page_id是否为首页
    if (page_id == 1) {
      sqlProxy.executeUpdate(client, "insert into page_jump_rate(last_page_id,page_id,next_page_id,num,jump_rate)" +
        "values(?,?,?,?,?) on duplicate key update num = ?", Array(last_page_id, page_id, next_page_id, num, "100%", num))
    } else {
      sqlProxy.executeUpdate(client, "insert into page_jump_rate(last_page_id,page_id,next_page_id,num)" +
        "values(?,?,?,?) on duplicate key update num = ?", Array(last_page_id, page_id, next_page_id, num, num))
    }
  }

  //计算跳转率
  def calcJumRate(sqlProxy: SqlProxy, client: Connection): Unit = {
    var page1_num = 0l
    var page2_num = 0l
    var page3_num = 0l
    sqlProxy.executeQuery(client, "select num from  page_jump_rate where page_id=?", Array(1), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          page1_num = rs.getLong(1)
        }
      }
    })
    sqlProxy.executeQuery(client, "select num from page_jump_rate where page_id=?", Array(2), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          page2_num = rs.getLong(1)
        }
      }
    })
    sqlProxy.executeQuery(client, "select num from page_jump_rate where page_id=?", Array(3), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          page3_num = rs.getLong(1)
        }
      }
    })
    val nf: NumberFormat = NumberFormat.getPercentInstance
    val page1ToPage2Rate = if (page1_num == 0) "0%" else nf.format(page2_num.toDouble / page1_num.toDouble)
    val page2ToPage3Rate = if (page2_num == 0) "0%" else nf.format(page3_num.toDouble / page2_num.toDouble)
    sqlProxy.executeUpdate(client, "update page_jump_rate set jump_rate=? where page_id = ?", Array(page1ToPage2Rate, 2))
    sqlProxy.executeUpdate(client, "update page_jump_rate set jump_rate=? where page_id=?", Array(page2ToPage3Rate, 3))
  }
}
