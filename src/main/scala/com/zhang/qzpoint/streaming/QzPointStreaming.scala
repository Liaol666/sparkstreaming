package com.zhang.qzpoint.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.lang
import java.sql.{Connection, ResultSet}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.zhang.qzpoint.util.{DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._

import scala.collection.mutable

object QzPointStreaming {
  private val groupid = "qz_point_group"

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
      //最大每个分区能从kafka消费的文件个数，消费的速度要大于生产的速度，不然造成kafka挤积压
      //topic如果有10个区的话，就是每秒一次性拉1000个文件
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      //开启背压，如果有延迟，拉取速度会降到1000以下，
      // 不延迟会达到1000拉取（具体拉取个数需要看几秒一个批次）
      .set("spark.streaming.backpressure.enabled", "true")
    //3秒拉取一个批次
    val ssc = new StreamingContext(conf, Seconds(3))
    val topics = Array("qz_log")
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop101:9092,hadoop102:9092,hadoop103:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    //查询mysql中是否存在偏移量
    val sqlProxy = new SqlProxy()
    //用一个hashmap存offset
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    //创建mysql客户端
    val client: Connection = DataSourceUtil.getConnection
    try {
      sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid=?",
        Array(groupid), new QueryCallback {
          override def process(rs: ResultSet): Unit = {
            //这个游标下标是从1开始的
            while (rs.next()) {
              val model = new TopicPartition(rs.getString(2), rs.getInt(3))
              val offset: Long = rs.getLong(4)
              offsetMap.put(model, offset)
            }
            //关闭游标
            rs.close()
          }
        })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      //最终要关闭连接
      sqlProxy.shutdown(client)
    }
    //设置kafka消费数据的参数 判断本地是否有偏移量， 有则从偏移量开始读取，没有则重新消费


    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
    }
    val dsStream: DStream[(String, String, String, String, String, String)] = stream.filter(item => item.value().split("\t").length == 6).mapPartitions(
      partition => {
        partition.map(item => {
          val line: String = item.value()
          val arr: Array[String] = line.split("\t")
          val uid: String = arr(0)
          //用户id
          val courseid: String = arr(1)
          //课程id
          val pointid: String = arr(2)
          //知识点id
          val questionid: String = arr(3)
          //题目id
          val istrue: String = arr(4)
          //是否正确
          val createtime: String = arr(5) //创建时间
          (uid, courseid, pointid, questionid, istrue, createtime)
        })
      }
    )
    dsStream.foreachRDD(rdd => {
      //获取相同用户 同一课程 同一知识点的数据
      val groupRdd: RDD[(String, Iterable[(String, String, String, String, String, String)])] = rdd.groupBy(item => item._1 + "_" + item._2 + "_" + item._3)
      groupRdd.foreachPartition(partition => {
        //在分区下获取jdbc连接
        val sqlProxy = new SqlProxy()
        val client: Connection = DataSourceUtil.getConnection
        try {
          partition.foreach { case (key, iters) =>
            qzQuestionUpdate(key, iters, sqlProxy, client) //对题库进行更新
          }
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      }
      )
    })
    //处理完业务逻辑后 后动提交offset维护到本地mysql中
    stream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client: Connection = DataSourceUtil.getConnection
      try {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (or <- offsetRanges) {
          sqlProxy.executeUpdate(client, "replace into `offset_manager` (groupid, topic, `partition`, untilOffset) values(?,?,?,?)",
            Array(groupid, or.topic, or.partition.toString, or.untilOffset))
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

  /*
  * 对题目表进行更新操作
  * */

  def qzQuestionUpdate(key: String, iters: Iterable[(String, String, String, String, String, String)], sqlProxy: SqlProxy, client: Connection) = {
    val keys: Array[String] = key.split("_")
    val userid: Int = keys(0).toInt
    val courseid: Int = keys(1).toInt
    val pointid: Int = keys(2).toInt
    //如果迭代器想使用多次就要转成list或者数组
    val array: Array[(String, String, String, String, String, String)] = iters.toArray
    val questionids: Array[String] = array.map(_._4).distinct
    //对当前批次的数据下questionid去重
    //查询历史数据下的questionid
    var questionids_history: Array[String] = Array()
    sqlProxy.executeQuery(client, "select questionids from qz_point_history where userid=? and courseid=? and pointid=?",
      Array(userid, courseid, pointid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            questionids_history = rs.getString(1).split(",")
          }
          rs.close() //关闭游标
        }
      })
    //获取到历史数据后再与当前数据进行拼接 去重
    val resultQuestionid: Array[String] = questionids.union(questionids_history).distinct
    val countSize: Int = resultQuestionid.length
    val resultQuestionid_str: String = resultQuestionid.mkString(",")
    val qz_count: Int = questionids.length //去重后的题个数
    var qz_sum: Int = array.length //获取当前批次题总数
    var qz_istrue: Int = array.filter(_._5.equals("1")).size //获取当前批次做正确的题个数
    val createtime: String = array.map(_._6).min
    //获取最早的创建时间，作为表中创建时间
    //更新qz_point_set 记录表 此表用于存当前用户做过的questionid表
    //使用这个格式化时间没有线程安全问题
    val updatetime: String = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now())
    //如果有就记录如果没有就插入，但是表中必须要有唯一主键
    sqlProxy.executeUpdate(client, "insert into qz_point_history(userid,courseid,pointid,questionids,createtime,updatetime) values(?,?,?,?,?,?) " +
      " on duplicate key update questionids=?,updatetime=?", Array(userid, courseid, pointid, resultQuestionid_str, createtime, createtime, resultQuestionid_str, updatetime))

    var qzSum_history = 0
    var istrue_history = 0
    sqlProxy.executeQuery(client, "select qz_sum,qz_istrue from qz_point_detail where userid=? and courseid=? and pointid=?",
      Array(userid, courseid, pointid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            qzSum_history += rs.getInt(1)
            istrue_history += rs.getInt(2)
          }
          rs.close()
        }
      })
    qz_sum += qzSum_history
    qz_istrue += istrue_history
    val correct_rate: Double = qz_istrue.toDouble / qz_sum.toDouble //计算正确率
    //计算完成率
    //加个每个知识点下一共有30道题 先计算题的做题情况 在计算知识点掌握度
    val qz_detail_rate: Double = countSize.toDouble / 30 //做题情况
    //做题情况乘以正确率得出完成率 假如30题都做了 那么正确率等于熟练度
    val mastery_rate: Double = qz_detail_rate * correct_rate
    sqlProxy.executeUpdate(client, "insert into qz_point_detail(userid,courseid,pointid,qz_sum,qz_count,qz_istrue,correct_rate,mastery_rate,createtime,updatetime)" +
      " values(?,?,?,?,?,?,?,?,?,?) on duplicate key update qz_sum=?,qz_count=?,qz_istrue=?,correct_rate=?,mastery_rate=?,updatetime=?",
      Array(userid, courseid, pointid, qz_sum, countSize, qz_istrue, correct_rate, mastery_rate, createtime, updatetime, qz_sum, countSize, qz_istrue, correct_rate, mastery_rate, updatetime))
  }
}
