package com.zhang.qzpoint.streaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming04_Source_Kafka {
    def main(args: Array[String]): Unit = {
    
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("kafka")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
    
        // kafka 参数
        //kafka参数声明
        val brokers = "linux1:9092,linux2:9092,linux3:9092"
        val topic = "source190826"
        val group = "bigdata"
        val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
        val kafkaParams = Map(
            ConsumerConfig.GROUP_ID_CONFIG -> group,
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
        )
    
        val kafkaDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc,
            kafkaParams,
            Set(topic)
        )
    
        val kafkaMapDS: DStream[String] = kafkaDS.map(_._2)
    
        kafkaMapDS.print()
    
        ssc.start()
        
        ssc.awaitTermination()
    
    }
}
