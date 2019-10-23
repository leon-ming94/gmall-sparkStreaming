package com.atguigu.test

import java.util

import com.atguigu.util.RedisUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * @author shkstart
  */
object KafkaStreaming {

    def main(args: Array[String]): Unit = {


        val ssc: StreamingContext = new StreamingContext(new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]"), Seconds(5))

        val kafkaParam = Map(
            "bootstrap.servers" -> "hadoop112:9092", //用于初始化链接到集群的地址
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            //用于标识这个消费者属于哪个消费团体
            "group.id" -> "gmall_consumer_group",
            //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
            //可以使用这个配置，latest自动重置偏移量为最新的偏移量
            "auto.offset.reset" -> "latest",
            //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
            //如果是false，会需要手动维护kafka偏移量
            "enable.auto.commit" -> (true: java.lang.Boolean)
        )

        val inputDStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Set("GMALL_STARTUP"), kafkaParam))

        // TODO 转换结构 得到(word,1) 再reducebykey _+_
        val wordCountDStream: DStream[(String, Int)] = inputDStream.map(_.value()).flatMap(line => {
            val wordArr: Array[String] = line.split(" ")
            wordArr.map((_, 1))
        }).reduceByKey(_ + _)

        // TODO 到redis中查询
        val wordCountFinalDStream: DStream[(String, Int)] = wordCountDStream.transform { rdd => {
            val jedis: Jedis = RedisUtil.getJedisClient
            val wordCountMap: util.Map[String, String] = jedis.hgetAll("word_count")
            jedis.close()
            val rdd2: RDD[(String, Int)] = rdd.map { t =>
                if (wordCountMap.containsKey(t._1)) {
                    val value: Int = wordCountMap.get(t._1).toInt
                    (t._1, t._2 + value)
                } else {
                    t
                }
            }
            rdd2
        }
        }

        // TODO 到缓存中更新redis数据 使用hash数据类型
        wordCountFinalDStream.foreachRDD { rdd =>
            rdd.foreachPartition { itor =>
                val jedis: Jedis = RedisUtil.getJedisClient
                val map: util.HashMap[String, String] = new util.HashMap[String, String]()
                while (itor.hasNext) {
                    map.put(itor.next()._1, itor.next()._2 + "")
                }
                jedis.hmset("word_count", map)
                jedis.close()
            }
        }

        ssc.start()

        ssc.awaitTermination()


    }

}
