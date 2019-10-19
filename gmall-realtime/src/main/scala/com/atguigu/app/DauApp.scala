package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constant.GmallConstant
import com.atguigu.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis


/**
  * @author shkstart
  */
object DauApp {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
        //TODO 1 打通kafka消费通道
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_STARTUP, ssc)

        val startUpDStream: DStream[StartUpLog] = kafkaDStream.map(datas => {
            val log: String = datas.value()
            val startUpLog: StartUpLog = JSON.parseObject(log, classOf[StartUpLog])

            val formattor: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
            val dateString: String = formattor.format(new Date())
            val dateArray: Array[String] = dateString.split(" ")
            startUpLog.logDate = dateArray(0)
            startUpLog.logHour = dateArray(1)

            startUpLog
        })

        //TODO 2 分区内去重
        val filtterDStream: DStream[StartUpLog] = startUpDStream.map(startUpLog => (startUpLog.mid, startUpLog)).groupByKey().flatMap {
            case (mid, datas) => {
                datas.toList.sortWith((startupLog1, startupLog2) => {
                    startupLog1.time < startupLog2.time
                }).take(1)
            }
        }


        //TODO 3 redis去重
        val totallyFilterDStream: DStream[StartUpLog] = filtterDStream.transform(rdd => {
            println("去重前" + rdd.count())
            val jedisClient: Jedis = RedisUtil.getJedisClient
            val formattor: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
            val dateString: String = formattor.format(new Date())
            val dateSet: util.Set[String] = jedisClient.smembers("dau:" + dateString)
            val broadcast: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dateSet)

            val totallyFilterRDD: RDD[StartUpLog] = rdd.filter(startUpLog => {
                val dataSet: util.Set[String] = broadcast.value
                val result: Boolean = dataSet.contains(startUpLog.mid)
                !result
            })
            println("去重后" + totallyFilterRDD.count())
            totallyFilterRDD
        })

        //TODO 4 更新redis数据 使用set结构 k:当天日期 v:设备id mid
        totallyFilterDStream.foreachRDD { rdd =>
            rdd.foreachPartition { itor =>
                val jedisClient: Jedis = RedisUtil.getJedisClient
                while (itor.hasNext) {
                    val startUpLog: StartUpLog = itor.next()
                    println(startUpLog)
                    val mid: String = startUpLog.mid
                    jedisClient.sadd("dau:" + startUpLog.logDate, mid)
                }
                jedisClient.close()
            }
        }

        ssc.start()

        ssc.awaitTermination()
    }

}
