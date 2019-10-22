package com.atguigu.util.testforkafka

//import com.atguigu.util.ValueUtils
//import kafka.common.TopicAndPartition
//import kafka.message.MessageAndMetadata
//import kafka.serializer.StringDecoder
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import scalikejdbc.{DB, SQL}
//import scalikejdbc.config.DBs

/**
  * @author shkstart
  */
object StreamingOffsetMySQL {
//    def main(args: Array[String]): Unit = {
//        val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingOffsetMySQL")
//
//        val ssc = new StreamingContext(conf, Seconds(10))
//
//        //Topic
//        val topics = ValueUtils.getStringValue("kafka.topics").split(",").toSet
//
//        //kafka参数
//        //这里应用了自定义的ValueUtils工具类，来获取application.conf里的参数，方便后期修改
//        val kafkaParams = Map[String, String](
//            "metadata.broker.list" -> ValueUtils.getStringValue("kafka.broker.list"),
//            "auto.offset.reset" -> ValueUtils.getStringValue("auto.offset.reset"),
//            "group.id" -> ValueUtils.getStringValue("group.id")
//        )
//
//
//        //先使用2222222从MySQL数据库中读取offset信息
//        //+------------+------------------+------------+------------+-------------+
//        //| topic      | groupid          | partitions | fromoffset | untiloffset |
//        //+------------+------------------+------------+------------+-------------+
//        //MySQL表结构如上，将“topic”，“partitions”，“untiloffset”列读取出来
//        //组成 fromOffsets: Map[TopicAndPartition, Long]，后面createDirectStream用到
//
//
//        DBs.setup()
//        val fromOffset = DB.readOnly(implicit session => {
//            SQL("select * from mysql_offset").map(rs => {
//                (TopicAndPartition(rs.string("topic"), rs.int("partitions")), rs.long("untiloffset"))
//            }).list().apply()
//        }).toMap
//
//
//        //如果MySQL表中没有offset信息，就从0开始消费；如果有，就从已经存在的offset开始消费
//        val messages: InputDStream[(String, String)] = if (fromOffset.isEmpty) {
//            println("从头开始消费...")
//            KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
//        } else {
//            println("从已存在记录开始消费...")
//            val messageHandler = (mm: MessageAndMetadata[String, String]) => (mm.key(), mm.message())
//            KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffset, messageHandler)
//        }
//
//
//        messages.foreachRDD(rdd => {
//            if (!rdd.isEmpty()) {
//                //输出rdd的数据量
//                println("数据统计记录为：" + rdd.count())
//                //官方案例给出的获得rdd offset信息的方法，offsetRanges是由一系列offsetRange组成的数组
//                //          trait HasOffsetRanges {
//                //            def offsetRanges: Array[OffsetRange]
//                //          }
//                val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//                offsetRanges.foreach(x => {
//                    //输出每次消费的主题，分区，开始偏移量和结束偏移量
//                    println(s"---${x.topic},${x.partition},${x.fromOffset},${x.untilOffset}---")
//                    //将最新的偏移量信息保存到MySQL表中
//                    DB.autoCommit(implicit session => {
//                        SQL("replace into mysql_offset(topic,groupid,partitions,fromoffset,untiloffset) values (?,?,?,?,?)")
//                                .bind(x.topic, ValueUtils.getStringValue("group.id"), x.partition, x.fromOffset, x.untilOffset)
//                                .update().apply()
//                    })
//                })
//            }
//        })
//
//        ssc.start()
//        ssc.awaitTermination()
//    }
}
