package com.atguigu.app


import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constant.GmallConstant
import com.atguigu.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
  * @author shkstart
  */
object OrderApp {
    def main(args: Array[String]): Unit = {
        //  TODO    获取上下文对象ssc
        val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        // TODO 消费kafka发送的数据
        val dStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_ORDER_INFO, ssc)

        // TODO 数据脱敏 增加日期字段
        val orderInfoDStream: DStream[OrderInfo] = dStream.map { record =>
            val order_info_json: String = record.value
            val orderInfo: OrderInfo = JSON.parseObject(order_info_json, classOf[OrderInfo])

            // 解析创建日期和创建小时
            val create_time: String = orderInfo.create_time
            val create_time_arr: Array[String] = create_time.split(" ")
            orderInfo.create_date = create_time_arr(0)
            orderInfo.create_hour = create_time_arr(1).split(":")(0)

            //手机号码脱敏
            orderInfo.consignee_tel = orderInfo.consignee_tel.substring(0, 3) + "****" + orderInfo.consignee_tel.substring(7)
            println(orderInfo.toString)
            orderInfo
        }
        orderInfoDStream.cache()

        // TODO 保存到hbase中
        orderInfoDStream.foreachRDD { rdd =>
            rdd.saveToPhoenix("GMALL2019_ORDER_INFO", Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
                new Configuration,
                Some("hadoop112:2181"))
        }

        ssc.start()

        ssc.awaitTermination()

    }
}
