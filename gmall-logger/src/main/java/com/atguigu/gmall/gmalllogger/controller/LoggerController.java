package com.atguigu.gmall.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.constant.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author shkstart
 * @create 2019-10-18 17:23
 */
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @PostMapping("/log")
    public String logSend(@RequestParam("logString") String logString){

        JSONObject jsonObject = JSON.parseObject(logString);
        if(null == jsonObject){
            log.error("日志记录有误");
        }

        jsonObject.put("ts", System.currentTimeMillis());
        String type = jsonObject.getString("type");
        if("startup".equals(type)){
            kafkaTemplate.send(GmallConstant.KAFKA_STARTUP, jsonObject.toJSONString());
        }else if("event".equals(type)){
            kafkaTemplate.send(GmallConstant.KAFKA_EVENT, jsonObject.toJSONString());
        }
        log.info(jsonObject.toJSONString());

        return "success";
    }
}
