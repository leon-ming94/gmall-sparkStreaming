package com.atguigu.gmall.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.gmallpublisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author shkstart
 * @create 2019-10-21 15:58
 */
@RestController
public class PublishController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String realtimeHourDate(@RequestParam("date") String date){

        List<Map<String, Object>> list = publisherService.getDauTotal(date);

        return JSON.toJSONString(list);

    }

    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id ,@RequestParam("date") String dateString){
        if("dau".equals(id)) {
            Map<String, Long> dauTotalHoursTD = publisherService.getDauTotalHours(dateString);
            String yesterday = getYesterday(dateString);
            Map<String, Long> dauTotalHoursYD = publisherService.getDauTotalHours(yesterday);

            Map hourMap = new HashMap();
            hourMap.put("today", dauTotalHoursTD);
            hourMap.put("yesterday", dauTotalHoursYD);

            return JSON.toJSONString(hourMap);
        }else{
            return  null;
        }
    }

    private String   getYesterday(String today){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        try {
            Date todayD = simpleDateFormat.parse(today);
            Date yesterdayD = DateUtils.addDays(todayD, -1);
            String yesterday = simpleDateFormat.format(yesterdayD);
            return  yesterday;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;

    }
}
