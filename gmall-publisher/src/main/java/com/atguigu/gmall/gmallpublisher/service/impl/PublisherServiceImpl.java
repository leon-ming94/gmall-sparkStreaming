package com.atguigu.gmall.gmallpublisher.service.impl;

import com.atguigu.gmall.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmall.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmall.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author shkstart
 * @create 2019-10-21 15:50
 */
@Service("publisherService")
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Override
    public List<Map<String,Object>> getDauTotal(String date) {
        Integer dauTotal = dauMapper.sleectDauTotal(date);
        List<Map<String,Object>> list = new ArrayList<>();
        // 日活总数
        Map<String,Object> dauMap=new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);
        list.add(dauMap);

        // 新增用户
        int newMidTotal = dauMapper.getNewMidTotal(date);
        Map newMidMap=new HashMap<String,Object>();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增用户");
        newMidMap.put("value",newMidTotal);
        list.add(newMidMap);
        return list;
    }

    @Override
    public Map<String, Long> getDauTotalHours(String date) {
        List<Map<String, Long>> dauListMap = dauMapper.selectDauTotalHours(date);
        Map<String ,Long> dauMap =new HashMap();
        for (Map map : dauListMap) {
            String  lh =(String) map.get("LH");
            Long  ct =(Long) map.get("CT");
            dauMap.put(lh,ct);
        }
        return dauMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmount(date);
    }

    @Override
    public Map<String, Double> getOrderAmountHours(String date) {
        //变换格式 [{"C_HOUR":"11","AMOUNT":489.0},{"C_HOUR":"12","AMOUNT":223.0}]
        //===》 {"11":489.0,"12":223.0 }
        Map<String, Double> hourMap=new HashMap<>();
        List<Map> mapList = orderMapper.selectOrderAmountHour(date);
        for (Map map : mapList) {
            hourMap.put((String)map.get("C_HOUR"),(Double) map.get("AMOUNT"));
        }

        return hourMap;
    }

}
