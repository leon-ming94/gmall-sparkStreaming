package com.atguigu.gmall.gmallpublisher.service;

import java.util.List;
import java.util.Map;

/**
 * @author shkstart
 * @create 2019-10-21 15:49
 */
public interface PublisherService {

    public List<Map<String,Object>> getDauTotal(String date );

    public Map<String,Long> getDauTotalHours(String date);

    public Double getOrderAmount(String date);

    public Map<String,Double> getOrderAmountHours(String date);

}
