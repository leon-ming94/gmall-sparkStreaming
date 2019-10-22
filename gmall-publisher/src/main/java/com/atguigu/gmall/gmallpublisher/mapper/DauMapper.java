package com.atguigu.gmall.gmallpublisher.mapper;


import java.util.List;
import java.util.Map;

/**
 * @author shkstart
 * @create 2019-10-21 15:11
 */
public interface DauMapper  {

    public int sleectDauTotal(String date);

    public int getNewMidTotal( String date);

    //查询某日用户活跃数的分时值
    public List<Map<String,Long>> selectDauTotalHours(String date );
}
