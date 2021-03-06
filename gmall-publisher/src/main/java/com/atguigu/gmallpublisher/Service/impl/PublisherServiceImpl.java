package com.atguigu.gmallpublisher.Service.impl;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.GmvMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private GmvMapper gmvMapper;

    @Override
    public Integer getDateTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHourMap(String date) {
        //查询日活分时统计数
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //2.创建Map用于存放转换结构之后的数据
        HashMap<String, Long> result = new HashMap<>();

        //3.遍历list，将数据取出放入result
        for(Map map:list){
            result.put((String) map.get("LH"),(Long) map.get("CT"));
        }
        return result;
    }

    //获取日活总数
    @Override
    public Double getOrderAmountTotal(String date) {
        return gmvMapper.selectOrderAmountTotal(date);
    }
    //获取日活分时统计数
    @Override
    public Map getOrderAmountHourMap(String date) {
        //1.查询日活分时统计数
        List<Map> list = gmvMapper.selectOrderAmountHourMap(date);
        //2.创建Map用于存放转换结构之后的数据
        HashMap<String, Double> result = new HashMap<>();
        //3.遍历list,将数据取出放入result
        for (Map map : list) {
            result.put((String)map.get("CREATE_HOUR"),(Double)map.get("SUM_AMOUNT"));
        }
        //4.返回结果
        return result;
    }

}
