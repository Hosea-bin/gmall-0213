package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.Service.impl.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PublisherController {
    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getRealTimeTotal(@RequestParam("date") String date){
        //1.查询phoenix获取数据
        Integer dauTotal = publisherService.getDateTotal(date);
        Double orderAmountTotal = publisherService.getOrderAmountTotal(date);

        //2.创建集合用于存放结果数据
        ArrayList<Map> result = new ArrayList<>();

        //3.创建Map用于存放日活数据
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);

        //创建Map存放新增数据
        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id","new_id");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233);

        //创建Map用于存放GMV数据
        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id","order_amount");
        gmvMap.put("name","新增交易额");
        gmvMap.put("value",orderAmountTotal);

        //将两个Map放入集合
        result.add(dauMap);
        result.add(newMidMap);
        result.add(gmvMap);
        //返回最终结果
        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauTotalHourMap(@RequestParam("id") String id,@RequestParam("date") String date){
        //创建Map存放数据结果
        HashMap<String, Map> result = new HashMap<>();
        //获取昨天的日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();
        //创建今天以及昨天分时数据的Map
        Map todayMap = null;
        Map yesterdayMap =null;
        //请求日活分时统计数据
        if("dau".equals(id)){
            //1.获取今天的分时统计数据‘
            todayMap = publisherService.getDauTotalHourMap(date);
            //2.获取昨天分时统计数
           //java8新特性
            yesterdayMap = publisherService.getDauTotalHourMap(yesterday);
        }else if("order_amount".equals(id)){
            //1.获取今天的分时统计数据
            todayMap = publisherService.getOrderAmountHourMap(date);
            //2.获取昨天的分时统计数据
            yesterdayMap=publisherService.getOrderAmountHourMap(yesterday);
        }else if("new_mid".equals(id)){
            todayMap=new HashMap<>();
            todayMap.put("05",50);
            todayMap.put("09",140);
            todayMap.put("13",153);

            yesterdayMap=new HashMap<>();
            yesterdayMap.put("04",78);
            yesterdayMap.put("03",90);
            yesterdayMap.put("01",104);
        }
        //3.将2天的分时数据放入result
        result.put("yesterday",yesterdayMap);
        result.put("today",todayMap);

        //返回最终结果
        return JSONObject.toJSONString(result);
    }
}
