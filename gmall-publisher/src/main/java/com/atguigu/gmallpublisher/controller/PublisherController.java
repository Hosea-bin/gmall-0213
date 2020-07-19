package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.Service.impl.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;

@RestController
public class PublisherController {
    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getrealTimeTotal(@RequestParam("date") String date){
        //1.查询phoenix获取数据
        Integer dauTotal = publisherService.getDateTotal(date);

        //2.创建集合用于存放结果数据
        ArrayList<Object> result = new ArrayList<>();

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

        //将两个Map放入集合
        result.add(dauMap);
        result.add(newMidMap);
        //返回最终结果
        return JSONObject.toJSONString(result);
    }
}
