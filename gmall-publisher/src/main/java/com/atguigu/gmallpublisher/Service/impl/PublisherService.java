package com.atguigu.gmallpublisher.Service.impl;

import java.util.Map;

public interface PublisherService {

    public Integer getDateTotal(String date);

    //获取日活分时统计数
    public Map getDauTotalHourMap(String date);

    //获取GMV总数
    public Double getOrderAmountTotal(String date);

    //获取GMV分时统计结果
    public Map getOrderAmountHourMap(String date);
}
