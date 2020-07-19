package com.atguigu.gmallpublisher.Service.impl;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    private DauMapper dauMapper;

    @Override
    public Integer getDateTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

}
