package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import atguigu.GmallConstants
import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
import org.apache.hadoop.hbase.HBaseConfiguration

object DauApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //3.消费Kafka数据，创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(Set(GmallConstants.GMALL_START),ssc)

    //4.转换为StartUpLog对象（添加日期和小时）
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.map { record =>
      //a.将Value转换为StartUpLog对象
      val value: String = record.value()
      val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
      //b.获取时间戳
      val ts: Long = startUpLog.ts
      //c.格式化时间
      val dateHour: String = sdf.format(new Date(ts))
      //d.将dateHour按照日期和小时分隔开
      val dateHourArr: Array[String] = dateHour.split(" ")
      //e.赋值日期和小时
      startUpLog.logDate = dateHourArr(0)
      startUpLog.logHour = dateHourArr(1)
      //f.返回数据
      startUpLog
    }
    startUpLogDStream.cache()
    startUpLogDStream.count().print()

    //5.去重1 -->根据Redis做跨批次去重
   val filteredByRedisLogDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream,ssc )
    filteredByRedisLogDStream.cache()
    filteredByRedisLogDStream.count().print()

    //6.去重2-->根据mid做同批次去重
   val filteredByGroupDStream: DStream[StartUpLog] = DauHandler.filterByGroup(filteredByRedisLogDStream)
   filteredByGroupDStream.cache()
    filteredByGroupDStream.count().print()

    //7.将两次数据去重后的Mid及日期写入Redis,提供给当天的批次做去重
    DauHandler.saveDateAndMidToRedis(filteredByGroupDStream)
    //DauHandler.saveDateAndMid(filteredByRedisLogDStream)
    //8.数据写入HBase(phoenix)
    //把数据写入HBase+phoenix

    filteredByGroupDStream.cache()

    filteredByGroupDStream.foreachRDD(rdd=>{
     // rdd.saveToPhoenix()
      rdd.saveToPhoenix("GMALL2020_DAU",
        //获取列名转成大写
        classOf[StartUpLog].getDeclaredFields.map(_.getName.toUpperCase),//反射获取列名，map转成大写，拿到属性名
        //Seq("mid","logdate"...)
        HBaseConfiguration.create(),
        //Option对象一个是Some。一个是None
        Some("hadoop14,hadoop15,hadoop16:2181"))
    })

    //打印
    filteredByGroupDStream.print()

    //开启任务
    ssc.start()
    ssc.awaitTermination()

  }
}
