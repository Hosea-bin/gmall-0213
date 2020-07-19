package com.atguigu.handler

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {

  def filterByGroup(filterByRedisLogDStream:DStream[StartUpLog])= {
    //
    val midDateToLogDStream: DStream[(String, StartUpLog)] = filterByRedisLogDStream.map(startUpLog => {
      (s"${startUpLog.mid}_${startUpLog.logDate}", startUpLog)
    })

    //分组
    val midDateToLogInterDStream: DStream[(String, Iterable[StartUpLog])] = midDateToLogDStream.groupByKey()
    //3.取value中时间戳最小的一条
    val midDateTologList: DStream[(String, List[StartUpLog])] = midDateToLogInterDStream.mapValues(iter => {
      //按照时间戳排序并取第一条
      iter.toList.sortWith(_.ts < _.ts).take(1)
    })
    midDateTologList
    //4.压平
    midDateTologList.flatMap {
      case (midDate, list) => list
    }

    //合并3.4步骤
    midDateToLogInterDStream.flatMap { case (midDate, iter) =>
      iter.toList.sortWith(_.ts < _.ts).take(1)
    }
    //flatMapValue 代替UDTF
  }
  private val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  /**
   * 去重1-》》根据Redis做跨批次去重
   * startUpLogDStream 从kafka读取原始数据
   */
  def filterByRedis(startUpLogDStream:DStream[StartUpLog],ssc:StreamingContext)={
    //方案一：单条过滤
//    val value: DStream[StartUpLog] = startUpLogDStream.filter(startUpLog => {
//      //a.获取Redis连接
//      val jedisClient: Jedis = RedisUtil.getJedisClient
//      //b.单条过滤（判断是否在Redis中存在）
//      val boolean: lang.Boolean = !jedisClient.sismember(s"DAU:${startUpLog.logDate}", startUpLog.mid)
//      //c.归还连接
//      jedisClient.close()
//      //d.将结果返回
//      boolean
//    })

      //单条过滤
      val value: DStream[StartUpLog] = startUpLogDStream.filter(startUpLog => {
        //a.获取Redis连接
        val client: Jedis = RedisUtil.getJedisClient
        //b.单条过滤（判断是否在Redis中存在）
        val boolean: Boolean = !client.sismember(s"DAU:${startUpLog.logDate}", startUpLog.mid)
        //c.归还连接
        client.close()
        //d.返回结果
        boolean
      })

   // 方案二：一个分区获取一次Redis连接
//    val value1: DStream[StartUpLog] = startUpLogDStream.mapPartitions(iter => {
//      //a.分区内获取Redis连接
//      val jedisClient: Jedis = RedisUtil.getJedisClient
//      //b.分区内过滤数据
//      val logs: Iterator[StartUpLog] = iter.filter(startUpLog => !jedisClient.sismember(s"DAU:${startUpLog.logDate}", startUpLog.mid))
//      //归还连接
//      jedisClient.close()
//      //返回结果
//      logs
//    })
      //方案二：一个分区内获取一次Redis连接
    startUpLogDStream.mapPartitions(iter =>{
      //a.分区内获取Redis连接
      val client: Jedis = RedisUtil.getJedisClient
      //b.分区内过滤数据
      val logs: Iterator[Nothing] = iter.filter(startUpLog=> !client.sismember(s"DAU:${startUpLog.logDate}",startUpLog.mid))
      //归还连接
      client.close()
      //返回结果
      logs
    })

    //方案三：每个批次获取一次Redis连接
    val value2: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //一个批次调用一次，且在Driver端，在这个位置获取Redis中所有的Mids,广播至Executor
      //a.获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //b.使用当天时间
      val date: String = sdf.format(new Date(System.currentTimeMillis()))
      val uids: util.Set[String] = jedisClient.smembers(s"DAU:$date")
      //c.广播uids
      val uidsBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(uids)
      //d.归还连接
      jedisClient.close()
      //e.对RDD中的数据做去重
      rdd.filter(startUpLogDStream => !uidsBC.value.contains(startUpLogDStream.mid))
    })

    startUpLogDStream.transform(rdd => {
      //a.获取Redis连接
      val client: Jedis = RedisUtil.getJedisClient
      //b.使用当天时间
      val date: String = sdf.format(new Date(System.currentTimeMillis()))
      val uid: util.Set[String] = client.smembers(s"DAU:${date}")
      //c.广播uids
      val uidsBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(uid)
      //d.归还连接
      client.close()
      //e.对RDD中的数据进行去重
      rdd.filter(startUpLogDStream => !uidsBC.value.contains(startUpLogDStream.mid))
    })
   value2
  }

  /**
   * 将两次去重之后Mid及日期写入Redis,提供给当天以后的批次做去重
   */
  def saveDateAndMidToRedis(startUpLogDStream:DStream[StartUpLog])={
    //将分区单独写入数据
    startUpLogDStream.foreachRDD(rdd=>{
      rdd.foreachPartition { iter =>
        //a.分区内获取一次Redis连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //b.对分区迭代，逐条写入
        iter.foreach(startUpLog => {
          jedisClient.sadd(s"DAU:${startUpLog.logDate}", startUpLog.mid)
        })
        //c.归还连接
        jedisClient.close()
      }
    })
  }
//  def saveDateAndMid(filteredByRedisLogDStream: DStream[StartUpLog]) = {
//    //将分区单独写入数据
//    filteredByRedisLogDStream.foreachRDD(rdd=>{
//      rdd.foreachPartition{iter=>
//        //a.分区内获取一次Redis连接
//        val client: Jedis = RedisUtil.getJedisClient
//        //b.对分区迭代逐条导入
//        iter.foreach(startUpLog=> {
//          client.sadd(s"DAU:${startUpLog.logDate}",startUpLog.mid)
//        })
//        //c.归还连接
//        client.close()
//      }
//    })
//  }


}
