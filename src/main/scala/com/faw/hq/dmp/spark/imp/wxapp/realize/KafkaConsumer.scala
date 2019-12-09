package com.faw.hq.dmp.spark.imp.wxapp.realize

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util
import java.util.Date
import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import com.faw.hq.dmp.spark.imp.wxapp.util.{HttpClientExtendUtil, _}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010._
import org.apache.commons.lang3.StringUtils
import com.alibaba.fastjson.serializer.SerializerFeature
import com.faw.hq.dmp.spark.imp.wxapp.bean.Result
import com.mysql.jdbc.PreparedStatement
import org.apache.commons.configuration2.FileBasedConfiguration
import redis.clients.jedis.Pipeline
import org.apache.log4j.Logger

/**
  * @author ZhangXiuYun
  * @todo spark消费kafka，并将数据解析，修改数据格式，上传到hdfs上；将过滤的数据更新到oneId平台中
  */
object KafkaConsumer {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    while (true) {
      //配置spark上下文环境对象
      val processInterval = 60
      val conf = new SparkConf().setAppName("wxapp").setMaster("local[*]")
      val sc = new SparkContext(conf)
      //设置采集器每十秒批次拉取数据
      val ssc = new StreamingContext(sc, Seconds(processInterval))

      /**
        * 参数获取
        */
      // Kafka 的 Offsets 以 module:groupId:topic 为 key 的 hash 结构存入 Redis 中
      val module: String = "Test"
      val config: FileBasedConfiguration = ConfigUtil("config.properties").config
      val chkDir = config.getString("hdfs.chk")
      ssc.sparkContext.setCheckpointDir(chkDir)
      val hdfs_url = config.getString("hdfs.url")
      val brokerList = config.getString("kafka.broker.list")
      val topic = config.getString("kafka.topic")
      val topics = Array(topic)
      //消费者组的名称
      val groupid = config.getString("group.id")
      //获取mysql配置
      val url = config.getString("jdbc.url")
      val userName = config.getString("jdbc.user")
      val password = config.getString("jdbc.password")
      // sparkstreaming 消费 kafka 时的 Consumer 参数
      val kafkaParams = Map[String, Object](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        ConsumerConfig.GROUP_ID_CONFIG -> groupid,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
      )
      // 初始化 Redis 连接池
      JedisPoolUtils.makePool(RedisConfig("prod.dbaas.private", 16359, 30000, "realtime123", 1000, 100, 50))
      val kafkaStream = KafkaRedisUtils.createDirectStream(ssc, kafkaParams, module, groupid, topics)
      //开始处理批次消息
      kafkaStream.foreachRDD(rdd => {
        //获取当前批次的RDD的偏移量
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        // 处理从获取 kafka 中的数据
        if (!rdd.isEmpty()) {
          // 获取 redis 连接
          val jedisClient = JedisPoolUtils.getPool.getResource
          //开启事务
          val pipeline: Pipeline = jedisClient.pipelined()
          pipeline.multi()

          try {

            //更新offset到Redis中
            offsetRanges.foreach({ offsetRange =>
              logger.info("==========> partition : " + offsetRange.partition + " fromOffset:  " + offsetRange.fromOffset
                + " untilOffset: " + offsetRange.untilOffset)

              // Kafka 的 Offsets 以 module:groupId:topic 为 key 的 hash 结构存入 Redis 中
              val key = s"${module}:${groupid}:${offsetRange.topic}"
              pipeline.hset(key, offsetRange.partition.toString, offsetRange.untilOffset.toString)
            })
            //提交事务
            pipeline.exec()
            //关闭pipeline
            pipeline.sync()
          } catch {
            case e: Exception => {
              logger.error("数据处理异常", e)
              pipeline.discard()
            }
          } finally {
            //关闭连接
            pipeline.close()
            jedisClient.close()
          }
        }
      })
      //解析json数据
      val resultDestream: DStream[String] = kafkaStream.map(_.value()).map(rdd => {
        val authInfo = GetMethods.getAuthInfo(rdd)
        authInfo
      })
      //若dataType为"auth"、"login"，且unionId和userId、phoneNo均不为空，
      //则调用oneid接口，将相应的数据送oneid平台
      val onidDestream: DStream[String] = resultDestream.filter(rdd => {
        val strings = rdd.split(";")
        strings(0).equals("auth") || strings(0).equals("login") && StringUtils.isNotBlank(strings(10)) &&
          StringUtils.isNotBlank(strings(13)) && StringUtils.isNoneBlank(strings(14))
      })
      //获取连接oneid地址
      val oneId = config.getString("oneId.ip")
      onidDestream.foreachRDD(rdd => {
        rdd.foreach(rdd => {
          val strings = rdd.split(";")
          val oneIdUnity = GetMethods.getOneIdUtil(strings(10), strings(2))
          val url = oneId
          val str: String = JSON.toJSONString(oneIdUnity, SerializerFeature.PrettyFormat)
          val result = HttpClientExtendUtil.doPostRequestBody(url, str)
          val json: Result = JSON.parseObject(result, classOf[Result])
          println(json.getCode)
        })
      })

      /**
        *
        * 将数据解析，并根据时间进行追加到hdfs上
        *
        */

      resultDestream.foreachRDD(rdd => {
        val rdd1 = rdd.map((_, " "))
        val currentTime: Date = new Date()
        //val formatter = new SimpleDateFormat("yyyy/MM/dd/HH/mm");
        val formatter = new SimpleDateFormat("yyyy-MM-dd/");
        val dateString = formatter.format(currentTime);
        RDD.rddToPairRDDFunctions(rdd1).partitionBy(new HashPartitioner(1)).
          saveAsHadoopFile(hdfs_url + "dt=" + dateString, classOf[String],
            classOf[String], classOf[AppendTextOutputFormat])
      })
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val dateString: String = dateFormat.format(new Date())
      val dateFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val dateString1: String = dateFormat1.format(new Date())
      val timeout = config.getString("redis.expireTime")

      //由于resultDestream这个RDD用的频繁，减少数据计算量，用cache基于内存的存储数据，便于计算
      resultDestream.cache()
      //将数据结构改变为（pageUrl，1）
      val pvResult: DStream[(String, Int)] = resultDestream.map(rdd => {
        val strings = rdd.split(";")
        (strings(17), 1)
      }).filter(rdd => {
        !rdd._1.equals("\\N")
      })
      val pvResultCounts: DStream[(String, Long)] = pvResult.map(rdd => (dateString, rdd._2)).updateStateByKey {
        case (seq, buffer) => { //seq序列当前周期中数量对集合，buffer表缓冲当中的值，所谓的checkPoint
          val sumCount = seq.sum + buffer.getOrElse(0L)
          Option(sumCount) //表往缓存里边更新对值　　它需要返回一个Option
        }
      }
      pvResultCounts.cache()
      //法一 ：将数据保存在mysql数据库,连接MySQL是一个高消耗的事情，一个分区连接一次
      pvResultCounts.foreachRDD(
        cs => {
          var conn: Connection = null;
          var ps: PreparedStatement = null;
          try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            cs.foreachPartition(f => {
              var conn = DriverManager.getConnection(url, userName, password);
              var ps = conn.prepareStatement("replace into dmp_behavior_wxapp_pv(wx_pv_dt,wx_pv_amounts,create_time,update_time) values(?,?,?,?)");
              f.foreach(s => {
                ps.setString(1, s._1);
                ps.setLong(2, s._2);
                ps.setString(3, dateString1);
                ps.setString(4, dateString1);
                ps.executeUpdate();
              })
            })
          } catch {
            case t: Throwable => t.printStackTrace()
          } finally {
            if (ps != null) {
              ps.close()
            }
            if (conn != null) {
              conn.close();
            }
          }
        })

      //uv是要去重
      //获取过期时间为一天
      //去重--根据今天访问过的用户清单进行过滤

      val mapRDD: DStream[PeopleVisit] = resultDestream.filter(rdd=>{
        val strings = rdd.split(";")
        !strings(10).equals("\\N")
      })
        .map(rdd => {
          val strings = rdd.split(";")
          PeopleVisit(strings(10), dateString)
        })
      var filterRDD = mapRDD.transform(rdd => {
        println("过滤前：" + rdd.count())
        // 获取 redis 连接
        val jedisClient = JedisPoolUtils.getPool.getResource
        val key: String = "uv:" + dateString
        val midSet: util.Set[String] = jedisClient.smembers(key)
        jedisClient.close()
        //问题：由于没有用广播变量导致，executor底层序列化方式，导致空指针异常。
        val midBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(midSet)
        val filterUser: RDD[PeopleVisit] = rdd.filter(rdd => {
          !midBC.value.contains(rdd.uninid)
        })
        println("过滤后的用户" + filterUser.count())
        filterUser
      })
      //本批次去重
      val distinctDstream: DStream[PeopleVisit] = filterRDD.map(rdd => (rdd.uninid, rdd)).groupByKey().flatMap {
        case (uninid, uv) => {
          uv.take(1)
        }
      }
      // 3 把所有今天访问过的用户保存起来
      distinctDstream.foreachRDD(rdd => {
        rdd.foreachPartition(uninid => {
          // 获取 redis 连接
          val jedisClient = JedisPoolUtils.getPool.getResource
          for (elem <- uninid) {
            val key = "uv" + elem.dateToday
            jedisClient.sadd(key, elem.uninid)
            //jedisClient.expire(key, timeout.toInt)
            //设置key的过期时间是24小时
            jedisClient.expire(key, timeout.toInt * 60 * 24)
            //    println(elem.uninid+">>>"+elem.dateToday)
          }
          jedisClient.close()
        })
      })
      distinctDstream.foreachRDD(rdd=>{
        rdd.foreach(println(_))
      })
      //利用有状态计算算子，进行跨批次累加

      val uvDstream: DStream[(String, Long)] = distinctDstream.map(rdd => (rdd.dateToday, 1)).updateStateByKey {
        case (seq, buffer) => { //seq序列当前周期中数量对集合，buffer表缓冲当中的值，所谓的checkPoint
          val sumCount = seq.sum + buffer.getOrElse(0L)
          Option(sumCount) //表往缓存里边更新对值　　它需要返回一个Option
        }
      }
      // 法一 ：将数据保存在mysql数据库,连接MySQL是一个高消耗的事情，一个分区连接一次
      uvDstream.foreachRDD(cs => {
        var conn: Connection = null;
        var ps: PreparedStatement = null;
        try {
          Class.forName("com.mysql.jdbc.Driver").newInstance();
          cs.foreachPartition(f => {
            var conn = DriverManager.getConnection(url, userName, password);
            var ps = conn.prepareStatement("replace into dmp_behavior_wxapp_uv(wx_uv_dt,wx_uv_amounts,create_time,update_time) values(?,?,?,?)");
            f.foreach(s => {
              ps.setString(1, s._1);
              ps.setLong(2, s._2);
              ps.setString(3, dateString1);
              ps.setString(4, dateString1);
              ps.executeUpdate();
            })
          })
        } catch {
          case t: Throwable => t.printStackTrace()
        } finally {
          if (ps != null) {
            ps.close()
          }
          if (conn != null) {
            conn.close();
          }
        }
      })

      //开启采集器
      ssc.start()
      ssc.awaitTerminationOrTimeout(GetMethods.msTillTomorrow)
      ssc.stop(false, true)
      Thread.sleep(processInterval * 2)

    }
  }
  case class PeopleVisit(uninid: String, dateToday: String)

}