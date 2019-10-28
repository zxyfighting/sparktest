package cn.scfl.util


import java.text.SimpleDateFormat
import java.util.Date

import cn.scfl.bean.UserCarInfo
import cn.scfl.util.Test.{AuthInfo, LoginInfo, VisitInfo}
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject
import org.json4s.DateFormat

  /*
  * author:张秀云
  * purpose:spark消费kafka，并将数据解析，修改数据格式，上传到hdfs上
  *
  * */
 object SparkTest {

    case class AuthInfo(openId:String, nickname:String, gender:String , city:String ,
                        country:String ,province:String,language:String ,subscribeTime:String ,unionid:String,
                        latitude:Double,longitude:Double ,userid:String ,phoneNo:String)
    case class LoginInfo( openId:String, nickname:String, gender:String , city:String ,
                          country:String ,province:String,language:String ,subscribeTime:String ,
                          unionid:String, latitude:Double,longitude:Double ,userid:String ,
                          phoneNo:String,succeed:Boolean,firstLogin:Boolean)

    case class VisitInfo(openId:String, nickname:String, gender:String , city:String ,
                         country:String ,province:String,language:String ,subscribeTime:String ,unionid:String,
                         pageUrl:String ,pageParam:String ,pageType:String ,modelId:String ,seriesId:String)


    def main(args: Array[String]): Unit = {

    //配置spark上下文环境对象
    val conf = new SparkConf().setAppName("SparkConsumer")
    val sc = new SparkContext(conf)

    //设置采集器每5秒批次拉取数据
    val ssc = new StreamingContext(sc, Seconds(5))

    //调用kafka工具，消费kafka
    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil
      .getKafkaStream("behavior_topic", ssc)
      /*kafkaDstream.map(_.value()).foreachRDD(rdd=>{
        rdd.collect().mkString(",")
      })*/
    val resultDstream: DStream[Any] = kafkaDstream.map(_.value()).map(rdd => {
      val jsonObject = new JSONObject(rdd)
      val dataType = jsonObject.get("dataType")
      var info: JSONObject = null
      if(rdd.startsWith("{") && rdd.endsWith("}")&& !rdd.contains("UTF-8")){
      if (dataType.equals("授权")) {
        val dataJson = jsonObject.getJSONObject("data")
        val authObject = dataJson.getJSONObject("wxInfo")
        val openId = authObject.getString("openid")
        val nickname = authObject.getString("nickname")
        val gender = authObject.getString("gender")
        val city = authObject.getString("city")
        val country = authObject.getString("country")
        val province = authObject.getString("province")
        val language = authObject.getString("language")
        val subscribeTime = authObject.getString("subscribeTime")
        val unionid = authObject.getString("unionid")
        val sddObject = dataJson.getJSONObject("addrInfo")
        val latitude = sddObject.getDouble("latitude")
        val longitude = sddObject.getDouble("longitude")
        val userObject = dataJson.getJSONObject("userInfo")
        val userid = userObject.getString("userid")
        val phoneNo = userObject.getString("phoneNo")
        val authInfo: AuthInfo = AuthInfo(openId, nickname, gender, city, country, province, language,
          subscribeTime, unionid, latitude, longitude, userid, phoneNo)

        //var info: JSONObject = new JSONObject(authInfo)
        authInfo.openId+","+authInfo.nickname+","+authInfo.gender+","+authInfo.city+","+authInfo.country+","+
          authInfo.province+","+authInfo.language+","+authInfo.subscribeTime+","+authInfo.unionid+
          ","+authInfo.latitude+","+authInfo.longitude+","+authInfo.userid+","+authInfo.phoneNo
      }

      else if (dataType.equals("登录")) {
        val dataJson = jsonObject.getJSONObject("data")
        val authObject = dataJson.getJSONObject("wxInfo")
        val openId = authObject.getString("openid")
        val nickname = authObject.getString("nickname")
        val gender = authObject.getString("gender")
        val city = authObject.getString("city")
        val country = authObject.getString("country")
        val province = authObject.getString("province")
        val language = authObject.getString("language")
        val subscribeTime = authObject.getString("subscribeTime")
        val unionid = authObject.getString("unionid")
        val sddObject = dataJson.getJSONObject("addrInfo")
        val latitude = sddObject.getDouble("latitude")
        val longitude = sddObject.getDouble("longitude")
        val userObject = dataJson.getJSONObject("userInfo")
        val userid = userObject.getString("userid")
        val phoneNo = userObject.getString("phoneNo")
        val loginObject = dataJson.getJSONObject("loginData")
        val succeed = loginObject.getBoolean("succeed")
        val firstLogin = loginObject.getBoolean("firstLogin")
        val loginInfo: LoginInfo = LoginInfo(openId, nickname, gender, city, country, province, language, subscribeTime,
          unionid, latitude, longitude, userid, phoneNo, succeed, firstLogin)
        loginInfo.openId+","+loginInfo.nickname+","+loginInfo.gender+","+loginInfo.city+","+loginInfo.country+","+
          loginInfo.province+","+loginInfo.language+ ","+loginInfo.subscribeTime+","+loginInfo.unionid+
          ","+loginInfo.latitude+","+loginInfo.longitude+","+loginInfo.userid+","+loginInfo.phoneNo+","+loginInfo.succeed+
          ","+loginInfo.firstLogin

      }
      else if (dataType.equals("访问")) {
        val dataJson = jsonObject.getJSONObject("data")
        val authObject = dataJson.getJSONObject("wxInfo")
        val openId = authObject.getString("openid")
        val nickname = authObject.getString("nickname")
        val gender = authObject.getString("gender")
        val city = authObject.getString("city")
        val country = authObject.getString("country")
        val province = authObject.getString("province")
        val language = authObject.getString("language")
        val subscribeTime = authObject.getString("subscribeTime")
        val unionid = authObject.getString("unionid")
        val visitObject = dataJson.getJSONObject("visitData")
        val pageUrl = visitObject.getString("pageUrl")
        val pageParam = visitObject.getString("pageParam")
        val pageType = visitObject.getString("pageType")
        val modelId = visitObject.getString("modelId")
        val seriesId = visitObject.getString("seriesId")

        val visitInfo: VisitInfo = VisitInfo(openId, nickname, gender, city, country, province, language,
          subscribeTime, unionid, pageUrl, pageParam, pageType, modelId, seriesId)
        visitInfo.openId + "," + visitInfo.nickname + "," + visitInfo.gender + "," + visitInfo.city + "," + visitInfo.country + "," +
          visitInfo.province + "," + visitInfo.language + "," + visitInfo.subscribeTime + "," + visitInfo.unionid +
          "," + visitInfo.pageUrl + "," + visitInfo.pageParam + "," + visitInfo.pageType + "," + visitInfo.modelId + "," + visitInfo.seriesId
      }
      }
    })
     val date = new Date();
      val format = new SimpleDateFormat("yyyy/MM/dd")
      val dd = format.format(date)
    resultDstream.foreachRDD(rdd => {

      rdd.saveAsTextFile("hdfs://10.6.207.146/tmp/wxinfo/dd")
    })

    //开启采集器
    ssc.start()
    //为了让采集器正常关闭
    ssc.awaitTermination()


  }


}
