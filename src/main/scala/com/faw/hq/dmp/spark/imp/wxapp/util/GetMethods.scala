package com.faw.hq.dmp.spark.imp.wxapp.util

import java.util.Date

import com.fasterxml.jackson.databind.ObjectMapper
import com.faw.hq.dmp.spark.imp.wxapp.bean.{AuthInfo, OneIdUnity}


/**
  * author:zhangxiuyun
  * purpose：存放方法
  */
object GetMethods {

  /**
  * json解析方法
  *
  */
  def getAuthInfo(json: String): String = {
    val objectMapper = new ObjectMapper
    val authInfo = objectMapper.readValue(json, classOf[AuthInfo])
    authInfo.JsonToLine
  }

 def getOneIdUtil(uninid:String,openId:String): OneIdUnity ={
     val oneIdUnity = new OneIdUnity
     oneIdUnity.setOneId(null)
     oneIdUnity.setIdCard(null)
     oneIdUnity.setDriverNo(null);
     oneIdUnity.setaId(null);
     oneIdUnity.setMobile(null);
     oneIdUnity.setCookieId(null);
     oneIdUnity.setDeviceId(null);
     oneIdUnity.setUnionId(uninid);
     oneIdUnity.setOpenId(openId);
     oneIdUnity.setVin(null);
     oneIdUnity.setFaceId(null);
     oneIdUnity.setSource(null);
     oneIdUnity.setCreateTime(null);
     oneIdUnity.setUpdateTime(null)
     oneIdUnity
 }

  def msTillTomorrow = {
    val now = new Date()
    val tomorrow = new Date(now.getYear, now.getMonth, now.getDate + 1)
    tomorrow.getTime - now.getTime
  }
  def main(args: Array[String]): Unit = {
    val json = "{\"dataType\":null,\"reportTime\":\"2019-11-21 09:55:47\",\"data\":{\"wxInfo\":{\"openid\":\"o1Gda5LjIpCjpMvdyD9Hs4aLRjhU\",\"nickname\":\"Mutare\",\"gender\":1,\"city\":\"\",\"country\":\"New Zealand\",\"province\":\"North Shore\",\"language\":\"zh_CN\",\"subscribeTime\":1574222595000,\"unionid\":null},\"visitData\":{\"pageUrl\":\"mine-qs/taskCenter/index/index\",\"pageParam\":\"\",\n" + "\"pageType\":\"任务中心加载\",\"modelId\":\"\",\"seriesId\":\"\"}}}"
    val json1="{\"dataType\":\"visit\",\"reportTime\":\"2019-11-25 10:01:04\",\"data\":{\"wxInfo\":{\"openid\":\"oXRuq5f27hAlqLKGDOSIGRdWPPz4\",\"nickname\":\"?.北\",\"gender\":1,\"city\":\"淇￠.\",\"country\":\"涓..\",\"province\":\"娌冲.\",\"language\":\"zh_CN\",\"subscribeTime\":1574306738000,\"unionid\":\"oFx8DwqPocUm6L7oE73F93nsgZow\"},\"visitData\":{\"pageUrl\":\"index/index\",\"pageParam\":\"\",\"pageType\":\"棣.〉?.浇\",\"modelId\":\"\",\"seriesId\":\"\"}}}"
    val objectMapper = new ObjectMapper
    val visitInfo = objectMapper.readValue(json1, classOf[AuthInfo])
    System.out.println(visitInfo.JsonToLine)
    val now = new Date()
    val tomorrow = new Date(now.getYear, now.getMonth, now.getDate + 1)
    println(tomorrow.getTime - now.getTime)

  }

}

