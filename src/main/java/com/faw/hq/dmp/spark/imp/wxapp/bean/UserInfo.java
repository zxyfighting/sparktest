package com.faw.hq.dmp.spark.imp.wxapp.bean;

public class UserInfo {
    private String userid;        //用户的登录账号
    private String phoneNo;       //客户电话号码

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getPhoneNo() {
        return phoneNo;
    }

    public void setPhoneNo(String phoneNo) {
        this.phoneNo = phoneNo;
    }

    public static String getHive(){
        StringBuilder sb = new StringBuilder();
        sb.append("\\N;").append("\\N");
        return sb.toString();
    }
    public String jsonToLine() {
        StringBuilder sb = new StringBuilder();
        if (userid==null||userid.length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(userid).append(";");
        }
        if (phoneNo==null||phoneNo.length()==0) {
            sb.append("\\N");
        } else {
            sb.append(phoneNo);
        }
        return sb.toString();
    }

}
