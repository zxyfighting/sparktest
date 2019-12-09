package com.faw.hq.dmp.spark.imp.wxapp.bean;
/**
 * @program: sparktest
 * @description
 * @author: ZhangXiuYun
 * @create: 2019-11-20 23:45
 **/
public class LoginData {
    private Boolean succeed;      //是否登录成功
    private Boolean firstLogin;   //是否是首次登录

    public Boolean getSucceed() {
        return succeed;
    }

    public void setSucceed(Boolean succeed) {
        this.succeed = succeed;
    }

    public Boolean getFirstLogin() {
        return firstLogin;
    }

    public void setFirstLogin(Boolean firstLogin) {
        this.firstLogin = firstLogin;
    }
    public static String getHive(){
        StringBuilder sb = new StringBuilder();
        sb.append("\\N;").append("\\N");
        return sb.toString();
    }
    public String jsonToLine() {
        StringBuilder sb = new StringBuilder();
        if (succeed==null) {
            sb.append("\\N;");
        } else {
            sb.append(succeed).append(";");
        }
        if (firstLogin==null) {
            sb.append("\\N");
        } else {
            sb.append(firstLogin);
        }
        return sb.toString();
    }
}
