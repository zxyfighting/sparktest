package com.faw.hq.dmp.spark.imp.wxapp.bean;

/**
 * @program: sparktest
 * @description
 * @author: ZhangXiuYun
 * @create: 2019-11-20 23:45
 **/
public class AuthData {

    private WXBaseInfo wxInfo;
    private UserLocationInfo addrInfo;
    private UserInfo userInfo;
    private LoginData loginData;
    private VisitData visitData;

    public LoginData getLoginData() {
        return loginData;
    }

    public void setLoginData(LoginData loginData) {
        this.loginData = loginData;
    }

    public VisitData getVisitData() {
        return visitData;
    }

    public void setVisitData(VisitData visitData) {
        this.visitData = visitData;
    }


    public WXBaseInfo getWxInfo() {
        return wxInfo;
    }

    public void setWxInfo(WXBaseInfo wxInfo) {
        this.wxInfo = wxInfo;
    }

    public UserLocationInfo getAddrInfo() {
        return addrInfo;
    }

    public void setAddrInfo(UserLocationInfo addrInfo) {
        this.addrInfo = addrInfo;
    }

    public UserInfo getUserInfo() {
        return userInfo;
    }

    public void setUserInfo(UserInfo userInfo) {
        this.userInfo = userInfo;
    }

    public static String getHive() {

        return WXBaseInfo.getHive() + UserLocationInfo.getHive() + UserInfo.getHive()+";"+
                LoginData.getHive()+";"+VisitData.getHive();
    }
    //将数据转为字符串拼接
    public String jsonToLine() {
        StringBuilder sb = new StringBuilder();
        if (wxInfo==null) {
            sb.append(WXBaseInfo.getHive());
        } else {
            sb.append(wxInfo.jsonToLine());
        }
        if (addrInfo==null) {
            sb.append(UserLocationInfo.getHive());
        } else {
            UserLocationInfo userLocationInfo = new UserLocationInfo();
            sb.append(addrInfo.jsonToLine());
        }
        if (userInfo==null) {
            sb.append(UserInfo.getHive()).append(";");
        } else {
            sb.append(userInfo.jsonToLine()).append(";");
        }
        if (loginData==null){
            sb.append(LoginData.getHive()).append(";");
        }
        else{
            sb.append(loginData.jsonToLine()).append(";");
        }
        if (visitData==null){
            sb.append(VisitData.getHive());
        }
        else{
            sb.append(visitData.jsonToLine());
        }

        return sb.toString();
    }


}
