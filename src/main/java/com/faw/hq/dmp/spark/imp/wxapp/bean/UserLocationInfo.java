package com.faw.hq.dmp.spark.imp.wxapp.bean;

/**
 * @program: sparktest
 * @description
 * @author: ZhangXiuYun
 * @create: 2019-11-20 23:45
 **/
public class UserLocationInfo {
    private Double latitude;      //用户位置（纬 度）
    private Double longitude;     //用户位置（经 度）

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public static String getHive() {
        StringBuilder sb = new StringBuilder();
        sb.append("\\N;").append("\\N;");
        return sb.toString();
    }

    public String jsonToLine() {
        StringBuilder sb = new StringBuilder();
        if (latitude == null) {
            sb.append("\\N;");
        } else {
            sb.append(latitude).append(";");
        }
        if (longitude == null) {
            sb.append("\\N;");
        } else {
            sb.append(longitude).append(";");
        }
        return sb.toString();
    }
}
