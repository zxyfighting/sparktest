package com.faw.hq.dmp.spark.imp.wxapp.bean;

/**
 * @program: sparktest
 * @description
 * @author: ZhangXiuYun
 * @create: 2019-11-20 23:48
 **/
public class AuthInfo {
    private String dataType;
    private String reportTime;
    private AuthData data;

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getReportTime() {
        return reportTime;
    }

    public void setReportTime(String reportTime) {
        this.reportTime = reportTime;
    }

    public AuthData getData() {
        return data;
    }

    public void setData(AuthData data) {
        this.data = data;
    }

    public String JsonToLine() {
        StringBuilder sb = new StringBuilder();
        if (dataType == null || dataType.length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(dataType).append(";");
        }
        if (reportTime == null || reportTime.length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(reportTime).append(";");
        }
        if (data == null) {
            sb.append(AuthData.getHive());
        } else {
            sb.append(data.jsonToLine());
        }
        return sb.toString();
    }

}
