package com.faw.hq.dmp.spark.imp.wxapp.bean;

public class VisitData {
    private String pageUrl;       //页面信息
    private String pageParam;     //页面跳转参数
    private String pageType;      //页面类型，包括预约试驾，车型浏览
    private String modelId;       //车型id
    private String seriesId;      //车系id

    public String getPageUrl() {
        return pageUrl;
    }

    public void setPageUrl(String pageUrl) {
        this.pageUrl = pageUrl;
    }

    public String getPageParam() {
        return pageParam;
    }

    public void setPageParam(String pageParam) {
        this.pageParam = pageParam;
    }

    public String getPageType() {
        return pageType;
    }

    public void setPageType(String pageType) {
        this.pageType = pageType;
    }

    public String getModelId() {
        return modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public String getSeriesId() {
        return seriesId;
    }

    public void setSeriesId(String seriesId) {
        this.seriesId = seriesId;
    }

    public static String getHive(){
        StringBuilder sb = new StringBuilder();
        sb.append("\\N;").append("\\N;").append("\\N;").append("\\N;").append("\\N");
        return sb.toString();
    }
    public String jsonToLine() {
        StringBuilder sb = new StringBuilder();
        if (pageUrl==null||pageUrl.length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(pageUrl).append(";");
        }
        if (pageParam==null||pageParam.length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(pageParam).append(";");
        }
        if (pageType==null||pageType.length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(pageType).append(";");
        }
        if (modelId==null||modelId.length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(modelId).append(";");
        }
        if (seriesId==null||seriesId.length()==0) {
            sb.append("\\N");
        } else {
            sb.append(seriesId);
        }
        return sb.toString();
    }
}
