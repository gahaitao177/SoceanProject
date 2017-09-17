package com.youyu.bean;

import java.util.Date;

/**
 * Created by Socean on 2016/11/29.
 */
public class ServerRequestRecord {

    //请求的标识
    private String sessionId;

    //用户名
    private String userName;

    //请求的结果0/1  1表示成功 0表示失败
    private String resultCode;

    //请求详情
    private String resultDesc;

    //请求类型
    private String resultType;

    //请求的时间
    private Date requestDate;

    //拓展字段
    private String tag;

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getResultCode() {
        return resultCode;
    }

    public void setResultCode(String resultCode) {
        this.resultCode = resultCode;
    }

    public String getResultDesc() {
        return resultDesc;
    }

    public void setResultDesc(String resultDesc) {
        this.resultDesc = resultDesc;
    }

    public String getResultType() {
        return resultType;
    }

    public void setResultType(String resultType) {
        this.resultType = resultType;
    }

    public Date getRequestDate() {
        return requestDate;
    }

    public void setRequestDate(Date requestDate) {
        this.requestDate = requestDate;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }
}
