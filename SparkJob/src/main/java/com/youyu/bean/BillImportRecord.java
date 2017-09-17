package com.youyu.bean;

/**
 * Created by Socean on 2016/11/29.
 */
public class BillImportRecord extends ServerRequestRecord {

    //操作类型 1获取验证码，2生成任务，3发送短信验证码，4验证短信并生成任务，5登陆，6解析，7数据入库
    private String actionType;

    //操作银行ID
    private String bankCode;

    public String getActionType() {
        return actionType;
    }

    public void setActionType(String actionType) {
        this.actionType = actionType;
    }

    public String getBankCode() {
        return bankCode;
    }

    public void setBankCode(String bankCode) {
        this.bankCode = bankCode;
    }
}
