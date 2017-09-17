package com.caiyi.spark.service;

import com.caiyi.spark.model.SessionDetail;

/**
 * Created by root on 2016/11/28.
 */
public interface SessionService {

    public int addSession(SessionDetail sessionDetail);

    public int modifySession(String sessionId);

    public SessionDetail getSessionByKey(String sessionId);

}
