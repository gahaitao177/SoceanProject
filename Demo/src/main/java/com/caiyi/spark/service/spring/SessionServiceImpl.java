package com.caiyi.spark.service.spring;

import com.caiyi.spark.dao.SessionDAO;
import com.caiyi.spark.model.SessionDetail;
import com.caiyi.spark.service.SessionService;

/**
 * Created by root on 2016/11/28.
 */
public class SessionServiceImpl implements SessionService{

    private SessionDAO sessionDao;

    public int addSession(SessionDetail sessionDetail) {
        return sessionDao.insertSessionDetail(sessionDetail);
    }

    public int modifySession(String sessionId) {
        return sessionDao.updateSessionDetailByKey(sessionId);
    }

    public SessionDetail getSessionByKey(String sessionId) {
        return sessionDao.selectSessionDetailByKey(sessionId);
    }
}
