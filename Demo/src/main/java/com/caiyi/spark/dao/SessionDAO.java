package com.caiyi.spark.dao;

import com.caiyi.spark.model.SessionDetail;

/**
 * Created by Socean on 2016/11/28.
 */
public interface SessionDAO {

    int insertSessionDetail(SessionDetail sessionDetail);

    int updateSessionDetailByKey(String sessionId);

    SessionDetail selectSessionDetailByKey(String sessionId);
}
