package org.shersfy.datahub.dbexecutor.service;

import org.shersfy.datahub.commons.meta.MessageData;
import org.shersfy.datahub.commons.utils.JobLogUtil.JobLogPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class LogManager {
    
    private Logger logger = LoggerFactory.getLogger(getClass());
    
    public void sendMsg(JobLogPayload message) {
        MessageData data = new MessageData(message.toString());
        data.setCode(10001);
        logger.info(data.toString());
    }
    
    /**
     * 
     * @param code 业务编码
     * @param message
     */
    public void sendMsg(int code, JobLogPayload message) {
        MessageData data = new MessageData(code, message.toString());
        data.setCode(10001);
        logger.info(data.toString());
    }

}
