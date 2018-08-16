package org.shersfy.datahub.dbexecutor.hystrix;

import org.shersfy.datahub.commons.beans.Result;
import org.shersfy.datahub.dbexecutor.feign.DhubJobManagerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class DhubJobManagerFallback implements DhubJobManagerClient {
    
    protected Logger LOGGER = LoggerFactory.getLogger(getClass());

    @Override
    public String callUpdateLog(Long logId, int status) {
        LOGGER.error("call update log error, logId={}, status={}", logId, status);
        return new Result(FAIL, "server error: "+serviceId).toString();
    }


}
