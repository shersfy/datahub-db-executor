package org.shersfy.datahub.dbexecutor.hystrix;

import org.shersfy.datahub.commons.beans.Result;
import org.shersfy.datahub.dbexecutor.feign.DhubDbExecutorClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 服务调用容错回调
 * @author py
 * @date 2018年8月11日
 */
@Component
public class DhubDbExecutorFallback implements DhubDbExecutorClient {
    
    protected Logger LOGGER = LoggerFactory.getLogger(getClass());

    @Override
    public String callExecuteJob(Long blockId) {
        LOGGER.error("check params error, blockId={}", blockId);
        return new Result(FAIL, "server error: "+serviceId).toString();
    }

}
