package org.shersfy.datahub.dbexecutor.hystrix;


import javax.annotation.Resource;

import org.shersfy.datahub.dbexecutor.feign.DhubDbExecutorClient;
import org.shersfy.datahub.dbexecutor.feign.ServicesFeignClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import feign.hystrix.FallbackFactory;

@Component
public class DhubDbExecutorFallbackFactory implements FallbackFactory<ServicesFeignClient> {

    protected Logger LOGGER = LoggerFactory.getLogger(getClass());
    
    @Resource
    private DhubDbExecutorFallback dhubDbExecutorFallback;
    
    @Override
    public ServicesFeignClient create(Throwable cause) {
        // 屏蔽服务启动报 java.lang.RuntimeException: null
        if(!(cause instanceof RuntimeException)) {
            String err = String.format("call service '%s' error: ", DhubDbExecutorClient.serviceId);
            LOGGER.error(err, cause);
        }
        
        return dhubDbExecutorFallback;
    }
    

}
