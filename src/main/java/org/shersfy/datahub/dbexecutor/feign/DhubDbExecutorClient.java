package org.shersfy.datahub.dbexecutor.feign;

import org.shersfy.datahub.dbexecutor.hystrix.FeignClientFallbackFactory;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@FeignClient(name=DhubDbExecutorClient.serviceId, 
fallbackFactory=FeignClientFallbackFactory.class)
public interface DhubDbExecutorClient extends ServicesFeignClient{
    
    String serviceId = "datahub-db-executor";
    
    @RequestMapping(method = RequestMethod.GET, value = "/job/block/execute")
    @ResponseBody
    String callExecuteJobBlock(@RequestParam("blockPk")String blockPk);
    
}
