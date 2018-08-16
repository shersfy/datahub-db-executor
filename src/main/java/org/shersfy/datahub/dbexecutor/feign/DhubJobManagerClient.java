package org.shersfy.datahub.dbexecutor.feign;

import org.shersfy.datahub.dbexecutor.hystrix.DhubJobManagerFallbackFactory;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@FeignClient(name=DhubJobManagerClient.serviceId, 
fallbackFactory=DhubJobManagerFallbackFactory.class)
public interface DhubJobManagerClient extends ServicesFeignClient{
    
    String serviceId = "datahub-job-manager";
    
    @RequestMapping(method = RequestMethod.GET, value = "/api/v1/log/update")
    @ResponseBody
    String callUpdateLog(@RequestParam("logId")Long logId, @RequestParam("status")int status);
    
}
