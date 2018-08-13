package org.shersfy.datahub.dbexecutor.rest;

import javax.annotation.Resource;

import org.shersfy.datahub.commons.beans.Result;
import org.shersfy.datahub.dbexecutor.params.config.JobConfig;
import org.shersfy.datahub.dbexecutor.service.JobServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.fastjson.JSON;

@RestController
@RefreshScope
public class DbExecutorController extends BaseController{

    protected Logger LOGGER = LoggerFactory.getLogger(getClass());
    
    @Value("${version}")
    private String version = "";
    
    @Resource
    private JobServices jobServices;

    @GetMapping("/")
    public Object index() {
        return "Welcom database executor application "+ version;
    }


    @GetMapping("/job/config")
    public Result jobConfig(Long jobId, Long logId, String config) {
        
        JobConfig cfg = JSON.parseObject(config, JobConfig.class);
        cfg.setJobId(jobId);
        cfg.setLogId(logId);
        
        jobServices.execute(cfg);
        
        return new Result(SUCESS, "received successful");
    }

    @GetMapping("/job/check")
    public Result checkJobConfig(@RequestParam("config")String config) {
        LOGGER.info("config={}", config);
        return new Result(SUCESS, "received successful");
    }

}
