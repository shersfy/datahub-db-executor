package org.shersfy.datahub.dbexecutor.rest;

import javax.annotation.Resource;

import org.shersfy.datahub.commons.beans.Result;
import org.shersfy.datahub.dbexecutor.model.JobBlock;
import org.shersfy.datahub.dbexecutor.service.JobBlockService;
import org.shersfy.datahub.dbexecutor.service.JobServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RefreshScope
public class DbExecutorController extends BaseController{

    protected Logger LOGGER = LoggerFactory.getLogger(getClass());
    
    @Value("${version}")
    private String version = "";
    
    @Resource
    private JobServices jobServices;
    
    @Resource
    private JobBlockService jobBlockService;

    @GetMapping("/")
    public Object index() {
        return "Welcom database executor application "+ version;
    }
    
    
    @GetMapping("/job/check")
    public Result checkJobConfig(@RequestParam("config")String config) {
        LOGGER.info("config={}", config);
        return new Result(SUCESS, "received successful");
    }

    @GetMapping("/job/config")
    public Result configJob(Long jobId, Long logId, String config) {
        jobServices.config(jobId, logId, config);
        return new Result(SUCESS, "received successful");
    }
    
    @GetMapping("/job/execute")
    public Result executeJobBlock(String blockPk) {
        jobServices.execute(blockPk);
        return new Result(SUCESS, "received successful");
    }
    
    @GetMapping("/job/blocks")
    public Object listBlocks() {
        return jobBlockService.findList(new JobBlock());
    }
   

}
