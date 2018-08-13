package org.shersfy.datahub.dbexecutor.service;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.shersfy.datahub.dbexecutor.feign.DhubDbExecutorClient;
import org.shersfy.datahub.dbexecutor.params.config.JobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Component
public class JobServices {
    
    Logger LOGGER = LoggerFactory.getLogger(JobServices.class);
    
    @Resource
    private DhubDbExecutorClient dhubDbExecutorClient;
    
    /***
     * 执行分块任务
     * @param blockConfig
     */
    @Async
    public void execute(JobConfig blockConfig) {
        LOGGER.info("block={}", blockConfig.getInputParams());
    }
    
    /**
     * 拆分任务为若干块子任务
     * @param config 任务配置
     */
    @Async
    public void splitJobConfig(JobConfig allConfig) {
        List<JobConfig> blocks = new ArrayList<>();
        
        dispatchBlocks(blocks);
    }
    
    /**
     * 分发任务
     * @param blocks
     */
    public void dispatchBlocks(List<JobConfig> blocks) {
        for(JobConfig blk : blocks) {
            dhubDbExecutorClient.callExecuteJob(blk.toString());
        }
    }
    
    public void split() {
        
    }

}
