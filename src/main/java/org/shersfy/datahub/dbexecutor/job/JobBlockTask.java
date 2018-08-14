package org.shersfy.datahub.dbexecutor.job;

import java.util.concurrent.Callable;

import org.shersfy.datahub.commons.constant.JobConst.JobLogStatus;
import org.shersfy.datahub.dbexecutor.model.JobBlock;
import org.shersfy.datahub.dbexecutor.service.JobBlockService;
import org.shersfy.datahub.dbexecutor.service.JobServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobBlockTask implements Callable<JobBlock>{
    
    Logger LOGGER = LoggerFactory.getLogger(JobServices.class);
    
    private JobBlock block;
    
    private JobBlockService service;

    public JobBlockTask(JobBlock block, JobBlockService service) {
        super();
        this.block = block;
        this.service = service;
    }

    @Override
    public JobBlock call() throws Exception {
        
        Long jobId = block.getJobId();
        Long logId = block.getLogId();
        Long blkId = block.getId();
        
        LOGGER.info("jobId={}, logId={}, blockId={}, begining ...", jobId, logId, blkId);
        
        LOGGER.info("jobId={}, logId={}, blockId={}, block:{}", jobId, logId, blkId, block);
        JobBlock udp = new JobBlock();
        udp.setId(blkId);
        udp.setJobId(jobId);
        udp.setLogId(logId);
        udp.setStatus(JobLogStatus.Successful.index());
        service.updateByPk(udp);
        LOGGER.info("jobId={}, logId={}, blockId={}, finished", jobId, logId, blkId);

        if(service.isFinished(jobId, logId)) {
            int cnt = service.deleteBlocks(block);
            LOGGER.info("jobId={}, logId={}, deleted blocks size={}, all blocks finished", 
                jobId, logId, cnt);
        }
        
        return block;
    }

    
}
