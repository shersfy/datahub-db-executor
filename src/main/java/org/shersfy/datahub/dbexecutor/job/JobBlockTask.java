package org.shersfy.datahub.dbexecutor.job;

import java.util.List;
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
        udp.setId(block.getId());
        udp.setJobId(block.getJobId());
        udp.setLogId(block.getLogId());
        udp.setStatus(JobLogStatus.Successful.index());
        service.updateByPk(udp);
        LOGGER.info("jobId={}, logId={}, blockId={}, finished", jobId, logId, blkId);

        boolean allFinished = true;
        JobBlock where = new JobBlock();
        where.setJobId(jobId);
        where.setLogId(logId);
        List<JobBlock> list = service.findList(where);
        for(JobBlock po :list) {
            if(po.getStatus()!=JobLogStatus.Successful.index()) {
                allFinished = false;
                break;
            }
        }
        
        if(allFinished) {
            int cnt = service.deleteBlocks(block);
            LOGGER.info("jobId={}, logId={}, all blocks size ={}, "
                + "deleted blocks size={}, all blocks finished", 
                jobId, logId, list.size(), cnt);
        }
        
        return block;
    }

    
}
