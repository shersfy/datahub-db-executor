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
    
    private Long jobId = null;
    private Long logId = null;
    private Long blkId = null;
    
    private JobBlock block;
    
    private JobBlockService service;

    public JobBlockTask(JobBlock block, JobBlockService service) {
        super();
        this.block = block;
        this.service = service;
    }

    @Override
    public JobBlock call() throws Exception {


        try {

            before();

            LOGGER.info("running: {}", block.toString());
            try {
                int sleep = 5;
                if(blkId<0) {
                    sleep = 10;
                }
                else if(blkId%2==0) {
                    sleep = 6;
                }
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            after();
            
        } catch (Exception ex) {
            exception(ex);
        } finally {
            finallyDo();
        }

        return block;
    }
    
    
    protected void before() {

        jobId = block.getJobId();
        logId = block.getLogId();
        blkId = block.getId();
        String thread = String.format("job_%s_%s_%s", jobId, logId, blkId);
        Thread.currentThread().setName(thread);

        LOGGER.info("jobId={}, logId={}, blockId={}, begining ...", jobId, logId, blkId);
        
        JobBlock udp = new JobBlock();
        udp.setId(blkId);
        udp.setJobId(jobId);
        udp.setLogId(logId);
        udp.setService(JobServices.SERVICE_NAME);
        udp.setStatus(JobLogStatus.Executing.index());

        service.updateByPk(udp);
    }
    
    protected void after() {
        
        JobBlock udp = new JobBlock();
        udp.setId(blkId);
        udp.setJobId(jobId);
        udp.setLogId(logId);
        udp.setStatus(JobLogStatus.Successful.index());
        service.updateByPk(udp);
        
        if(service.isFinished(jobId, logId)) {
            int cnt = service.deleteBlocks(block);
            LOGGER.info("jobId={}, logId={}, deleted blocks size={}, all blocks finished", 
                jobId, logId, cnt);
        }
        
        LOGGER.info("jobId={}, logId={}, blockId={}, execute successful", jobId, logId, blkId);
    }
    
    protected void exception(Throwable ex) {
        LOGGER.error("", ex);
    }
    
    protected void finallyDo() {
        LOGGER.info("jobId={}, logId={}, blockId={}, finished", jobId, logId, blkId);
    }

    
}
