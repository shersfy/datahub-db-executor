package org.shersfy.datahub.dbexecutor.service;

import java.util.List;

import org.shersfy.datahub.dbexecutor.model.JobBlock;
import org.shersfy.datahub.dbexecutor.model.JobBlockPk;

public interface JobBlockService extends BaseService<JobBlock, Long> {
    
    JobBlock findByPk(JobBlockPk pk);

    /**
     * 更新记录<br/>
     * 条件 id=? and jobId=? and logId=?
     * @param block
     * @return
     */
    int updateByPk(JobBlock block);

    /**
     * 删除所有块<br/>
     * 条件 jobId=? and logId=?
     * @param block
     * @return
     */
    int deleteBlocks(JobBlock block);

    /**
     * 任务所有分块是否执行完毕，没有记录返回true
     * @param jobId
     * @param logId
     * @return
     */
    boolean isFinished(Long jobId, Long logId);
    /**
     * 任务所有分块是否执行完毕，没有记录返回true
     * @param blocks 分块
     * @return
     */
    boolean isFinished(List<JobBlock> blocks);

}
