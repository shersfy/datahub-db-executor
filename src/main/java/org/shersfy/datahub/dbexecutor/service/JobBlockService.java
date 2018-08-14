package org.shersfy.datahub.dbexecutor.service;

import org.shersfy.datahub.dbexecutor.model.JobBlock;
import org.shersfy.datahub.dbexecutor.model.JobBlockPk;

public interface JobBlockService extends BaseService<JobBlock, Long> {
    
    JobBlock findByPk(JobBlockPk pk);

    int updateByPk(JobBlock block);

    int deleteBlocks(JobBlock block);

}
