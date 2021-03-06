package org.shersfy.datahub.dbexecutor.mapper;

import org.shersfy.datahub.dbexecutor.model.JobBlock;
import org.shersfy.datahub.dbexecutor.model.JobBlockPk;

public interface JobBlockMapper extends BaseMapper<JobBlock, Long>{

    JobBlock findByPk(JobBlockPk pk);

    int updateByPk(JobBlock block);

    int deleteBlocks(JobBlock block);
    
}