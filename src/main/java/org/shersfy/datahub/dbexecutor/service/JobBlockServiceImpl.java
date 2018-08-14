package org.shersfy.datahub.dbexecutor.service;

import javax.annotation.Resource;

import org.shersfy.datahub.dbexecutor.mapper.BaseMapper;
import org.shersfy.datahub.dbexecutor.mapper.JobBlockMapper;
import org.shersfy.datahub.dbexecutor.model.JobBlock;
import org.shersfy.datahub.dbexecutor.model.JobBlockPk;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;


@Transactional(rollbackFor= {Exception.class, Throwable.class})
@Service
public class JobBlockServiceImpl extends BaseServiceImpl<JobBlock, Long> 
	implements JobBlockService{
    
    @Resource
    private JobBlockMapper mapper;

    @Override
    public BaseMapper<JobBlock, Long> getMapper() {
        return mapper;
    }

    @Override
    public JobBlock findById(Long id) {
        return null;
    }

    @Override
    public JobBlock findByPk(JobBlockPk pk) {
        return mapper.findByPk(pk);
    }

    
    @Override
    public int updateById(JobBlock entity) {
        return 0;
    }
    
    @Override
    public int updateByPk(JobBlock block) {
        return mapper.updateByPk(block);
    }

    @Override
    public int deleteBlocks(JobBlock block) {
        return mapper.deleteBlocks(block);
    }

    
    
}
