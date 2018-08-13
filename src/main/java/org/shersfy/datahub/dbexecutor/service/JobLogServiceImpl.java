package org.shersfy.datahub.dbexecutor.service;

import javax.annotation.Resource;

import org.shersfy.datahub.dbexecutor.mapper.BaseMapper;
import org.shersfy.datahub.dbexecutor.mapper.JobBlockMapper;
import org.shersfy.datahub.dbexecutor.model.JobBlock;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;


@Transactional
@Service("jobLogService")
public class JobLogServiceImpl extends BaseServiceImpl<JobBlock, Long> 
	implements JobBlockService{
    
    @Resource
    private JobBlockMapper mapper;

    @Override
    public BaseMapper<JobBlock, Long> getMapper() {
        return mapper;
    }
    
}
