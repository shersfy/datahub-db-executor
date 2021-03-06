package org.shersfy.datahub.dbexecutor.service;

import java.util.List;

import javax.annotation.Resource;

import org.shersfy.datahub.commons.beans.Result;
import org.shersfy.datahub.commons.constant.JobConst.JobLogStatus;
import org.shersfy.datahub.dbexecutor.feign.DhubJobManagerClient;
import org.shersfy.datahub.dbexecutor.mapper.BaseMapper;
import org.shersfy.datahub.dbexecutor.mapper.JobBlockMapper;
import org.shersfy.datahub.dbexecutor.model.JobBlock;
import org.shersfy.datahub.dbexecutor.model.JobBlockPk;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.alibaba.fastjson.JSON;


@Transactional
@Service
public class JobBlockServiceImpl extends BaseServiceImpl<JobBlock, Long> 
	implements JobBlockService{
    
    @Resource
    private JobBlockMapper mapper;
    
    @Resource
    private LogManager logManager;
    
    @Resource
    private TableLockService lockService;
    
    @Resource
    private DhubJobManagerClient dhubJobManagerClient;

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

    @Override
    public boolean isFinished(Long jobId, Long logId) {

        JobBlock where = new JobBlock();
        where.setJobId(jobId);
        where.setLogId(logId);

        List<JobBlock> blocks = findList(where);
        return isFinished(blocks);
    }
    
    @Override
    public boolean isFinished(List<JobBlock> blocks) {
        if(blocks==null||blocks.isEmpty()) {
            return true;
        }
        for(JobBlock po :blocks) {
            if(po.getStatus()!=JobLogStatus.Successful.index()) {
                return false;
            }
        }
        
        return true;
    }

    @Override
    public void callUpdateLog(Long logId, int status) {
        String text = dhubJobManagerClient.callUpdateLog(logId, status);
        Result res  = JSON.parseObject(text, Result.class);
        if(res.getCode()!=SUCESS) {
            LOGGER.error(res.getMsg());
        }
    }
    
    @Override
    public LogManager getLogManager() {
        return logManager;
    }

    @Override
    public TableLockService getTableLockService() {
        return lockService;
    }

    
}
