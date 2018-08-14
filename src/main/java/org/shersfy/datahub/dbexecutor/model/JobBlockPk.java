package org.shersfy.datahub.dbexecutor.model;

import java.io.Serializable;

import com.alibaba.fastjson.JSON;

public class JobBlockPk implements Serializable{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    /** 块ID **/
    private Long id;
    /** 任务ID **/
    private Long jobId;
    /** 任务日志ID **/
    private Long logId;
    
    public JobBlockPk(Long id, Long jobId, Long logId) {
        super();
        this.id = id;
        this.jobId = jobId;
        this.logId = logId;
    }

    public JobBlockPk(JobBlock block) {
        this.id    = block.getId();
        this.jobId = block.getJobId();
        this.logId = block.getLogId();
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    public Long getLogId() {
        return logId;
    }

    public void setLogId(Long logId) {
        this.logId = logId;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
    
    
}
