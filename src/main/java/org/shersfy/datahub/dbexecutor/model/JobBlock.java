package org.shersfy.datahub.dbexecutor.model;

public class JobBlock extends BaseEntity {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /** 任务ID **/
    private Long jobId;

    /** 任务日志ID **/
    private Long logId;

    /** 结果状态(1：执行中(默认)，2：执行成功，3：执行失败) **/
    private Integer status;

    /** 配置参数 **/
    private String config;

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

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public String getConfig() {
        return config;
    }

    public void setConfig(String config) {
        this.config = config;
    }
}