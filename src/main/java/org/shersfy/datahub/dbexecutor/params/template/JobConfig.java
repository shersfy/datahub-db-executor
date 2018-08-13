package org.shersfy.datahub.dbexecutor.params.template;

import org.shersfy.datahub.commons.meta.BaseMeta;

public class JobConfig extends BaseMeta{
    
    private Long jobId;
    
    private Long logId;
    
    private DataSourceConfig dataSource;

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

    public DataSourceConfig getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSourceConfig dataSource) {
        this.dataSource = dataSource;
    }


}
