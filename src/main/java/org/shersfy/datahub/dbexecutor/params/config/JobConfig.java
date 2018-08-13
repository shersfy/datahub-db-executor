package org.shersfy.datahub.dbexecutor.params.config;

import org.shersfy.datahub.commons.meta.BaseMeta;
import org.shersfy.datahub.dbexecutor.params.template.InputDbParams;
import org.shersfy.datahub.dbexecutor.params.template.OutputDbParams;
import org.shersfy.datahub.dbexecutor.params.template.OutputHdfsParams;
import org.shersfy.datahub.dbexecutor.params.template.OutputHiveParams;

public class JobConfig extends BaseMeta{
    
    /**任务ID**/
    private Long jobId;
    
    /**任务执行日志ID**/
    private Long logId;
    
    /**数据源输入参数**/
    private InputDbParams inputParams;
    
    /**目标输出类型**/
    private String outputType;
    
    /**目标数据库参数**/
    private OutputDbParams outputDbParams;
    
    /**目标HDFS参数**/
    private OutputHdfsParams outputHdfsParams;
    
    /**目标Hive参数**/
    private OutputHiveParams outputHiveParams;

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

    public InputDbParams getInputParams() {
        return inputParams;
    }

    public void setInputParams(InputDbParams inputParams) {
        this.inputParams = inputParams;
    }

    public String getOutputType() {
        return outputType;
    }

    public void setOutputType(String outputType) {
        this.outputType = outputType;
    }

    public OutputDbParams getOutputDbParams() {
        return outputDbParams;
    }

    public void setOutputDbParams(OutputDbParams outputDbParams) {
        this.outputDbParams = outputDbParams;
    }

    public OutputHdfsParams getOutputHdfsParams() {
        return outputHdfsParams;
    }

    public void setOutputHdfsParams(OutputHdfsParams outputHdfsParams) {
        this.outputHdfsParams = outputHdfsParams;
    }

    public OutputHiveParams getOutputHiveParams() {
        return outputHiveParams;
    }

    public void setOutputHiveParams(OutputHiveParams outputHiveParams) {
        this.outputHiveParams = outputHiveParams;
    }
    
}
