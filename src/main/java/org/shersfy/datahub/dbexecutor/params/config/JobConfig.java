package org.shersfy.datahub.dbexecutor.params.config;

import org.shersfy.datahub.commons.meta.BaseMeta;
import org.shersfy.datahub.dbexecutor.params.template.InputDbParams;
import org.shersfy.datahub.dbexecutor.params.template.OutputDbParams;
import org.shersfy.datahub.dbexecutor.params.template.OutputHdfsParams;
import org.shersfy.datahub.dbexecutor.params.template.OutputHiveParams;

public class JobConfig extends BaseMeta{
    
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
    
    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof JobConfig)) {
            return false;
        }
        
        return this.toString().equals(obj.toString());
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
