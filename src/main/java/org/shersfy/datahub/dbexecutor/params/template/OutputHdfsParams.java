package org.shersfy.datahub.dbexecutor.params.template;

import org.shersfy.datahub.commons.meta.BaseMeta;
import org.shersfy.datahub.commons.meta.HdfsMeta;

public class OutputHdfsParams extends BaseMeta {
    
    /**HDFS连接信息**/
    private HdfsMeta hdfs;
    
    /**输出hdfs目录路径**/
    private String directory;
    
    /**输出hdfs文件路径**/
    private String file;

    public HdfsMeta getHdfs() {
        return hdfs;
    }

    public void setHdfs(HdfsMeta hdfs) {
        this.hdfs = hdfs;
    }

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    
    
}
