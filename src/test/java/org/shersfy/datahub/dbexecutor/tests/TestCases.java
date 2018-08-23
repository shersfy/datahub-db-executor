package org.shersfy.datahub.dbexecutor.tests;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;
import org.shersfy.datahub.commons.connector.hadoop.HdfsUtil;
import org.shersfy.datahub.commons.exception.DatahubException;
import org.shersfy.datahub.commons.meta.HdfsMeta;
import org.shersfy.datahub.dbexecutor.params.config.JobConfig;

import com.alibaba.fastjson.JSON;

public class TestCases {
    
    @Test
    public void test01() throws CloneNotSupportedException{
        
        String text = "{\"inputParams\":{\"block\":{\"condition\":\"`id` IS NULL\",\"index\":-1,\"name\":\"part_-1\",\"partColumn\":{\"alias\":\"`id`\",\"charOctetLength\":0,\"columnSize\":19,\"dataType\":-5,\"decimalDigits\":0,\"isNullable\":\"NO\",\"keySeq\":1,\"name\":\"id\",\"nullable\":0,\"numPrecRadix\":0,\"ordinalPosition\":0,\"partitionColumn\":false,\"pk\":true,\"pkName\":\"PRIMARY\",\"remarks\":\"主键\",\"typeName\":\"BIGINT\",\"uk\":false}},\"dataSource\":{\"dbType\":\"MySQL\",\"password\":\"MkAYQFiDoq/n0M/5ALLkBg==\",\"url\":\"jdbc:mysql://127.0.0.1:3306/datahub_test?useSSL=false\",\"username\":\"root\"},\"table\":{\"catalog\":\"datahub_test\",\"mppTable\":false,\"name\":\"job_log\",\"partitionTable\":false},\"where\":\"\"},\"jobId\":2,\"logId\":2,\"outputHdfsParams\":{\"directory\":\"/mysql/datahub_test/job_log\",\"hdfs\":{\"authType\":0,\"url\":\"hdfs://192.168.186.129:9000\"}},\"outputType\":\"hdfs\"}";
        JobConfig obj1 = JSON.parseObject(text, JobConfig.class);
        JobConfig obj2 = JSON.parseObject(text, JobConfig.class);
        obj2.getInputParams().setSql("123");
        
        System.out.println(obj1);
        System.out.println(obj2);
        System.out.println(obj1.equals(obj2));
    }
    
    @Test
    public void test02() throws DatahubException, IOException {
        HdfsMeta meta = new HdfsMeta();
        meta.setUserName("hdfs");
        meta.setUrl("hdfs://192.168.186.129:9000");
        
        DistributedFileSystem fs = (DistributedFileSystem) HdfsUtil.getFileSystem(meta);
        
        Path path = new Path("/data/mysql/datahub_test/job_info/job_info.txt");
        Path part = new Path("/data/mysql/datahub_test/job_log/job_log_part_0.tmp");
        
        fs.recoverLease(path);
        fs.recoverLease(part);

        FSDataOutputStream output = fs.append(path);
        FSDataInputStream input   = fs.open(part);
        IOUtils.copyLarge(input, output, new byte[1024]);
        
        
        input.close();
        output.flush();
        output.close();
        
        
    }

}
