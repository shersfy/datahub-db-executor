package org.shersfy.datahub.dbexecutor.tests;

import org.junit.Test;
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

}
