package org.shersfy.datahub.dbexecutor.boot;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@EnableAsync
@EnableDiscoveryClient
@EnableTransactionManagement
@EnableFeignClients(basePackages="org.shersfy.datahub.dbexecutor.feign")
@MapperScan("org.shersfy.datahub.dbexecutor.mapper")
@ComponentScan(basePackages="org.shersfy.datahub.dbexecutor",
excludeFilters= @Filter(type=FilterType.ANNOTATION, value=FeignClient.class))
@SpringBootApplication
public class DbExecutorApplication {

	public static void main(String[] args) {
	    SpringApplication.run(DbExecutorApplication.class, args);
	}

}
