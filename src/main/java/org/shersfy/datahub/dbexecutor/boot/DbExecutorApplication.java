package org.shersfy.datahub.dbexecutor.boot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@EnableDiscoveryClient
@EnableFeignClients(basePackages="org.shersfy.datahub.dbexecutor.feign")
@ComponentScan(basePackages="org.shersfy.datahub.dbexecutor",
excludeFilters= @Filter(type=FilterType.ANNOTATION, value=FeignClient.class))
@SpringBootApplication
public class DbExecutorApplication {

	public static void main(String[] args) {
	    SpringApplication.run(DbExecutorApplication.class, args);
	}

}
