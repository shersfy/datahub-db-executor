package org.shersfy.datahub.dbexecutor.boot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@EnableDiscoveryClient
@ComponentScan("org.shersfy.datahub.dbexecutor")
@SpringBootApplication
public class DbExecutorApplication {

	public static void main(String[] args) {
	    SpringApplication.run(DbExecutorApplication.class, args);
	}

}
