package org.shersfy.datahub.dbexecutor.boot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.Banner.Mode;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.ComponentScan;

@EnableDiscoveryClient
@ComponentScan("org.shersfy.datahub.dbexecutor")
@SpringBootApplication
public class DbExecutorApplication {

	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(DbExecutorApplication.class);
		app.setBannerMode(Mode.OFF);
		app.run(args);
	}

}
