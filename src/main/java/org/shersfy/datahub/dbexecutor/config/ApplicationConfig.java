package org.shersfy.datahub.dbexecutor.config;

import javax.sql.DataSource;

import org.shersfy.datahub.commons.utils.LocalHostUtil;
import org.shersfy.datahub.dbexecutor.service.JobServices;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.web.context.WebServerInitializedEvent;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApplicationConfig implements ApplicationListener<WebServerInitializedEvent>{
    
    @Bean
    @RefreshScope
    @ConfigurationProperties(prefix = "spring.datasource.hikari")
    public DataSource dataSource() {
        DataSource ds = DataSourceBuilder.create().build();
        return ds;
    }

    @Override
    public void onApplicationEvent(WebServerInitializedEvent event) {
        int port = event.getWebServer().getPort();
        JobServices.SERVICE_NAME = String.format("%s:%s", LocalHostUtil.IP, port);
    }


}
