package org.shersfy.datahub.dbexecutor.service;

import org.shersfy.datahub.dbexecutor.params.config.JobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Component
public class JobServices {
    
    Logger LOGGER = LoggerFactory.getLogger(JobServices.class);
    
    @Async
    public void execute(JobConfig config) {
        for(int i=0; i<30; i++) {
            LOGGER.info("{}", config);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
