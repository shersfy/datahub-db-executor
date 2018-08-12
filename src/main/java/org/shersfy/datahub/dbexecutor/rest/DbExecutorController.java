package org.shersfy.datahub.dbexecutor.rest;

import org.shersfy.datahub.commons.beans.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DbExecutorController {

    protected Logger LOGGER = LoggerFactory.getLogger(getClass());

    @GetMapping("/")
    public Object index() {
        return "Welcom database executor application";
    }


    @GetMapping("/job/config")
    public Result jobConfig(Long jobId, Long logId, String config) {
        LOGGER.info("jobId={}, logId={}, config={}", jobId, logId, config);
        return new Result();
    }

    @GetMapping("/job/check")
    public Result checkJobConfig(@RequestParam("config")String config) {
        LOGGER.info("config={}", config);
        return new Result();
    }

}
