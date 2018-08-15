package org.shersfy.datahub.dbexecutor.model;

import java.util.Date;

public class TableLock extends TableLockKey {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /** 服务标识 **/
    private String service;

    /** 锁表时间 **/
    private Date lockTime;

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public Date getLockTime() {
        return lockTime;
    }

    public void setLockTime(Date lockTime) {
        this.lockTime = lockTime;
    }
}