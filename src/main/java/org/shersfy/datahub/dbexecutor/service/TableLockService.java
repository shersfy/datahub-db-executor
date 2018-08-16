package org.shersfy.datahub.dbexecutor.service;

import org.shersfy.datahub.dbexecutor.model.TableLock;
import org.shersfy.datahub.dbexecutor.model.TableLockKey;

public interface TableLockService extends BaseService<TableLock, TableLockKey> {
    
    /**
     * 锁表
     * @param tableName 表名
     * @return true成功，false失败
     */
    boolean lock(String tableName);
    
    /**
     * 锁记录
     * @param tableName 表名
     * @param recordPk 记录主键
     * @return true成功，false失败
     */
    boolean lock(String tableName, String recordPk);
    
    /**
     * 释放表锁，谁加锁谁解锁原则
     * @param tableName 表名
     * @return true成功，false失败
     */
    boolean unlock(String tableName);

    /**
     * 释放记录锁，谁加锁谁解锁原则
     * @param tableName 表名
     * @param recordPk 记录主键
     * @return true成功，false失败
     */
    boolean unlock(String tableName, String recordPk);
    
}
