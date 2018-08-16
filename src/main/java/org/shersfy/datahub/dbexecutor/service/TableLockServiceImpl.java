package org.shersfy.datahub.dbexecutor.service;

import java.util.Date;

import javax.annotation.Resource;

import org.apache.commons.lang.StringUtils;
import org.shersfy.datahub.dbexecutor.mapper.BaseMapper;
import org.shersfy.datahub.dbexecutor.mapper.TableLockMapper;
import org.shersfy.datahub.dbexecutor.model.TableLock;
import org.shersfy.datahub.dbexecutor.model.TableLockKey;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Transactional
@Service
public class TableLockServiceImpl extends BaseServiceImpl<TableLock, TableLockKey> 
implements TableLockService{

    @Value("${lock.table.timeoutSeconds}")
    private int tableTimeoutSeconds = 60;
    
    @Value("${lock.record.timeoutSeconds}")
    private int recordTimeoutSeconds = 60;

    
    @Resource
    private TableLockMapper mapper;

    @Override
    public BaseMapper<TableLock, TableLockKey> getMapper() {
        return mapper;
    }

    @Override
    public boolean lock(String tableName) {
        return lock(tableName, StringUtils.EMPTY);
    }
    
    @Override
    public boolean lock(String tableName, String recordPk) {

        boolean locked = true;
        
        TableLockKey id = new TableLockKey();
        id.setTableName(tableName);
        id.setRecordPk(recordPk);
        
        TableLock lock = new TableLock();
        lock.setTableName(id.getTableName());
        lock.setRecordPk(id.getRecordPk());
        lock.setService(JobServices.SERVICE_NAME);
        lock.setLockTime(new Date());

        
        try {
            TableLock old = findById(id);
            if(old == null) {
                locked = insert(lock)==1;
                
            } else {
                
                // timeout
                long time = lock.getLockTime().getTime() - old.getLockTime().getTime();
                if(time > tableTimeoutSeconds*1000) {
                    locked = updateById(lock) == 1;
                } else {
                    locked = false;
                }
            }
            
        } catch (Exception e) {
            locked = false;
        }
        
        if(locked) {
            LOGGER.info("locked table '{}', record id '{}'", tableName, recordPk);
        }

        return locked;
    }

    @Override
    public boolean unlock(String tableName) {
        return unlock(tableName, StringUtils.EMPTY);
    }

    @Override
    public boolean unlock(String tableName, String recordPk) {
        
        boolean unlocked = true;
        
        TableLockKey id = new TableLockKey();
        id.setTableName(tableName);
        id.setRecordPk(recordPk);
        
        try {
            
            TableLock old = findById(id);
            if(old==null) {
                return true;
            }
            
            if(old.getService() != null 
                && !old.getService().equals(JobServices.SERVICE_NAME)) {
                return false;
            }
            
            unlocked = deleteById(id)==1;

        } catch (Exception e) {
            // ignore
            unlocked = false;
        }

        if(unlocked) {
            LOGGER.info("unlocked table '{}', record id '{}'", tableName, recordPk);
        }
        
        return unlocked;
    }

}
