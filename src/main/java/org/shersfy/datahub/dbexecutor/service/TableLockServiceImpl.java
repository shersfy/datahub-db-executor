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
    public boolean lock(String tableName, String service) {
        return lock(tableName, StringUtils.EMPTY, service);
    }
    
    @Override
    public boolean lock(String tableName, String recordPk, String service) {

        TableLockKey id = new TableLockKey();
        id.setTableName(tableName);
        id.setRecordPk(recordPk);
        
        TableLock lock = new TableLock();
        lock.setTableName(id.getTableName());
        lock.setRecordPk(id.getRecordPk());
        lock.setService(service);
        lock.setLockTime(new Date());

        
        try {
            int cnt = 0;
            TableLock old = findById(id);
            if(old == null) {
                cnt = insert(lock);
                
            } else {
                
                // timeout
                long time = lock.getLockTime().getTime() - old.getLockTime().getTime();
                if(time > tableTimeoutSeconds*1000) {
                    cnt = updateById(lock);
                }
            }
            
            return cnt==1;

        } catch (Exception e) {
            return false;
        }

    }

    @Override
    public boolean unlock(String tableName) {
        return unlock(tableName, StringUtils.EMPTY);
    }

    @Override
    public boolean unlock(String tableName, String recordPk) {
        
        TableLockKey id = new TableLockKey();
        id.setTableName(tableName);
        id.setRecordPk(recordPk);
        
        int cnt = 0;
        try {
            cnt = deleteById(id);
            if(cnt==1) {
                return true;
            }
        } catch (Exception e) {
            // ignore
        }

        return findById(id)==null;
    }

}
