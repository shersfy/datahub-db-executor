package org.shersfy.datahub.dbexecutor.model;

public class TableLockKey extends BaseEntity {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /** 表名称 **/
    private String tableName;

    /** 记录主键（锁表空串） **/
    private String recordPk;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getRecordPk() {
        return recordPk;
    }

    public void setRecordPk(String recordPk) {
        this.recordPk = recordPk;
    }
}