package org.shersfy.datahub.dbexecutor.params.template;

import org.shersfy.datahub.commons.connector.db.TablePartition;
import org.shersfy.datahub.commons.meta.BaseMeta;
import org.shersfy.datahub.commons.meta.TableMeta;
import org.shersfy.datahub.dbexecutor.params.config.DataSourceConfig;

public class InputDbParams extends BaseMeta {
    
    /**数据源配置**/
    private DataSourceConfig dataSource;
    
    /**完整表名称**/
    private TableMeta table;
    
    /**条件**/
    private String where;
    
    /**执行SQL**/
    private String sql;
    
    /**分块**/
    private TablePartition block;

    public DataSourceConfig getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSourceConfig dataSource) {
        this.dataSource = dataSource;
    }

    public String getWhere() {
        return where;
    }

    public void setWhere(String where) {
        this.where = where;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public TableMeta getTable() {
        return table;
    }

    public void setTable(TableMeta table) {
        this.table = table;
    }

    public TablePartition getBlock() {
        return block;
    }

    public void setBlock(TablePartition block) {
        this.block = block;
    }
    
}
