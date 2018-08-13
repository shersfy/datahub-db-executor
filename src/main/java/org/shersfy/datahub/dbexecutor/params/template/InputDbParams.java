package org.shersfy.datahub.dbexecutor.params.template;

import org.shersfy.datahub.commons.meta.BaseMeta;
import org.shersfy.datahub.dbexecutor.params.config.DataSourceConfig;

public class InputDbParams extends BaseMeta {
    
    /**数据源配置**/
    private DataSourceConfig dataSource;
    
    /**完整表名称**/
    private String fullTableName;
    
    /**条件**/
    private String where;
    
    /**执行SQL**/
    private String sql;

    public DataSourceConfig getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSourceConfig dataSource) {
        this.dataSource = dataSource;
    }

    public String getFullTableName() {
        return fullTableName;
    }

    public void setFullTableName(String fullTableName) {
        this.fullTableName = fullTableName;
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
    
}
