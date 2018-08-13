package org.shersfy.datahub.dbexecutor.params.config;

import org.shersfy.datahub.commons.meta.BaseMeta;

public class DataSourceConfig extends BaseMeta{

    /**数据库类型**/
    private String dbType;
    /**JDBC连接url**/
    private String url;
    /**JDBC连接用户名**/
    private String username;
    /**JDBC连接密码(密文)**/
    private String password;

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

}
