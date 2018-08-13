package org.shersfy.datahub.dbexecutor.params.template;

import org.shersfy.datahub.commons.meta.BaseMeta;

public class OutputHiveParams extends BaseMeta {
    
    /**完整表名称**/
    private String fullTableName;

    public String getFullTableName() {
        return fullTableName;
    }

    public void setFullTableName(String fullTableName) {
        this.fullTableName = fullTableName;
    }
    
    
}
