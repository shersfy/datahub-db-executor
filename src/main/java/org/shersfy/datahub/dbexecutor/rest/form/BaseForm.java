package org.shersfy.datahub.dbexecutor.rest.form;

import com.alibaba.fastjson.JSON;

/**
 * 表单参数公用
 */
public class BaseForm {

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }


}
