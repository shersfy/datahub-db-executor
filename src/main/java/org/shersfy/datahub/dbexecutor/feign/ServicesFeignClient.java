package org.shersfy.datahub.dbexecutor.feign;

import org.shersfy.datahub.commons.beans.Result.ResultCode;

public interface ServicesFeignClient {
    
    /**处理成功**/
    int SUCESS = ResultCode.SUCESS;
    /**处理失败**/
    int FAIL   = ResultCode.FAIL;

}
