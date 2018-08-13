package org.shersfy.datahub.dbexecutor.service;

import java.io.Serializable;
import java.util.List;

import org.shersfy.datahub.commons.beans.Page;
import org.shersfy.datahub.commons.beans.Result.ResultCode;
import org.shersfy.datahub.dbexecutor.model.BaseEntity;

public interface BaseService<T extends BaseEntity, Id extends Serializable> {
    
    /**处理成功**/
    public final int SUCESS = ResultCode.SUCESS;
    /**处理失败**/
    public final int FAIL   = ResultCode.FAIL;
	
	int deleteById(Id id);
	
	int deleteByIds(List<Long> ids);

	int insert(T entity);

	T findById(Id id);

	int updateById(T entity);
	
	
	long findListCount(T where);
	
	List<T> findList(T where);

	Page<T> findPage(T where, int pageNum, int pageSize);
}
