package org.shersfy.datahub.dbexecutor.mapper;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.shersfy.datahub.dbexecutor.model.BaseEntity;

public interface BaseMapper<T extends BaseEntity, Id extends Serializable> {
	
	int deleteById(Id id);
	
	int deleteByIds(List<Long> ids);

	int insert(T entity);

	T findById(Id id);

	int updateById(T entity);

	long findListCount(Map<String, Object> map);
	
	List<T> findList(Map<String, Object> map);
}
