package org.shersfy.datahub.dbexecutor.service;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.shersfy.datahub.commons.beans.Page;
import org.shersfy.datahub.dbexecutor.mapper.BaseMapper;
import org.shersfy.datahub.dbexecutor.model.BaseEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

public abstract class BaseServiceImpl<T extends BaseEntity, Id extends Serializable> 
    implements BaseService<T, Id> {

    protected final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    public abstract BaseMapper<T, Id> getMapper();

    @Override
    public int deleteById(Id id) {
        return getMapper().deleteById(id);
    }

    @Override
    public int deleteByIds(List<Id> ids) {
        return getMapper().deleteByIds(ids);
    }

    @Override
    public int insert(T entity) {
        if(entity.getCreateTime()==null) {
            entity.setCreateTime(new Date());
        }
        if(entity.getUpdateTime()==null) {
            entity.setUpdateTime(entity.getCreateTime());
        }
        return getMapper().insert(entity);
    }

    @Override
    public T findById(Id id) {
        return getMapper().findById(id);
    }

    @Override
    public int updateById(T entity) {
        if(entity.getUpdateTime()==null) {
            entity.setUpdateTime(new Date());
        }
        return getMapper().updateById(entity);
    }

    @Override
    public long findListCount(T where) {
        return getMapper().findListCount(parseMap(where));
    }

    @Override
    public List<T> findList(T where) {

        Map<String, Object> map = parseMap(where);
        Page<T> page = new Page<>();
        map.put("startIndex", page.getStartIndex());
        map.put("pageSize", page.getPageSize());

        return getMapper().findList(map);
    }

    @Override
    public Page<T> findPage(T where, int pageNum, int pageSize) {
        Map<String, Object> map = parseMap(where);
        Page<T> page = new Page<>(pageNum, pageSize);
        map.put("startIndex", page.getStartIndex());
        map.put("pageSize", page.getPageSize());


        long count   = getMapper().findListCount(map);
        List<T> list = getMapper().findList(map);

        page.setTotalCount(count);
        page.setData(list);
        return page;

    }


    protected Map<String, Object> parseMap(T obj){
        Map<String, Object> map = new HashMap<>();
        map.putAll(JSON.parseObject(JSON.toJSONString(obj)));
        map.put("createTime", obj.getCreateTime());
        map.put("updateTime", obj.getUpdateTime());
        map.put("startTime", obj.getStartTime());
        map.put("endTime", obj.getEndTime());
        map.put("sort", obj.getSort());
        map.put("order", obj.getOrder());

        return map;
    }

}
