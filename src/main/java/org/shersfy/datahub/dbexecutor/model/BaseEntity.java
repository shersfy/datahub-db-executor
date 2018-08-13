package org.shersfy.datahub.dbexecutor.model;

import java.io.Serializable;
import java.util.Date;

import com.alibaba.fastjson.JSONObject;

public class BaseEntity implements Serializable {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/** 主键 **/
    private Long id;
	/** 创建时间 **/
    private Date createTime;
    /** 更新时间 **/
    private Date updateTime;
    /** 检索开始时间 **/
    private Date startTime;
    /** 检索结束时间 **/
    private Date endTime;
    /** 排序字段 **/
    private transient String sort;
    /** 排序方式 **/
    private transient String order;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public Date getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}

	/**
	 * 获取排序字段
	 * 
	 * @return
	 */
	public String getSort() {
		return sort;
	}

	/**
	 * 设置排序字段
	 * 
	 * @param sort
	 */
	public void setSort(String sort) {
		this.sort = sort;
	}

	/**
	 * 获取排序方式
	 * 
	 * @return
	 */
	public String getOrder() {
		return order;
	}

	/**
	 * 设置排序方式(ASC|DESC)
	 * 
	 * @param order
	 */
	public void setOrder(String order) {
		this.order = order;
	}


	@Override
	public String toString() {
		return JSONObject.toJSONString(this);
	}

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }
    
}
