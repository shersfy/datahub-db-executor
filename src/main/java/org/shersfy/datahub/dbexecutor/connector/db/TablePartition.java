package org.shersfy.datahub.dbexecutor.connector.db;

import java.util.List;

import org.shersfy.datahub.commons.meta.BaseMeta;
import org.shersfy.datahub.commons.meta.ColumnMeta;

/**
 * 分块条件
 */
public class TablePartition extends BaseMeta{

	/**分块索引**/
	private int index;
	/**分块执行SQL**/
	private String partSql;
	/**分块条件**/
	private String condition;
	/**分块条件参数**/
	private List<Object> conditionArgs;
	/**分块字段**/
	private ColumnMeta partColumn;

	public TablePartition(){}
	
	public TablePartition (String condition){
		this.condition = condition;
	}

	public int getIndex() {
		return index;
	}

	public String getCondition() {
		return condition;
	}

	public List<Object> getConditionArgs() {
		return conditionArgs;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public void setCondition(String condition) {
		this.condition = condition;
	}

	public void setConditionArgs(List<Object> conditionArgs) {
		this.conditionArgs = conditionArgs;
	}

	public ColumnMeta getPartColumn() {
		return partColumn;
	}

	public void setPartColumn(ColumnMeta partColumn) {
		this.partColumn = partColumn;
	}

	public String getPartSql() {
		return partSql;
	}

	public void setPartSql(String partSql) {
		this.partSql = partSql;
	}

	@Override
	public String getName() {
		String name = super.getName();
		if(name==null){
			name = String.format("part_%s", this.index);
		}
		return name;
	}

}
