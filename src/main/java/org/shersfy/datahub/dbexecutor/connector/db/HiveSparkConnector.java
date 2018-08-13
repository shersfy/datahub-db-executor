package org.shersfy.datahub.dbexecutor.connector.db;

import java.sql.Connection;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.shersfy.datahub.commons.exception.DatahubException;
import org.shersfy.datahub.commons.meta.ColumnMeta;
import org.shersfy.datahub.commons.meta.TableMeta;
import org.shersfy.datahub.dbexecutor.connector.hadoop.HiveUtil;
import org.shersfy.datahub.dbexecutor.connector.hadoop.HiveUtil.HiveTableFormat;

/**
 * Hive on Spark provides Hive with the ability to utilize Apache Spark as its execution engine
 */
public class HiveSparkConnector extends HiveConnector {
	
	@Override
	public String getDDLByTable(TableMeta table, List<ColumnMeta> columns, Connection conn) throws DatahubException {
		if(StringUtils.isBlank(table.getFormat())){
			table.setFormat(HiveTableFormat.orc.name());
		}
		String ddl = super.getDDLByTable(table, columns, conn);
		// 列为空，返回已存在表的DDL
		if(columns == null || columns.isEmpty()){
			return ddl;
		}
//		HiveTableFormat foramt = HiveTableFormat.valueOf(table.getFormat());
		HiveTableFormat foramt = HiveUtil.getTableFormatByDDL(ddl);
		if (HiveTableFormat.orc == foramt) {
			int cIndex = ddl.indexOf("CLUSTERED BY");
			if (cIndex == -1) {
				int index = ddl.indexOf("\nSTORED AS");
				StringBuilder builder = new StringBuilder();
				builder.append(ddl.substring(0, index));
				builder.append(" ");
				builder.append("\nCLUSTERED BY (");
				builder.append(columns.get(0).getName());
				builder.append(") INTO 2 BUCKETS ");
				builder.append(ddl.substring(index+1));
				builder.append("\nTBLPROPERTIES ('transactional'='true')");
				return builder.toString();
			}
		} 
		return ddl;
	}

}
