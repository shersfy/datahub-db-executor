package org.shersfy.datahub.dbexecutor.connector.db;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;
import org.shersfy.datahub.commons.constant.CommConst;
import org.shersfy.datahub.commons.exception.DatahubException;
import org.shersfy.datahub.commons.exception.NetException;
import org.shersfy.datahub.commons.exception.TooManyConnectionException;
import org.shersfy.datahub.commons.meta.ColumnMeta;
import org.shersfy.datahub.commons.meta.DBAccessType;
import org.shersfy.datahub.commons.meta.DBMeta;
import org.shersfy.datahub.commons.meta.FieldData;
import org.shersfy.datahub.commons.meta.GridData;
import org.shersfy.datahub.commons.meta.RowData;
import org.shersfy.datahub.commons.meta.TableMeta;
import org.shersfy.datahub.commons.meta.TableType;
import org.shersfy.datahub.commons.utils.FunUtil;

import oracle.sql.Datum;


/**
 * Oracle数据库连接器
 */
public class OracleConnector extends DbConnectorInterface {

	@Override
	public String loadDriverClass(DBAccessType type){

		String driver 	= "oracle.jdbc.driver.OracleDriver";
		switch (type) {
		case ODBC:
			driver = "sun.jdbc.odbc.JdbcOdbcDriver";
			break;
		case OCI:
			break;
		case JNDI:
			break;
		default:
			break;
		}

		return driver;
	}
	
	@Override
	public List<TableMeta> getTables(String catalog, String schema, TableType[] types, Connection conn)
			throws DatahubException {
		List<TableMeta> list = super.getTables(catalog, schema, types, conn);
		list.removeIf(tbl->{
			boolean flg = tbl.getName().matches("SYS_IOT_OVER_[0-9]+");
			if(flg){
				logger.warn("skip system table '{}'", tbl.getName());
			}
			return flg;
		});
		return list;
	}

	@Override
	public String queryByPage(String baseSql, long pageNo,
			long pageSize) throws DatahubException {
		
		Connection conn = null;
		try {
			// 1.查询字段，排除分页的RN字段列
			String sql = FunUtil.formatArgs(makeSql(baseSql, 1, 0), "*");
			conn = this.connection();
			GridData data = super.executeQuery(conn, sql);
			List<ColumnMeta> header = data.getHeaders();
			// 2.生成真实字段的SQL语句
			if(header.size()>1){
				StringBuffer fields = new StringBuffer(0);
				for(int i=1; i<header.size(); i++){
					fields.append("\"");
					fields.append(header.get(i).getName());
					fields.append("\"");
					fields.append(", ");
				}
				sql = FunUtil.formatArgs(makeSql(baseSql, pageNo, pageSize), 
						fields.subSequence(0, fields.length()-2));
			}
			else{
				sql = FunUtil.formatArgs(makeSql(baseSql, pageNo, pageSize), "*");
			}
			
			return sql;
		} catch (Exception ex) {
		    throw DatahubException.throwDatahubException("execute query error:"+baseSql, ex);
		} finally {
			close(conn);
		}
	}
	@Override
	public void setDbMeta(DBMeta dbMeta) {
		dbMeta.setSchema(dbMeta.getUserName().toUpperCase());
		Properties params = dbMeta.getParams();
		if(StringUtils.isBlank(params.getProperty("oracle.net.CONNECT_TIMEOUT"))){
			params.put("oracle.net.CONNECT_TIMEOUT", String.valueOf(TIMEOUT));
		}
		dbMeta.setParams(params);
		if(StringUtils.isBlank(dbMeta.getUrl())){
			dbMeta.setUrl(String.format("jdbc:oracle:thin:@%s:%s:%s", 
					dbMeta.getHost(),
					dbMeta.getPort(),
					dbMeta.getDbName()));
		}
		this.dbMeta = dbMeta;
		
	}
	
	
	private String makeSql(String baseSql, long pageNo, long pageSize){
		if(StringUtils.isBlank(baseSql) ){
			return null;
		}
		if(pageNo<1 || pageSize<0){
			return baseSql;
		}
		StringBuffer sql = new StringBuffer(0);
		long start = (pageNo-1) * pageSize+1;//RN从1开始
		long end = start + pageSize;
		String aliasA = "TA"+System.currentTimeMillis();
		String aliasB = "TB"+System.currentTimeMillis();
		String RN = "RN"+System.currentTimeMillis();
		// baseSql未分页
		sql.append("SELECT {0} FROM (");
		sql.append("SELECT ROWNUM "+RN+", ");
		sql.append(aliasA);
		sql.append(".*");
		sql.append(" FROM ( ");
		sql.append(baseSql);
		sql.append(" ) ");
		sql.append(aliasA);
		sql.append(" WHERE ROWNUM < ").append(end);
		sql.append(" ) ").append(aliasB);
		sql.append(" WHERE "+RN+" >= ").append(start);
		
		return sql.toString();
	}

	@Override
	public List<DBMeta> getDatabases(Connection conn) throws DatahubException {
		List<DBMeta> dbs 	= new ArrayList<DBMeta>();
		ResultSet rs 		= null;
		try {
			if(conn == null){
				throw new SQLException("connection is null");
			}
			rs = conn.getMetaData().getSchemas();

			while(rs.next()){
				DBMeta meta = new DBMeta();
				BeanUtils.copyProperties(meta, getDbMeta());
				meta.setName(rs.getString("TABLE_SCHEM"));
				meta.setCatalog(conn.getCatalog());
				meta.setSchema(meta.getName());
				//meta.setDbName(rs.getString("TABLE_CAT"));
				dbs.add(meta);
			}

		} catch (Exception ex) {
		    throw DatahubException.throwDatahubException("list all database error", ex);
		} finally {
			close(rs);
		}
		//		 DBMeta db = getDbMeta();
		//		 db.setName(db.getSchema());
		//		 dbs.add(db);
		return dbs;
	}

	/**java.sql.Types映射为数据库类型**/
	private static Map<Integer, String> javaTypeToDbTypeMap = new HashMap<Integer, String>();
	static {

		javaTypeToDbTypeMap.put(Integer.valueOf(Types.ARRAY), "RAW");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BIGINT), "NUMERIC");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BINARY), "RAW");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BIT), "NUMERIC");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BLOB), "BLOB");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BOOLEAN), "VARCHAR");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.CHAR), "CHAR");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.CLOB), "CLOB");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DECIMAL), "NUMERIC");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DATE), "DATE");	
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DOUBLE), "FLOAT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DATALINK), "VARCHAR2");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DISTINCT), "VARCHAR2");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.FLOAT), "FLOAT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.INTEGER), "NUMERIC");	
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.JAVA_OBJECT), "RAW");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.LONGNVARCHAR), "NVARCHAR2");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.LONGVARBINARY), "LONG RAW");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.LONGVARCHAR), "VARCHAR2");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NCLOB), "NCLOB");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NCHAR), "NCHAR");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NUMERIC), "NUMERIC");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NVARCHAR), "NVARCHAR2");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NULL), "RAW");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.OTHER), "RAW");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.REAL), "NUMERIC");	
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.REF), "RAW");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.REF_CURSOR), "RAW");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.ROWID), "ROWID");	
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.SMALLINT), "NUMERIC");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.STRUCT), "VARCHAR2");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.SQLXML), "VARCHAR2");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TIME), "DATE");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TIME_WITH_TIMEZONE), "DATE");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TIMESTAMP), "TIMESTAMP");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TIMESTAMP_WITH_TIMEZONE), "TIMESTAMP");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TINYINT), "NUMERIC");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.VARBINARY), "RAW");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.VARCHAR), "VARCHAR2");

	}
	
	@Override
	public String javaTypeToDbType(int javaType, ColumnMeta column) throws DatahubException {
		String type = javaTypeToDbTypeMap.get(Integer.valueOf(javaType));
		if( StringUtils.isBlank(type)){
			type = javaTypeToDbTypeMap.get(Integer.valueOf(Types.VARCHAR));
			javaType = Types.VARCHAR;
		}
		
		switch (javaType) {
		case Types.BIGINT:
		case Types.BIT:
		case Types.DECIMAL:
		case Types.INTEGER:
		case Types.NUMERIC:
		case Types.REAL:
		case Types.SMALLINT:
		case Types.TINYINT:
			if(column.getDecimalDigits()>0 && column.getDecimalDigits() <= column.getColumnSize()){
				type = type + String.format("(%s, %s)", column.getColumnSize(), column.getDecimalDigits());
			}
			else if(column.getColumnSize()>0){
				type = type + String.format("(%s, 0)", column.getColumnSize());
			}
			break;
		case Types.CHAR:
		case Types.NCHAR:
			type = type + String.format("(%s)", column.getColumnSize());
			break;
		case Types.BOOLEAN:
		case Types.DATALINK:
		case Types.DISTINCT:
		case Types.VARCHAR:
		case Types.NVARCHAR:
		case Types.LONGVARCHAR:
		case Types.LONGNVARCHAR:
			type = type + String.format("(%s)", column.getColumnSize());
			if(column.getColumnSize() > 255){
				type = javaTypeToDbTypeMap.get(Integer.valueOf(Types.LONGVARCHAR));
			}
			break;
		default:
			break;
		}
		
		return type;
	}

	@Override
	public String showCreateTable(TableMeta table, Connection conn) throws DatahubException {
		
		if(table == null || conn == null){
			return "";
		}
		table.setCatalog(StringUtils.isBlank(table.getCatalog())?null:table.getCatalog());
		table.setSchema(StringUtils.isBlank(table.getSchema())?null:table.getSchema());
		
		String res = StringUtils.EMPTY;
		String showSql = "SELECT DBMS_LOB.SUBSTR(DBMS_METADATA.GET_DDL(%s), "
				+ "LENGTH(DBMS_METADATA.GET_DDL(%s))) FROM dual";
		try {
			
			StringBuffer params = new StringBuffer();
			params.append("'TABLE', '");
			params.append(table.getName());
			params.append("'");
			if(StringUtils.isNotBlank(table.getSchema())){
				params.append(", '");
				params.append(table.getSchema());
				params.append("'");
				
			}
			showSql = String.format(showSql, params.toString() , params.toString());
			
			GridData data = this.executeQuery(conn, showSql);
			List<RowData> rows = data.getRows();
			
			if(rows.size()>0 && rows.get(0).getFields().size()>0){
				res = String.valueOf(rows.get(0).getFields().get(0).getValue());
				res = res.trim();
			}
			
		} catch (Exception ex) {
		    throw DatahubException.throwDatahubException("show table DDL error: "+showSql, ex);
		} 
		
		return res;
	}

	@Override
	public void formatFieldData(Connection conn, ColumnMeta column, FieldData field) throws SQLException {
		super.formatFieldData(conn, column, field);
		if(field == null){
			return;
		}
		switch (column.getDataType()) {
		case oracle.jdbc.OracleTypes.TIMESTAMPTZ:
			// oracle TIMESTAMPTZ
			if(field.getValue()  instanceof oracle.sql.Datum){
				oracle.sql.Datum data = (Datum) field.getValue();
				field.setValue(new String(data.stringValue(conn)));
			}
			
			break;
		case oracle.jdbc.OracleTypes.TIMESTAMPLTZ:
			// oracle TIMESTAMPLTZ
			if(field.getValue()  instanceof oracle.sql.Datum){
				oracle.sql.Datum data = (Datum) field.getValue();
				field.setValue(oracle.sql.TIMESTAMPLTZ.toTimestamp(conn, data.getBytes()));
			}
			break;
		case oracle.jdbc.OracleTypes.BFILE:
			// oracle TIMESTAMPLTZ
			field.setValue("");
			break;
		case Types.ROWID:
			// ROWID
			if(field.getValue()  instanceof RowId){
				RowId rowid = (RowId)field.getValue();
				field.setValue(new String(rowid.getBytes()));
			}
			break;

		default:
			break;
		}
	}
	
	@Override
	public Throwable connectionException(Throwable ex) {
		ex = super.connectionException(ex);
		String err = ex.getMessage();
		if(StringUtils.containsIgnoreCase(err, "ORA-12519")
				|| StringUtils.containsIgnoreCase(err, "ORA-12518")){
			return new TooManyConnectionException(ex);
		}
		
		if(StringUtils.containsIgnoreCase(err, "Socket read timed out")){
			return new NetException(ex);
		}
		
		return ex;
	}

	@Override
	public String getCutPointSql(TableMeta table, ColumnMeta partColumn, 
			String where, long totalSize, int blockCnt) {
		
		StringBuffer partSql = new StringBuffer(0);
		if(table == null
				|| partColumn == null 
				|| blockCnt<2
				|| totalSize<1){
			return partSql.toString();
		}
		
		long pageSize = (long)(Math.ceil((double)totalSize/blockCnt));
		
		String RN = "RN"+System.currentTimeMillis();
		String TA = "TA"+System.currentTimeMillis();
		String TB = "TB"+System.currentTimeMillis();
		String union = "UNION\n";
		
		String fullName = this.getFullTableName(table);
		String colName  = this.quotObject(partColumn.getName());
		
		StringBuffer baseSql = new StringBuffer(0);
		baseSql.append("SELECT ").append(colName);
		baseSql.append(" FROM ").append(fullName).append(" ");
		baseSql.append(where==null?"":where.trim());
		baseSql.append(" ORDER BY ").append(colName);
		
		partSql.append("SELECT ");
		partSql.append("MIN(").append(colName).append(") miv, ");
		partSql.append("MAX(").append(colName).append(") mav ");
		partSql.append("FROM (");
		
		partSql.append("SELECT ").append(colName);
		partSql.append(" FROM (");
		partSql.append("SELECT ROWNUM ").append(RN).append(", ").append(colName);
		partSql.append(" FROM (").append(baseSql);
		partSql.append(" ) WHERE ROWNUM < {0} ) ").append(TA);
		partSql.append(" WHERE ").append(TA).append(".").append(RN).append(" >= {1} ");

		partSql.append(")").append(TB);
		
		
		StringBuffer unionSql = new StringBuffer(0);
		for(int ponit =2; ponit<=blockCnt;  ponit+=2){
			long start = (ponit-1) * pageSize+1;//RN从1开始
			long end   = start + pageSize;
			unionSql.append(FunUtil.formatArgs(partSql.toString(), end, start));
			unionSql.append(CommConst.LINE_SEP);
			unionSql.append(union);
		}
		String sql = unionSql.toString();
		if(sql.endsWith(union)){
			sql = sql.substring(0, sql.length()-union.length());
		}
		if(sql.length()>0){
			sql = sql + "ORDER BY miv";
		}
		
		return sql;
	}

	/**
	 * 将传递过来的where条件sql进行重装
	 * 
	 * @param sql
	 * @param data
	 * @return 重装后的sql
	 */
	@Override
	public String getDateFunction(String formatDateStr) {
		
		StringBuffer sb = new StringBuffer();
		sb.append("TO_DATE('").append(formatDateStr).append("', '").append("yyyy-MM-dd HH24:mi:ss").append("')");
		
		return sb.toString();
	}
	
	/**
	 * 获取同步插入的sql，实现表中有数据就更新，无则插入</br>
	 * 
	 *  MERGE INTO JOB_LOG T1
	 *  USING ( SELECT 10 AS "ID", 10 AS "JOB_ID", TO_DATE('2017-08-25 10:35:14','yyyy-mm-dd hh24:mi:ss')
	 *  AS "START_TIME"  FROM dual) T2"
	 *  ON(T1."ID" = T2."ID")
	 *  WHEN MATCHED THEN
	 *  UPDATE SET T1."JOB_ID"=T2."JOB_ID",T1."START_TIME"=T2."START_TIME"
	 *  WHEN NOT MATCHED THEN
	 *  INSERT ("ID" ,"JOB_ID" ,"START_TIME") VALUES( T2."ID", T2."JOB_ID", T2."START_TIME")
	 * 
	 * @author LinXibiao
	 * @date 2017-11-16	 
	 * 
	 * @param cols
	 * @param fullTableName
	 * @exception DatahubException
	 * @return sql
	 */
	@Override
	public String getUpdateInsert(List<ColumnMeta> cols, String fullTableName)
			throws DatahubException {
		
		StringBuffer fields = new StringBuffer();
		StringBuffer values = new StringBuffer();
		String sep = ", ";
		String sql = "MERGE INTO %s USING %s ON (%s) "
				+ "WHEN MATCHED THEN UPDATE SET %s "
				+ "WHEN NOT MATCHED THEN  INSERT(%s) VALUES (%s)";
		String t1 = " T1" + System.currentTimeMillis();
		String t2 = " T2" + System.currentTimeMillis();
		StringBuffer s4 = new StringBuffer();
		StringBuffer s3 = new StringBuffer();
		StringBuffer s2 = new StringBuffer();
		StringBuffer s1 = new StringBuffer();
		// 目标表
		s1.append(fullTableName).append(t1);
		// 拼接数据临时表的列和数据
		s2.append("( SELECT ");
		for (ColumnMeta col : cols) {
			// 拼接数据临时表的列和数据
			s2.append("?");
			s2.append(" AS ");
			s2.append(quotObject(col.getName()));
			s2.append(sep);
			// 拼接主键
			if (col.isPk()) {
				s3.append(t1).append(".").append(quotObject(col.getName()));
				s3.append(" = ");
				s3.append(t2).append(".").append(quotObject(col.getName()));
				s3.append(sep);
			} else {
				// 拼接 set语句
				s4.append(t1).append(".").append(quotObject(col.getName()));
				s4.append(" = ");
				s4.append(t2).append(".").append(quotObject(col.getName()));
				s4.append(sep);
			}
			// 拼接insert语句
			fields.append(quotObject(col.getName()));
			fields.append(sep);
			values.append(t2).append(".").append(quotObject(col.getName())).append(sep);
		}
		s2 = s2.deleteCharAt(s2.length() - sep.length());
		s2.append(" FROM dual) ");
		s2.append(t2);
		s3 = s3.deleteCharAt(s3.length() - sep.length());
		s4 = s4.deleteCharAt(s4.length() - sep.length());
		fields = fields.deleteCharAt(fields.length() - sep.length());
		values = values.deleteCharAt(values.length() - sep.length());
		sql = String.format(sql, s1, s2, s3, s4, fields, values);
		return sql;
	}
	
	@Override
	public boolean checkAvailable() throws DatahubException {
		checkUrl("jdbc:oracle");
		return true;
	}

	@Override
	public ResultSet getIndexInfo(TableMeta table, Connection conn) throws SQLException {
		String sql = "SELECT LOWER('0') TYPE, USER_IND_COLUMNS.* from USER_IND_COLUMNS WHERE TABLE_NAME= '%s'";
		sql = String.format(sql, table.getName());
		Statement stm = conn.createStatement();
		ResultSet res = stm.executeQuery(sql);
		return res;
	}
	
}
