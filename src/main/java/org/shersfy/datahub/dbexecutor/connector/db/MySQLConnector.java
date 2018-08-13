package org.shersfy.datahub.dbexecutor.connector.db;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
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
import org.shersfy.datahub.commons.utils.DateUtil;
import org.shersfy.datahub.commons.utils.FunUtil;


/**
 * MySQL数据库连接器
 */
public class MySQLConnector extends DbConnectorInterface {

	@Override
	public String loadDriverClass(DBAccessType type) {

		String driver 	= "com.mysql.jdbc.Driver";
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
	public String queryByPage(String baseSql, long pageNo,
			long pageSize) throws DatahubException {
		if(StringUtils.isBlank(baseSql) ){
			return null;
		}
		if(pageNo<1 || pageSize<0){
			return baseSql;
		}
		long start = (pageNo-1) * pageSize;
		String aliasA = "T"+System.currentTimeMillis();
		StringBuffer sql = new StringBuffer(0);
		
		// baseSql未分页
		if(!StringUtils.containsIgnoreCase(baseSql, " LIMIT ")){
			sql.append(baseSql);
			sql.append(" LIMIT ");
			sql.append(start);
			sql.append(", ");
			sql.append(pageSize);
			return sql.toString();
		}
		// baseSql在已经分页的基础上二次分页
		sql.append("SELECT * FROM (");
		sql.append(baseSql);
		sql.append(") ");
		sql.append(aliasA);
		sql.append(" LIMIT ");
		sql.append(start);
		sql.append(", ");
		sql.append(pageSize);
		return sql.toString();
	}

	@Override
	public void setDbMeta(DBMeta dbMeta) throws DatahubException {
		Properties params = dbMeta.getParams();
		if(StringUtils.isBlank(params.getProperty("connectTimeout"))){
			params.put("connectTimeout", String.valueOf(TIMEOUT));
		}
		dbMeta.setCatalog(dbMeta.getDbName());
		if(StringUtils.isBlank(dbMeta.getUrl())){
			dbMeta.setUrl(String.format("jdbc:mysql://%s:%s/%s", 
					dbMeta.getHost(),
					dbMeta.getPort(),
					dbMeta.getDbName()));
		}
		
		this.dbMeta = dbMeta;
	}

	@Override
	public List<DBMeta> getDatabases(Connection conn) throws DatahubException {
		List<DBMeta> dbs 	= new ArrayList<DBMeta>();
		ResultSet rs 		= null;
		try {
			if(conn == null){
				throw new SQLException("connection is null");
			}
			rs = conn.getMetaData().getCatalogs();

			while(rs.next()){
				DBMeta meta = new DBMeta();
				BeanUtils.copyProperties(meta, getDbMeta());
				meta.setName(rs.getString("TABLE_CAT"));
				meta.setCatalog(meta.getName());
				//meta.setSchema(conn.getSchema());
				meta.setDbName(meta.getName());
				if("information_schema".equals(meta.getName())
						|| "mysql".equals(meta.getName())
						|| "performance_schema".equals(meta.getName())
						|| "test".equals(meta.getName())){
					continue;
				}
				//initPublicInfo(meta);
				dbs.add(meta);
			}

		} catch (Exception ex) {
            throw DatahubException.throwDatahubException("list all database error", ex);
		} finally {
			close(rs);
		}

		return dbs;
	}

	@Override
	public String getDDLByTable(TableMeta table, List<ColumnMeta> columns, 
			Connection conn) throws DatahubException {
		
		if(table == null || StringUtils.isBlank(table.getName())){
			return StringUtils.EMPTY;
		}
		
		table.setCatalog(StringUtils.isBlank(table.getCatalog())?null:table.getCatalog());
		table.setSchema(StringUtils.isBlank(table.getSchema())?null:table.getSchema());
		
		if(columns == null || columns.isEmpty()){
			return showCreateTable(table, conn);
		}
		
		for(int i=0; i<columns.size(); i++){
			if(StringUtils.isBlank(columns.get(i).getName())){
				throw new DatahubException("field name cannot be null, index="+i);
			}
		}
		
		String ddl = "CREATE TABLE %s (\n%s\n)";
		String pk  = "PRIMARY KEY (%s)";

		String blank 	= " ";
		String colSep 	= ", ";
		String lineSep 	= ", \n";
		
		StringBuffer pks	= new StringBuffer(0);
		StringBuffer cols	= new StringBuffer(0);
		
		for(int i=0; i<columns.size(); i++){
			// name
			cols.append(quotObject(columns.get(i).getName()));
			cols.append(blank);
			// type
			String type = javaTypeToDbType(columns.get(i).getDataType(), columns.get(i));
			cols.append(type);
			// is null
			cols.append(blank);
			
			if (ColumnMeta.NO.equals(columns.get(i).getIsNullable())){
				cols.append(" NOT NULL ");
			}
			// 注释
			if(columns.get(i).getRemarks()!=null){
				cols.append(" COMMENT '");
				cols.append(columns.get(i).getRemarks()).append("'");
			}
			// PK
			if(columns.get(i).isPk()){
				pks.append(quotObject(columns.get(i).getName()));
				pks.append(colSep);
			}
			cols.append(lineSep);
		}
		
		if(pks.length() != 0){
			pk = String.format(pk, pks.substring(0, pks.length()-colSep.length()));
			cols.append(pk).append(lineSep);
		}
		
		ddl = String.format(ddl, 
				getFullTableName(table), 
				cols.substring(0, cols.length()-lineSep.length()));
		
		return ddl;
	}

	@Override
	public String javaTypeToDbType(int javaType, ColumnMeta column) throws DatahubException {
		String type = javaTypeToDbTypeMap.get(Integer.valueOf(javaType));
		if( StringUtils.isBlank(type)){
			type = javaTypeToDbTypeMap.get(Integer.valueOf(Types.LONGVARCHAR));
			javaType = Types.LONGVARCHAR;
		}
		
		switch (javaType) {
		case Types.DECIMAL:
		case Types.DOUBLE:
		case Types.NUMERIC:
			if(column.getDecimalDigits()>0 && column.getDecimalDigits() <= column.getColumnSize()){
				type = type + String.format("(%s, %s)", column.getColumnSize(), column.getDecimalDigits());
			}
			else if(column.getColumnSize()>0){
				type = type + String.format("(%s, 0)", column.getColumnSize());
			}
			else{
				type = type + String.format("(38, 10)");
			}
			break;
		case Types.BIGINT:
		case Types.BIT:
		case Types.INTEGER:
		case Types.SMALLINT:
		case Types.TINYINT:
			type = type + String.format("(%s)", column.getColumnSize());
			break;
		case Types.CHAR:
		case Types.NCHAR:
		case Types.BOOLEAN:
		case Types.VARCHAR:
		case Types.NVARCHAR:
			type = type + String.format("(%s)", column.getColumnSize());
			if(column.getColumnSize() > 255){
				type = javaTypeToDbTypeMap.get(Integer.valueOf(Types.LONGVARCHAR));
			}
			break;
		case Types.OTHER:
		default:
			mapOracleType(javaType, column);
			break;
		}
		return type;
	}
	
	private String mapOracleType(int javaType, ColumnMeta column)
			throws DatahubException {
		String type = javaTypeToDbTypeMap.get(Integer.valueOf(javaType));
		String typeName = column.getTypeName();

		// 处理oracle中的timestamp,timestamp TZ,
		// timestamp LTZ三种日期格式，统一当成timestamp
		if (typeName.startsWith("TIMESTAMP")) {
			typeName = "TIMESTAMP";
		}

		switch (typeName) {
		// 对于无法识别N开头的类型处理
		case "NVARCHAR2":
			type = String.format("VARCHAR(%s)", column.getColumnSize());
			if (column.getColumnSize() > 255) {
				type = javaTypeToDbTypeMap.get(Integer
						.valueOf(Types.LONGVARCHAR));
			}
			break;
		case "NCHAR":
			type = String.format("CHAR(%s)", column.getColumnSize());
			if (column.getColumnSize() > 255) {
				type = javaTypeToDbTypeMap.get(Integer
						.valueOf(Types.LONGVARCHAR));
			}
			break;
		case "NCLOB":
		case "ROWID":
			type = "TEXT";
			break;
		// oracle中对于timestamp，timestampTZ，timestampLTZ，统一处理成mysql中的datetime类型
		case "TIMESTAMP":
			type = "DATETIME";
			break;
		default:
			break;
		}

		return type;
	}
	
	/**java.sql.Types映射为数据库类型**/
	private static Map<Integer, String> javaTypeToDbTypeMap = new HashMap<Integer, String>();
	static {

		javaTypeToDbTypeMap.put(Integer.valueOf(Types.ARRAY), "BINARY");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BIGINT), "BIGINT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BINARY), "BINARY");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BIT), "BIT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BLOB), "LONGBLOB");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BOOLEAN), "VARCHAR");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.CHAR), "CHAR");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.CLOB), "TEXT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DATALINK), "TEXT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DATE), "DATE");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DECIMAL), "DECIMAL");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DISTINCT), "TEXT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DOUBLE), "DOUBLE");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.FLOAT), "FLOAT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.INTEGER), "INT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.JAVA_OBJECT), "BINARY");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.LONGNVARCHAR), "TEXT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.LONGVARBINARY), "LONGBLOB");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.LONGVARCHAR), "TEXT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NCHAR), "CHAR");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NCLOB), "TEXT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NULL), "BINARY");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NUMERIC), "NUMERIC");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NVARCHAR), "VARCHAR");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.OTHER), "BINARY");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.REAL), "DOUBLE");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.REF), "BINARY");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.REF_CURSOR), "BINARY");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.ROWID), "TEXT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.SMALLINT), "SMALLINT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.SQLXML), "TEXT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.STRUCT), "TEXT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TIME), "TIME");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TIME_WITH_TIMEZONE), "TIME");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TIMESTAMP), "DATETIME");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TIMESTAMP_WITH_TIMEZONE), "DATETIME");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TINYINT), "TINYINT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.VARBINARY), "BINARY");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.VARCHAR), "VARCHAR");
	}
	
	@Override
	public String getFullTableName(TableMeta table){
		
		StringBuffer name = new StringBuffer(0);
		if(table != null && StringUtils.isNotBlank(table.getName())){
			
			if(StringUtils.isNotBlank(table.getCatalog())){
				name.append(quotObject(table.getCatalog()));
				name.append(".");
			}
			
			name.append(quotObject(table.getName()));
		}
		
		return name.toString();
	}

	@Override
	public String showCreateTable(TableMeta table, 
			Connection conn) throws DatahubException {
		
		if(table == null || conn == null){
			return "";
		}
		table.setCatalog(StringUtils.isBlank(table.getCatalog())?null:table.getCatalog());
		table.setSchema(StringUtils.isBlank(table.getSchema())?null:table.getSchema());
		
		String showSql = "SHOW CREATE TABLE %s";
		showSql = String.format(showSql, getFullTableName(table));
		GridData data = this.executeQuery(conn, showSql);
		List<RowData> rows = data.getRows();
		String res = StringUtils.EMPTY;
		if(rows.size()>0 && rows.get(0).getFields().size()>0){
			res = String.valueOf(rows.get(0).getFields().get(1).getValue());
		}
		return res;
	}
	
	@Override
	public String quotObject(String objName) {
		if(StringUtils.isNotBlank(objName)){
			objName = "`" + objName + "`";
		}
		return objName;
	}
	
	@Override
	public FieldData formatFieldData(Connection conn, ColumnMeta column, FieldData field) throws SQLException {
		super.formatFieldData(conn, column, field);
		if(field == null){
			return null;
		}
		switch (column.getDataType()) {
		case Types.DATE:
			// year
			if(column.getColumnSize() == 4 && field.getValue()  instanceof Date){
				field.setValue(DateUtil.format((Date)field.getValue(), CommConst.yyyy));
				field.setFormat(CommConst.yyyy);
			}
			break;
		default:
			break;
		}
		return field;
	}

	@Override
	public Throwable connectionException(Throwable ex) {
		super.connectionException(ex);
		String err = ex.getMessage();
		if(StringUtils.containsIgnoreCase(err, "Too many connections")){
			return new TooManyConnectionException(ex);
		}
		
		if(StringUtils.containsIgnoreCase(err, "Read timed out")
				|| StringUtils.containsIgnoreCase(err, "Connection timed out")
				|| StringUtils.containsIgnoreCase(err, "Connection reset")){
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
		String TA = "TA"+System.currentTimeMillis();
		String union = "UNION\n";
		
		String fullName = this.getFullTableName(table);
		String colName  = this.quotObject(partColumn.getName());
		
		partSql.append("SELECT ");
		partSql.append("MIN(").append(colName).append(") miv, ");
		partSql.append("MAX(").append(colName).append(") mav ");
		partSql.append("FROM (");
		
		// base sql 必须为未分页过的SQL
		partSql.append("SELECT ").append(colName);
		partSql.append(" FROM ").append(fullName).append(" ");
		partSql.append(where==null?"":where.trim());
		partSql.append(" ORDER BY ").append(colName).append(" LIMIT {0}, {1} )").append(TA);
		
		StringBuffer unionSql = new StringBuffer(0);
		for(int ponit =2; ponit<=blockCnt;  ponit+=2){
			long start = (ponit-1) * pageSize;
			unionSql.append(FunUtil.formatArgs(partSql.toString(), start, pageSize));
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

	@Override
	public String concatFieldName(List<ColumnMeta> columns) {
		StringBuffer all = new StringBuffer("*");
		String colSep = ", ";
		if(columns == null || columns.isEmpty()){
			return all.toString();
		}
		all.setLength(0);
		for(ColumnMeta col :columns){
			String colName 	= col.getName();
			String type 	= col.getTypeName().toLowerCase();
			switch (type) {
			case "linestring":
			case "multilinestring":
			case "point":
			case "multipoint":
			case "polygon":
			case "multipolygon":
			case "geometry":
			case "geometrycollection":
				colName = String.format("AsText(%s) AS %s", quotObject(colName), quotObject(colName));
				break;

			default:
				colName = quotObject(colName);
				break;
			}
			all.append(colName).append(colSep);
		}
		
		return all.substring(0, all.length()-colSep.length());
	}

	/**
	 * 获取同步插入的sql，实现表中有数据就更新，无则插入</br>
	 * 
	 * INSERT INTO T_USER(userid,name,sex) 
	 * SELECT * FROM (SELECT ? AS `userid`, ? AS `name`, ? AS `sex`) AS T  
 	 * ON DUPLICATE KEY UPDATE `name`=T.`name`,`sex`=T.`usersex`;
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
	public String getUpdateInsert(List<ColumnMeta> cols, String fullTableName)throws DatahubException {
		
		String sql = "INSERT INTO %s (%s) SELECT * FROM (SELECT %s) AS %s ON DUPLICATE KEY UPDATE %s ";
		
		StringBuffer fields  = new StringBuffer();
		StringBuffer tmpTb   = new StringBuffer();
		StringBuffer updates = new StringBuffer();
		String sep 			 = ", ";
		String tbAlias		 = " T" + System.currentTimeMillis();
		
		for(ColumnMeta col : cols){
			fields.append(quotObject(col.getName()));
			fields.append(sep);
			tmpTb.append("?");
			tmpTb.append(" AS ");
			tmpTb.append(quotObject(col.getName()));
			tmpTb.append(sep);
			
			if(!col.isPk()){
				updates.append(quotObject(col.getName()));
				updates.append("=");
				updates.append(tbAlias);
				updates.append(".");
				updates.append(quotObject(col.getName()));
				updates.append(sep);
			}
		}
		sql = String.format(sql, fullTableName,
				fields.substring(0, fields.length()- sep.length()),
				tmpTb.substring(0, tmpTb.length()- sep.length()),tbAlias,
				updates.substring(0, updates.length()- sep.length()));
		
		return sql;
	}
	
	@Override
	public void prepareStatementByCursor(PreparedStatement pstmt) throws SQLException {
		if(pstmt==null){
			return;
		}
		pstmt.setFetchSize(1);
		pstmt.setFetchDirection(ResultSet.FETCH_REVERSE);
	}
	
	@Override
	public boolean checkAvailable() throws DatahubException {
		checkUrl("jdbc:mysql");
		return true;
	}

}
