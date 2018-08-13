package org.shersfy.datahub.dbexecutor.connector.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.shersfy.datahub.commons.constant.CommConst;
import org.shersfy.datahub.commons.exception.DatahubException;
import org.shersfy.datahub.commons.meta.ColumnMeta;
import org.shersfy.datahub.commons.meta.DBAccessType;
import org.shersfy.datahub.commons.meta.DBMeta;
import org.shersfy.datahub.commons.meta.GridData;
import org.shersfy.datahub.commons.meta.TableMeta;
import org.shersfy.datahub.commons.utils.FunUtil;
import org.springframework.beans.BeanUtils;

public class DB2Connector extends DbConnectorInterface {

	@Override
	public String loadDriverClass(DBAccessType type) {

		String driver = "com.ibm.db2.jcc.DB2Driver";
		switch (type) {
		case ODBC:
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
			long pageSize)  throws DatahubException{

		Connection conn = null;
		String sql      = null;
		try {
			// 1.查询字段，排除分页的RN字段列
			sql = FunUtil.formatArgs(makeSql(baseSql, 1, 0), "*");
			conn = this.connection();
			GridData data = super.executeQuery(conn, sql);
			List<ColumnMeta> header = data.getHeaders();
			// 2.生成真实字段的SQL语句
			if (header.size() > 1) {
				StringBuffer fields = new StringBuffer(0);
				for (int i = 1; i < header.size(); i++) {
					fields.append(quotObject(header.get(i).getName()));
					fields.append(", ");
				}
				sql = FunUtil.formatArgs(makeSql(baseSql, pageNo, pageSize),
						fields.subSequence(0, fields.length() - 2));
			} else {
				sql = FunUtil.formatArgs(makeSql(baseSql, pageNo, pageSize), "*");
			}

			return sql;
		} catch (DatahubException de) {
			throw de;
		} catch (Throwable ex) {
		    DatahubException de = null;
		    if(ex instanceof DatahubException) {
		        de = (DatahubException)ex;
		    } else {
		        de = new DatahubException("query record error:"+sql, ex);;
		    }
			throw de;
		} finally {
			close(conn);
		}
	}

	private String makeSql(String baseSql, long pageNo,
			long pageSize) {

		if (StringUtils.isBlank(baseSql)) {
			return null;
		}
		if (pageNo < 1 || pageSize < 0) {
			return baseSql;
		}

		long start = (pageNo - 1) * pageSize + 1;// RN从1开始
		long end = start + pageSize;
		String aliasA = "TA" + System.currentTimeMillis();
		String aliasB = "TB" + System.currentTimeMillis();
		String RN = "RN" + System.currentTimeMillis();
		
		StringBuffer sql = new StringBuffer(0);
		sql.append("SELECT {0} FROM (");
		sql.append("SELECT ROW_NUMBER() OVER() AS " + RN + ", ");
		sql.append(aliasA);
		sql.append(".*");
		super.appendBaseSql(sql, baseSql);
		sql.append(aliasA);
		sql.append(" ) ");
		sql.append(aliasB);
		sql.append(" WHERE ").append(RN).append(" >= ").append(start);
		sql.append(" AND ").append(RN).append(" < ").append(end);

		return sql.toString();
	}

	@Override
	public void setDbMeta(DBMeta dbMeta) {
		String schema = dbMeta.getParams().getProperty("currentSchema");
		dbMeta.setSchema(schema);
		if(StringUtils.isBlank(schema)){
			dbMeta.setSchema(dbMeta.getUserName().toUpperCase());
		}
		if(StringUtils.isBlank(dbMeta.getUrl())){
			dbMeta.setUrl(String.format("jdbc:db2://%s:%s/%s", dbMeta.getHost(),
					dbMeta.getPort(), dbMeta.getDbName()));
		}
		this.dbMeta = dbMeta;
	}

	@Override
	public List<DBMeta> getDatabases(Connection conn) throws DatahubException {
		List<DBMeta> dbs = new ArrayList<DBMeta>();
		ResultSet rs = null;
		try {
			if(conn == null){
				throw new SQLException("connection is null");
			}
			rs = conn.getMetaData().getSchemas();

			while (rs.next()) {
				DBMeta meta = new DBMeta();
				BeanUtils.copyProperties(meta, getDbMeta());
				meta.setName(rs.getString("TABLE_SCHEM"));
				meta.setCatalog(rs.getString("TABLE_CATALOG"));
				meta.setSchema(meta.getName());
				//meta.setDbName(rs.getString("TABLE_CAT"));
				// meta.setDbName(rs.getString("TABLE_CAT"));
				String currentSchema = getDbMeta().getParams().getProperty("currentSchema");
				if(StringUtils.isBlank(currentSchema)){
					// 未指定当前Schema
					dbs.add(meta);
				}
				else if(meta.getSchema().equals(currentSchema)){
					// 和指定当前Schema相等
					dbs.add(meta);
				}
			}
		} catch (Exception e) {
			throw new DatahubException("list databses error", e);
		} finally {
			close(rs);
		}
//		 DBMeta db = getDbMeta();
//		 db.setName(db.getSchema());
//		 dbs.add(db);
		return dbs;
	}


	@Override
	public String javaTypeToDbType(int javaType, ColumnMeta column)
			throws DatahubException {
		String type = javaTypeToDbTypeMap.get(Integer.valueOf(javaType));
		if( StringUtils.isBlank(type)){
			type = javaTypeToDbTypeMap.get(Integer.valueOf(Types.LONGVARCHAR));
			javaType = Types.LONGVARCHAR;
		}
		
		switch (javaType) {
		case Types.BIGINT:
		case Types.BIT:
		case Types.DECIMAL:
		case Types.NUMERIC:
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
		case Types.ROWID:
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

	/** java.sql.Types映射为数据库类型 **/
	private static Map<Integer, String> javaTypeToDbTypeMap = new HashMap<Integer, String>();
	static {

		javaTypeToDbTypeMap.put(Integer.valueOf(Types.ARRAY), "BINARY_FLOAT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BIGINT), "NUMERIC");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BINARY), "BINARY_FLOAT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BIT), "NUMERIC");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BLOB), "BLOB");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BOOLEAN), "VARCHAR");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.CHAR), "CHAR");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.CLOB), "CLOB");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DECIMAL), "DECIMAL");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DATE), "DATE");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DATALINK), "VARCHAR");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DISTINCT), "VARCHAR");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DOUBLE), "DOUBLE PRECISION");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.FLOAT), "FLOAT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.INTEGER), "INTEGER");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.JAVA_OBJECT), "BINARY_FLOAT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.LONGVARBINARY), "BINARY_DOUBLE");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.LONGNVARCHAR), "LONG VARCHAR");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.LONGVARCHAR), "LONG VARCHAR");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NCHAR), "NCHAR");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NCLOB), "NCLOB");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NULL), "BINARY_FLOAT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NUMERIC), "NUMERIC");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NVARCHAR), "NVARCHAR2");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.OTHER), "BINARY_FLOAT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.REAL), "REAL");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.REF), "BINARY_FLOAT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.REF_CURSOR), "BINARY_FLOAT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.ROWID), "VARCHAR");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.SMALLINT), "SMALLINT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.SQLXML), "LONG VARCHAR");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.STRUCT), "LONG VARCHAR");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TIME), "DATE");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TIME_WITH_TIMEZONE), "DATE");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TIMESTAMP), "TIMESTAMP");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TIMESTAMP_WITH_TIMEZONE), "TIMESTAMP");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TINYINT), "SMALLINT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.VARBINARY), "BINARY_FLOAT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.VARCHAR), "VARCHAR");
		
	}

	@Override
	public String showCreateTable(TableMeta table, Connection conn) throws DatahubException {
		// 读取列
		if(table == null || conn == null){
			return "";
		}
		table.setCatalog(StringUtils.isBlank(table.getCatalog())?null:table.getCatalog());
		table.setSchema(StringUtils.isBlank(table.getSchema())?null:table.getSchema());
		
		List<ColumnMeta> cols = getColumns(table, conn);
		if(cols.isEmpty()){
			return StringUtils.EMPTY;
		}
		
		return getDDLByTable(table, cols, conn);
//		String showSql = "SELECT column_name, data_type,is_nullable, character_maximum_length, numeric_precision "
//				+ "FROM sysibm.columns WHERE %s table_name = '%s'";
//		
//		String schema = StringUtils.EMPTY;
//		if(StringUtils.isNotBlank(table.getSchema())){
//			schema = "table_schema='" + table.getSchema() + "' AND ";
//		}
//		showSql = String.format(showSql, schema, table.getName());
//		
//		Connection conn = null;
//		Statement st 	= null;
//		ResultSet rs 	= null;
//		
//		String ddl	= "CREATE TABLE %s (\n%s\n)";
//		String pk	= "PRIMARY KEY (%s)";
//		
//		StringBuffer cols = new StringBuffer(0);
//		StringBuffer pks = new StringBuffer(0);
//
//		// 查询字段列表
//		try {
//			
//			conn = this.connection();
//			st = conn.createStatement();
//			rs = st.executeQuery(showSql);
//			
//			while (rs.next()) {
//				
//				String colName = rs.getString(1);
//				String type = rs.getString(2);
//				String len = rs.getString(4);
//				String prec = rs.getString(5);
//				
//				if (type.equalsIgnoreCase("NUMERIC")
//						|| type.equalsIgnoreCase("DECIMAL")) {
//					
//					String col = "\"" + colName + "\" " + type + " (" + len
//							+ "," + prec + ") ";
//					
//					cols.append(col);
//				} else if (type.toUpperCase().contains("DATE")
//						|| type.toUpperCase().contains("TIMESTAMP")
//						|| type.toUpperCase().contains("SMALLINT")
//						|| type.toUpperCase().contains("REAL")
//						|| type.toUpperCase().contains("DOUBLE")
//						|| type.toUpperCase().contains("CHAR")
//						|| type.toUpperCase().contains("BLOB")
//						|| type.toUpperCase().contains("INTEGER")
//						|| type.toUpperCase().contains("LONG VARCHAR")
//						|| type.toUpperCase().contains("BIGINT")
//						|| type.toUpperCase().contains("TIME")) {
//					String col = "\"" + colName + "\" " + type + " ";
//					cols.append(col);
//				} else {
//					String col = "\"" + colName + "\" " + type + " (" + len
//							+ ") ";
//					cols.append(col);
//				}
//
//				if (rs.getBoolean(3)) {
//					cols.append("NOT NULL");
//				}
//				cols.append(",\n");
//			}
//
//			// 查询主键列表
//			showSql = "SELECT a.tabname, b.colname  FROM syscat.tabconst a ,"
//					+ "syscat.keycoluse b WHERE a.constname = b.constname AND "
//					+ "a.TYPE='P' AND a.tabname = '%s'";
//			showSql = String.format(showSql, table.getName());
//			rs.close();
//			rs = st.executeQuery(showSql);
//			while (rs.next()) {
//				pks.append("\"").append(rs.getString(2)).append("\",");
//			}
//			if (pks.length() != 0) {
//				pk = String.format(pk,
//						pks.substring(0, pks.length() - ",".length()));
//				cols.append(pk).append("\n");
//			}
//
//			ddl = String.format(ddl,
//					getFullTableName(table)
//					, cols.substring(0, cols.length() - ",".length()));
//			return ddl;
//		} catch (Exception e) {
//			logger.info("查询表失败", e);
//			return null;
//		} finally {
//			try {
//				rs.close();
//				st.close();
//				conn.close();
//			} catch (SQLException e) {
//				logger.info("关闭连接失败", e);
//				return null;
//			}
//		}
//
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
		String union = "UNION\n";
		
		String fullName = this.getFullTableName(table);
		String colName  = this.quotObject(partColumn.getName());
		
		// SELECT min("name"), max("name") FROM ( SELECT ROW_NUMBER() OVER() AS  RN, "name" FROM  
		// (select * from "city_copy" where "name" is not null  order by "name" ) TA ) WHERE RN between 90000 and 110985
		StringBuffer baseSql = new StringBuffer(0);
		baseSql.append("SELECT ").append(colName);
		baseSql.append(" FROM ").append(fullName).append(" ");
		baseSql.append(where==null?"":where.trim());
		baseSql.append(" ORDER BY ").append(colName);
		
		partSql.append("SELECT ");
		partSql.append("MIN(").append(colName).append(") miv, ");
		partSql.append("MAX(").append(colName).append(") mav ");
		partSql.append("FROM (");
		
		partSql.append("SELECT ROW_NUMBER() OVER() AS ").append(RN).append(", ");
		partSql.append(colName);
		partSql.append(" FROM (").append(baseSql).append(") ").append(TA);
		
		partSql.append(") ");
		partSql.append(" WHERE ").append(RN).append(" BETWEEN {0} AND {1} ");

		StringBuffer unionSql = new StringBuffer(0);
		for(int ponit =2; ponit<=blockCnt;  ponit+=2){
			long start = (ponit - 1) * pageSize + 1;// RN从1开始
			long end = start + pageSize;
			
			unionSql.append(FunUtil.formatArgs(partSql.toString(), start, end));
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
	public boolean checkAvailable() throws DatahubException {
		checkUrl("jdbc:db2");
		return true;
	}

}
