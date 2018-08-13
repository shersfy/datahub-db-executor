package org.shersfy.datahub.dbexecutor.connector.db;

import java.sql.Connection;
import java.sql.ResultSet;
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
import org.shersfy.datahub.commons.meta.ColumnMeta;
import org.shersfy.datahub.commons.meta.DBAccessType;
import org.shersfy.datahub.commons.meta.DBMeta;
import org.shersfy.datahub.commons.meta.GridData;
import org.shersfy.datahub.commons.meta.TableMeta;
import org.shersfy.datahub.commons.utils.FunUtil;

/**
 * MS SQL Server (Native)数据库连接器
 * 
 */
public class MSSQLNativeConnector extends DbConnectorInterface {

	@Override
	public String loadDriverClass(DBAccessType type){

		String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
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
	public String queryByPage(String baseSql, long pageNo, long pageSize)
	    throws DatahubException {
		// SQL server第一个字段要求不能为null, 且可排序
		if (StringUtils.isBlank(baseSql)) {
			return null;
		}
		if (pageNo < 1 || pageSize < 0) {
			return baseSql;
		}
		
		Connection conn = null;
		try {
			
			String aliasA = "T" + System.currentTimeMillis();
			String cols	  = " * ";
			
			StringBuffer sql = new StringBuffer(0);
			sql.append("SELECT TOP 0 ");
			sql.append(cols);
			super.appendBaseSql(sql, baseSql);
			sql.append(aliasA);

			conn = this.connection();
			GridData data = this.executeQuery(conn, sql.toString());
			List<ColumnMeta> header = data.getHeaders();
			if (header.size() < 1) {
				return baseSql;
			}
			ColumnMeta firstField = header.get(0);
			// 排除第一个字段不支持类型
			if("text".equalsIgnoreCase(firstField.getTypeName())
					|| "ntext".equalsIgnoreCase(firstField.getTypeName())
					|| "image".equalsIgnoreCase(firstField.getTypeName())
					|| "geography".equalsIgnoreCase(firstField.getTypeName())
					|| "geometry".equalsIgnoreCase(firstField.getTypeName())
					|| "xml".equalsIgnoreCase(firstField.getTypeName())){
				String err = "name=%s, type=%s\nfirst field not support types are text, ntext, image, geography, geometry, xml.";
				err = String.format(err, firstField.getName(), firstField.getTypeName());
				throw new DatahubException(err);
			}
			String first = header.get(0).getName();
			first = quotObject(first);
			sql.setLength(0);
			sql.append("SELECT TOP ");
			sql.append(pageSize);
			sql.append(cols);
			super.appendBaseSql(sql, baseSql);
			sql.append(aliasA);
			sql.append(" WHERE ");
			sql.append(first);
			sql.append(" NOT IN ");
			sql.append("(SELECT TOP ");
			sql.append(pageSize * (pageNo - 1));
			sql.append(" " + first);
			super.appendBaseSql(sql, baseSql);
			sql.append(aliasA);
			sql.append(" ORDER BY ");
			sql.append(first);
			sql.append(") ");
			sql.append(" ORDER BY ");
			sql.append(first);
			return sql.toString();
			
		} catch (Exception ex) {
		    throw DatahubException.throwDatahubException("execute query count error:"+baseSql, ex);
		} finally {
			close(conn);
		}

	}
	
	@Override
	public void setDbMeta(DBMeta dbMeta) {
		dbMeta.setCatalog(dbMeta.getDbName());

		Properties params = dbMeta.getParams();
		if (StringUtils.isBlank(params.getProperty("loginTimeout"))) {
			params.put("loginTimeout", String.valueOf(TIMEOUT / 1000));
		}
		dbMeta.setParams(params);
		if(StringUtils.isBlank(dbMeta.getUrl())){
			dbMeta.setUrl(String.format("jdbc:sqlserver://%s:%s;DatabaseName=%s",
					dbMeta.getHost(), dbMeta.getPort(), dbMeta.getDbName()));
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
				meta.setSchema(rs.getString("TABLE_SCHEM"));
				meta.setDbName(meta.getName());
				if("information_schema".equalsIgnoreCase(meta.getName())
						|| "db_accessadmin".equalsIgnoreCase(meta.getName())
						|| "db_backupoperator".equalsIgnoreCase(meta.getName())
						|| "db_datareader".equalsIgnoreCase(meta.getName())
						|| "db_datawriter".equalsIgnoreCase(meta.getName())
						|| "db_ddladmin".equalsIgnoreCase(meta.getName())
						|| "db_denydatareader".equalsIgnoreCase(meta.getName())
						|| "db_denydatawriter".equalsIgnoreCase(meta.getName())
						|| "db_owner".equalsIgnoreCase(meta.getName())
						|| "db_securityadmin".equalsIgnoreCase(meta.getName())
						|| "sys".equalsIgnoreCase(meta.getName())){
					continue;
				}
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
	public String javaTypeToDbType(int javaType, ColumnMeta column)
			throws DatahubException {
		
		String type = javaTypeToDbTypeMap.get(Integer.valueOf(javaType));
		if( StringUtils.isBlank(type)){
			type = javaTypeToDbTypeMap.get(Integer.valueOf(Types.LONGVARCHAR));
			javaType = Types.LONGVARCHAR;
		}
		
		switch (javaType) {
		case Types.DECIMAL:
		case Types.NUMERIC:
			if(column.getDecimalDigits()>0 && column.getDecimalDigits() <= column.getColumnSize()){
				type = type + String.format("(%s, %s)", column.getColumnSize(), column.getDecimalDigits());
			}
			else if(column.getColumnSize()>0){
				type = type + String.format("(%s, 0)", column.getColumnSize());
			}
			else{
				type = type + String.format("(1, 0)");
			}
			break;
		case Types.CHAR:
		case Types.NCHAR:
			type = type + String.format("(%s)", column.getColumnSize());
			break;
		case Types.BOOLEAN:
		case Types.VARCHAR:
		case Types.NVARCHAR:
			type = type + String.format("(%s)", column.getColumnSize());
			if(column.getColumnSize() > 8000){
				type = type + String.format("(MAX)");
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

		javaTypeToDbTypeMap.put(Integer.valueOf(Types.ARRAY), "BINARY");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BIGINT), "BIGINT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BINARY), "BINARY");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BIT), "BIT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BLOB), "BINARY");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BOOLEAN), "VARCHAR");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.CHAR), "CHAR");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.CLOB), "BINARY");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DATALINK), "TEXT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DISTINCT), "TEXT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DECIMAL), "DECIMAL");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DATE), "DATE");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DOUBLE), "FLOAT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.FLOAT), "FLOAT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.INTEGER), "INT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.JAVA_OBJECT), "BINARY");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.LONGNVARCHAR), "TEXT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.LONGVARBINARY),"VARBINARY");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.LONGVARCHAR), "TEXT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NCHAR), "NCHAR");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NUMERIC), "NUMERIC");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NVARCHAR), "NVARCHAR");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NCLOB), "VARBINARY");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NULL), "BINARY");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.OTHER), "BINARY");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.REAL), "REAL");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.REF), "BINARY");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.REF_CURSOR), "BINARY");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.SMALLINT), "SMALLINT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.ROWID), "TEXT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.SQLXML), "XML");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.STRUCT), "TEXT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TIME), "TIME");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TIME_WITH_TIMEZONE), "TIME");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TIMESTAMP), "DATETIME");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TIMESTAMP_WITH_TIMEZONE), "DATETIME");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TINYINT), "TINYINT");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.VARBINARY), "VARBINARY");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.VARCHAR), "VARCHAR");
		
	}

	@Override
	public String showCreateTable(TableMeta table, Connection conn) throws DatahubException {

		if(table == null || conn == null){
			return "";
		}
		table.setCatalog(StringUtils.isBlank(table.getCatalog())?null:table.getCatalog());
		table.setSchema(StringUtils.isBlank(table.getSchema())?null:table.getSchema());
		
		String showSql 	= "SP_HELP %s";
		String ddl 		= "CREATE TABLE %s (\n%s )";
		String pk 		= "PRIMARY KEY (%s)";
		String uk 		= "UNIQUE (%s)";

		StringBuffer name = new StringBuffer(0);
		if(StringUtils.isNotBlank(table.getSchema())){
			name.append(table.getSchema());
			name.append(".");
		}
		name.append(table.getName());
		
		showSql = String.format(showSql, quotObject(name.toString()));

		String blank 	= " ";
		String colSep 	= ", ";
		String lineSep 	= ", \n";

		Statement st = null;
		ResultSet rs = null;
		try {
			
			st = conn.createStatement();
			boolean flg = st.execute(showSql);

			StringBuffer pks = new StringBuffer(0);
			StringBuffer uks = new StringBuffer(0);
			StringBuffer cols = new StringBuffer(0);

			int index = 1;
			if (flg) {
				while (st.getMoreResults()) {
					index++;
					// 字段列表信息
					if (index == 2) {
						rs = st.getResultSet();
						while (rs.next()) {

							String colName 	= StringUtils.EMPTY;
							String type 	= StringUtils.EMPTY;
							String collation= StringUtils.EMPTY;

							long len	= 0;
							long prec 	= 0;
							long scale 	= 0;
							boolean isNull = false;

							// 列名
							colName = rs.getString(1).trim();
							// 数据类型 
							type = rs.getString(2).trim();
							// 长度
							len = StringUtils.isBlank(rs.getString(4))?0:Long.valueOf(rs.getString(4).trim());
							// 精度
							prec = StringUtils.isBlank(rs.getString(5))?0:Long.valueOf(rs.getString(5).trim());
							// scale小数点位数
							scale = StringUtils.isBlank(rs.getString(6))?0:Long.valueOf(rs.getString(6).trim());
							// nullable
							if (ColumnMeta.YES.equalsIgnoreCase(rs.getString(7).trim())) {
								isNull = true;
							}
							else{
								isNull = false;
							}
							// 字符集
							collation = rs.getString(10);
							if(StringUtils.isBlank(collation)){
								collation = StringUtils.EMPTY;
							}
							// 拼装字段
							// 指定两位参数
							if (type.equalsIgnoreCase("NUMERIC")
									|| type.equalsIgnoreCase("DECIMAL")) {

								cols.append(quotObject(colName));
								cols.append(blank);
								cols.append(type);
								cols.append(String.format("(%s, %s)", prec, scale));
								cols.append(blank);

							} 
							// 指定1位参数
							else if (type.equalsIgnoreCase("CHAR")
									|| type.equalsIgnoreCase("NCHAR")
									|| type.equalsIgnoreCase("VARCHAR")
									|| type.equalsIgnoreCase("NVARCHAR")) {

								cols.append(quotObject(colName));
								cols.append(blank);
								cols.append(type);
								cols.append(String.format("(%s)", len>8000?"MAX":len));
								cols.append(blank);

							}
							// 其它原样显示
							else {
								cols.append(quotObject(colName));
								cols.append(blank);
								cols.append(type);
								cols.append(blank);
							}
							// COLLATE
							cols.append(collation.trim());
							// nullable
							if(!isNull){
								cols.append(" NOT NULL ");
							}
							else{
								cols.append(" NULL ");
							}
							cols.append(lineSep);
						}
					} 
					// 约束列表信息
					else if (index == 6) {
						rs = st.getResultSet();
						while (rs.next()) {
							
							String indexDesc = rs.getString(2).trim();
							String indexKeys = rs.getString(3).trim();
							if(indexDesc.contains("primary key located on PRIMARY")){
								String[] keys = indexKeys.split(",");
								for(String key :keys){
									pks.append(quotObject(key.trim()));
									pks.append(colSep);
								}
							}
							else if(indexDesc.contains("unique located on PRIMARY")){
								String[] keys = indexKeys.split(",");
								for(String key :keys){
									uks.append(quotObject(key.trim()));
									uks.append(colSep);
								}
							}
						}
						break;
					} 
					
					else {
						continue;
					}

				}
			}

			if (pks.length() != 0) {
				pk = String.format(pk,
						pks.substring(0, pks.length() - colSep.length()));
				cols.append(pk).append(lineSep);
			}
			
			if (uks.length() != 0) {
				uk = String.format(uk,
						uks.substring(0, uks.length() - colSep.length()));
				cols.append(uk).append(lineSep);
			}

			ddl = String.format(ddl, 
					getFullTableName(table), 
					cols.substring(0, cols.length()-lineSep.length()));

		} catch (Exception ex) {
			throw DatahubException.throwDatahubException("show table DDL error: "+showSql, ex);
		} finally {
			close(rs, st);
		}
		
		return ddl;
	}

	@Override
	public String quotObject(String objName) {
		if(StringUtils.isNotBlank(objName)){
			objName = "[" + objName + "]";
		}
		return objName;
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
			String colName = col.getName();
			if("sql_variant".equalsIgnoreCase(col.getTypeName())
					|| "geography".equalsIgnoreCase(col.getTypeName())
					|| "geometry".equalsIgnoreCase(col.getTypeName())){
				colName = String.format("CONVERT(VARCHAR(8000), %s) AS %s", quotObject(colName), quotObject(colName));
			}
			else{
				colName = quotObject(colName);
			}
			all.append(colName).append(colSep);
		}
		
		return all.substring(0, all.length()-colSep.length());
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
		
		partSql.append("SELECT TOP {0} ").append(colName);
		partSql.append(" FROM ").append(fullName).append(" ");
		if(StringUtils.isBlank(where)){
			partSql.append(" WHERE ").append(colName).append(" NOT IN (");
		} else{
			partSql.append(where.trim());
			partSql.append(" AND ").append(colName).append(" NOT IN (");
		}
		
		partSql.append("SELECT TOP {1} ").append(colName);
		partSql.append(" FROM ").append(fullName).append(" ");
		partSql.append(where==null?"":where.trim());
		partSql.append(" ORDER BY ").append(colName);
		partSql.append(" ) ");
		
		partSql.append("ORDER BY ").append(colName);
		partSql.append(") ").append(TA);
		
		StringBuffer unionSql = new StringBuffer(0);
		for(int ponit =2; ponit<=blockCnt;  ponit+=2){
			long end   = pageSize * (ponit - 1);
			unionSql.append(FunUtil.formatArgs(partSql.toString(), pageSize, end));
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
		checkUrl("jdbc:sqlserver");
		return true;
	}

}
