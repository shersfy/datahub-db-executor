package org.shersfy.datahub.dbexecutor.connector.db;



import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.shersfy.datahub.commons.constant.CommConst;
import org.shersfy.datahub.commons.exception.DatahubException;
import org.shersfy.datahub.commons.exception.NetException;
import org.shersfy.datahub.commons.meta.ColumnMeta;
import org.shersfy.datahub.commons.meta.DBAccessType;
import org.shersfy.datahub.commons.meta.DBMeta;
import org.shersfy.datahub.commons.meta.FieldData;
import org.shersfy.datahub.commons.meta.GridData;
import org.shersfy.datahub.commons.meta.HdfsMeta;
import org.shersfy.datahub.commons.meta.PartitionMeta;
import org.shersfy.datahub.commons.meta.RowData;
import org.shersfy.datahub.commons.meta.TableMeta;
import org.shersfy.datahub.commons.meta.TableType;
import org.shersfy.datahub.commons.utils.CharsetUtil;
import org.shersfy.datahub.commons.utils.DateUtil;
import org.shersfy.datahub.dbexecutor.connector.hadoop.HadoopAuthTypes;
import org.shersfy.datahub.dbexecutor.connector.hadoop.HdfsUtil;
import org.shersfy.datahub.dbexecutor.connector.hadoop.HiveUtil;
import org.shersfy.datahub.dbexecutor.connector.hadoop.HiveUtil.HiveTableFormat;
import org.springframework.beans.BeanUtils;


/**
 * Hive数据库连接器
 */
public class HiveConnector extends MySQLConnector {
	
	/**默认超时5*60s**/
	public static final int QUERY_TIMEOUT = 300;


	@Override
	public String loadDriverClass(DBAccessType type) {

		String driver 	= "org.apache.hive.jdbc.HiveDriver";
		switch (type) {
		case ODBC:
			driver = "org.apache.hive.jdbc.HiveDriver";
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
	public String queryByPage(String baseSql, long pageNo, long pageSize) {
		if(StringUtils.isBlank(baseSql) ){
			return null;
		}
		if(pageNo<1 || pageSize<0){
			return baseSql;
		}
		String aliasA = "T"+System.currentTimeMillis();
		StringBuffer sql = new StringBuffer(0);
		
		// baseSql未分页
		if(!StringUtils.containsIgnoreCase(baseSql, " LIMIT ")){
			sql.append(baseSql);
			sql.append(" LIMIT ");
			sql.append(pageSize);
			return sql.toString();
		}
		// baseSql在已经分页的基础上二次分页
		sql.append("SELECT * FROM (");
		sql.append(baseSql);
		sql.append(") ");
		sql.append(aliasA);
		sql.append(" LIMIT ");
		sql.append(pageSize);
		return sql.toString();
	}


	@Override
	public boolean checkAvailable() throws DatahubException {
		checkUrl("jdbc:hive2");
		if(dbMeta.isCheckValid() && dbMeta.getBundledHdfs()!=null){
			// 测试验证hive绑定的hdfs是否连通
			return HdfsUtil.testConn(dbMeta.getBundledHdfs());
		}
		return true;
	}


	@Override
	public void setDbMeta(DBMeta dbMeta) throws DatahubException {
		
		dbMeta.setSchema(dbMeta.getDbName());
		if(StringUtils.isBlank(dbMeta.getUrl())){
			dbMeta.setUrl(String.format("jdbc:hive2://%s:%s/%s", 
					dbMeta.getHost(),
					dbMeta.getPort(),
					dbMeta.getDbName()==null?"":dbMeta.getDbName()));
		}
		
		if(StringUtils.isBlank(dbMeta.getParams().getProperty("hive.server2.query.timeout"))){
			dbMeta.getParams().put("hive.server2.query.timeout", String.valueOf(QUERY_TIMEOUT));
		}
		
		HdfsMeta hdfs = dbMeta.getBundledHdfs();
		if(hdfs == null){
			throw new DatahubException("Bundled hdfs meta is null");
		}
		// 非leap集群, hive的代理用户为hive db的用户
//		if(!CommConst.LEAP_SYSTEM_HDFS.equals(hdfs.getName())){
//			hdfs.setAppUser(dbMeta.getUserName());
//		}
		
		HadoopAuthTypes auth = HadoopAuthTypes.indexOf(hdfs.getAuthType());
		switch (auth) {
		case SENTRY:
		case KERBEROS:
			if(StringUtils.isNotBlank(hdfs.getAppUser()) && !dbMeta.getUrl().contains("hive.server2.proxy.user")){
				dbMeta.setUrl(String.format("%s;hive.server2.proxy.user=%s", dbMeta.getUrl(), hdfs.getAppUser()));
			}
			break;
		case SIMPLE:
		default:
			if(StringUtils.isNotBlank(dbMeta.getUserName()) && !dbMeta.getUrl().contains("hive.server2.proxy.user")){
				dbMeta.setUrl(String.format("%s;hive.server2.proxy.user=%s", dbMeta.getUrl(), dbMeta.getUserName()));
			}
			break;
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
			rs = conn.getMetaData().getSchemas();
			
			while(rs.next()){
				DBMeta meta = new DBMeta();
				BeanUtils.copyProperties(meta, getDbMeta());
				meta.setName(rs.getString("TABLE_SCHEM"));
				meta.setCatalog(rs.getString("TABLE_CATALOG"));
				meta.setSchema(rs.getString("TABLE_SCHEM"));
				meta.setDbName(meta.getName());
				
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
	public List<TableMeta> getTables(String catalog, String schema, TableType[] types,
			Connection conn) throws DatahubException {
		
		List<TableMeta> list = super.getTables(catalog, schema, types, conn);
		List<TableMeta> tables = new ArrayList<TableMeta>();
		// 设置分隔符
		list.forEach(e->{
			// 是否分区表
//			List<ColumnMeta> pcols = this.getPartitionColumns(table, conn);
//			table.setPartitionTable(!pcols.isEmpty());
			e.setColumnSep(null);
			tables.add(e);
		});
		
		return tables;
	}

	@Override
	public String getDDLByTable(TableMeta table, List<ColumnMeta> columns, 
			Connection conn) throws DatahubException {
		
		if(table == null || StringUtils.isBlank(table.getName())){
			return StringUtils.EMPTY;
		}
		
		if(columns == null || columns.isEmpty()){
			return showCreateTable(table, conn);
		}
		
		for(int i=0; i<columns.size(); i++){
			if(StringUtils.isBlank(columns.get(i).getName())){
			    throw new DatahubException("field name cannot be null, index="+i);
			}
		}
		
		if(StringUtils.isBlank(table.getFormat())){
			table.setFormat(HiveTableFormat.text.name());
		}
		
		String ddl 		 = "CREATE TABLE %s ( \n%s )%s %s";
		String parts 	 = "\nPARTITIONED BY ( \n%s)";
		String separator = "\nROW FORMAT DELIMITED  FIELDS TERMINATED BY '%s'";
		
		StringBuffer part	= new StringBuffer(0);
		StringBuffer cols	= new StringBuffer(0);
		
		String blank 	= " ";
//		String colSep 	= ", ";
		String lineSep 	= ", \n";
		
		StringBuffer field = new StringBuffer(0);
		for(int i=0; i<columns.size(); i++){
			field.setLength(0);
			// name
			field.append(quotObject(columns.get(i).getName()));
			field.append(blank);
			// type
			String type = javaTypeToDbType(columns.get(i).getDataType(), columns.get(i));
			field.append(type);

			if(columns.get(i).isPartitionColumn()){
				part.append(field);
				part.append(lineSep);
			}
			else{
				cols.append(field);
				cols.append(lineSep);
			}
		}
		
		if(part.length()>0){
			parts = String.format(parts, part.substring(0, part.length()-lineSep.length()));
		}
		else{
			parts = StringUtils.EMPTY;
		}
		
		String sep = table.getColumnSep();
		sep = sep == null?CommConst.COLUMN_SEP:sep;
		separator = HiveUtil.getTableFormatStr(HiveTableFormat.valueOf(table.getFormat()), StringEscapeUtils.escapeJava(sep));
		//separator = String.format(separator, StringEscapeUtils.escapeJava(table.getColumnSep()));
		
		ddl = String.format(ddl, 
				getFullTableName(table), 
				cols.substring(0, cols.length()-lineSep.length()),
				parts,
				separator);
		
		return ddl;
	}

	@Override
	public String javaTypeToDbType(int javaType, ColumnMeta column) throws DatahubException {
		String type = javaTypeToDbTypeMap.get(Integer.valueOf(javaType));
		
		switch (javaType) {
		case Types.ARRAY:
			type = type+"<string>";
			break;
		case Types.BIT:
			// 兼容postgresql boolean类型
			if("bool".equalsIgnoreCase(column.getTypeName())){
				type = javaTypeToDbTypeMap.get(Types.BOOLEAN);
			}
			break;
		case Types.JAVA_OBJECT:
			type = type+"<string, string>";
			break;
		case Types.STRUCT:
			type = type+"<str1:string, int1:int, date1:date>";
			break;
		case Types.DECIMAL:
		case Types.NUMERIC:
			if(column.getDecimalDigits()>0){
				type = type + String.format("(%s, %s)", column.getColumnSize(), column.getDecimalDigits());
			}
			else {
				// hive Decimal precision out of allowed range [1,38]
				int scale = Math.abs(column.getColumnSize()+Math.abs(column.getDecimalDigits()));
				if(scale<1){
					scale = 1;
				}
				else if(scale>38){
					scale = 38;
				}
				type = type + String.format("(%s, 0)", scale);
			}
			break;
		case Types.CHAR:
		case Types.NCHAR:
			// 兼容mysql enum和set数据类型
			if("enum".equalsIgnoreCase(column.getTypeName())
					|| "set".equalsIgnoreCase(column.getTypeName())){
				type = javaTypeToDbTypeMap.get(Types.LONGNVARCHAR);
				break;
			}
			if(column.getColumnSize()>255){
				type = javaTypeToDbTypeMap.get(Integer.valueOf(Types.LONGVARCHAR));
			}
			else if(column.getColumnSize()<1){
				type = String.format(type+"(%s)", 1);
			}
			else{
				type = String.format(type+"(%s)", column.getColumnSize());
			}
			break;
		case Types.VARCHAR:
		case Types.NVARCHAR:
			if(column.getColumnSize()>255){
				type = javaTypeToDbTypeMap.get(Integer.valueOf(Types.LONGVARCHAR));
			}
			else if(column.getColumnSize()<1){
				type = String.format(type+"(%s)", 1);
			}
			else{
				type = String.format(type+"(%s)", column.getColumnSize());
			}
			break;
		case Types.DATE:
			// 兼容mysql的year
			if("year".equalsIgnoreCase(column.getTypeName())){
				type = javaTypeToDbTypeMap.get(Integer.valueOf(Types.VARCHAR));
				type = String.format(type+"(%s)", 4);
			}
			break;
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			// 兼容oracle的RAW
			// 兼容Mysql的GEOMETRY类型
			if("RAW".equalsIgnoreCase(column.getTypeName())
					|| "GEOMETRY".equalsIgnoreCase(column.getTypeName())){
				type = javaTypeToDbTypeMap.get(Types.LONGNVARCHAR);
			}
			break;
		case oracle.jdbc.OracleTypes.BFILE:
			// 兼容oracle的BFILE
			type = javaTypeToDbTypeMap.get(Integer.valueOf(Types.BINARY));
			break;
		default:
			break;
		}
		
		if(StringUtils.isBlank(type)){
			type = javaTypeToDbTypeMap.get(Integer.valueOf(Types.LONGVARCHAR));
		}
		
		return type;
	}
	
	/**java.sql.Types映射为数据库类型**/
	private static Map<Integer, String> javaTypeToDbTypeMap = new HashMap<Integer, String>();
	static {
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.ARRAY), "array");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BIGINT), "bigint");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BINARY), "binary");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BIT), "int");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BLOB), "binary");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.BOOLEAN), "boolean");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.CHAR), "char");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.CLOB), "string");
//		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DATALINK), "string");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DATE), "date");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DECIMAL), "decimal");
//		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DISTINCT), "string");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.DOUBLE), "double");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.FLOAT), "double");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.INTEGER), "int");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.JAVA_OBJECT), "map");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.LONGNVARCHAR), "string");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.LONGVARBINARY), "binary");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.LONGVARCHAR), "string");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NCHAR), "char");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NCLOB), "string");
//		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NULL), "string");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NUMERIC), "decimal");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.NVARCHAR), "varchar");
//		javaTypeToDbTypeMap.put(Integer.valueOf(Types.OTHER), "string");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.REAL), "double");
//		javaTypeToDbTypeMap.put(Integer.valueOf(Types.REF), "string");
//		javaTypeToDbTypeMap.put(Integer.valueOf(Types.REF_CURSOR), "string");
//		javaTypeToDbTypeMap.put(Integer.valueOf(Types.ROWID), "string");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.SMALLINT), "smallint");
//		javaTypeToDbTypeMap.put(Integer.valueOf(Types.SQLXML), "string");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.STRUCT), "struct");
//		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TIME), "string");
//		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TIME_WITH_TIMEZONE), "string");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TIMESTAMP), "timestamp");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TIMESTAMP_WITH_TIMEZONE), "timestamp");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.TINYINT), "tinyint");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.VARBINARY), "binary");
		javaTypeToDbTypeMap.put(Integer.valueOf(Types.VARCHAR), "varchar");

	}

	@Override
	public String showCreateTable(TableMeta table, Connection conn) throws DatahubException {
		if(table == null || conn == null){
			return "";
		}
		table.setCatalog(StringUtils.isBlank(table.getCatalog())?null:table.getCatalog());
		table.setSchema(StringUtils.isBlank(table.getSchema())?null:table.getSchema());
		
		String showSql = "SHOW CREATE TABLE %s";
		showSql = String.format(showSql, getFullTableName(table));
		GridData data = this.executeQuery(conn, showSql);
		List<RowData> rows = data.getRows();
		StringBuffer res = new StringBuffer(0);
		if(rows.size()>0 && rows.get(0).getFields().size()>0){
			for(RowData row : rows){
				res.append(row.getFields().get(0).getValue());
				res.append("\n");
			}
		}
		int index = res.indexOf("LOCATION");
		if (index != -1) {
			res = res.replace(index, res.length(), "");
		}
		table.setFormat(HiveUtil.getTableFormatByDDL(res.toString()).name());
		return res.toString();
	}

	@Override
	public String getFullTableName(TableMeta table){
		
		StringBuffer name = new StringBuffer(0);
		if(table != null && StringUtils.isNotBlank(table.getName())){
			
			if(StringUtils.isNotBlank(table.getSchema())){
				name.append(quotObject(table.getSchema()));
				name.append(".");
			}
			
			name.append(quotObject(table.getName()));
		}
		
		return name.toString();
	}

	@Override
	public List<ColumnMeta> getColumnsWithPartition(TableMeta table, Connection conn, String ddl)
			throws DatahubException {
		List<ColumnMeta> cols = super.getColumnsWithPartition(table, conn);
		List<String> names 	  = new ArrayList<String>();
		
		ddl = StringUtils.isBlank(ddl)?showCreateTable(table, conn):ddl;
		String tmp 		= ddl;
		String search 	= "PARTITIONED BY (";
		int index 		= ddl.indexOf(search);
		
		if(index!=-1){
			tmp = ddl.substring(index).trim();
			int start = index+tmp.indexOf("(")+"(".length();
			int end	= -1;
			int from = start-index;
			// 判断PARTITIONED BY ()括号中是否含有括号start
			int nextR = tmp.indexOf(")", search.length());
			int nextL = tmp.indexOf("(", search.length());
			if(nextL == -1){
				//  没有左括号
				end = nextR;
			}
			else if(nextR>nextL){
				end  = -1;
			}
			else{
				end = nextR;
			}
			// 判断PARTITIONED BY ()括号中是否含有括号end
			// 跳过中间嵌套括号start
			while(end==-1){
				// 找到(左括号，就要找右括号)与之配对
				if(tmp.indexOf("(", from) != -1){
					end  = -1;
					from = tmp.indexOf(")", from)+")".length();
				}
				else{
					end = tmp.indexOf(")", from);
					break;
				}
				// 配对完毕找下一个左括号和下一个右括号，如果右括号的索引值小于左括号的的索引值，循环结束
				nextL = tmp.indexOf("(", from);
				nextR = tmp.indexOf(")", from);
				
				if(nextR<nextL){
					end  = nextR;
				}
			}
			end += index;
			// 跳过中间嵌套括号end
			tmp = ddl.substring(start, end).trim();
			// 逗号拆分为分区字段
			String[] parts = tmp.split(",");
			for(String p :parts){
				p = p.trim();
				// 空格拆分为字段名称和数据类型
				names.add(p.split(" ")[0].trim());
			}
		}
		
		for(ColumnMeta col: cols){
			if(names.contains(quotObject(col.getName()))){
				col.setColumnDef(this.initColumnDef(col));
				col.setPartitionColumn(true);
			}
		}
		
		return cols;
	}

	@Override
	public List<ColumnMeta> getColumnsWithPartition(TableMeta table, Connection conn)
			throws DatahubException {
		return getColumnsWithPartition(table, conn, null);
	}

	@Override
	public List<ColumnMeta> getPartitionColumns(TableMeta table, Connection conn) throws DatahubException {
		return getPartitionColumns(table, conn, null);
	}
	

	@Override
	public List<ColumnMeta> getPartitionColumns(TableMeta table, Connection conn, String ddl) throws DatahubException {
		List<ColumnMeta> partCols = super.getPartitionColumns(table, conn);
		List<ColumnMeta> columns  = this.getColumnsWithPartition(table, conn, ddl);
		for(ColumnMeta col :columns){
			if(col.isPartitionColumn()){
				partCols.add(col);
			}
		}
		return partCols;
	}

	@Override
	public List<PartitionMeta> getPartitions(TableMeta table, Connection conn) throws DatahubException {
		
		List<PartitionMeta> parts 	= super.getPartitions(table, conn);
		List<ColumnMeta> columns    = this.getColumnsWithPartition(table, conn);
		
		boolean isPartition = false;
		for(ColumnMeta col :columns){
			isPartition = col.isPartitionColumn();
			if(isPartition){
				break;
			}
		}
		if(!isPartition){
			return parts;
		}
		
		String sql = "SHOW PARTITIONS %s";
		sql = String.format(sql, getFullTableName(table));
		
		List<String> pvalues = new ArrayList<>();
		Map<String, Set<String>> map = new HashMap<String, Set<String>>();
		try {
			GridData data = executeQuery(conn, sql);
			// country=US/city=NY
			// country=CN/city=BJ
			for(RowData row : data.getRows()){
				String part = String.valueOf(row.getFields().get(0).getValue());
				pvalues.add(part);
				
				String[] cols = part.split("/");
				for(String pc : cols){
					String key	= pc.substring(0, pc.indexOf("=")).trim();
					String value= pc.substring(pc.indexOf("=")+1).trim();
					Set<String> set = map.get(key);
					if(set == null){
						set = new HashSet<String>();
					}
					set.add(URLDecoder.decode(value, CharsetUtil.getUTF8().toString()));
					map.put(key, set);
				}
			}
		} catch (Exception e) {
			// nothing do
		}
		
		Set<String> keys = map.keySet();
		for(ColumnMeta col : columns){
			if(keys.contains(col.getName())){
				
				PartitionMeta part = new PartitionMeta();
				col.setPartitionColumn(true);
				part.setColumn(col);
				part.setName(col.getName());
				
				Set<String> values = map.get(col.getName());
				String[] arr = values.toArray(new String[values.size()]);
				ArrayList<FieldData> plist = new ArrayList<FieldData>();
				for(String el : arr){
					FieldData value = new FieldData(el, el);
					plist.add(value);
				}
				plist.sort(new Comparator<FieldData>() {

					@Override
					public int compare(FieldData o1, FieldData o2) {
						String name1 = o1.getName().toLowerCase();
						String name2 = o2.getName().toLowerCase();
						return name1.compareTo(name2);
					}
				});
				part.setParts(plist);
				part.setValues(pvalues);
				parts.add(part);
			}
		}
		
		if(parts.isEmpty()){
			for(ColumnMeta col : columns){
				if(!col.isPartitionColumn()){
					continue;
				}
				PartitionMeta part = new PartitionMeta();
				part.setName(col.getName());
				part.setColumn(col);
				parts.add(part);
			}
		}
		
		return parts;
	}
	
	/**
	 * 初始化字段默认值
	 * 
	 * @author PengYang
	 * @date 2017-07-27
	 * 
	 * @param col
	 * @return
	 */
	private String initColumnDef(ColumnMeta col){
		String def = "null";
		
		switch (col.getDataType()) {
		
		case Types.BIGINT: 
		case Types.BIT:
		case Types.DECIMAL:
		case Types.DOUBLE:
		case Types.FLOAT:
		case Types.INTEGER:
		case Types.NUMERIC:
		case Types.REAL:
		case Types.SMALLINT:
		case Types.TINYINT: 
			def = String.valueOf(0);
			break;
		case Types.BOOLEAN:
			def = String.valueOf(false);
			break;
		case Types.DATE:
			def = DateUtil.format(CommConst.FORMAT_DATE);
			break;
		case Types.CHAR:
		case Types.NCHAR:
			def = String.valueOf('N');
			break;
		case Types.TIMESTAMP:
		case Types.TIMESTAMP_WITH_TIMEZONE:
		def = DateUtil.format(CommConst.FORMAT_TIMESTAMP);
		break;
		case Types.ARRAY:
		case Types.BINARY:
		case Types.BLOB:
		case Types.CLOB:
		case Types.DATALINK:
		case Types.DISTINCT:
		case Types.JAVA_OBJECT:
		case Types.LONGNVARCHAR:
		case Types.LONGVARBINARY:
		case Types.LONGVARCHAR:
		case Types.NCLOB: 
		case Types.NULL:
		case Types.NVARCHAR:
		case Types.OTHER:
		case Types.REF:
		case Types.REF_CURSOR: 
		case Types.ROWID:
		case Types.SQLXML:
		case Types.STRUCT:
		case Types.TIME:
		case Types.TIME_WITH_TIMEZONE: 
		case Types.VARBINARY:
		case Types.VARCHAR:
		default:
			break;
		}
		return def;
	}


	@Override
	public String getDBWarehouseLocation(Connection conn, String dbName, boolean isShort) throws DatahubException {
		String location = "";
		if(StringUtils.isBlank(dbName)){
			dbName = "default";
		}
		String sql = "desc database "+this.quotObject(dbName);
		GridData data = this.executeQuery(conn, sql);
		//+------------+----------+--------------------------------------------------------------+-------------+-------------+-------------+--+
		//|  db_name   | comment  |                           location                           | owner_name  | owner_type  | parameters  |
		//+------------+----------+--------------------------------------------------------------+-------------+-------------+-------------+--+
		//| datahub01  |          | hdfs://demo1.leap.com:8020/apps/hive/warehouse/datahub01.db  | hive        | USER        |             |
		//+------------+----------+--------------------------------------------------------------+-------------+-------------+-------------+--+
		if(data.getRows().size()>0 && data.getRows().get(0).getFields().size()>2){
			Object obj = data.getRows().get(0).getFields().get(2).getValue();
			location = String.valueOf(obj);
			if(isShort){
				location = location.replace("hdfs://", "");
				location = location.substring(location.indexOf("/"));
			}
		}
		return location;
	}


	@Override
	public Throwable connectionException(Throwable ex) {
		ex = super.connectionException(ex);
		String err = ex.getMessage();
		if(StringUtils.containsIgnoreCase(err, "Read timed out")
				|| StringUtils.containsIgnoreCase(err, " Connection timed out")
				|| StringUtils.containsIgnoreCase(err, "Connection reset")){
			return new NetException(ex);
		}
		
		return ex;
	}
	
	
}
