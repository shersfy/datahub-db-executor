package org.shersfy.datahub.dbexecutor.connector.hadoop;

import java.io.File;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.shersfy.datahub.commons.constant.CommConst;
import org.shersfy.datahub.commons.meta.TableMeta;
import org.shersfy.datahub.commons.utils.DateUtil;
import org.shersfy.datahub.commons.utils.FileUtil;
import org.shersfy.datahub.dbexecutor.connector.db.DbConnectorInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class HiveUtil {

	private static final Logger LOGGER = LoggerFactory.getLogger(HiveUtil.class);

	/**Hive表存储格式类型**/
    public static enum HiveTableFormat {
        
        text("TextInputFormat"),
        
        orc("OrcInputFormat"),
        
        parquet("MapredParquetInputFormat"),
        
        rcfile("RCFileInputFormat"),
        
        sequencefile("SequenceFileInputFormat");
        
        private String alias;
        
        HiveTableFormat(String alias){
            this.alias = alias;
        }
        
        public static HiveTableFormat indexOf(Integer index) {
            if (index == null) {
                return text;
            }
            switch (index) {
            case 0:
                return text;
            case 1:
                return orc;
            case 2:
                return parquet;
            case 3:
                return rcfile;
            case 4:
                return sequencefile;
            default:
                return text;
            }
        }
        
        public int index() {
            return this.ordinal();
        }

        public String alias() {
            return alias;
        }

    }
	/**
	 * 加载hdfs文件到hive表, 加载成功返回true, 失败返回false
	 * 
	 * @param meta hive连接信息
	 * @param hiveTable hive表
	 * @param patition hive表分区
	 * @param hdfsFile hdfs文件
	 * @param overrite 是否覆盖,true覆盖, false追加
	 * @return boolean
	 */
	public static boolean loadToHive(DbConnectorInterface hiveConn, TableMeta hiveTable, 
			String patition, String hdfsFile, boolean overrite){
		
		Connection connection = null;
		Statement statement   = null;
		boolean res = false;
		try {
			
			if(hiveConn == null){
				LOGGER.error("Hive Connector is null.");
				return false;
			}
			
			if(hiveTable == null || StringUtils.isBlank(hiveTable.getName())){
				LOGGER.error("the parameter: hive table name cannot be empty");
				return false;
			}
			if(StringUtils.isBlank(hdfsFile)){
			    LOGGER.error("the parameter: hdfs file path cannot be empty");
				return false;
			}
			if(hiveConn.getDbMeta().getBundledHdfs() == null){
			    LOGGER.error("the parameter: bundled HdfsMeta cannot be null");
				return false;
			}
			if(!HdfsUtil.testConn(hiveConn.getDbMeta().getBundledHdfs())){
				LOGGER.error("test connection failed: {}", hiveConn.getDbMeta().getBundledHdfs().toString());
				return false;
			}
			
			connection = hiveConn.connection();
			
			StringBuilder sqlBuffer = new StringBuilder();
			sqlBuffer.append("LOAD DATA INPATH ").append("'").append(hdfsFile).append("'");
			if (overrite) {
				sqlBuffer.append(" OVERWRITE ");
			}
			sqlBuffer.append("INTO TABLE ").append(hiveConn.getFullTableName(hiveTable));
			if (StringUtils.isNotBlank(patition)) {
				sqlBuffer.append(" PARTITION(").append(patition).append(")");
			}
			
			statement = connection.createStatement();
			String hiveSQL = sqlBuffer.toString().replace(File.separatorChar, '/');
			
			long start = System.currentTimeMillis();
			LOGGER.info("Start execute sql={} at {}.", hiveSQL, DateUtil.format(new Date()));
			statement.execute(hiveSQL);
			long spend = (System.currentTimeMillis() - start)/1000;
			LOGGER.info("End   execute sql={} at {}, spend={} seconds.", hiveSQL, DateUtil.format(new Date()), spend);
			
			res = true;
		} catch (Exception e){
			LOGGER.error("", e);
		} finally {
			hiveConn.close(statement, connection);
		}
		
		return res;
	}
	
	public static String getPartitionDir(FileSystem fs, String tableLocation, String partition) {
		return getPartitionDir(fs, tableLocation, partition, true);
	}
	
	/**
	 * 获取分区位置路径, 分区不存在返回null
	 * @param fs hdfs文件系统对象
	 * @param tableLocation 表位置路径
	 * @param partition 分区字符串
	 * @param testPath 是否需要测试path是否存在
	 * @return 分区位置路径
	 */
	public static String getPartitionDir(FileSystem fs, String tableLocation, String partition, boolean testPath){
		
		if(fs == null || StringUtils.isBlank(tableLocation) || StringUtils.isBlank(partition)){
			return null;
		}
		
		LOGGER.info("find hive partition location, table location=\"{}\", partition=\"{}\"", tableLocation, partition);
		StringBuffer dir = new StringBuffer(0);
		try {
			List<String> list = new ArrayList<>();
			list.addAll(Arrays.asList(partition.split(",")));
			
			int cnt = list.size();
			for(int i=0; i<cnt; i++){
				String[] arr = list.toArray(new String[list.size()]);
				for(int j=0; j<arr.length; j++){
					// 去掉单引号
					if(StringUtils.contains(arr[j], "='")){
						arr[j] = arr[j].replaceFirst("='", "=");
						arr[j] = arr[j].substring(0, arr[j].length()-1);
					}
					arr[j] = arr[j].startsWith("'") && arr[j].length()>1?arr[j].substring(1, arr[j].length()-1):arr[j];
					String pdir = FileUtil.concat(tableLocation, dir.toString()+arr[j]);
					if(testPath&&fs.isDirectory(new Path(pdir))){
						dir.append(arr[j]).append("/");
						list.remove(j);
						break;
					}
					if (!testPath) {
						dir.append(arr[j]).append("/");
						list.remove(j);
						break;
					}
				}
			}
			
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		
		if(dir.length() == 0){
			return null;
		}
		
		return FileUtil.concat(tableLocation, dir.toString());
	}
	
	
	
	/**转换为hive可执行的一个分区字符串**/
	public static String getPartitionStr(List<Partition> partition) {
		if(partition == null || partition.isEmpty()){
			return null;
		}
		
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < partition.size(); i++) {
			Partition part = partition.get(i);
			sb.append(StringUtils.isNotBlank(part.getName())?part.getName():part.getField());
			sb.append("='").append(part.getValue()).append("'");
			if (i != partition.size() - 1) {
				sb.append(",");
			}
		}
		return sb.toString();
	}
	
	/**
	 * 
	 * hive原始字符串转换为分区对象
	 * 
	 * @param originStr 原始分区字符串
	 * @return 分区对象
	 */
	public static List<Partition> getPartition(String originStr) {
		if(StringUtils.isBlank(originStr) || !originStr.contains("=")){
			return null;
		}
		
		List<Partition> part = new ArrayList<>();
		String[] cols = originStr.split("/");
		for(String pc : cols){
			String key	= pc.substring(0, pc.indexOf("=")).trim();
			String value= pc.substring(pc.indexOf("=")+1).trim();
			value = value.startsWith("'")?value.substring(1):value;
			value = value.endsWith("'")?value.substring(0, value.length()-1):value;
//			try {
//				// 分区使用URL解码
//				value = URLDecoder.decode(value, CharsetUtil.getUTF8().toString());
//			} catch (Exception e) {
//				LOGGER.error("", e.getMessage());
//			}
			
			part.add(new Partition(key, value));
		}
		return part;
	}
	
	/**Json格式拼接partition字符串**/
	public static String getPartitionStr(String jsonStr) {
		List<Partition> list = new ArrayList<Partition>();
		if(StringUtils.isBlank(jsonStr)){
			return null;
		}
		
		try {
			JSONObject json = JSONObject.parseObject(jsonStr);
			if(json == null){
				return null;
			}
			
			for(Entry<String, Object> e :json.entrySet()){
				Partition p = new Partition();
				p.setField(e.getKey());
				p.setValue(String.valueOf(e.getValue()));
				list.add(p);
			}
			
		} catch (Exception e) {
			try {
				list = JSONObject.parseArray(jsonStr, Partition.class);
			} catch (Exception e2) {
				LOGGER.error(e2.getMessage());
			}
		}
		
		return getPartitionStr(list);
	}
	/**
	 * 生成创建临时表SQL
	 * 
	 * @param oldSql 源表建表SQL
	 * @param tempMeta 临时表
	 * @param connector 连接器
	 * @return 返回临时表SQL
	 */
	public static String createTempTableSql(String oldSql, TableMeta tempMeta, DbConnectorInterface connector) {
		String search1 = "TABLE";
		String search2 = "(";
		if(StringUtils.isBlank(oldSql) 
				|| !StringUtils.containsIgnoreCase(oldSql, search1)
				|| !StringUtils.containsIgnoreCase(oldSql, search2)){
			return oldSql;
		}
		// 替换表名
		String newFullName 	= connector.getFullTableName(tempMeta);
		
		StringBuffer sql = new StringBuffer(0);
		sql.append(oldSql.substring(0, oldSql.indexOf(search1)+search1.length()));
		sql.append(" ");
		sql.append(newFullName);
		sql.append(" ");
		sql.append(oldSql.substring(oldSql.indexOf(search2)));
		
		String newSql = sql.toString();
		
		int index = newSql.indexOf("ROW FORMAT ");
		int cindex = newSql.indexOf("CLUSTERED BY");
		int length = cindex != -1 && cindex < index ? cindex : index; 
		if (length != -1) {
			newSql = newSql.substring(0, length-1);
		}
		newSql = newSql+" ROW FORMAT DELIMITED FIELDS TERMINATED BY '%s'";
		if(tempMeta.getColumnSep() == null){
			newSql = String.format(newSql, StringEscapeUtils.escapeJava(CommConst.COLUMN_SEP));
		}
		else{
			newSql = String.format(newSql, StringEscapeUtils.escapeJava(tempMeta.getColumnSep()));
		}
		
		LOGGER.debug("old sql= {}", oldSql);
		LOGGER.debug("new sql= {}", newSql);
		
		return newSql;
	}
	
	@Deprecated
	public static String createColumns(String oldSql) {
		StringBuilder builder = new StringBuilder();
		int firstIndex = oldSql.indexOf("(");
		int endIndex = oldSql.indexOf(")");
		oldSql = oldSql.substring(firstIndex+1, endIndex);
		String[] strs = oldSql.split(",");
		String[] columStrs = null;
		for (String str : strs) {
			columStrs = str.split("`");
			builder.append("`"+columStrs[1]+"`");
			builder.append(",");
		}
		return builder.toString().substring(0, builder.length()-1);
	}
	
	@Deprecated
	public static String gerentFullName(TableMeta meta) {
		StringBuilder builder = new StringBuilder();
		if(StringUtils.isNotBlank(meta.getName())) {
			if (StringUtils.isNotBlank(meta.getSchema())) {
				builder.append("`");
				builder.append(meta.getSchema());
				builder.append(".");
			} else {
				builder.append("`");
			}
			builder.append(meta.getName());
			builder.append("`");
		}
		return builder.toString();
	}
	
	/**
	 * 
	 * hive可执行分区转换为hive原始分区.<br/>
	 * year='2017',month='12',date='01' --> year=2017/month=12/date=01
	 * 
	 * @param exeStr 可执行分区字符串
	 * @return hive原始分区字符串
	 */
	public static String getPartitionOriginStr(String exeStr) {
		if(StringUtils.isBlank(exeStr)){
			return exeStr;
		}
		
//		try {
//			// 分区使用URL编码
//			exeStr = URLEncoder.encode(exeStr, CharsetUtil.getUTF8().toString());
//		} catch (Exception e) {
//			LOGGER.error("", e.getMessage());
//		}
		
		String originStr = exeStr.replace(",", "/");
		originStr = originStr.replace("'", "");
		return originStr;
	}
	
	/**原始分区转为可执行分区json数组字符串**/
	public static String getPartitionJsonArrayStr(List<String> parts) {
		if(parts == null){
			return null;
		}
		List<String> list = new ArrayList<>();
		for(String p :parts){
			String one = getPartitionStr(getPartition(p));
			if(one!=null){
				list.add(one);
			}
		}
		return JSON.toJSONString(list);
	}
	@Deprecated
	public static String gerentFullName1(TableMeta meta) {
		StringBuilder builder = new StringBuilder();
		if(StringUtils.isNotBlank(meta.getName())) {
			if (StringUtils.isNotBlank(meta.getSchema())) {
				builder.append(meta.getSchema());
				builder.append(".");
			}
			builder.append(meta.getName());
		}
		return builder.toString();
	}

	/**
	 * 在hive可执行分区字符串后面追加扩展子分区, 返回hive可执行分区字符串
	 * 
	 * @param exePartitionStr hive可执行分区字符串
	 * @param partitionExpJson 扩展子分区json字符串
	 * @return hive可执行分区字符串
	 */
	public static String appendPartitionExp(String exePartitionStr, String partitionExpJson){
		
		List<Partition> part = null;
		if(StringUtils.isNotBlank(exePartitionStr)){
			String originStr = getPartitionOriginStr(exePartitionStr);
			part = getPartition(originStr);
		}
		
		if(StringUtils.isNotBlank(partitionExpJson)){
			List<Partition> defaultp = JSON.parseArray(partitionExpJson, Partition.class);
			if(part == null){
				part = defaultp;
			} else {
				part.addAll(defaultp);
			}
		}
		String exePart = HiveUtil.getPartitionStr(part);
		return exePart;
	}
	
	/**
	 * 根据hive表的建表ddl得到表的存储格式
	 * 
	 * @param ddl 建表DDL
	 * @return
	 */
	public static String getStoredFormat(String ddl){
		String format = "TextInputFormat";
		if(StringUtils.isBlank(ddl)){
			return null;
		}
		String stored = "STORED AS";
		String inputf = "INPUTFORMAT ";
		if(!StringUtils.containsIgnoreCase(ddl, stored)
				|| !StringUtils.containsIgnoreCase(ddl, inputf)){
			return format;
		}
		
		int index = ddl.indexOf(stored);
		index = ddl.indexOf(inputf, index);
		
		int beginIndex = ddl.indexOf("'", index)+1;
		int endIndex   = ddl.indexOf("'", beginIndex+1);
		
		format = ddl.substring(beginIndex, endIndex);
		String ext = FilenameUtils.getExtension(format);
		format = StringUtils.isBlank(ext)?format:ext;
		format = format.trim();
		
		return format;
	}
	
	public static String getTableFormatStr(HiveTableFormat tableType, String sp) {
		if(tableType == null){
			return sp;
		}
		if (tableType == HiveTableFormat.text) {
			return String.format("\nROW FORMAT DELIMITED  FIELDS TERMINATED BY '%s'", sp);
		} else if (tableType == HiveTableFormat.orc) {
			return "\nSTORED AS ORC";
		} else if (tableType == HiveTableFormat.parquet) {
			return "\nSTORED AS PARQUET";
		} else if (tableType == HiveTableFormat.rcfile) {
			return "\nSTORED AS RCFILE";
		} else {
			return "\nSTORED AS SEQUENCEFILE";
		} 
	}
	
	/***
	 * 根据DDL判断表存储格式
	 * 
	 * @author PengYang
	 * @date 2017-12-01
	 * 
	 * @param ddl
	 * @return
	 */
	public static HiveTableFormat getTableFormatByDDL(String ddl) {
		
		if (ddl == null) {
			return null;
		}
		if (ddl.contains("SequenceFileInputFormat")||ddl.contains("STORED AS SEQUENCEFILE")) {
			return HiveTableFormat.sequencefile;
		} else if (ddl.contains("OrcInputFormat")||ddl.contains("STORED AS ORC")) {
			return HiveTableFormat.orc;
		} else if (ddl.contains("MapredParquetInputFormat")||ddl.contains("STORED AS PARQUET")) {
			return HiveTableFormat.parquet;
		} else if (ddl.contains("RCFileInputFormat")||ddl.contains("STORED AS RCFILE")) {
			return HiveTableFormat.rcfile;
		} else {
			return HiveTableFormat.text;
		}
		
	}
	
	/***
	 * hive可执行分区字符串转换为查询SQL条件
	 * 
	 * @author PengYang
	 * @date 2018-01-05
	 * 
	 * @param partition 逗号分隔分区字符串
	 * @return AND分隔字符串
	 */
	public static String partitionToWhere(String partition){
		if(partition==null){
			return null;
		}
		StringBuffer where = new StringBuffer();
		String[] arr = partition.split(",");
		for(int i=0; i<arr.length; i++){
			where.append(arr[i]);
			if(i!=arr.length-1){
				// 后一个元素包含"=", 添加and
				if(arr[i+1].contains("=")){
					where.append(" AND ");
				}
			}
		}
		
		return where.toString();
	}
	
	/**
	 * MPP表定义
	 * 
	 * @author PengYang
	 * @date 2018-03-15
	 * 
	 * @param ddl 建表语句
	 * @return true是mpp表, false不是mpp表
	 */
	public static boolean isMPPTable(String ddl){
		
		if(!StringUtils.containsIgnoreCase(ddl, "CLUSTERED BY")
				|| !StringUtils.containsIgnoreCase(ddl, "BUCKETS")
				|| !StringUtils.containsIgnoreCase(ddl, "ORC")){
			return false;
		}

		boolean flg = StringUtils.contains(ddl, "\"transactional\"=\"true\"");
		flg = flg || StringUtils.contains(ddl, "\"transactional\" = \"true\"");
		flg = flg || StringUtils.contains(ddl, "'transactional'='true'");
		flg = flg || StringUtils.contains(ddl, "'transactional' = 'true'");
		if(!flg){
			return false;
		}

		return true;
	} 

}
