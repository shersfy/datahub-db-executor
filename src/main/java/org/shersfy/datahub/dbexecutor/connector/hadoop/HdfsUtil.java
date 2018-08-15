package org.shersfy.datahub.dbexecutor.connector.hadoop;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.shersfy.datahub.commons.beans.Result;
import org.shersfy.datahub.commons.beans.Result.ResultCode;
import org.shersfy.datahub.commons.constant.CommConst;
import org.shersfy.datahub.commons.exception.DatahubException;
import org.shersfy.datahub.commons.meta.FileMeta;
import org.shersfy.datahub.commons.meta.HdfsMeta;
import org.shersfy.datahub.commons.utils.CharsetUtil;
import org.shersfy.datahub.commons.utils.FileUtil;
import org.shersfy.datahub.commons.utils.FileUtil.FileSizeUnit;
import org.shersfy.datahub.commons.utils.FileUtil.FileType;
import org.shersfy.datahub.commons.utils.FileUtil.RenameType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hdfs工具类
 * 
 */
public class HdfsUtil {

	private static final Logger LOGGER = LoggerFactory.getLogger(HdfsUtil.class);
	/**HDFS环境变量**/
    public static final String HADOOP_USER_NAME = "HADOOP_USER_NAME";
    /**默认文件列分隔符**/
    public static final String DEFAULT_COLUMN_SEP = "\u0001";
	
	/**
	 * 创建hdfs目录
	 * 
	 * @param fs FileSystem对象
	 * @param hdfsDir hdfs目录
	 * @param createUser 创建者
	 * @param permission 权限
	 * @throws DatahubException
	 */
	public static void createHdfsDirs(FileSystem fs, final String hdfsDir, 
			String createUser, FsPermission permission) throws DatahubException{
		
		try {
			
			if(StringUtils.isBlank(hdfsDir) || fs == null){
				new IOException("FileSystem is null or hdfsDir is null.");
			}

			Path dir = new Path(hdfsDir);

			if(fs.isDirectory(dir)){
				LOGGER.info(String.format("%s is exists", dir));
				return;
			}

			UserGroupInformation user = UserGroupInformation.getCurrentUser();
			if (StringUtils.isNotBlank(createUser)) {
				user = UserGroupInformation.createProxyUser(createUser, user);
			}

			IOException ioe = user.doAs(new PrivilegedAction<IOException>() {

				@Override
				public IOException run() {
					try {
						fs.mkdirs(new Path(hdfsDir), permission);
					} catch (IOException e) {
						return e;
					} catch (Throwable th) {
						return new IOException(th);
					}
					return null;
				}

			});

			if(ioe != null){
				throw ioe;
			}

		} catch (Exception e) {
			throw new DatahubException(e, "create hdfs directory error:%s", hdfsDir);
		}
		
	}
	
	public static FSDataOutputStream createHdfsFile(FileSystem fs, final String hdfsFile, 
	                                                String createUser) throws DatahubException{
	    return createHdfsFile(fs, hdfsFile, createUser, true);
	}
	
	/**
	 * 创建hdfs文件
	 * 
	 * @author PengYang
	 * @date 2017-06-08
	 * 
	 * @param fs FileSystem对象
	 * @param hdfsDir hdfs目录
	 * @param createUser 创建者
	 * @param permission 权限
	 * @throws DatahubException
	 */
	public static FSDataOutputStream createHdfsFile(FileSystem fs, final String hdfsFile, 
			String createUser, boolean overwrite) throws DatahubException{
		
		FSDataOutputStream output = null;
		try {
			
			if(StringUtils.isBlank(hdfsFile) || fs == null){
				new IOException("FileSystem is null or hdfs file is null.");
			}
			
			Path file = new Path(hdfsFile);
			
			if(fs.isFile(file)){
				new IOException(String.format("%s is exists", file));
			}
			
			UserGroupInformation user = UserGroupInformation.getCurrentUser();
			if (StringUtils.isNotBlank(createUser)) {
				user = UserGroupInformation.createProxyUser(createUser, user);
			}
			
			Result result = user.doAs(new PrivilegedAction<Result>() {
                
                @Override
                public Result run() {
                    Result res = new Result();
                    try {
                        res.setModel(fs.create(file, overwrite));
                    } catch (Throwable th) {
                        IOException ioe = null;
                        if(th instanceof IOException){
                            ioe = (IOException)th;
                        }
                        else{
                            ioe = new IOException(th);
                        }
                        res.setCode(ResultCode.FAIL);
                        res.setModel(ioe);
                    }
                    return res;
                }
                
            });
			if(ResultCode.FAIL == result.getCode()){
				throw (IOException)result.getModel();
			}
			
			output = (FSDataOutputStream) result.getModel();
			return output;
			
		} catch (Exception e) {
			throw new DatahubException(e, "create hdfs file error:%s", hdfsFile);
		}
		
	}

	/**
	 * 
	 * @param srcFile
	 * @param hdfsWriteDir
	 * @return
	 * @throws IOException
	 */
	public static boolean writeToHdfs(File srcFile, String hdfsWriteDir, boolean useTmp, FileSystem fs)
			throws IOException {
		boolean isSuccess = false;
		String filePath = srcFile.getAbsolutePath();
		BufferedInputStream inputStream = null;
		FSDataOutputStream outputStream = null;
		try {
			String[] filePaths = hdfsWriteDir.split("\u0001"); //前半部分是文件夹名，后半是文件名（可以为空）
			String fileName = getFilePath(filePaths);
			Path hdfsDirPath = new Path(filePaths[0]);
			if (!fs.exists(hdfsDirPath)) {
				FsPermission fsp = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
				fs.mkdirs(hdfsDirPath, fsp);
			}
			String destPathStr = filePaths[0] + "/" + (StringUtils.isBlank(fileName) ? srcFile.getName():fileName);
			String destPathTmpStr = destPathStr + ".tmp";
			Path destPath = new Path(destPathStr);
			Path destTmpPath = new Path(destPathTmpStr);
			if (useTmp) {
				outputStream = fs.create(destTmpPath, true);
			} else {
				outputStream = fs.create(destPath, true);
			}

			inputStream = new BufferedInputStream(new FileInputStream(srcFile));
			int length = 0;
			// 2M
			byte bytes[] = new byte[2097152];
			while ((length = inputStream.read(bytes)) > 0) {
				outputStream.write(bytes, 0, length);
			}

			if (useTmp) {
				isSuccess = fs.rename(destTmpPath, destPath);
				if (!isSuccess) {
					LOGGER.error("rename {} to {} failure", destPathTmpStr, destPathStr);
				}
			} else {
				isSuccess = true;
			}
			LOGGER.info("write {} to hdfs result=>{} ", filePath, isSuccess);
		} finally {
			if (outputStream != null) {
				try {
					outputStream.close();
				} catch (IOException e) {
					LOGGER.error("close {} hdfs outputStream IOException", filePath, e);
				}
			}
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					LOGGER.error("close {} hdfs inputStream IOException", filePath, e);
				}
			}
		}

		return isSuccess;
	}
	
	/**
	 * web server上的源文件上传到hdfs, 返回hdfs文件名全路径
	 * 
	 * @param meta hdfs连接信息
	 * @param srcFile 源文件
	 * @param toFileName hdfs文件全路径名
	 * @param override 文件存在是否覆盖, true覆盖, false追加
	 * @return String
	 * @throws DatahubException
	 */
	public static String writeToHdfs(FileSystem fs, File srcFile, String toFileName,  boolean override)
			throws DatahubException {
		String res 						= null;
		BufferedInputStream inputStream = null;
		FSDataOutputStream outputStream = null;
		
		FSDataOutputStream outputStreamTmp = null;
		try {
			
			if(fs == null){
				throw new DatahubException("HDFS FileSystem instance is null.");
			}
			
			if(srcFile==null || !srcFile.isFile()){
				throw new DatahubException("file not exist: "+srcFile.getAbsolutePath());
			}
			
			String destPathTmpStr	= toFileName + ".tmp";
			Path destPath 			= new Path(toFileName);
			Path destTmpPath 		= new Path(destPathTmpStr);
			outputStreamTmp			= fs.create(destTmpPath, true);
			
			inputStream = new BufferedInputStream(new FileInputStream(srcFile));
			int length = 0;
			// 2M
			byte bytes[] = new byte[2097152];
			while ((length = inputStream.read(bytes)) > 0) {
				outputStreamTmp.write(bytes, 0, length);
			}
			outputStreamTmp.hflush();
			
			// 覆盖
			if(override){
				if(!rename(fs, destPathTmpStr, toFileName, true)){
				    String err = String.format("hdfs rename error: '%s'-->'%s'", destPathTmpStr, toFileName);
					throw new DatahubException(err);
				}
			}
			// 追加
			else{
				// 把缓存文件内容追加到目标文件
				outputStream = fs.create(destPath, true);
				org.apache.hadoop.io.IOUtils.copyBytes(fs.open(destTmpPath) , outputStream, 1024, false);
				outputStream.hflush();
			}
			
			LOGGER.info("write {} to hdfs result=>{} ", toFileName, true);
			res = toFileName;
			
		} catch (Throwable ex) {
			throw DatahubException.throwDatahubException("write to hdfs error: "+toFileName, ex);
		} finally {
			org.apache.hadoop.io.IOUtils.closeStream(outputStreamTmp);
			org.apache.hadoop.io.IOUtils.closeStream(outputStream);
			org.apache.hadoop.io.IOUtils.closeStream(inputStream);
		}
		
		return res;
	}

	/**
	 * 删除文件<br/>
	 * 返回true删除成功, false删除失败
	 * 
	 * @param hdfsPath hdfs文件或目录
	 * @param meta hdfs连接信息
	 * @return boolean
	 */
	public static boolean deleteFile(String hdfsPath, FileSystem fs) {
		if(fs == null || StringUtils.isBlank(hdfsPath)){
			return false;
		}
		
		try {
			Path path = new Path(hdfsPath);
			if(fs.exists(path)){
				return fs.delete(path, true);
			}
		} catch (Exception e) {
			LOGGER.error(hdfsPath);
			LOGGER.error(e.getMessage());
		}
		
		return false;
	}
	
	public static FileSystem getFileSystem(Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		return fs;
	}

	/**
	 * 获取hdfs FileSystem对象
	 * 
	 * @param conf
	 * @return
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static FileSystem getFileSystem(Configuration conf, HdfsMeta meta) throws DatahubException {
		FileSystem fs = null;
		if(conf == null){
			return null;
		}
		
		try {
			HadoopAuthTypes auth = HadoopAuthTypes.indexOf(meta.getAuthType());
			switch (auth) {
			case SIMPLE:
			case SENTRY:
				if(StringUtils.isNotBlank(meta.getUrl())){
					fs = FileSystem.get(new URI(meta.getUrl()), conf, meta.getAppUser());
					break;
				}
				fs = getFileSystem(conf);
				fs = FileSystem.get(fs.getUri(), fs.getConf(), meta.getAppUser());
				break;
			case KERBEROS:
				String appUser = meta.getAppUser();
				appUser = StringUtils.isBlank(meta.getAppUser())?meta.getUserName():appUser;
				
				UserGroupInformation userGroup = UserGroupInformation.getCurrentUser();
				userGroup = UserGroupInformation.createProxyUser(appUser, userGroup);
				fs = userGroup.doAs(new PrivilegedAction<FileSystem>() {

					@Override
					public FileSystem run() {
						FileSystem dfs = null;
						try {
							dfs = getFileSystem(conf);
						} catch (IOException e) {
							// 异常返回null
						}
						return dfs;
					}
				});
				fs = fs==null?getFileSystem(conf):fs;
			default:
				break;
			}
			
			if(fs!=null){
				LOGGER.debug("FileSystem instance of {}.", fs.getClass().getName());
			}
			
		} catch (Throwable ex) {
		    throw DatahubException.throwDatahubException("connection to hdfs error: "+meta.getName(), ex);
		}
		
		return fs;
	}
	
	/**
	 * 获取hdfs FileSystem对象
	 * 
	 * @author PengYang
	 * @date 2017-02-20
	 * 
	 * @param meta hdfs连接信息
	 * @return FileSystem
	 * @throws DatahubException
	 */
	public static FileSystem getFileSystem(HdfsMeta meta) throws DatahubException {
		FileSystem fs = null;
		if(meta == null){
			return null;
		}
		
		Configuration conf = getConfiguration(meta);
		fs = getFileSystem(conf, meta);
		
		return fs;
	}
	
	/**
	 * 获取hdfs配置
	 * 
	 * @author PengYang
	 * @date 2017-04-18
	 * 
	 * @param meta hdfs连接信息
	 * @return Configuration
	 * @throws DatahubException
	 */
	public static synchronized Configuration getConfiguration(HdfsMeta meta) throws DatahubException {
		Configuration conf = new Configuration(false);
		if(meta == null){
			return conf;
		}
		
		try {
			if(StringUtils.isNotBlank(meta.getCoreSiteXml())){
				File config = new File(meta.getCoreSiteXml());
				if(!config.isFile()){
					throw new IOException(String.format("file not exists %s.", config.getPath()));
				}
				conf.addResource(new FileInputStream(config));
			}
			if(StringUtils.isNotBlank(meta.getHdfsSiteXml())){
				File config = new File(meta.getHdfsSiteXml());
				if(!config.isFile()){
					throw new IOException(String.format("file not exists %s.", config.getPath()));
				}
				conf.addResource(new FileInputStream(config));
			}
			
			// 不使用代理用户
			String appUser = meta.getAppUser();
			appUser = StringUtils.isBlank(appUser)?meta.getUserName():appUser;
//			if(!Const.LEAP_SYSTEM_HDFS.equals(meta.getName()) || Const.AUTH_TYPE_SIMPLE==meta.getAuthType()){
//				appUser = meta.getUserName();
//			}
			
			if(StringUtils.isBlank(appUser)) {
			    throw new DatahubException("userName cannot be empty");
			}
			
			System.setProperty(HADOOP_USER_NAME, appUser);
			HadoopAuthTypes auth = HadoopAuthTypes.indexOf(meta.getAuthType());
			switch (auth) {
			case KERBEROS:
				kinit(conf, meta);
				
				break;
			case SENTRY:
			case SIMPLE:
			default:
				UserGroupInformation.setConfiguration(conf);
				break;
			}
			
			// 支持追加流
			conf.set(DFSConfigKeys.DFS_SUPPORT_APPEND_KEY, "true");
			// 是否使用FileSystem.CACHE 共享的文件系统
			// String disableCacheName = String.format("fs.%s.impl.disable.cache", scheme);
			conf.set("fs.hdfs.impl.disable.cache", "true");
		} catch (Throwable ex) {
            throw DatahubException.throwDatahubException("hdfs configuration error: "+meta.getName(), ex);
        }	
		
		return conf;
	}
	
	/**kerberos认证**/
	private static void kinit(Configuration conf, HdfsMeta meta) throws IOException{
		LOGGER.info("kerberos kinit... {}", meta.getKrb5Conf());
		if(StringUtils.isBlank(meta.getKrb5Conf())){
			throw new IOException("krb5.conf file can not be null");
		}
		
		File krb5 = new File(meta.getKrb5Conf());
		if(!krb5.isFile()){
			throw new IOException(String.format("file not exists %s.", krb5.getPath()));
		}
		
		if(!isVaildKrb5(krb5)){
		    String err = "krb5.conf file content is invalid, the file exist and size not more than 10M, and includes content \"[libdefaults]\", \"[realms]\", \"[domain_realm]\"\n";
			throw new IOException(err+krb5.getAbsolutePath());
		}
		System.setProperty("java.security.krb5.conf", meta.getKrb5Conf());
		
		conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
		// *.keytab路径
		String keytabPath 	= meta.getKeytab();
		File config = new File(keytabPath);
		if(!config.isFile()){
			throw new IOException(String.format("file not exists %s.", config.getPath()));
		}
		// 域
		String principal 	= meta.getPrincipal();
		
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
	}
	
	/**
	 * 测试连接
	 * 
	 * @param meta hdfs连接信息
	 * @return boolean
	 * @throws DatahubException 
	 */
	public static synchronized boolean testConn(HdfsMeta meta) throws DatahubException{
		boolean flg = false;
		try {
			Configuration conf = getConfiguration(meta);
			// 重连次数
			conf.set(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY, "2");
			// 每次超时
			conf.set(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY, "5000");
			
			FileSystem fs = getFileSystem(conf, meta);
			if(fs!=null){
				long used = fs.getStatus().getUsed();
				flg = true;
				LOGGER.info("HDFS {} connect successful, used {}", meta.getUrl()==null?"":meta.getUrl(), FileUtil.getLengthWithUnit(used));
			}
		} catch (DatahubException de) {
			throw de;
		}catch (Exception e) {
			throw new DatahubException(e);
			//throw new DatahubException(e, "尝试连接异常\n%s", e.getMessage());
		}

		return flg;
	}
	
	
	/**
	 * 获取用户根目录
	 * 
	 * @author PengYang
	 * @date 2017-04-21
	 * 
	 * @param meta
	 * @param userName
	 * @return
	 * @throws DatahubException
	 */
	public static String getHomeDirectory(FileSystem fs, String user) throws DatahubException{
		String home = "/";
		if(fs == null){
			return home;
		}
		if(user == null){
			user = "";
		}
		
		try {
			if(fs != null){
				home = String.format("/user/%s", user);
				if(!fs.isDirectory(new Path(home))){
					home =  fs.getHomeDirectory().toUri().getPath();
				}
				
			}
			
		} catch (Exception ex) {
			throw DatahubException.throwDatahubException("get the user home directory error: "+fs.getUri().getPath(), ex);
		}
		
		return home;
	}
	
	/** 
	 * 获取指定路径下的所有文件，根据参数recursive的值来判断是否递归获取 
	 * true表示递归获取，false表示不递归获取 
	 * 
	 * @author SunQinglinwen 
	 * @date 2017-05-23 
	 * 
	 * @return 
	 * @throws DatahubException 
	 */ 
	public static List<Path> listFiles(FileSystem fs, String basePath, Boolean recursive) throws DatahubException{ 

		List<Path> list = new ArrayList<Path>(); 
		try { 

			if(fs != null){ 
				Path path = null; 
				if(StringUtils.isBlank(basePath)){ 
					return list; 
				} 
				path = new Path(basePath); 

				RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, recursive); 

				while(files.hasNext()){ 

					LocatedFileStatus file = files.next();
					if(file.isDirectory()){
						continue;
					}
					Path filePath = file.getPath(); 
					list.add(filePath); 
				} 
			} 

		} catch (Exception ex) { 
		    throw DatahubException.throwDatahubException("list directory error: "+fs.getUri().getPath(), ex);
		} 
		return list; 
	} 

	/**
	 * 
	 * @param writeDir
	 * @param fileName
	 * @param useTmp
	 * @param fs
	 * @param type 1 删目标文件，2备份,其他抛出文件存在异常
	 * @return
	 * @throws IOException
	 */
	public static FSDataOutputStream getOutputStream(String writeDir,String fileName,
			boolean useTmp,int type,FileSystem fs) throws IOException{
		FSDataOutputStream outputStream = null;
		String destPathStr = writeDir + "/" + fileName;
		String destPathTmpStr = destPathStr + ".tmp";
		Path destPath = new Path(destPathStr);
		Path destTmpPath = new Path(destPathTmpStr);
		if (useTmp) {
			outputStream = fs.create(destTmpPath, true);
			if(fs.exists(destPath)){
				if(type == 1){
					if(!fs.delete(destPath, false)){
						throw new IOException("delete " + destPathStr + " fail");
					}
				}else if(type == 2){
					String renameStr = destPathStr + "_" + DateFormatUtils.format(new Date(), "yyyy_MM_dd_HH_mm_ss_SSS");
					if(!rename(fs, destPathStr, renameStr, false)){
						throw new IOException("rename " + destPathStr + " to " + renameStr + " fail");
					}
				}else{
					throw new IOException("file " + destPath + " exist");
				}
				
			}
			
		} else {
			boolean overrite = false;
			if(type == 1){
				overrite = true;
			}else if(type == 2){
				String renameStr = destPathStr + "_" + DateFormatUtils.format(new Date(), "yyyy_MM_dd_HH_mm_ss_SSS");
				if(!rename(fs, destPathStr, renameStr, false)){
					throw new IOException("rename " + destPathStr + " to " + renameStr + " fail");
				}
			}
			outputStream = fs.create(destPath, overrite);
		}
		return outputStream;
	}
	
	/**
	 * 获取数据输出流。
	 * @param fileFullName 文件全路径名
	 * @param useTmp 是否使用缓存文件, true使用，文件内容写到*.tmp文件中, false不是用，文件内容直接写到fileFullName中
	 * @param override 文件存在是否覆盖，true覆盖，false文件重命名(添加时间戳)
	 * @param fs 输出到文件系统
	 * @return FSDataOutputStream
	 * @throws IOException 
	 */
	public static FSDataOutputStream getOutputStream(String fileFullName,
			boolean useTmp, boolean override, FileSystem fs) throws IOException{
		
		FSDataOutputStream outputStream = null;
		String destPathStr = fileFullName;
		String destPathTmpStr = destPathStr + ".tmp";
		Path destPath = new Path(destPathStr);
		Path destTmpPath = new Path(destPathTmpStr);
		
		// 使用缓存文件
		if (useTmp) {
			outputStream = fs.create(destTmpPath, true);
			
		} 
		// 不使用缓存文件
		else {
			// 目标文件存在且重命名，rename destPath
			if(fs.exists(destPath) && !override){
				String dir = FilenameUtils.getFullPath(destPathStr);
				String baseName = FilenameUtils.getBaseName(destPathStr);
				baseName = baseName + "_" + DateFormatUtils.format(new Date(), "yyyy_MM_dd_HH_mm_ss_SSS.");
				String renameStr = baseName + FilenameUtils.getExtension(destPathStr);
				destPath = new Path(dir, renameStr);
			}
			outputStream = fs.create(destPath, true);
			
		}
		return outputStream;
	}
	
	/**
	 * 写内容到输出流
	 * 
	 * @author PengYang
	 * @date 2017-07-03
	 * 
	 * @param str
	 * @param outputStream
	 * @throws IOException
	 */
	public static void writeToHdfs(String content, FSDataOutputStream outputStream)
			throws IOException {
		if(content == null){
			return;
		}
		byte bytes[] = content.getBytes(CharsetUtil.getUTF8());
		outputStream.write(bytes, 0, bytes.length);
		outputStream.flush();
	}
	/**
	 * 写内容到hdfs文件
	 * 
	 * @author PengYang
	 * @date 2017-05-26
	 * 
	 * @param fs FileSystem
	 * @param content 内容
	 * @param hdfsFile hdfs文件
	 * @throws DatahubException
	 */
	public static void writeToHdfs(FileSystem fs, String content, String hdfsFile, boolean isAppend)
			throws DatahubException {
		
		FSDataOutputStream out = null;
		Path file = new Path(hdfsFile);
		try {
			if(fs == null){
				throw new IOException("Hdfs file system is null");
			}
			if(isAppend){
				out = fs.append(file);
			}
			else{
				out = fs.create(file, true);
			}
			writeToHdfs(content, out);
			
		} catch (Exception e) {
			throw new DatahubException(e, "%s write to hdfs error :", hdfsFile);
		} finally {
			IOUtils.closeQuietly(out);
		}
	}
	
	/**
	 * 
	 * 
	 * @param fs hdfs文件系统对象
	 * @param src hdfs源文件
	 * @param dest hdfs目标文件
	 * @param delDestOnExit 目标文件存在, true删除目标文件, false不删除
	 * @return 成功返回true, 失败返回false
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public static boolean rename(FileSystem fs, String src, String dest, boolean delDestOnExit) 
			throws IllegalArgumentException, IOException{
		
		if(fs == null || StringUtils.isBlank(src) || StringUtils.isBlank(dest)){
			return false;
		}
		src = src.replace("\\", "/");
		dest = dest.replace("\\", "/");
		Path destPath = new Path(dest);
		if(delDestOnExit){
			fs.delete(destPath, true);
		}
		
		return fs.rename(new Path(src), destPath);
	}

	/**
	 * 读取文件前n行数据内容。返回读取的内容。
	 * 
	 * @author PengYang
	 * @date 2016-12-19
	 * 
	 * @param path hdfs文件路径
	 * @param lines 前n行数据，为空读取全部内容
	 * @param hdfsUser hdfs用户名
	 * @return String 返回读取的内容。
	 * @throws DatahubException
	 */
	public static String readFile(FileSystem fs, String path, Integer lines ) 
			throws DatahubException {
		StringBuffer buff = new StringBuffer(0);
		FSDataInputStream in = null;
		BufferedReader br = null;
		try {
			if(fs == null){
				return "";
			}
			Path pp = new Path(path);
			in = fs.open(pp);  
			Charset cs = CharsetUtil.getUTF8();
			br = new BufferedReader(new InputStreamReader(in, cs));
			int line = 1;
			while (br.ready()) {
				if(lines!=null && line > lines){
					break;
				}

				buff.append(br.readLine());
				buff.append(CommConst.LINE_SEP);
				line ++;
			}
		} catch (Exception ex) {
			throw DatahubException.throwDatahubException("read hdfs file error: "+path, ex);
		} finally {
			IOUtils.closeQuietly(in);
			IOUtils.closeQuietly(br);
		}
		
		return buff.toString();
	}
	
	/**
	 * hdfs文件是否存在<br/>
	 * 返回：true存在
	 * 返回：false不存在
	 * 
	 * @author PengYang
	 * @date 2017-02-13
	 * 
	 * @param meta hdfs连接信息
	 * @param hdfsFileName hdfs文件全路径名
	 * @return boolean
	 */
	public static boolean exist(FileSystem fs, String hdfsFileName) {
		try {
			if(fs == null){
				throw new DatahubException("FileSystem instance is null.");
			}
			return fs.exists(new Path(hdfsFileName));
		} catch (DatahubException e) {
			LOGGER.error("", e);
		}  catch (Exception e) {
			LOGGER.error("", e);
		}
		return false;
	}
	
	/**
	 * 拷贝文件
	 * 
	 * @author PengYang
	 * @date 2017-02-16
	 * 
	 * @param meta hdfs连接信息
	 * @param srcFileName hdfs源文件全路径名
	 * @param toFileName hdfs目标全路径名
	 * @param override 是否覆盖，true覆盖，false追加
	 * @throws DatahubException
	 */
	public static void copyFile(FileSystem fs, String srcFileName, 
			String toFileName, boolean override) throws DatahubException{
		
		InputStream in = null;
		OutputStream out = null;
		try {
			if(fs == null){
				throw new DatahubException("HDFS FileSystem instance is null.");
			}
			in = fs.open(new Path(srcFileName));
			Path to = new Path(toFileName);
			out = HdfsFileLock.appendTryLock(fs, to, !override, true);
			// copy并关闭流
			org.apache.hadoop.io.IOUtils.copyBytes(in, out, 1024, false);
			
		} catch (Exception ex) {
		    String err = String.format("copy hdfs file error: '%s'-->'%s'", srcFileName, toFileName);
			throw DatahubException.throwDatahubException(err, ex);
		} finally {
			org.apache.hadoop.io.IOUtils.closeStream(in);
			org.apache.hadoop.io.IOUtils.closeStream(out);
		}
	}
	/**
	 * 重命名hdfs文件, 如果文件存在根据后缀规则重命名文件
	 * 
	 * @param fs hdfs连接信息
	 * @param fileName hdfs文件名
	 * @param suffix 后缀规则
	 * @return
	 * @throws DatahubException
	 */
	public synchronized static Path renameWithSuffix(FileSystem fs, Path src, Path dest, RenameType suffix)
			throws DatahubException {
		try {
			String newname = dest.toString();
			if(fs.exists(dest)){
				switch (suffix) {
				case Number:
					newname = renameWithNumber(fs, newname, Integer.MAX_VALUE);
					break;
				case Timestamp:
					newname = renameWithTimestamp(fs, newname, 3);
				case None:
				default:
					break;
				}		
				dest = new Path(newname);
			}
			fs.rename(src, dest);

		} catch (IOException io) {
			throw new DatahubException(io);
		}
		return dest;
	}
	
	/**
	 * 文件名添加时间戳+随机数，如果文件仍然存在, 再追加随机数
	 * 
	 * @author PengYang
	 * @date 2017-03-13
	 * 
	 * @param hdfs hdfs连接信息
	 * @param fileName hdfs文件名
	 * @param count 随机数位数
	 * @return String
	 * @throws DatahubException
	 */
	public synchronized static String renameWithTimestamp(FileSystem fs, String fileName, int count) 
			throws DatahubException {
		
		try {
			
			if(fs == null || fileName == null){
				return fileName;
			}

			String fullPath = FilenameUtils.getFullPath(fileName);
			String baseName = FilenameUtils.getBaseName(fileName);
			String ext 		= FilenameUtils.getExtension(fileName);
			String format	= "_yyyyMMdd_HHmmssSSS";
			
			
			StringBuffer tmp = new StringBuffer(0);

			fileName = FileUtil.concat(FilenameUtils.getFullPath(fileName), tmp.toString());

			count = count<1?1:count;
			int cnt = 1;
			while(cnt<=count){
				
				tmp.append(baseName);
				tmp.append(DateFormatUtils.format(new Date(), format));
				tmp.append("_");
				tmp.append(RandomStringUtils.randomNumeric(count));
				tmp.append(StringUtils.isNotBlank(ext)?"."+ext:ext);
				
				Path path = new Path(FileUtil.concat(fullPath, tmp.toString()));
				if(!fs.exists(path)){
					fileName = path.toString().replace(File.separatorChar, '/');
					break;
				}
				cnt++;
			}
			
			return fileName;

		} catch (Throwable ex) {
            throw DatahubException.throwDatahubException("hdfs file name add timestamp error: "+fileName, ex);
		}
	}
	
	
	public synchronized static String renameWithNumber(FileSystem fs, String fileName, int maxNumber) 
			throws DatahubException {
		
		try {
			
			if(fs == null || fileName == null){
				return fileName;
			}
			
			maxNumber = maxNumber<1?1:maxNumber;
			String fullPath = FilenameUtils.getFullPath(fileName);
			String baseName = FilenameUtils.getBaseName(fileName);
			String ext = FilenameUtils.getExtension(fileName);
			
			StringBuffer tmp = new StringBuffer(0);
			int cnt = 1;
			while(cnt<=maxNumber){
				
				tmp.setLength(0);
				tmp.append(baseName);
				tmp.append("_").append(cnt);
				tmp.append(StringUtils.isNotBlank(ext)?"."+ext:ext);
				
				Path path = new Path(FileUtil.concat(fullPath, tmp.toString()));
				if(cnt == maxNumber || !fs.exists(path)){
					fileName = path.toString().replace(File.separatorChar, '/');
					break;
				}
				cnt ++;
			}
			
			return fileName;
			
		} catch (Throwable e) {
			throw new DatahubException(e, "hdfs file rename error: %s", fileName);
		}
	}
	
	private static String getFilePath(String[] paths) {
		String filePath = null;
		if (paths.length == 2) {
			filePath = paths[1];
		}
		return filePath;
	}
	
	/**
	 * 重载listPath，根据指定参数的值来选择是获取指定路径下文件或者目录
	 * type的值为File，就只获取指定路径下的文件
	 * type的值为Directory，就只获取指定路径下的目录
	 * type的值为All，就获取指定路径下的目录和文件
	 * 
	 * @author SunQinglinwen
	 * @date 2017-05-23
	 * 
	 * @return
	 * @throws DatahubException
	 */
	public static List<FileMeta> listPath(FileSystem fs, String basePath, FileType type) throws DatahubException{
		
		List<FileMeta> list = new ArrayList<FileMeta>();
		try {
			if(fs == null){
				return list;
			}
			
			if(fs != null){
				
				Path path = null;
				
				if(StringUtils.isBlank(basePath)){
					return list;
				}
				
				path = new Path(basePath);
				if(!fs.exists(path)){
					throw new DatahubException("no such file or directory "+path);
				}
				FileStatus baseSt = fs.getFileStatus(path);
				if(type == FileType.Directory && !baseSt.isDirectory()){
					throw new DatahubException("no such directory "+path);
				}
				
				if(type == FileType.File && baseSt.isFile()){
					FileMeta file = new FileMeta(baseSt.getPath().toUri().getPath(), baseSt.getLen());
					file.setFile(type == FileType.File);
					file.setDirectory(type == FileType.Directory);
					if(type == FileType.All){
						file.setFile(fs.isFile(baseSt.getPath()));
						file.setDirectory(!file.isFile());
					}
					file.setOwner(baseSt.getOwner());
					file.setGroup(baseSt.getGroup());
					file.setPermission(baseSt.getPermission().toString());
					
					list.add(file);
					return list;
				}
				
				FileStatus[] status = null;
				switch(type){
				case File:
					status = fs.listStatus(path, new PathFilter() {
						
						@Override
						public boolean accept(Path path) {
							
							try {
								return fs.isFile(path);
							} catch (IOException e) {
								return false;
							}
						}
					});
					break;
				case Directory:
					status = fs.listStatus(path, new PathFilter() {
						
						@Override
						public boolean accept(Path path) {
							
							try {
								return fs.isDirectory(path);
							} catch (IOException e) {
								return false;
							}
						}
					});
					break;
				case All:
					status = fs.listStatus(path);
					break;
				default:
					status = fs.listStatus(path);
				}
				
				for(FileStatus st:status){
					FileMeta file = new FileMeta(st.getPath().toUri().getPath(), st.getLen());
					file.setFile(type == FileType.File);
					file.setDirectory(type == FileType.Directory);
					if(type == FileType.All){
						file.setFile(fs.isFile(st.getPath()));
						file.setDirectory(!file.isFile());
					}
					file.setOwner(st.getOwner());
					file.setGroup(st.getGroup());
					file.setPermission(st.getPermission().toString());
					list.add(file);
				}
			}
			
		} catch (Exception e) {
			//throw new DatahubException(I18nMessages.getI18nMsg(e, basePath));
			//throw new DatahubException(Messages.getMessage("ERROR_0051", basePath), e);
			throw new DatahubException(e);
		}
		
		return list;
	}
	
	
	/**
	 * 创建带时间戳+随机数的文件夹
	 * 
	 * @author Sun Qinglinwen
	 * @date 2017-05-27
	 * 
	 * @param meta hdfs元信息
	 * @param parent 创建文件夹的父目录
	 * @param count 随机数个数
	 * @return String
	 * @throws DatahubException 
	 */
	public synchronized static String createFolderWithTimestamp(FileSystem fs, String parent, int count) throws DatahubException{
		if(fs == null || StringUtils.isBlank(parent)){
			return StringUtils.EMPTY;
		}
		Path path = null;
		try {
			String folder = DateFormatUtils.format(new Date(), CommConst.FOLDER_TIMESTAMP)+RandomStringUtils.randomNumeric(3);
			path = new Path(parent, folder);
			int i=1;
			while(i<count){
				if(fs.exists(path)){
					folder = DateFormatUtils.format(new Date(), CommConst.FOLDER_TIMESTAMP)+RandomStringUtils.randomNumeric(3);
					path = new Path(parent, folder);
				}
				else{
					fs.mkdirs(path);
					break;
				}
				i++;
			}
		} catch (Exception ex) {
		    String err = String.format("create a folder exception with time stamp + random number for directory: %s", parent);
            throw DatahubException.throwDatahubException(err, ex);
		}
		
		return path.toUri().getPath().replace(File.separatorChar, '/');
	}
	
	/**
	 * 复制Hdfs上的文件到web server本地
	 * 
	 * @param meta  HdfsMeta信息
	 * @param srcFileName   源文件路径
	 * @param toFileName    目标文件路径
	 * @return
	 * @throws DatahubException 
	 */
	public static void copyFileToLocal(FileSystem fs, String srcFileName, String toFileName) throws DatahubException{
		
		InputStream input   = null;
		OutputStream output = null;
		try {
			if(fs == null || StringUtils.isBlank(srcFileName) || StringUtils.isBlank(toFileName)){
				return;
			}
			srcFileName = srcFileName.replace("\\", "/");
			toFileName  = toFileName.replace("\\", "/");
			Path srcPath = new Path(srcFileName);
			if(!fs.exists(srcPath)){
				return;
			}
			
			input  = fs.open(srcPath);
			File localFile = new File(toFileName);
			if(!localFile.getParentFile().exists()){
				localFile.getParentFile().mkdirs();
			}
			output = new FileOutputStream(localFile, false);
			LOGGER.info("copy hdfs file to local {} --> {}", srcFileName, toFileName);
			IOUtils.copyLarge(input, output, new byte[1024]);
			output.flush();
			
		}  catch (Throwable ex) {
		    String err = String.format("copy file from hdfs to local error: '%s' --> '%s'", srcFileName, toFileName);
			throw DatahubException.throwDatahubException(err, ex);
		} finally {
			IOUtils.closeQuietly(input);
			IOUtils.closeQuietly(output);
		}
	}
	
	/**
	 * krb5文件内容的有效性check， 符合以下条件的返回true, 否则返回false<br/>
	 * 1. 文件存在<br/>
	 * 2. 文件大小<=10M<br/>
	 * 3. 包含以下内容<br/>
	 * 	<DD>[libdefaults]</DD>
	 * 	<DD>[realms]</DD>
	 * 	<DD>[domain_realm]</DD>
	 * 
	 * @param krb5
	 * @return
	 */
	public static boolean isVaildKrb5(File krb5){
		if(krb5 == null || !krb5.isFile()){
			return false;
		}
		// 大小check
		long max = FileSizeUnit.countBytes(10, FileSizeUnit.MB);
		if(krb5.length() > max){
			return false;
		}
		// 检查内容
		// [libdefaults]
		// [realms]
		// [domain_realm]
		boolean hasLibdefaults 	= false;
		boolean hasRealms 		= false;
		boolean hasDomainRealm 	= false;
		boolean ok				= false;
		
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(krb5)));
			while(reader.ready()){
				String str = reader.readLine();
				if(StringUtils.isBlank(str)){
					continue;
				}
				if(!hasLibdefaults){
					hasLibdefaults = StringUtils.containsIgnoreCase(str, "[libdefaults]");
				}
				if(!hasRealms){
					hasRealms = StringUtils.containsIgnoreCase(str, "[realms]");
				}
				if(!hasDomainRealm){
					hasDomainRealm = StringUtils.containsIgnoreCase(str, "[domain_realm]");
				}
				ok = hasLibdefaults && hasRealms && hasDomainRealm;
				if(ok){
					return ok;
				}
			}
			
		} catch (Exception e) {
			LOGGER.error("isVaildKrb5() error", e);
		} finally {
			IOUtils.closeQuietly(reader);
		}
		
		return ok;
	}
}
