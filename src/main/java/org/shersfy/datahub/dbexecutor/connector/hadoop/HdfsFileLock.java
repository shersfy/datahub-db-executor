package org.shersfy.datahub.dbexecutor.connector.hadoop;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;
import org.shersfy.datahub.commons.exception.DatahubException;
import org.shersfy.datahub.dbexecutor.constant.Const;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsFileLock {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(HdfsFileLock.class);
	
	private HdfsFileLock() {}

	/**
	 * 获取HDFS文件追加输出流, 若文件不存在, 则创建, 文件存在获取追加流。 <br/>
	 * 为了避免多线程同时获得一个文件的输出流, 使用了同步锁。
	 * 
	 * @param fs hdfs文件系统对象
	 * @param path hdfs文件
	 * @param append 内容以覆写或追加方式获取文件输出流, true追加, false覆盖
	 * @param isWait 文件正在被其它程序使用时是否等待, true等待, false
	 * @return FSDataOutputStream
	 * @throws DatahubException
	 */
	public static synchronized FSDataOutputStream appendTryLock(FileSystem fs, Path path, 
			boolean append, boolean isWait) throws DatahubException {
		
		FSDataOutputStream output = null;
		try {
			if(isWait){
				long wait = 1000;
				while(true){
					try {
						if(append){
							// 获取文件追加流
							if(!fs.exists(path)){
								fs.createNewFile(path);
							}
							output = fs.append(path);
						}
						else{
							output = fs.create(path, true);
						}
						break;
						
					} catch (AccessControlException e) {
						throw new DatahubException(e, "Permission denied");
					} catch (Exception e) {
						if(wait > Const.FILE_LOCK_TIMEOUT){
							throw new DatahubException("wait for file to unlock timeout: "+wait);
						}
						LOGGER.info(path.toString());
						LOGGER.info("File is locked, waiting for %s seconds ...", wait/1000);
						Thread.sleep(1000);
						wait += 1000;
					}
				}
			}
			else{
				if(append){
					// 获取文件追加流
					if(!fs.exists(path)){
						fs.createNewFile(path);
					}
					output = fs.append(path);
				}
				else{
					output = fs.create(path, true);
				}
			}
			
		} catch (Throwable ex) {
            throw DatahubException.throwDatahubException("get output stream of HDFS file error: "+fs.getUri(), ex);
		}

		return output;
	}
	
	/**
	 * 解锁HDFS文件
	 */
	public static void release(FSDataOutputStream output){
		if(output != null){
			IOUtils.closeStream(output);
		}
	}

}
