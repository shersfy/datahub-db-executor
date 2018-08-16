package org.shersfy.datahub.dbexecutor.job;

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.shersfy.datahub.commons.constant.JobConst.JobLogStatus;
import org.shersfy.datahub.commons.constant.JobConst.JobType;
import org.shersfy.datahub.commons.meta.ColumnMeta;
import org.shersfy.datahub.commons.meta.FieldData;
import org.shersfy.datahub.commons.meta.HdfsMeta;
import org.shersfy.datahub.commons.meta.TableMeta;
import org.shersfy.datahub.commons.utils.FileUtil;
import org.shersfy.datahub.commons.utils.JobLogUtil;
import org.shersfy.datahub.dbexecutor.connector.db.DbConnectorInterface;
import org.shersfy.datahub.dbexecutor.connector.db.TablePartition;
import org.shersfy.datahub.dbexecutor.connector.hadoop.HdfsUtil;
import org.shersfy.datahub.dbexecutor.model.JobBlock;
import org.shersfy.datahub.dbexecutor.model.JobBlockPk;
import org.shersfy.datahub.dbexecutor.params.config.DataSourceConfig;
import org.shersfy.datahub.dbexecutor.params.config.JobConfig;
import org.shersfy.datahub.dbexecutor.service.JobBlockService;
import org.shersfy.datahub.dbexecutor.service.JobServices;
import org.shersfy.datahub.dbexecutor.service.LogManager;
import org.shersfy.datahub.dbexecutor.service.TableLockService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import com.alibaba.fastjson.JSON;

public class JobBlockTask implements Callable<JobBlock>{

    protected Logger LOGGER = LoggerFactory.getLogger(JobServices.class);

    private Long jobId = null;
    private Long logId = null;
    private Long blkId = null;

    /**切片副本数量**/
    private int repeatDispatch;
    /**进度汇报周期**/
    private int progressPeriodSeconds;
    /**临时数据**/
    private String tmp;

    private JobBlock block;

    private JobConfig config;

    private JobBlockService service;

    private LogManager logManager;

    private TableLockService lockService;

    private LinkedList<Method> finishedMethods;
    
    public JobBlockTask() {}

    public JobBlockTask(JobBlock block, JobBlockService service, Map<String, Object> datamap) {
        super();
        this.block = block;
        this.config = JSON.parseObject(block.getConfig(), JobConfig.class);

        this.service = service;
        this.logManager = service.getLogManager();
        this.lockService = service.getTableLockService();
        this.finishedMethods = new LinkedList<>();

        this.jobId = block.getJobId();
        this.logId = block.getLogId();
        this.blkId = block.getId();

        this.progressPeriodSeconds = Integer.parseInt(datamap.get("progressPeriodSeconds").toString());

    }

    @Override
    public JobBlock call() throws Exception {

        try {

            before();

            execute();

            after();

        } catch (Exception ex) {
            exception(ex);
        } finally {
            finallyDo();
        }

        return block;
    }


    protected void execute() throws Exception{

        JobType outType  = JobType.valueOfAlias(config.getOutputType().toLowerCase());
        switch (outType) {
            case HDFS:
                writeToHdfs();
                break;
            case DatabaseMove:
                break;
            case Hive:
            case HiveSpark:
                break;
            default:
                sendMsg(Level.ERROR, "not support output type "+config.getOutputType());
                break;
        }

    }

    protected void before() {

        String thread = String.format("job_%s_%s_%s", jobId, logId, blkId);
        Thread.currentThread().setName(thread);

        sendMsg(Level.INFO, "begining ...");

        JobBlock udp = new JobBlock();
        udp.setId(blkId);
        udp.setJobId(jobId);
        udp.setLogId(logId);
        udp.setService(JobServices.SERVICE_NAME);
        udp.setStatus(JobLogStatus.Executing.index());

        service.updateByPk(udp);
    }

    protected void after() throws Exception {

        JobBlockPk pk = new JobBlockPk(block);
        JobBlock old  = service.findByPk(pk);
        // 是否已有切片副本执行完毕
        if(old!=null && old.getStatus() != JobLogStatus.Successful.index()) {
            JobBlock udp = new JobBlock();
            udp.setId(blkId);
            udp.setJobId(jobId);
            udp.setLogId(logId);
            udp.setTmp(tmp);
            udp.setStatus(JobLogStatus.Successful.index());
            service.updateByPk(udp);
        }

        JobBlock where = new JobBlock();
        where.setJobId(jobId);
        where.setLogId(logId);
        List<JobBlock> blocks = service.findList(where);
        if(service.isFinished(blocks) 
            && lockService.lock("job_block", logId.toString(), JobServices.SERVICE_NAME)) {
            // 所有切片完成执行方法, 必须包含一个参数blocks
            while(finishedMethods.listIterator().hasNext()) {
                Method method = finishedMethods.poll();
                method.invoke(this, blocks);
            }

            // 向job manager汇报执行成功
            service.callUpdateLog(logId, JobLogStatus.Successful.index());
            
            int cnt = service.deleteBlocks(block);
            String msg = String.format("deleted blocks size=%s, all blocks finished", cnt);
            sendMsg(Level.INFO, msg);
        }

        sendMsg(Level.INFO, "execute successful");

    }

    protected void exception(Throwable ex) {
        sendMsg(Level.ERROR, "execute job block error");
        sendMsg(Level.ERROR, ex.getMessage());
        LOGGER.error("", ex);
    }

    protected void finallyDo() {
        sendMsg(Level.INFO, "finished");
        if(logId!=null) {
            lockService.unlock("job_block", logId.toString());
        }
    }

    protected void sendMsg(Level level, String msg) {
        msg = msg==null?"":msg;
        msg = String.format("blockId=%s, %s", blkId, msg);
        logManager.sendMsg(JobLogUtil.getMsgData(level, jobId, logId, msg));
    }

    protected void progress(int progresId, long write) {
        String progress = String.format("{\"progresId\":%s, \"write\":%s}", progresId, write);
        sendMsg(Level.INFO, progress);
    }


    /**写hdfs**/
    protected void writeToHdfs() {

        DataSourceConfig ds = config.getInputParams().getDataSource();

        DbConnectorInterface connector = null;
        Connection conn         = null;
        PreparedStatement pstmt = null;
        ResultSet rs            = null;

        FSDataOutputStream outputStream = null;
        try {
            // 新建连接
            connector = DbConnectorInterface.getInstance(ds.getDBMeta());
            conn = connector.connection();

            TableMeta table  = config.getInputParams().getTable();
            TablePartition part = config.getInputParams().getBlock();

            String fullname = connector.getFullTableName(table);
            String baseSql  = String.format("SELECT * FROM %s", fullname);
            String quotColName = connector.quotObject(part.getPartColumn().getName());

            String where = config.getInputParams().getWhere();
            String sql   = this.makePartSql(part, quotColName, baseSql, where);

            // 文件名
            String hdfsFilename = getHdfsFilename();

            String partFilename = "%s_part_%s.tmp";
            partFilename = String.format(partFilename, FilenameUtils.getBaseName(hdfsFilename), part.getIndex());
            partFilename = FileUtil.concat(FilenameUtils.getFullPath(hdfsFilename), partFilename);
            // 游标式读取数据
            pstmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            connector.prepareStatementByCursor(pstmt);

            if(StringUtils.isNotBlank(part.getCondition())) {

                StringBuffer condition = new StringBuffer(part.getCondition());
                String[] arr = part.getCondition().split("\\?");

                List<Object> sqlArgs = part.getConditionArgs();
                if(sqlArgs!=null){
                    int parameterIndex = 1;
                    condition.setLength(0);
                    for(Object arg :sqlArgs){
                        condition.append(arr[parameterIndex-1]).append(arg);
                        pstmt.setObject(parameterIndex++, arg);
                    }
                }
            }

            sendMsg(Level.INFO, "loading data ...");
            sendMsg(Level.INFO, "sql= "+sql);
            rs = pstmt.executeQuery();

            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();

            // header
            List<ColumnMeta> header = new ArrayList<ColumnMeta>();
            for (int i = 1; i <= colCount; i++) {
                ColumnMeta fm = new ColumnMeta(meta.getColumnName(i));
                fm.setTypeName(meta.getColumnTypeName(i));
                fm.setDataType(meta.getColumnType(i));
                fm.setColumnSize(meta.getColumnDisplaySize(i));
                fm.setDecimalDigits(meta.getScale(i));
                fm.setRemarks(meta.getColumnLabel(i));
                fm.setNullable(meta.isNullable(i));
                header.add(fm);
            }

            int progresId = 0;
            long writeCnt = 0;
            long start    = System.currentTimeMillis();

            HdfsMeta hdfs = config.getOutputHdfsParams().getHdfs();
            FileSystem fs = HdfsUtil.getFileSystem(hdfs);
            // 副本文件名处理
            partFilename  = HdfsUtil.renameWithNumber(fs, partFilename, repeatDispatch);
            outputStream  = HdfsUtil.createHdfsFile(fs, partFilename, hdfs.getUserName());
            

            StringBuilder cache = new StringBuilder();
            String colSep = config.getOutputHdfsParams().getColumnSep();
            while (rs.next()) {
                // 读取一行
                String line = readLine(connector, conn, rs, header, colSep);

                writeCnt++;
                cache.append(line).append("\n");
                flush(cache, outputStream);
                
                long end = System.currentTimeMillis();
                if(end-start > progressPeriodSeconds*1000){
                    // 汇报进度
                    progresId++;
                    progress(progresId, writeCnt);
                    start = end;
                }
            }
            
            progress(progresId, writeCnt);

            sendMsg(Level.INFO, "part "+partFilename);
            tmp = partFilename;
            
            String name = "mergeHdfsParts";
            finishedMethods.add(this.getClass().getDeclaredMethod(name, List.class));

        } catch (Exception ex) {
            LOGGER.error("", ex);
            sendMsg(Level.ERROR, "write block to hdfs error");
            sendMsg(Level.ERROR, ex.getMessage());

        } finally {
            if(connector!=null) {
                connector.close(rs, pstmt, conn);
            }
            IOUtils.closeQuietly(outputStream);
        }
    }

    /**合并分块文件**/
    protected void mergeHdfsParts(List<JobBlock> blocks) {

        sendMsg(Level.INFO, "merge start ...");
        FSDataOutputStream output = null;
        FileSystem fs  = null;
        Path hdfsFile = new Path(getHdfsFilename());
        try {
            HdfsMeta hdfs = config.getOutputHdfsParams().getHdfs();
            fs = HdfsUtil.getFileSystem(hdfs);

            if(!HdfsUtil.exist(fs, hdfsFile.toUri().getPath())) {
                output = HdfsUtil.createHdfsFile(fs, hdfsFile.toUri().getPath(), hdfs.getAppUser(), false);
            } else {
                output = HdfsUtil.append(fs, hdfsFile.toUri().getPath());
            }

            for(JobBlock blk :blocks) {
                if(StringUtils.isBlank(blk.getTmp())) {
                    continue;
                }
                FSDataInputStream input = fs.open(new Path(blk.getTmp()));
                IOUtils.copyLarge(fs.open(new Path(blk.getTmp())), output, new byte[1024]);
                output.flush();
                IOUtils.closeQuietly(input);
                String msg = String.format("append: %s --> %s", blk.getTmp(), hdfsFile.toUri().getPath());
                sendMsg(Level.INFO, msg);
                HdfsUtil.deleteFile(blk.getTmp(), fs);
            }
            
            String msg = String.format("hdfs file %s, size %s", hdfsFile.toUri().getPath(), 
                FileUtil.getLengthWithUnit(fs.getFileStatus(hdfsFile).getLen()));
            sendMsg(Level.INFO, msg);
            sendMsg(Level.INFO, "merge successful");
            
        } catch (Exception ex) {
            LOGGER.error("", ex);
            sendMsg(Level.ERROR, "merge hdfs part files error");
            sendMsg(Level.ERROR, ex.getMessage());
        } finally {
            IOUtils.closeQuietly(output);
        }
    }

    private String getHdfsFilename() {
        TableMeta table  = config.getInputParams().getTable();
        // 文件名
        String hdfsFilename = config.getOutputHdfsParams().getFile();

        String hdfsDir = config.getOutputHdfsParams().getDirectory();
        hdfsDir = StringUtils.isBlank(hdfsDir)?FilenameUtils.getFullPath(hdfsFilename):hdfsDir;

        hdfsFilename = StringUtils.isBlank(hdfsFilename) ?FileUtil.concat(hdfsDir, table.getName()+".txt") :hdfsFilename;
        return hdfsFilename;
    }

    /**读取一行数据**/
    private String readLine(DbConnectorInterface connector, Connection conn, ResultSet rs, 
                            List<ColumnMeta> header, String columnSep) throws SQLException {

        columnSep = columnSep==null?HdfsUtil.DEFAULT_COLUMN_SEP:columnSep;
        StringBuilder line = new StringBuilder();
        for(int i=0; i<header.size(); i++) {
            FieldData field = new FieldData(rs.getObject(header.get(i).getName()));
            field.setName(header.get(i).getName());
            connector.formatFieldData(conn, header.get(i), field);
            line.append(field.getValue());
            if(i != header.size()-1) {
                line.append(columnSep);
            }
        }

        return line.toString();
    }

    /**生成分块执行SQL**/
    private String makePartSql(TablePartition part, String quotColName, 
                               String baseSql, String where) {

        StringBuffer partSql = new StringBuffer(0);
        partSql.append(baseSql);

        if(StringUtils.isNotBlank(part.getCondition())){
            if(StringUtils.isBlank(where)){
                partSql.append(" WHERE ");
                partSql.append(part.getCondition());
            } else{
                partSql.append(where);
                partSql.append(" AND (").append(part.getCondition()).append(") ");
            }
            partSql.append(" ORDER BY ").append(quotColName);
        }

        return partSql.toString();
    }

    /**
     * 将余下的缓冲区数据写入到HDFS
     * @param cache
     * @param outputStream 
     * @throws IOException 
     */
    private void flush(StringBuilder cache, FSDataOutputStream outputStream) throws IOException {
        if(cache.length()==0) {
            return;
        }
        HdfsUtil.writeToHdfs(cache.toString(), outputStream);
        cache.setLength(0);
    }

}
