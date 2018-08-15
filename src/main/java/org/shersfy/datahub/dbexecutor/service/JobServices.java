package org.shersfy.datahub.dbexecutor.service;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.commons.lang.StringUtils;
import org.shersfy.datahub.commons.beans.Result;
import org.shersfy.datahub.commons.beans.Result.ResultCode;
import org.shersfy.datahub.commons.constant.JobConst.JobLogStatus;
import org.shersfy.datahub.commons.exception.DatahubException;
import org.shersfy.datahub.commons.meta.ColumnMeta;
import org.shersfy.datahub.commons.meta.DBMeta;
import org.shersfy.datahub.commons.meta.FieldData;
import org.shersfy.datahub.commons.meta.GridData;
import org.shersfy.datahub.commons.meta.RowData;
import org.shersfy.datahub.commons.meta.TableMeta;
import org.shersfy.datahub.commons.utils.JobLogUtil;
import org.shersfy.datahub.commons.utils.JobLogUtil.JobLogPayload;
import org.shersfy.datahub.commons.utils.LocalHostUtil;
import org.shersfy.datahub.dbexecutor.connector.db.DbConnectorInterface;
import org.shersfy.datahub.dbexecutor.connector.db.TablePartition;
import org.shersfy.datahub.dbexecutor.feign.DhubDbExecutorClient;
import org.shersfy.datahub.dbexecutor.job.JobBlockTask;
import org.shersfy.datahub.dbexecutor.model.JobBlock;
import org.shersfy.datahub.dbexecutor.model.JobBlockPk;
import org.shersfy.datahub.dbexecutor.params.config.DataSourceConfig;
import org.shersfy.datahub.dbexecutor.params.config.JobConfig;
import org.shersfy.datahub.dbexecutor.params.template.InputDbParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.alibaba.fastjson.JSON;

@RefreshScope
@Transactional
@Component
public class JobServices {

    public static String SERVICE_NAME = LocalHostUtil.IP;
    
    Logger LOGGER = LoggerFactory.getLogger(JobServices.class);

    @Value("${job.block.max}")
    private int blockMax = 1;

    @Value("${job.block.maxIndexNotExist}")
    private int blockMaxIndexNotExist = 1;

    @Value("${job.block.records}")
    private int blcokRecords = 10000;
    
    @Value("${job.block.threads}")
    private int threadsSize = 100;
    
    @Value("${job.block.progressPeriodSeconds}")
    private int progressPeriodSeconds = 10;
    
    @Value("${job.block.cacheSize}")
    private int cacheSize = 10; // 10M

    @Resource
    private LogManager logManager;

    @Resource
    private JobBlockService jobBlockService;
    
    @Resource
    private DhubDbExecutorClient dhubDbExecutorClient;
    
    private ExecutorService executor;
    
    @PostConstruct
    private void init() {
        executor = Executors.newFixedThreadPool(threadsSize);
    }


    /***
     * 执行分块任务
     * @param blockConfig
     */
    @Async
    public void executeJobBlock(String blockPk) {
        if(StringUtils.isBlank(blockPk)) {
            return;
        }
        
        JobBlockPk pk = JSON.parseObject(blockPk, JobBlockPk.class);
        JobBlock block= jobBlockService.findByPk(pk);
        // 记录不存在
        if(block == null) {
            LOGGER.error("block not exist: {}", pk);
            JobLogPayload payload = new JobLogPayload(pk.getJobId(), pk.getLogId(), 
                "block not exist: block id="+pk.getId());
            logManager.sendMsg(payload);
            return;
        } 
        
        // 添加到执行器
        Map<String, Object> datamap = new HashMap<>();
        datamap.put("cacheSize", cacheSize);
        datamap.put("progressPeriodSeconds", progressPeriodSeconds);
        executor.submit(new JobBlockTask(block, jobBlockService, datamap));
    }

    /**
     * 拆分任务为若干块子任务
     * @param config 任务配置
     */
    @Async
    @Transactional(propagation=Propagation.NOT_SUPPORTED)
    public void executeJob(Long jobId, Long logId, String cfg) {

        JobConfig config = JSON.parseObject(cfg, JobConfig.class);
        
        List<JobConfig> blocks = new ArrayList<>();
        try {
            List<InputDbParams> parts = split(config.getInputParams());

            for(InputDbParams input : parts) {
                JobConfig blk = (JobConfig) config.clone();
                blk.setInputParams(input);
                blocks.add(blk);
            }

        } catch (Throwable ex) {
            LOGGER.error("", ex);
            String err = ex.getMessage();
            logManager.sendMsg(JobLogUtil.getMsgData(Level.ERROR, jobId, logId, err));
        }

        // 历史残留数据处理
        JobBlock where = new JobBlock();
        where.setJobId(jobId);
        where.setLogId(logId);
        
        List<JobBlock> history = jobBlockService.findList(where);
        
        // 历史切片和当前切片数量不等
        boolean where1 = !history.isEmpty() && history.size()!=blocks.size();
        // 历史切片全部正确执行完毕
        boolean where2 = !history.isEmpty() && jobBlockService.isFinished(history);
        
        if(where1 || where2) {
            int cnt = jobBlockService.deleteBlocks(history.get(0));
            history.clear();
            LOGGER.info("jobId={}, logId={}, deleted history blocks size={}", jobId, logId, cnt);
        }
        
        
        // 切片配置写入数据库
        // 全部插入
        if(history.isEmpty()) {
            blocks.forEach(block -> jobBlockService.insert(parse(jobId, logId, block)));
        } 
        // 全部更新
        else {
            
            blocks.removeIf(block->{
                
                int index = history.size()==1?0:block.getInputParams().getBlock().getIndex()+1;
                JobBlock old = history.get(index);
                // 参数无变化，已执行成功，无需再执行
                if(old.getStatus() == JobLogStatus.Successful.index()
                    && old.getConfig().equals(block.toString())){
                    LOGGER.info("block already executed successful: {}", block);
                    return true;
                }
                
                return false;
            });
            
            blocks.forEach(block -> jobBlockService.updateByPk(parse(jobId, logId, block)));
        }
        
        // 不需要事务支持，因为一旦删除操作事务没提交，分发出去的切片在数据库中还能查到
        dispatch(jobId, logId, blocks);

    }
    
    private JobBlock parse(Long jobId, Long logId, JobConfig block) {
        JobBlock info = new JobBlock();
        info.setId(Long.valueOf(block.getInputParams().getBlock().getIndex()));
        info.setJobId(jobId);
        info.setLogId(logId);
        info.setConfig(block.toString());
        info.setStatus(JobLogStatus.Dummy.index());
        return info;
    }

    /**
     * 分发任务
     * @param blocks
     * @param retry 重试次数
     */
    public void dispatch(Long jobId, Long logId, List<JobConfig> blocks) {
        List<JobConfig> errors = new ArrayList<>();
        for(JobConfig block : blocks) {
            // 下发配置
            String text = dhubDbExecutorClient.callExecuteJobBlock(new JobBlockPk(parse(jobId, logId, block)).toString());
            Result res  = JSON.parseObject(text, Result.class);
            if(res.getCode()!=ResultCode.SUCESS) {
                LOGGER.error("dispatch block error: {}", block);
                errors.add(block);
            }
        }
        
    }

    /***
     * 分块算法
     * @param param
     * @return
     * @throws DatahubException 
     * @throws CloneNotSupportedException 
     */
    public List<InputDbParams> split(InputDbParams param) throws DatahubException {

        param.setWhere(param.getWhere()==null?"":param.getWhere());
        
        List<InputDbParams> blocks = new ArrayList<>();
        DataSourceConfig ds = param.getDataSource();
        DBMeta dbMeta = ds.getDBMeta();

        DbConnectorInterface connector = DbConnectorInterface.getInstance(dbMeta);
        Connection conn = null;
        try {
            conn = connector.connection();
            // 第一步，选择分块字段
            TableMeta table = param.getTable();
            List<ColumnMeta> columns = connector.getColumns(table, conn);
            ColumnMeta blockColumn   = getBlockColumn(columns);
            
            TablePartition defaultPart = new TablePartition();
            defaultPart.setPartColumn(columns.get(0));

            // 没有满足条件的字段，返回1块
            if(blockColumn == null){
                param.setBlock(defaultPart);
                blocks.add(param);
                return blocks;
            }

            // 第二步，计算合适的分块块数，考虑datahub节点尽量负载均衡
            // 获取分块字段的最小值和最大值
            String countSql = String.format("SELECT COUNT(1) FROM %s %s", connector.getFullTableName(table), param.getWhere());
            long totalSize  = connector.queryCount(countSql, conn);
            
            double max     = 0;
            double min     = 0;

            int blockCnt = countBlockCnt(totalSize, blcokRecords, blockMax);
            blockCnt = blockColumn.isPk()||blockColumn.isUk() ?blockCnt :blockMaxIndexNotExist;

            // 不处理1个分块
            if(blockCnt <= 1){
                param.setBlock(defaultPart);
                blocks.add(param);
                return blocks;
            }

            String fullName = connector.getFullTableName(table);
            String blockColumnName = connector.quotObject(blockColumn.getName());

            // 分块最小值和最大值查询
            StringBuffer minAndMaxSql = new StringBuffer(0);
            minAndMaxSql.append("SELECT MIN(").append(blockColumnName).append(") miv, ");
            minAndMaxSql.append("MAX(").append(blockColumnName).append(") mav ");
            minAndMaxSql.append("FROM ").append(fullName);
            // where
            minAndMaxSql.append(param.getWhere());

            GridData data = connector.executeQuery(conn, minAndMaxSql.toString());
            List<RowData> rows = data.getRows();
            if(!rows.isEmpty()){
                RowData row = rows.get(0);
                FieldData fdMin = row.getFields().get(0);
                FieldData fdMax = row.getFields().get(1);
                switch (blockColumn.getDataType()) {
                    //整型
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                    case Types.BIT:
                        min =  Long.valueOf(String.valueOf(fdMin.getValue()));
                        max =  Long.valueOf(String.valueOf(fdMax.getValue()));
                        break;
                    case Types.BOOLEAN:
                        min = 0;
                        max = 1;
                        break;

                        //双精度浮点型
                    case Types.REAL:
                    case Types.FLOAT:
                    case Types.DOUBLE:
                        //数字型
                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        if(fdMin.getValue() instanceof BigDecimal){
                            BigDecimal minBd = (BigDecimal) fdMin.getValue();
                            BigDecimal maxBd = (BigDecimal) fdMax.getValue();
                            min = minBd.doubleValue();
                            max = maxBd.doubleValue();
                        }
                        else{
                            min =  (double) fdMin.getValue();
                            max =  (double) fdMax.getValue();
                        }
                        break;

                        //日期时间型
                    case Types.TIMESTAMP:
                    case Types.TIMESTAMP_WITH_TIMEZONE:
                        String tt0 = (String) fdMin.getValue();
                        String tt1 = (String) fdMax.getValue();
                        min = Timestamp.valueOf(tt0).getTime();
                        max = Timestamp.valueOf(tt1).getTime();
                        break;
                    case Types.DATE:
                        Date d0 = (Date) fdMin.getValue(); 
                        Date d1 = (Date) fdMax.getValue();;
                        min = Date.valueOf(d0.toString()).getTime();
                        max = Date.valueOf(d1.toString()).getTime();
                        break;
                    case Types.TIME:
                    case Types.TIME_WITH_TIMEZONE:
                        Time t0 = (Time) fdMin.getValue(); 
                        Time t1 = (Time) fdMax.getValue();
                        min = Time.valueOf(t0.toString()).getTime();
                        max = Time.valueOf(t1.toString()).getTime();
                        break;
                    default:
                        blocks.add(param);
                        param.setBlock(defaultPart);
                        return blocks;
                }

            }

            // 第三步，计算切点
            // 2. 分块字段IS NULL的情况
            StringBuffer condition = new StringBuffer(0);

            TablePartition part = new TablePartition();
            condition.append(blockColumnName).append(" IS NULL");
            part.setIndex(-1);
            part.setCondition(condition.toString());
            part.setPartColumn(blockColumn);

            InputDbParams clone = (InputDbParams) param.clone();
            clone.setBlock(part);
            blocks.add(clone);

            // 2. 分块字段IS NOT NULL的情况
            // 分块区间值计算(公差)
            double section = (max - min) / blockCnt;
            for (int p = 0; p < blockCnt; p++) {

                double start = min   + section * p;
                double end   = start + section;

                part = new TablePartition();
                condition.setLength(0);
                List<Object> conditionArgs = new ArrayList<>();

                if(max == min){
                    condition.append(blockColumnName).append(" = ? ");
                }
                // 第一块
                else if (p == 0) {
                    condition.append(blockColumnName).append(" >= ?").append(" AND ");
                    condition.append(blockColumnName).append(" < ?");
                    start = min;
                } 
                // 分区字段是非字符串类型, 最后一块
                else if (p == blockCnt - 1) {
                    condition.append(blockColumnName).append(" >= ?").append(" AND ");
                    condition.append(blockColumnName).append(" <= ?");
                    end = max;
                }
                // 分区字段是非字符串类型, 中间块
                else {
                    condition.append(blockColumnName).append(" >= ?").append(" AND ");
                    condition.append(blockColumnName).append(" < ?");
                }

                conditionArgs.add(parseToObject(start, blockColumn));
                if(max != min){
                    conditionArgs.add(parseToObject(end, blockColumn));
                }

                part.setIndex(p);
                part.setCondition(condition.toString());
                part.setConditionArgs(conditionArgs);
                part.setPartColumn(blockColumn);

                clone = (InputDbParams) param.clone();
                clone.setBlock(part);
                blocks.add(clone);
                
                if(max == min){
                    break;
                }
            }

            
        } catch (Throwable ex) {
            throw DatahubException.throwDatahubException("split job config error:", ex);
        } finally {
            if(connector!=null) {
                connector.close(conn);
            }
        }

        return blocks;
    }

    /**计算分块数**/
    private int countBlockCnt(long totalSize, int minSizePerBlock, int maxBlockSize){
        long blockSize = 1;
        if(totalSize<=minSizePerBlock){
            return 1;
        }

        blockSize = totalSize%minSizePerBlock==0?totalSize/minSizePerBlock:totalSize/minSizePerBlock+1;

        return Long.valueOf(blockSize<maxBlockSize?blockSize:maxBlockSize).intValue();
    }

    protected ColumnMeta getBlockColumn(List<ColumnMeta> columns){
        ColumnMeta blockColumn = null;
        // 1. 主键或Unique Index, 且 数值型、时间型字段作为分块字段，字符型字段不做分块
        // 2. 步骤1没有得到分块字段的，选择数值型、时间型字段作为分块字段，字符型字段不做分块
        //    以上两种都不满足，返回空
        List<Integer> types = Arrays.asList(new Integer[] {
            Types.BIGINT,
            Types.BIT,
            Types.BOOLEAN,
            Types.DATE,
            Types.DECIMAL,
            Types.DOUBLE,
            Types.FLOAT,
            Types.INTEGER,
            //字符型
            // Types.CHAR,
            // Types.NCHAR,
            // Types.VARCHAR,
            // Types.NVARCHAR,
            // Types.LONGVARCHAR,
            // Types.LONGNVARCHAR,
            Types.NUMERIC,
            Types.REAL,
            Types.SMALLINT,
            Types.TIME,
            Types.TIME_WITH_TIMEZONE,
            Types.TIMESTAMP,
            Types.TIMESTAMP_WITH_TIMEZONE,
            Types.TINYINT
        });

        // 步骤1， 分块字段有主键或唯一索引，优先选择第一个数值型或时间型索引字段作为分块字段，不满足条件继续第2步；
        for(ColumnMeta col : columns){
            if((col.isPk()||col.isUk()) && types.contains(col.getDataType())){
                blockColumn = col;
                return blockColumn;
            }
        }
        // 步骤2， 分块字段无索引或不满足步骤1，优先选择第一个数值型或时间型字段作为分块字段，不包含字数值型或时间型字段的，不做分块处理，即1块；
        for(ColumnMeta col : columns){
            if(types.contains(col.getDataType())){
                blockColumn = col;
                break;
            }
        }

        return blockColumn;
    }

    /**
     * 转为object对象
     * 
     * @param value long值
     * @param partColumn 分区字段
     * @return
     */
    private Object parseToObject(double value, ColumnMeta partColumn){
        Object obj = value;
        if(partColumn==null){
            return obj;
        }
        int type = partColumn.getDataType();
        switch (type) {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.BIT:
                obj = (long)value;
                break;
            case Types.BOOLEAN:
                obj = Boolean.valueOf(((int)value)!=0);
                break;

                //双精度浮点型
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
                //数字型
            case Types.NUMERIC:
            case Types.DECIMAL:
                obj = value;
                break;

                //日期时间型
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                obj = new Timestamp((long)value);
                break;
            case Types.DATE:
                obj = new Date((long)value);
                break;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                obj = new Time((long)value);
                break;
            default:
                break;
        }

        return obj;
    }

}
