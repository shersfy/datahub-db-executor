/*
Database Name:	datahub
MySQL Version:	5.7.17 MySQL Community Server (GPL) for Linux (x86_64)
Author: 		py
Release Date:	2018-08-13
SQL_MODE:		STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION
CMD: 			mysql -u -p -h --default-character-set=utf8 < ../datahub.sql
*/

CREATE DATABASE datahub_db_executor DEFAULT CHARACTER SET utf8 ;
use datahub_db_executor;

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for job_block
-- ----------------------------
DROP TABLE IF EXISTS `job_block`;
CREATE TABLE `job_block` (
  `id` bigint(20) NOT NULL COMMENT '分块ID',
  `job_id` bigint(20) NOT NULL COMMENT '任务ID',
  `log_id` bigint(20) NOT NULL COMMENT '任务日志ID',
  `status` int(1) NOT NULL DEFAULT '1' COMMENT '结果状态(1：执行中(默认)，2：执行成功，3：执行失败)',
  `config` longtext COMMENT '配置参数',
  `create_time` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`,`job_id`,`log_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='任务切片';
