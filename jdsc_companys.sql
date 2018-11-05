/*
Navicat MySQL Data Transfer

Source Server         : zjc_test
Source Server Version : 50623
Source Host           : 10.10.10.16:10025
Source Database       : csc_spider

Target Server Type    : MYSQL
Target Server Version : 50623
File Encoding         : 65001

Date: 2018-10-15 15:23:06
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for jdsc_companys
-- ----------------------------
DROP TABLE IF EXISTS `jdsc_companys`;
CREATE TABLE `jdsc_companys` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键Id',
  `companyName` varchar(50) DEFAULT '' COMMENT '企业名称',
  `productCategory` varchar(500) DEFAULT '' COMMENT '产品分类',
  `address` varchar(100) DEFAULT '' COMMENT '地址',
  `contact` varchar(40) DEFAULT '' COMMENT '联系人',
  `contactNumber` varchar(40) DEFAULT '' COMMENT '联系电话',
  `hotline` varchar(40) DEFAULT '' COMMENT '订货热线',
  `source` varchar(100) DEFAULT '' COMMENT '来源',
  `url` varchar(50) DEFAULT '' COMMENT '网址',
  `fax` varchar(40) DEFAULT NULL COMMENT '传真',
  `createTime` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '获取日期',
  `updateTime` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '更新日期',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_jdsc_source` (`source`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=23599 DEFAULT CHARSET=utf8 COMMENT='采集jdsc35的商户';
