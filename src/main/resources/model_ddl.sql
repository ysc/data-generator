/*
 Navicat Premium Data Transfer

 Source Server         : slave2 mysql
 Source Server Type    : MySQL
 Source Server Version : 50721
 Source Host           : slave2
 Source Database       : demo

 Target Server Type    : MySQL
 Target Server Version : 50721
 File Encoding         : utf-8

 Date: 04/24/2018 10:00:52 AM
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
--  Table structure for `area`
-- ----------------------------
DROP TABLE IF EXISTS `area`;
CREATE TABLE `area` (
  `id` int(11) NOT NULL,
  `city` varchar(10) NOT NULL,
  `city_full` varchar(50) DEFAULT NULL,
  `province` varchar(10) DEFAULT NULL,
  `province_full` varchar(50) DEFAULT NULL,
  `longitude` decimal(10,2) DEFAULT NULL,
  `latitude` decimal(10,2) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
--  Table structure for `brand`
-- ----------------------------
DROP TABLE IF EXISTS `brand`;
CREATE TABLE `brand` (
  `id` int(11) NOT NULL,
  `name` varchar(100) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
--  Table structure for `category`
-- ----------------------------
DROP TABLE IF EXISTS `category`;
CREATE TABLE `category` (
  `id` int(11) NOT NULL,
  `name` varchar(100) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
--  Table structure for `contract`
-- ----------------------------
DROP TABLE IF EXISTS `contract`;
CREATE TABLE `contract` (
  `id` int(11) NOT NULL,
  `contract_price` decimal(10,0) NOT NULL,
  `state` varchar(50) NOT NULL,
  `sign_day` varchar(19) NOT NULL,
  `sales_staff_id` int(11) NOT NULL,
  `customer_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `sales_staff_id` (`sales_staff_id`),
  KEY `customer_id` (`customer_id`),
  KEY `sign_day` (`sign_day`),
  CONSTRAINT `contract_ibfk_1` FOREIGN KEY (`sales_staff_id`) REFERENCES `sales_staff` (`id`),
  CONSTRAINT `contract_ibfk_2` FOREIGN KEY (`customer_id`) REFERENCES `customer` (`id`),
  CONSTRAINT `contract_ibfk_3` FOREIGN KEY (`sign_day`) REFERENCES `day_dimension` (`day_str`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
--  Table structure for `contract_detail`
-- ----------------------------
DROP TABLE IF EXISTS `contract_detail`;
CREATE TABLE `contract_detail` (
  `id` int(11) NOT NULL,
  `item_id` int(11) NOT NULL,
  `item_quantity` int(11) NOT NULL,
  `detail_price` decimal(10,0) NOT NULL,
  `contract_id` int(11) NOT NULL,
  `sign_day` varchar(19) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `item_id` (`item_id`),
  KEY `contract_id` (`contract_id`),
  KEY `sign_day` (`sign_day`),
  CONSTRAINT `contract_detail_ibfk_1` FOREIGN KEY (`item_id`) REFERENCES `item` (`id`),
  CONSTRAINT `contract_detail_ibfk_2` FOREIGN KEY (`contract_id`) REFERENCES `contract` (`id`),
  CONSTRAINT `contract_detail_ibfk_3` FOREIGN KEY (`sign_day`) REFERENCES `day_dimension` (`day_str`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
--  Table structure for `customer`
-- ----------------------------
DROP TABLE IF EXISTS `customer`;
CREATE TABLE `customer` (
  `id` int(11) NOT NULL,
  `age` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `gender` varchar(10) NOT NULL,
  `area_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `area_id` (`area_id`),
  CONSTRAINT `customer_ibfk_1` FOREIGN KEY (`area_id`) REFERENCES `area` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;

-- ----------------------------
--  Table structure for `day_dimension`
-- ----------------------------
DROP TABLE IF EXISTS `day_dimension`;
CREATE TABLE `day_dimension` (
  `day_str` varchar(19) NOT NULL,
  `dayofweek` int(11) NOT NULL,
  `weekofyear` int(11) NOT NULL,
  `month` int(11) NOT NULL,
  `dayofmonth` int(11) NOT NULL,
  `quarter` int(11) NOT NULL,
  `year` int(11) NOT NULL,
  `dayofyear` int(11) NOT NULL,
  PRIMARY KEY (`day_str`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
--  Table structure for `item`
-- ----------------------------
DROP TABLE IF EXISTS `item`;
CREATE TABLE `item` (
  `id` int(11) NOT NULL,
  `price` decimal(11,2) NOT NULL,
  `category_id` int(11) NOT NULL,
  `brand_id` int(11) NOT NULL,
  `name` varchar(100) NOT NULL,
  `discount` decimal(11,2) NOT NULL,
  `color` varchar(10) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `category_id` (`category_id`),
  KEY `brand_id` (`brand_id`),
  CONSTRAINT `item_ibfk_1` FOREIGN KEY (`category_id`) REFERENCES `category` (`id`),
  CONSTRAINT `item_ibfk_2` FOREIGN KEY (`brand_id`) REFERENCES `brand` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
--  Table structure for `sales_staff`
-- ----------------------------
DROP TABLE IF EXISTS `sales_staff`;
CREATE TABLE `sales_staff` (
  `id` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `gender` varchar(10) NOT NULL,
  `area_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `area_id` (`area_id`),
  CONSTRAINT `sales_staff_ibfk_1` FOREIGN KEY (`area_id`) REFERENCES `area` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;

SET FOREIGN_KEY_CHECKS = 1;
