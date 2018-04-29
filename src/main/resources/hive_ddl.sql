CREATE TABLE `area` (
  `id` int ,
  `city` varchar(10) ,
  `city_full` varchar(50) ,
  `province` varchar(10) ,
  `province_full` varchar(50) ,
  `longitude` float ,
  `latitude` float
) row format delimited fields terminated by ',' stored as textfile;
CREATE TABLE `contract` (
  `id` int ,
  `contract_price` float ,
  `state` varchar(50) ,
  `sign_day` varchar(19) ,
  `sales_staff_id` int ,
  `customer_id` int
) row format delimited fields terminated by ',' stored as textfile;
CREATE TABLE `contract_detail` (
  `id` int ,
  `item_id` int ,
  `item_quantity` int ,
  `detail_price` float ,
  `contract_id` int,
  `sign_day` varchar(19)
) row format delimited fields terminated by ',' stored as textfile;
CREATE TABLE `customer` (
  `id` int ,
  `name` varchar(255) ,
  `gender` varchar(10) ,
  `area_id` int ,
  `age` int
) row format delimited fields terminated by ',' stored as textfile;
CREATE TABLE `day_dimension` (
  `day_str` varchar(19) ,
  `dayofweek` int ,
  `weekofyear` int ,
  `month` int ,
  `dayofmonth` int ,
  `quarter` int ,
  `year` int ,
  `dayofyear` int
) row format delimited fields terminated by ',' stored as textfile;
CREATE TABLE `item` (
  `id` int ,
  `name` varchar(255) ,
  `price` float ,
  `category_id` int,
  `brand_id` int,
  `discount` float,
  `color` varchar(10)
) row format delimited fields terminated by ',' stored as textfile;
CREATE TABLE `sales_staff` (
  `id` int ,
  `name` varchar(255) ,
  `gender` varchar(10) ,
  `area_id` int
) row format delimited fields terminated by ',' stored as textfile;
CREATE TABLE `brand` (
  `id` int ,
  `name` varchar(100)
) row format delimited fields terminated by ',' stored as textfile;
CREATE TABLE `category` (
  `id` int ,
  `name` varchar(100)
) row format delimited fields terminated by ',' stored as textfile;