/*
Navicat MySQL Data Transfer

Source Server         : gmall
Source Server Version : 50624
Source Host           : hadoop105:3306
Source Database       : gmall_report

Target Server Type    : MYSQL
Target Server Version : 50624
File Encoding         : 65001

Date: 2020-03-27 16:08:23
*/


topN
CREATE TABLE `ads_appraise_bad_topN` (
  `dt` date NOT NULL,
  `sku_id` int(255) NOT NULL DEFAULT '0',
  `appraise_bad_ratio` double(255,2) DEFAULT NULL,
  PRIMARY KEY (`sku_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

回流用户
CREATE TABLE `ads_back_count` (
  `dt` date NOT NULL,
  `wk_dt` varchar(255) DEFAULT NULL,
  `wastage_count` bigint(255) DEFAULT NULL,
  PRIMARY KEY (`dt`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;


CREATE TABLE `ads_continuity_uv_count` (
  `dt` date NOT NULL,
  `wk_dt` varchar(255) NOT NULL,
  `continuity_count` bigint(255) DEFAULT NULL,
  PRIMARY KEY (`dt`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

CREATE TABLE `ads_continuity_wk_count` (
  `dt` date NOT NULL,
  `wk_dt` varchar(255) DEFAULT NULL,
  `continuity_count` bigint(255) DEFAULT NULL,
  PRIMARY KEY (`dt`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;


CREATE TABLE `ads_new_mid_count` (
  `create_date` date NOT NULL,
  `new_mid_count` bigint(255) DEFAULT NULL,
  PRIMARY KEY (`create_date`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;


CREATE TABLE `ads_order_daycount` (
  `dt` date NOT NULL,
  `order_count` bigint(255) DEFAULT NULL,
  `order_amount` bigint(255) DEFAULT NULL,
  `order_users` bigint(255) DEFAULT NULL,
  PRIMARY KEY (`dt`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;


CREATE TABLE `ads_payment_daycount` (
  `dt` date NOT NULL,
  `order_count` bigint(255) DEFAULT NULL,
  `order_amount` bigint(255) DEFAULT NULL,
  `payment_user_count` bigint(255) DEFAULT NULL,
  `payment_sku_count` bigint(255) DEFAULT NULL,
  `payment_avg_time` double(255,2) DEFAULT NULL,
  PRIMARY KEY (`dt`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

CREATE TABLE `ads_product_cart_topN` (
  `dt` date NOT NULL,
  `sku_id` int(255) DEFAULT NULL,
  `cart_num` bigint(255) DEFAULT NULL,
  PRIMARY KEY (`sku_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

CREATE TABLE `ads_product_favor_topN` (
  `dt` date DEFAULT NULL,
  `sku_id` int(11) DEFAULT NULL,
  `favor_count` bigint(255) DEFAULT NULL,
  PRIMARY KEY(`sku_id`)  USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;


CREATE TABLE `ads_product_info` (
  `dt` date NOT NULL,
  `sku_num` bigint(255) DEFAULT NULL,
  `spu_num` bigint(255) DEFAULT NULL,
  PRIMARY KEY (`dt`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;


CREATE TABLE `ads_product_refund_topN` (
  `dt` date NOT NULL,
  `sku_id` int(255) DEFAULT NULL,
  `refund_ratio` double(255,2) NOT NULL,
  PRIMARY KEY (`sku_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

CREATE TABLE `ads_product_sale_topN` (
  `dt` date NOT NULL,
  `sku_id` int(255) DEFAULT NULL,
  `payment_amount` bigint(255) DEFAULT NULL,
  PRIMARY KEY (`sku_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

CREATE TABLE `ads_sale_tm_category1_stat_mn` (
  `tm_id` int(255) DEFAULT NULL,
  `category1_id` int(255) DEFAULT NULL,
  `category1_name` varchar(255) DEFAULT NULL,
  `buycount` bigint(255) DEFAULT NULL,
  `buy_twice_last` bigint(255) DEFAULT NULL,
  `buy_twice_last_ratio` double(255,2) DEFAULT NULL,
  `buy_3times_last` bigint(255) DEFAULT NULL,
  `buy_3times_last_ratio` bigint(255) DEFAULT NULL,
  `stat_mn` date DEFAULT NULL,
  `stat_date` date NOT NULL,
  PRIMARY KEY (`stat_date`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

CREATE TABLE `ads_silent_count` (
  `dt` date NOT NULL,
  `silent_count` bigint(255) DEFAULT NULL,
  PRIMARY KEY (`dt`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

CREATE TABLE `ads_user_action_convert_day` (
  `dt` date NOT NULL,
  `total_visitor_count` bigint(255) DEFAULT NULL,
  `cart_u_count` bigint(255) DEFAULT NULL,
  `visitor2cart_convert_ratio` double(255,2) DEFAULT NULL,
  `order_u_count` bigint(255) DEFAULT NULL,
  `cart2order_convert_ratio` double(255,2) DEFAULT NULL,
  `payment_u_count` bigint(255) DEFAULT NULL,
  `order2payment_convert_ratio` double(255,2) DEFAULT NULL,
  PRIMARY KEY (`dt`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

CREATE TABLE `ads_user_retention_day_rate` (
  `stat_date` date NOT NULL,
  `create_date` date NOT NULL,
  `retention_count` bigint(255) DEFAULT NULL,
  `new_mid_count` bigint(255) DEFAULT NULL,
  `retention_ratio` double(255,2) DEFAULT NULL,
  PRIMARY KEY (`stat_date`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;
-------------------------------------------------------
DROP TABLE IF EXISTS `ads_user_topic`;
CREATE TABLE `ads_user_topic`  (
  `dt` date NOT NULL,
  `day_users` bigint(255) NULL DEFAULT NULL,
  `day_new_users` bigint(255) NULL DEFAULT NULL,
  `day_new_payment_users` bigint(255) NULL DEFAULT NULL,
  `payment_users` bigint(255) NULL DEFAULT NULL,
  `users` bigint(255) NULL DEFAULT NULL,
  `day_users2users` double(255, 2) NULL DEFAULT NULL,
  `payment_users2users` double(255, 2) NULL DEFAULT NULL,
  `day_new_users2users` double(255, 2) NULL DEFAULT NULL,
  PRIMARY KEY (`dt`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;
============================================================
CREATE TABLE `ads_uv_count` (
  `dt` date NOT NULL,
  `day_count` bigint(255) DEFAULT NULL,
  `wk_count` bigint(255) DEFAULT NULL,
  `mn_count` bigint(255) DEFAULT NULL,
  `is_weekend` varchar(255) DEFAULT NULL,
  `is_monthend` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`dt`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

CREATE TABLE `ads_wastage_count` (
  `dt` date NOT NULL,
  `wastage_count` bigint(255) DEFAULT NULL,
  PRIMARY KEY (`dt`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;


insert into ads_province_order
values (
'2020-03-17',
'CN-11',
321
);

insert into ads_province_order
values (
'2020-03-17',
'CN-50',
322
);

insert into ads_province_order
values (
'2020-03-17',
'CN-31',
400
);

insert into ads_province_order
values (
'2020-03-17',
'CN-12',
201
);


insert into ads_province_order
values (
'2020-03-17',
'CN-34',
222
);


insert into ads_province_order
values (
'2020-03-17',
'CN-35',
324
);



insert into ads_province_order
values (
'2020-03-17',
'CN-44',
101
);


insert into ads_province_order
values (
'2020-03-17',
'CN-52',
135
);


insert into ads_province_order
values (
'2020-03-17',
'CN-46',
172
);


insert into ads_province_order
values (
'2020-03-17',
'CN-13',
193
);


insert into ads_province_order
values (
'2020-03-17',
'CN-23',
432
);


insert into ads_province_order
values (
'2020-03-17',
'CN-41',
345
);


insert into ads_province_order
values (
'2020-03-17',
'CN-42',
231
);


insert into ads_province_order
values (
'2020-03-17',
'CN-43',
129
);


insert into ads_province_order
values (
'2020-03-17',
'CN-32',
42
);


insert into ads_province_order
values (
'2020-03-17',
'CN-36',
84
);


insert into ads_province_order
values (
'2020-03-17',
'CN-22',
76
);


insert into ads_province_order
values (
'2020-03-17',
'CN-21',
333
);



insert into ads_province_order
values (
'2020-03-17',
'CN-63',
164
);


insert into ads_province_order
values (
'2020-03-17',
'CN-37',
100
);


insert into ads_province_order
values (
'2020-03-17',
'CN-14',
150
);


insert into ads_province_order
values (
'2020-03-17',
'CN-51',
93
);


insert into ads_province_order
values (
'2020-03-17',
'CN-53',
42
);


insert into ads_province_order
values (
'2020-03-17',
'CN-33',
77
);


insert into ads_province_order
values (
'2020-03-17',
'CN-45',
99
);


insert into ads_province_order
values (
'2020-03-17',
'CN-15',
66
);


insert into ads_province_order
values (
'2020-03-17',
'CN-64',
200
);


insert into ads_province_order
values (
'2020-03-17',
'CN-65',
111
);


insert into ads_province_order
values (
'2020-03-17',
'CN-54',
32
);


insert into ads_province_order
values (
'2020-03-17',
'CN-91',
99
);


insert into ads_province_order
values (
'2020-03-17',
'CN-92',
78
);