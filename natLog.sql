CREATE TABLE IF NOT EXISTS cmdb_applink.nat_log(
    `out_second` DateTime COMMENT '日志输出时间',
    `source_ip` VARCHAR(100) COMMENT '源IP地址',
    `src_nat_ip` VARCHAR(100) COMMENT 'nat后的源IP地址',
    `dest_ip` VARCHAR(100) COMMENT '目的IP地址',
    `dest_nat_ip` VARCHAR(100) COMMENT 'IP地址',
    `src_port` INT COMMENT '源端口',
    `src_nat_port` INT COMMENT 'nat后的源端口',
    `dest_port` INT COMMENT '目的端口',
    `dest_nat_port` INT COMMENT 'nat后的目的端口',
    `start_time` DateTime  COMMENT '会话开始时间',
    `end_time` DateTime  COMMENT '会话结束时间',
    `in_total_pkg` BIGINT  COMMENT '接收的包数',
    `in_total_byte` BIGINT  COMMENT '接收的字节数',
    `out_total_pkg` BIGINT  COMMENT '发出的包数',
    `out_total_byte` BIGINT  COMMENT '接收的包数',    
) ENGINE=olap
DUPLICATE KEY(out_second, source_ip)
PARTITION BY RANGE(out_second) ()
DISTRIBUTED BY HASH(out_second) BUCKETS AUTO
PROPERTIES
(
    "dynamic_partition.enable" = "true",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.time_unit" = "day",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "natlog-",
    "compression"="zstd"
);