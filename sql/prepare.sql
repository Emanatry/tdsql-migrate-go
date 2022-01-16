CREATE DATABASE IF NOT EXISTS `meta_migration`;
USE `meta_migration`;
CREATE TABLE IF NOT EXISTS `migration_log` (
  `dbname` varchar(255) NOT NULL,
  `tablename` varchar(255) NOT NULL,
  `src` varchar(255) NOT NULL,
  `seek` bigint(32) NOT NULL,
  `temp_prikey` int(16) NOT NULL,
  PRIMARY KEY (`dbname`,`tablename`,`src`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 shardkey=noshardkey_allset;