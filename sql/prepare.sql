CREATE DATABASE IF NOT EXISTS `meta_migration`;
USE `meta_migration`;
CREATE TABLE IF NOT EXISTS `migration_log` (
  `dbname` varchar(32) NOT NULL,
  `tablename` varchar(32) NOT NULL,
  `src` varchar(32) NOT NULL,
  `last_finished_line` bigint(20) NOT NULL,
  PRIMARY KEY (`dbname`,`tablename`,`src`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;