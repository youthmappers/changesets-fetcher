-- BEGIN osm-pds-changesets
CREATE EXTERNAL TABLE IF NOT EXISTS `changesets`(
  `id` bigint, 
  `tags` map<string,string>, 
  `created_at` timestamp, 
  `open` boolean, 
  `closed_at` timestamp, 
  `comments_count` bigint, 
  `min_lat` decimal(9,7), 
  `max_lat` decimal(9,7), 
  `min_lon` decimal(10,7), 
  `max_lon` decimal(10,7), 
  `num_changes` bigint, 
  `uid` bigint, 
  `user` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  's3://osm-pds/changesets';
--END

-- BEGIN osm-pds-planet-history
CREATE EXTERNAL TABLE IF NOT EXISTS `planet_history`(
  `id` bigint, 
  `type` string, 
  `tags` map<string,string>, 
  `lat` decimal(9,7), 
  `lon` decimal(10,7), 
  `nds` array<struct<ref:bigint>>, 
  `members` array<struct<type:string,ref:bigint,role:string>>, 
  `changeset` bigint, 
  `timestamp` timestamp, 
  `uid` bigint, 
  `user` string, 
  `version` bigint, 
  `visible` boolean)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  's3://osm-pds/planet-history';
-- END

-- BEGIN osm-pds-planet
CREATE EXTERNAL TABLE IF NOT EXISTS `planet`(
  `id` bigint, 
  `type` string, 
  `tags` map<string,string>, 
  `lat` decimal(9,7), 
  `lon` decimal(10,7), 
  `nds` array<struct<ref:bigint>>, 
  `members` array<struct<type:string,ref:bigint,role:string>>, 
  `changeset` bigint, 
  `timestamp` timestamp, 
  `uid` bigint, 
  `user` string, 
  `version` bigint, 
  `visible` boolean)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  's3://osm-pds/planet';
--END 

-- BEGIN natural-earth-boundaries
CREATE EXTERNAL TABLE IF NOT EXISTS `natural_earth_admin0` (
	`a3` STRING,
	`country` STRING,
	`name` STRUCT <
        `planet_rollupimary`: STRING,
        `long`: STRING,
        `abbrev`: STRING,
        `formal`: STRING 
    >,
	`entity` STRUCT <
        `sovereignty`: STRING,
        `sovereignty_a3`: STRING,
        `type`: STRING,
        `geounit`: STRING,
        `geounit_a3`: STRING,
        `subunit`: STRING,
        `subunit_a3`: STRING
    >,
	`region` STRUCT <
	    `wb`: STRING,
	    `subregion`: STRING,
	    `continent`: STRING 
    >,
	`geometry` BINARY
)
STORED AS PARQUET
LOCATION 's3://youthmappers-internal-us-east1/country_boundaries/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');
--END 

-- BEGIN youthmappers-from-parquet
CREATE EXTERNAL TABLE IF NOT EXISTS `youthmappers`(
  `uid` bigint, 
  `username` string, 
  `gender` string, 
  `team_id` bigint, 
  `alumni` date, 
  `ymsc` date, 
  `regional_ambassador` date, 
  `mentor_faculty_advisor` date, 
  `chapter` string, 
  `university` string, 
  `city` string, 
  `country` string, 
  `account_created` date, 
  `description` string, 
  `img` string, 
  `changesets` double, 
  `company` string, 
  `geometry` binary)
PARTITIONED BY ( 
  `ds` string)
STORED AS PARQUET
LOCATION 's3://youthmappers-internal-us-east1/mappers'
TBLPROPERTIES ('parquet.compression' = 'ZSTD');
-- END