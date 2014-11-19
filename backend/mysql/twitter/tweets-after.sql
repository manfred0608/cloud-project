CREATE INDEX `user_and_time` ON `tweets` (`user_id`, `created_at`) USING HASH;
LOAD DATA LOCAL INFILE '/path/to/retweets.csv'
INTO TABLE retweets
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE '/path/to/hashtags.csv'
INTO TABLE hashtags
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';
