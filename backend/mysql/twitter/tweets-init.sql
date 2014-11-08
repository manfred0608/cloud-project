CREATE TABLE `tweets` (
    `id` bigint(64) unsigned NOT NULL,
    `user_id` bigint(64) unsigned NOT NULL,
    `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `text_censored` varchar(1023) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `sentiment_score` int(10) NOT NULL DEFAULT '0'
)
ENGINE=MyISAM
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
DATA DIRECTORY='/data/'
INDEX DIRECTORY='/data/';

CREATE TABLE `retweets` (
  `id` bigint(64) NOT NULL,
    `response` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  PRIMARY KEY (`id`)
  )
ENGINE=MyISAM
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
DATA DIRECTORY = '/data'
INDEX DIRECTORY = '/data'
;

CREATE TABLE `hashtags` (
  `timeLocation` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
    `rank` int(8) NOT NULL DEFAULT 1,
  `hashtag` varchar(128) NOT NULL DEFAULT '',
    `tweet_id` bigint(64) NOT NULL,
  KEY `time_location` (`time_location`) USING HASH
  )
ENGINE=MyISAM
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
DATA DIRECTORY = '/data'
INDEX DIRECTORY = '/data'
;
