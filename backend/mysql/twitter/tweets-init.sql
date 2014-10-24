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
