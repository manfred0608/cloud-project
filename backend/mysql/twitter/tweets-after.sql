CREATE INDEX `user_and_time` ON `tweets` (`user_id`, `created_at`) USING HASH;
