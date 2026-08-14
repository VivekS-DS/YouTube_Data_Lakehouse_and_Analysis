-- =====================================================================
-- schema.sql
-- YouTube Data Lakehouse and Analysis — MySQL warehouse schema
--
-- Creates the `youtube` database and the four tables that
-- migrate_data_to_mysql() in app.py inserts into: channel, playlist,
-- video, comment. Column names/types are derived from the fields
-- extracted via the YouTube Data API and the values app.py passes to
-- each INSERT statement.
--
-- Usage:
--   mysql -u root -p < schema.sql
-- =====================================================================

CREATE DATABASE IF NOT EXISTS youtube
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE youtube;

-- ---------------------------------------------------------------------
-- channel
-- One row per YouTube channel (from fetch_channel_details()).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS channel (
    channel_id           VARCHAR(32)     NOT NULL,
    channel_name          VARCHAR(255)    NOT NULL,
    country               VARCHAR(10),
    channel_views         BIGINT UNSIGNED NOT NULL DEFAULT 0,
    subscription          BIGINT UNSIGNED NOT NULL DEFAULT 0,
    channel_uploads       INT UNSIGNED    NOT NULL DEFAULT 0,
    channel_status        VARCHAR(20),
    channel_playlist_id   VARCHAR(64),
    PRIMARY KEY (channel_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------
-- playlist
-- One row per playlist owned by a channel (from fetch_playlist()).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS playlist (
    playlist_id    VARCHAR(64)     NOT NULL,
    channel_id     VARCHAR(32)     NOT NULL,
    playlist_name  VARCHAR(255),
    PRIMARY KEY (playlist_id),
    KEY idx_playlist_channel_id (channel_id),
    CONSTRAINT fk_playlist_channel
        FOREIGN KEY (channel_id) REFERENCES channel (channel_id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------
-- video
-- One row per uploaded video (from fetch_videos()).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS video (
    video_id        VARCHAR(32)      NOT NULL,
    channel_id      VARCHAR(32)      NOT NULL,
    channel_name    VARCHAR(255)     NOT NULL,
    video_title     VARCHAR(500)     NOT NULL,
    duration        INT UNSIGNED     NOT NULL DEFAULT 0,   -- seconds
    release_date    DATETIME,
    thumbnail       VARCHAR(500),
    video_quality   VARCHAR(10),
    views           BIGINT UNSIGNED  NOT NULL DEFAULT 0,
    likes           BIGINT UNSIGNED  NOT NULL DEFAULT 0,
    favorite        INT UNSIGNED     NOT NULL DEFAULT 0,
    comment_count   INT UNSIGNED     NOT NULL DEFAULT 0,
    description     TEXT,
    caption_status  VARCHAR(10),
    PRIMARY KEY (video_id),
    KEY idx_video_channel_id (channel_id),
    KEY idx_video_release_date (release_date),
    KEY idx_video_views (views),
    KEY idx_video_likes (likes),
    KEY idx_video_comment_count (comment_count),
    CONSTRAINT fk_video_channel
        FOREIGN KEY (channel_id) REFERENCES channel (channel_id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------
-- comment
-- One row per top-level comment (from fetch_video_comments()).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS comment (
    comment_id      VARCHAR(64)     NOT NULL,
    video_id        VARCHAR(32)     NOT NULL,
    author_name     VARCHAR(255),
    comments        TEXT,
    commented_date  DATETIME,
    PRIMARY KEY (comment_id),
    KEY idx_comment_video_id (video_id),
    CONSTRAINT fk_comment_video
        FOREIGN KEY (video_id) REFERENCES video (video_id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
