CREATE STREAM youtube_videos (
  video_id VARCHAR KEY,
  title VARCHAR,
  views INTEGER,
  comments INTEGER,
  likes INTEGER
) WITH (
  KAFKA_TOPIC ='youtube_videos',
  PARTITIONS =1,
  VALUE_FORMAT='Avro'
);
--to check excution of producer
SELECT * 
FROM YOUTUBE_VIDEOS
EMIT CHANGES;

-- to track the change on comments count of videos
CREATE TABLE youtube_changes WITH (KAFKA_TOPIC='youtube_changes') AS SELECT
  video_id,
  latest_by_offset(title) AS title,
  latest_by_offset(comments, 2)[1] AS comments_previous,
  latest_by_offset(comments, 2)[2] AS comments_current
FROM  YOUTUBE_VIDEOS 
GROUP BY video_id;

--
SELECT *
FROM YOUTUBE_CHANGES
WHERE comments_previous <> comments_current
EMIT CHANGES;

--

CREATE STREAM telegram_outbox (
  `chat_id` VARCHAR,
  `text` VARCHAR
) WITH (
  KAFKA_TOPIC='telegram_outbox',
  PARTITIONS=1,
  VALUE_FORMAT='Avro'
);

--
INSERT INTO telegram_outbox (
  `chat_id`,
  `text`
) VALUES ('5371495410','Hello from Kafka');
--
CREATE STREAM youtube_changes_stream WITH (
  KAFKA_TOPIC='youtube_changes', 
  VALUE_FORMAT='Avro'
);