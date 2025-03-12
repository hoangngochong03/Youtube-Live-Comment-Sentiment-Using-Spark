-- Total comment by each video live
SELECT video_id, COUNT(id) AS total_comments
FROM YOUTUBE_LIVE_COMMENT
GROUP BY video_id
ORDER BY total_comments DESC;
-- Number of comment by sentiemnt label
-- 0: Clean, 1: Offensive, 2: Hate 
SELECT sentiment, COUNT(id) AS total_comments
FROM YOUTUBE_LIVE_COMMENT
GROUP BY sentiment
ORDER BY total_comments DESC;
--Top 3 author with most comment each video
WITH RankedAuthors AS (
    SELECT 
        video_id, 
        author, 
        COUNT(id) AS total_comments,
        ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY COUNT(id) DESC) AS rank_num
    FROM YOUTUBE_LIVE_COMMENT
    GROUP BY video_id, author
)
SELECT video_id, author, total_comments
FROM RankedAuthors
WHERE rank_num <= 3;

-- Number of comment in hour of the day
SELECT EXTRACT(HOUR FROM time_comment) AS hour_of_day, COUNT(id) AS total_comments
FROM YOUTUBE_LIVE_COMMENT
GROUP BY hour_of_day
ORDER BY hour_of_day;
-- Number of comment by each video separate by sentiment
SELECT video_id, sentiment, COUNT(id) AS total_comments
FROM YOUTUBE_LIVE_COMMENT
GROUP BY video_id, sentiment
ORDER BY video_id, total_comments DESC;
-- Average length of comment in each video
SELECT video_id, AVG(LENGTH(raw_comment)) AS avg_comment_length
FROM YOUTUBE_LIVE_COMMENT
GROUP BY video_id
ORDER BY avg_comment_length DESC;
-- Top 5 word that use most in each video
WITH words AS (
    SELECT 
        video_id,
        unnest(string_to_array(processed_comment, ' ')) AS word
    FROM YOUTUBE_LIVE_COMMENT
), word_frequency AS (
    SELECT 
        video_id, 
        word, 
        COUNT(*) AS frequency,
        RANK() OVER (PARTITION BY video_id ORDER BY COUNT(*) DESC) AS rank
    FROM words
    GROUP BY video_id, word
)
SELECT video_id, word, frequency
FROM word_frequency
WHERE rank <= 5
ORDER BY video_id, rank;



