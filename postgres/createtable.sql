CREATE TABLE YOUTUBE_LIVE_COMMENT(
	id serial primary key,
	video_id varchar(50),
	author varchar(100),
	time_comment timestamp without time zone,
	raw_comment text,
	processed_comment text,
	sentiment varchar(20)
	
)