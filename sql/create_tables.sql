CREATE DATABASE ETL_POSTS_ANALYTICS;

CREATE TABLE ETL_POSTS_ANALYTICS.post_analytics(
    post_id INT PRIMARY KEY,
    user_id INT NOT NULL,
    user_name VARCHAR(50) NOT NULL,
    title VARCHAR(225) NOT NULL,
    title_length INT NOT NULL,
    comment_count INT NOT NULL
)