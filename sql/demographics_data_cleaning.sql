-- Data Cleaning project using participant demographic data from a user study about LLM overreliance
-- 1. Remove any unnecessary columns or rows
-- 2. Remove duplicates, if any
-- 3. Rename columns
-- 4. Standardize the data
-- 5. Handle null values



-- make a staging table
CREATE TABLE unfiltered_demographics_staging
(LIKE unfiltered_demographics INCLUDING ALL);

INSERT INTO unfiltered_demographics_staging
SELECT * FROM unfiltered_demographics;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'unfiltered_demographics_staging';

SELECT * FROM unfiltered_demographics_staging;




-- 1. Remove any unnecessary columns or rows
ALTER TABLE unfiltered_demographics_staging
DROP COLUMN "Event Index",
DROP COLUMN "UTC Timestamp",
DROP COLUMN "Local Timestamp",
DROP COLUMN "Local Timezone",
DROP COLUMN "Local Date and Time",
DROP COLUMN "Experiment ID",
DROP COLUMN "Experiment Version",
DROP COLUMN "Tree Node Key",
DROP COLUMN "Schedule ID",
DROP COLUMN "Repeat Key",
DROP COLUMN "Participant Private ID",
DROP COLUMN "Participant Starting Group",
DROP COLUMN "Participant Status",
DROP COLUMN "Participant Completion Code",
DROP COLUMN "Participant External Session ID",
DROP COLUMN "Participant Device Type",
DROP COLUMN "Participant Monitor Size",
DROP COLUMN "Participant Viewport Size",
DROP COLUMN "checkpoint",
DROP COLUMN "Room ID",
DROP COLUMN "Room Order",
DROP COLUMN "Task Name",
DROP COLUMN "Task Version",
DROP COLUMN "counterbalance-yrzj",
DROP COLUMN "counterbalance-o9ux",
DROP COLUMN "branch-vwbd",
DROP COLUMN "checkpoint-16x6",
DROP COLUMN "checkpoint-rn1s",
DROP COLUMN "checkpoint-axe4",
DROP COLUMN "Question2 object-9 Quantised",
DROP COLUMN "Question3 object-10 Quantised",
DROP COLUMN "Question1 object-14 Quantised",
DROP COLUMN "Question1 object-14 Not listed",
DROP COLUMN "Question3 object-17 Quantised",
DROP COLUMN "Question4 object-18 Quantised",
DROP COLUMN "Question5 object-19 Quantised";
-- went down from 51 to 15 columns




-- 2. Remove duplicates, if any
WITH row_num_cte AS
    (
        SELECT ctid, row_number() OVER (PARTITION BY "Participant Public ID" ORDER BY "Participant Public ID") AS row_num
        FROM unfiltered_demographics_staging
    )
DELETE FROM unfiltered_demographics_staging u
USING row_num_cte r
WHERE u.ctid = r.ctid
AND r.row_num > 1;




-- 3. Rename columns
ALTER TABLE unfiltered_demographics_staging
RENAME COLUMN "UTC Date and Time" TO "utc_timestamp";

ALTER TABLE unfiltered_demographics_staging
RENAME COLUMN "Participant Public ID" TO "participant_id";

ALTER TABLE unfiltered_demographics_staging
RENAME COLUMN "Participant Device" TO "device";

ALTER TABLE unfiltered_demographics_staging
RENAME COLUMN "Participant OS" TO "os";

ALTER TABLE unfiltered_demographics_staging
RENAME COLUMN "Participant Browser" TO "browser";

ALTER TABLE unfiltered_demographics_staging
RENAME COLUMN  "randomiser-icfw" TO "study_group";

ALTER TABLE unfiltered_demographics_staging
RENAME COLUMN "Question1 object-8 Value" TO "answering_strategy";

ALTER TABLE unfiltered_demographics_staging
RENAME COLUMN "Question2 object-9 Response" TO "ai_familiarity";

ALTER TABLE unfiltered_demographics_staging
RENAME COLUMN "Question3 object-10 Response" TO "ai_detection";

ALTER TABLE unfiltered_demographics_staging
RENAME COLUMN "Question4 object-11 Value" TO "feedback";

ALTER TABLE unfiltered_demographics_staging
RENAME COLUMN "Question1 object-14 Response" TO "gender_identity_num";

ALTER TABLE unfiltered_demographics_staging
RENAME COLUMN "Question2 object-16 Value" TO "country";

ALTER TABLE unfiltered_demographics_staging
RENAME COLUMN "Question3 object-17 Response" TO "age_range_num";

ALTER TABLE unfiltered_demographics_staging
RENAME COLUMN "Question4 object-18 Response" TO "english_fluency_num";

ALTER TABLE unfiltered_demographics_staging
RENAME COLUMN "Question5 object-19 Response" TO "education_num";




-- 4. Standardize the data

-- 4.a. The question about participant residence country has the country name in different spellings/formats,
-- since it was a free-text field

SELECT DISTINCT country
FROM unfiltered_demographics_staging;

UPDATE unfiltered_demographics_staging
SET country = TRIM(country);

UPDATE unfiltered_demographics_staging
SET country = 'United States'
WHERE country IN ('United States of America', 'USA', 'us', 'united States', 'United states of America');

UPDATE unfiltered_demographics_staging
SET country = 'South Africa'
WHERE country IN ('south africa');

UPDATE unfiltered_demographics_staging
SET country = 'India'
WHERE country IN ('india');

UPDATE unfiltered_demographics_staging
SET country = 'Germany'
WHERE country IN ('germany','german', 'DE');

UPDATE unfiltered_demographics_staging
SET country = 'Poland'
WHERE country IN ('poland');

UPDATE unfiltered_demographics_staging
SET country = 'Netherlands'
WHERE country IN ('the Netherlands');

UPDATE unfiltered_demographics_staging
SET country = 'United Kingdom'
WHERE country IN ('UK');

UPDATE unfiltered_demographics_staging
SET country = 'France'
WHERE country IN ('Paris, France');

UPDATE unfiltered_demographics_staging
SET country = 'UAE'
WHERE country IN ('Dubai, UAE');



-- 4.b. Remove version from browser name
UPDATE unfiltered_demographics_staging
SET browser = split_part(browser, ' ', 1);



-- 4.c. Map gender_identity_num, age_range_num, english_fluency_num, education_num from numbers to labels
-- on the survey platform, the first gender identity was "woman" but the platform returned the
-- option number, i.e., 1 as the answer
-- similarly for other categories


-- 4.c.1. gender_identity_num
-- create a lookup table to get the labels
CREATE TABLE lookup_gender_identity (
    option_num DOUBLE PRECISION PRIMARY KEY,
    label TEXT
);

INSERT INTO lookup_gender_identity VALUES
(1, 'Woman'), (2, 'Man'), (3, 'Transgender'),
(4, 'Non-binary'), (5, 'Prefer not to answer'), (6, 'Other');

-- join this with unfiltered_demographics_staging table to see the labels
SELECT u.participant_id, u.gender_identity_num, l.label AS gender_identity
FROM unfiltered_demographics_staging u
LEFT JOIN lookup_gender_identity l
ON u.gender_identity_num = l.option_num;

-- update the unfiltered_demographics_staging with the labels
ALTER TABLE unfiltered_demographics_staging
ADD COLUMN gender_identity TEXT;

UPDATE unfiltered_demographics_staging u
SET gender_identity = l.label
FROM lookup_gender_identity l
WHERE u.gender_identity_num = l.option_num;



-- 4.c.2. age_range_num
CREATE TABLE lookup_age_range(
    option_num DOUBLE PRECISION PRIMARY KEY,
    label TEXT
);

INSERT INTO lookup_age_range VALUES
(1, 'Under 18'), (2, '18-25'), (3, '26-40'),
(4, '41-60'), (5, 'Over 61'), (6, 'Prefer not to answer');

-- update the unfiltered_demographics_staging with the labels
ALTER TABLE unfiltered_demographics_staging
ADD COLUMN age_range TEXT;

UPDATE unfiltered_demographics_staging u
SET age_range = l.label
FROM lookup_age_range l
WHERE u.age_range_num = l.option_num;



-- 4.c.3. english_fluency_num
CREATE TABLE lookup_english_fluency(
    option_num DOUBLE PRECISION PRIMARY KEY,
    label TEXT
);

INSERT INTO lookup_english_fluency VALUES
(1, 'Basic'), (2, 'Conversational'), (3, 'Fluent');

-- update the unfiltered_demographics_staging with the labels
ALTER TABLE unfiltered_demographics_staging
ADD COLUMN english_fluency TEXT;

UPDATE unfiltered_demographics_staging u
SET english_fluency = l.label
FROM lookup_english_fluency l
WHERE u.english_fluency_num = l.option_num;



-- 4.c.4. education_num
CREATE TABLE lookup_education(
    option_num DOUBLE PRECISION PRIMARY KEY,
    label TEXT
);

INSERT INTO lookup_education VALUES
(1, 'No school-leaving certificate'), (2, 'High-school'),
(3, 'Bachelor''s'), (4, 'Master''s'), (5, 'Doctorate'),
(6, 'Prefer not to answer');

-- update the unfiltered_demographics_staging with the labels
ALTER TABLE unfiltered_demographics_staging
ADD COLUMN education TEXT;

UPDATE unfiltered_demographics_staging u
SET education = l.label
FROM lookup_education l
WHERE u.education_num = l.option_num;



-- Deleting all temp lookup tables
DROP TABLE lookup_gender_identity;
DROP TABLE lookup_age_range;
DROP TABLE lookup_english_fluency;
DROP TABLE lookup_education;



-- Deleting old unnecessary columns
ALTER TABLE unfiltered_demographics_staging
DROP COLUMN gender_identity_num,
DROP COLUMN age_range_num,
DROP COLUMN english_fluency_num,
DROP COLUMN education_num;



-- 4.d. Convert timestamp column to a standard date
SELECT utc_timestamp
FROM unfiltered_demographics_staging;

-- some dates are in Excel serial numbers format, so they have to be handled first
SELECT utc_timestamp
FROM unfiltered_demographics_staging
WHERE utc_timestamp ~ '^\d+(\.\d+)?$';

-- change the Excel serial format to a timestamp
-- 1899-12-30 is the base date
-- the serial number is multiplied with a 1-day interval, so it's converted into number of days
-- for e.g., if the number is 10 it will become 10 days
-- then the converted number of days is added to the base date
-- to_char converts it into the format we want
SELECT to_char(timestamp '1899-12-30' + utc_timestamp::float * interval '1 day', 'MM/DD/YYYY HH24:MI:SS')
FROM unfiltered_demographics_staging
WHERE utc_timestamp ~ '^\d+(\.\d+)?$';

UPDATE unfiltered_demographics_staging
SET utc_timestamp = to_char(timestamp '1899-12-30' + utc_timestamp::float * interval '1 day', 'MM/DD/YYYY HH24:MI:SS')
WHERE utc_timestamp ~ '^\d+(\.\d+)?$';

-- now split the utc_timestamp column to remove the time
UPDATE unfiltered_demographics_staging
SET utc_timestamp = split_part(utc_timestamp, ' ', 1);



-- 4.e. Change columns to appropriate types

-- utc_timestamp from text to date
ALTER TABLE unfiltered_demographics_staging
ALTER COLUMN utc_timestamp TYPE DATE
USING to_date(utc_timestamp, 'DD/MM/YYYY');

-- ai_familiarity from text to int
ALTER TABLE unfiltered_demographics_staging
ALTER COLUMN ai_familiarity TYPE INT
USING NULLIF(ai_familiarity, '')::int;

-- ai_detection from text to int
ALTER TABLE unfiltered_demographics_staging
ALTER COLUMN ai_detection TYPE INT
USING NULLIF(ai_detection, '')::int;




-- 5. Handle null values
-- the only null values are in answering_strategy, ai_familiarity, ai_detection, and feedback
-- these questions were optional and the answers are subjective so they can be left as is