# Data imported from .csv to local connection on 02-15-2025 at 4:17PM EST by Youssef Agour

# Confirmation of successful import
SELECT *
FROM shift_views
LIMIT 5;

# Total Number of 'Shift Views'
SELECT COUNT(*) AS total_num_views
FROM shift_views;

# Disitnct shift start times - Exported to use in jupyter
SELECT 
	DISTINCT SHIFT_ID,
    SHIFT_START_AT
FROM shift_views;
    
# Time period of my data
## by finding the min and max of the time a shift was created at, we by default get the start and end of our data
## since workers cannot obviously view shifts till they are created
SELECT
	TIMESTAMPDIFF(DAY, first_shift, last_shift) AS num_days 
FROM
(SELECT
	MIN(SHIFT_CREATED_AT) AS first_shift, -- 2024-07-29 17:36:11
    MAX(SHIFT_CREATED_AT) AS last_shift -- 2025-01-21 23:45:56
FROM shift_views) A; -- 176 days worth of data

# Roughly then, on average how many views were accumulated across all job listings in a day?
SELECT 
	ROUND(COUNT(*)/TIMESTAMPDIFF(DAY, MIN(SHIFT_CREATED_AT), MAX(SHIFT_CREATED_AT))) AS Average_Views_Per_Day
FROM shift_views;

# Average views per shift
SELECT
	ROUND(AVG(total_num_views)) AS Average_Views_Per_Shift
FROM
(SELECT 
	SHIFT_ID,
    COUNT(*) AS total_num_views
FROM shift_views
GROUP BY SHIFT_ID) A;

# Median views per shift
WITH ShiftViews AS (
    SELECT SHIFT_ID, COUNT(*) AS total_num_views
    FROM shift_views
    GROUP BY SHIFT_ID
),
RankedViews AS (
    SELECT 
        SHIFT_ID, 
        total_num_views,
        ROW_NUMBER() OVER (ORDER BY total_num_views) as row_num,
        COUNT(*) OVER () as total_rows
    FROM ShiftViews
)
SELECT 
    AVG(total_num_views) AS Median_Views_Per_Shift
FROM RankedViews
WHERE row_num IN (FLOOR((total_rows + 1) / 2), CEILING((total_rows + 1) / 2));


# What percentage of shifts are viewed below the 'average' views a shift recieves?
WITH Below_Average_Views AS
( -- Returning count of views per shift, only returning those that got views below the average views 
SELECT 
	SHIFT_ID,
    COUNT(*) AS total_num_views
FROM shift_views
GROUP BY SHIFT_ID
HAVING COUNT(*) <= (SELECT
						ROUND(AVG(total_num_views)) AS Average_Views_Per_Shift
					FROM (SELECT SHIFT_ID, COUNT(*) AS total_num_views FROM shift_views GROUP BY SHIFT_ID) A)
ORDER BY total_num_views DESC
)
SELECT
	ROUND((SELECT COUNT(*) FROM Below_Average_Views)/(SELECT COUNT(DISTINCT SHIFT_ID) AS Unique_Shifts FROM shift_views)*100, 2)
    AS Percent_of_Shifts_Viewed_Below_Average; 


# Distribution of participating marketplace parties
SELECT
	COUNT(DISTINCT SHIFT_ID) AS Unique_Shifts,
    COUNT(DISTINCT WORKER_ID) AS Unique_Workers,
    COUNT(DISTINCT WORKPLACE_ID) AS Unique_Workplaces
FROM shift_views;

# Is there a large majority of shifts being distrbuted by a handful of workplaces?
WITH Shifts_by_Workplace AS
( -- Number of shifts by workplace, and that share of the total available shifts
SELECT
	WORKPLACE_ID AS Unique_Workplaces,
    COUNT(DISTINCT SHIFT_ID) AS Num_Unique_Shifts,
    ROUND(COUNT(DISTINCT SHIFT_ID)/(SELECT COUNT(DISTINCT SHIFT_ID) FROM shift_views)*100, 2) Percent_of_Total_Shifts
FROM shift_views
GROUP BY Unique_Workplaces
HAVING COUNT(DISTINCT SHIFT_ID) >= -- Including only workplaces that posted more shifts than days for our data, indicating heavy leverage 
	(SELECT 
		TIMESTAMPDIFF(DAY, MIN(SHIFT_CREATED_AT), MAX(SHIFT_CREATED_AT))
	FROM shift_views)
ORDER BY Num_Unique_Shifts DESC
)
SELECT SW.*, SUM(SW.Percent_of_Total_Shifts) OVER () AS Total_Share_of_Shifts
FROM Shifts_by_Workplace SW; 


# Average number of shifts per workplace
SELECT
	WORKPLACE_ID AS Unique_Workplaces,
    COUNT(DISTINCT SHIFT_ID) AS Num_Unique_Shifts,
    AVG(COUNT(DISTINCT SHIFT_ID)) OVER() AS avg_num_shifts
FROM shift_views
GROUP BY Unique_Workplaces
ORDER BY Num_Unique_Shifts DESC;



###########################################################
# SHIFT DATA MINING
###########################################################

# Shift derived values for correlation analysis
SELECT
	DISTINCT SHIFT_ID,
    SHIFT_START_AT,
    SHIFT_CREATED_AT,
    TIME_FORMAT(SEC_TO_TIME(TIMESTAMPDIFF(SECOND, SHIFT_CREATED_AT, SHIFT_START_AT)), '%H:%i:%s') 
    AS time_posted_before_shift,
    DURATION AS hr_duration,
    SLOT AS shift_slot
FROM shift_views;

# total shift views
SELECT 
	SHIFT_ID,
    COUNT(*) AS total_num_views
FROM shift_views
GROUP BY SHIFT_ID
ORDER BY total_num_views DESC;

# Shift specific rates offered
SELECT
	SHIFT_ID,
    MIN(PAY_RATE) AS min_offer,
    MAX(PAY_RATE) AS max_offer,
    ROUND(AVG(PAY_RATE), 2) AS avg_offer
FROM shift_views
GROUP BY SHIFT_ID;

# Accepted rates
SELECT SHIFT_ID, accepted_rate FROM
(SELECT
	DISTINCT SHIFT_ID,
    IS_VERIFIED,
    PAY_RATE AS accepted_rate,
    RANK() OVER(PARTITION BY SHIFT_ID ORDER BY PAY_RATE DESC) AS rn
    -- The same shift appears in the data as worked at different rates, reference record '66fcba4b86cd008f19411a31'
    -- This appears to happen due to a cancelation error, however, since verified to have been working the shift, the maximum rate
    -- was taken to be the implied rate at which the shift was worked
FROM shift_views
WHERE IS_VERIFIED='TRUE') A
WHERE rn =1;

# Final status of a shift: Worked, Canceled, Deleted, Unclaimed
SELECT SHIFT_ID, shift_status
FROM
	(SELECT -- Identifies the final status of a shift based on a logical ranking
		DISTINCT SHIFT_ID, -- When looking at distinct shifts, you look at the distinct pair in their status
		CASE 
        -- For example this query without the window to rank could return 2 rows for the same shift like 'Canceled' if the shift was claimed, 
        -- then canceled by the worker and then 'Deleted' if the shift could not then be backfilled after
        -- This query takes events by logical priority, to assess to final status of the shift at the end of said shift
			WHEN IS_VERIFIED = 'TRUE' THEN 'Worked' 
			WHEN TRIM(CANCELED_AT) != '' THEN 'Canceled'
			WHEN TRIM(DELETED_AT) != '' THEN 'Deleted'
		ELSE 'Unclaimed'
		END AS shift_status,
		ROW_NUMBER() OVER (PARTITION BY SHIFT_ID ORDER BY 
				CASE
                -- Priority ranking here is to match with case statements in previous column
					WHEN IS_VERIFIED = 'TRUE' THEN 1  -- Highest priority
					WHEN TRIM(CANCELED_AT) != '' THEN 2
					WHEN TRIM(DELETED_AT) != '' THEN 3
				ELSE 4                               -- Lowest priority
				END) AS rn
FROM shift_views) A
WHERE rn = 1;

# Putting it together - Exported to use in jupyter
WITH ShiftInfo AS ( -- Query 1
    SELECT
        DISTINCT SHIFT_ID,
        TIME_FORMAT(SEC_TO_TIME(TIMESTAMPDIFF(SECOND, SHIFT_CREATED_AT, SHIFT_START_AT)), '%H:%i:%s') AS time_posted_before_shift,
        DURATION AS hr_duration,
        SLOT AS shift_slot
    FROM shift_views
),
TotalViews AS ( -- Query 2
    SELECT
        SHIFT_ID,
        COUNT(*) AS total_num_views
    FROM shift_views
    GROUP BY SHIFT_ID
),
ShiftRates AS ( -- Query 3
    SELECT
        SHIFT_ID,
        MIN(PAY_RATE) AS min_offer,
        MAX(PAY_RATE) AS max_offer,
        ROUND(AVG(PAY_RATE), 2) AS avg_offer
    FROM shift_views
    GROUP BY SHIFT_ID
),
AcceptedRates AS ( -- Query 4
    SELECT SHIFT_ID, accepted_rate
    FROM (
        SELECT
            DISTINCT SHIFT_ID,
            IS_VERIFIED,
            PAY_RATE AS accepted_rate,
            RANK() OVER (PARTITION BY SHIFT_ID ORDER BY PAY_RATE DESC) AS rn
        FROM shift_views
        WHERE IS_VERIFIED = 'TRUE'
    ) A
    WHERE rn = 1
),
FinalStatuses AS ( -- Query 5
    SELECT SHIFT_ID, shift_status
    FROM (
        SELECT
            DISTINCT SHIFT_ID,
            CASE
                WHEN IS_VERIFIED = 'TRUE' THEN 'Worked'
                WHEN TRIM(CANCELED_AT) != '' THEN 'Canceled'
                WHEN TRIM(DELETED_AT) != '' THEN 'Deleted'
                ELSE 'Unclaimed'
            END AS shift_status,
            ROW_NUMBER() OVER (PARTITION BY SHIFT_ID ORDER BY
                CASE
                    WHEN IS_VERIFIED = 'TRUE' THEN 1
                    WHEN TRIM(CANCELED_AT) != '' THEN 2
                    WHEN TRIM(DELETED_AT) != '' THEN 3
                    ELSE 4
                END) AS rn
        FROM shift_views
    ) A
    WHERE rn = 1
)
SELECT -- Final combined result
    si.SHIFT_ID,
    si.time_posted_before_shift,
    si.hr_duration,
    si.shift_slot,
    tv.total_num_views,
    sr.min_offer,
    sr.max_offer,
    sr.avg_offer,
    ar.accepted_rate,
    fs.shift_status
FROM ShiftInfo si
LEFT JOIN TotalViews tv ON si.SHIFT_ID = tv.SHIFT_ID
LEFT JOIN ShiftRates sr ON si.SHIFT_ID = sr.SHIFT_ID
LEFT JOIN AcceptedRates ar ON si.SHIFT_ID = ar.SHIFT_ID
LEFT JOIN FinalStatuses fs ON si.SHIFT_ID = fs.SHIFT_ID;

# Percentage of workers who actively worked and picked up shifts via the offers pushed to them
WITH ActiveWorkers AS
(-- Workers who were confirmed to have worked a shift in the timeframe
SELECT
	WORKER_ID,
    COUNT(*) AS count_of
FROM shift_views
WHERE IS_VERIFIED='TRUE'
GROUP BY WORKER_ID
ORDER BY 2 DESC
)
SELECT
	(SELECT COUNT(*) FROM ActiveWorkers) / (SELECT COUNT(DISTINCT WORKER_ID) FROM shift_views);
    
# Estimating earnings per worker
SELECT WORKER_ID AS 'Worker ID', ROUND(SUM(PAY_RATE * DURATION)) * 2 AS 'Estimated Annualized Earnings', COUNT(*) AS 'Shifts Worked'
FROM (
SELECT
	DISTINCT WORKER_ID,
    SHIFT_ID, 
    PAY_RATE,
    DURATION
FROM shift_views
WHERE IS_VERIFIED='TRUE' AND TRIM(CANCELED_AT) = '' ) A
GROUP BY WORKER_ID
ORDER BY 2 DESC
LIMIT 10;





