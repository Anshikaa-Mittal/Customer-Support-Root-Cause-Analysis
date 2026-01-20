create database RCA;
use RCA;
drop table support_tickets;
CREATE TABLE support_tickets (
    Ticket_ID VARCHAR(12) PRIMARY KEY,
    Open_Time DATETIME NOT NULL,
    Close_Time DATETIME NULL,
    Category VARCHAR(50),
    Issue_Type VARCHAR(100),
    Priority VARCHAR(20),
    SLA_Limit_Hours INT,
    Resolution_Hours INT,
    SLA_Breached TINYINT(1),
    Agent VARCHAR(50),
    Status VARCHAR(30)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Deloitte_IT_Incidents - Deloitte_IT_Incidents.csv'
INTO TABLE support_tickets
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
 Ticket_ID,
 Open_Time,
 @Close_Time,
 Category,
 Issue_Type,
 Priority,
 SLA_Limit_Hours,
 Resolution_Hours,
 SLA_Breached,
 Agent,
 Status
)
SET Close_Time = NULLIF(@Close_Time, '');


#Query 1
SELECT 
    Category,
    Issue_Type,
    COUNT(Ticket_ID) AS Total_Complaints,
    -- Calculate percentage contribution to total volume
    ROUND(COUNT(Ticket_ID) * 100.0 / (SELECT COUNT(*) FROM support_tickets), 2) AS Percentage_Impact
FROM support_tickets
GROUP BY Category, Issue_Type
ORDER BY Total_Complaints DESC
LIMIT 10;

#query 2
SELECT 
    Issue_Type,
    AVG(Resolution_Hours) AS Avg_Resolution_Time,
    MAX(Resolution_Hours) AS Worst_Case_Time
FROM support_tickets
WHERE close_time is not null
GROUP BY Issue_Type
HAVING COUNT(Ticket_ID) > 50 -- Ignore one-off rare bugs
ORDER BY Avg_Resolution_Time DESC
LIMIT 5;

# Query 3
SELECT * FROM (
    SELECT 
        Category,
        Issue_Type,
        COUNT(Ticket_ID) AS Issue_Count,
        -- Rank issues 1, 2, 3... based on volume within their specific Category
        RANK() OVER (PARTITION BY Category ORDER BY COUNT(Ticket_ID) DESC) as Rank_In_Category
    FROM support_tickets
    GROUP BY Category, Issue_Type
) AS Ranked_Issues
WHERE Rank_In_Category = 1; -- Show me only the #1 top problem for each category

# Query 4
SELECT 
    DATE(Open_Time) AS Ticket_Date,
    COUNT(Ticket_ID) AS Daily_Tickets,
    SUM(SLA_Breached) AS Daily_Breaches,
    -- Running total of breaches to visualize the trend line
    SUM(SUM(SLA_Breached)) OVER (ORDER BY DATE(Open_Time)) AS Cumulative_Breaches
FROM support_tickets
GROUP BY DATE(Open_Time)
ORDER BY Ticket_Date;


# Query 5
SELECT 
    Agent,
    COUNT(Ticket_ID) as Tickets_Handled,
    AVG(Resolution_Hours) as Agent_Avg_Time,
    -- Compare agent's time to the overall company average
    AVG(Resolution_Hours) - AVG(AVG(Resolution_Hours)) OVER () as Diff_From_Company_Avg
FROM support_tickets
WHERE close_time is not null
GROUP BY Agent
ORDER BY Diff_From_Company_Avg DESC; -- Positive numbers = Slower than average


