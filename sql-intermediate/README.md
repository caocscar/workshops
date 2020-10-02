# Intermediate SQL

Here is the [Google Slide Deck](https://docs.google.com/presentation/d/1sx7FL58BHbzPWb59Tq1S38QBL1KjNEjse3IyqK4nohY/edit?usp=sharing) for the workshop.

Link to web-based database [db-fiddle](https://www.db-fiddle.com) for practicing SQL.

Link to the [Covid dataset](https://gist.github.com/caocscar/b9a1418e5fd9c2cd69bb6f9d67fbc05a) for the exercises.
<hr>

## Workshop Material
Query Syntax Covered:
- IF
- CASE
- WHEN
- ROLLUP
- GROUPING
- REPLACE
- OVER (Window Functions)
- RANK
- DENSE_RANK
- WINDOW
- PARTITION BY
- WITH (Common Table Expressions)

Schema Syntax Covered:
- CREATE TABLE
- INSERT
- DELETE
- DROP
- IF [NOT] EXISTS
- NOT NULL
- PRIMARY KEY
- AUTO_INCREMENT
- SHOW COLUMNS
- INSERT IGNORE INTO
- UNIQUE
- ALTER TABLE
- ADD COLUMN
- DROP COLUMN
- MODIFY COLUMN
- UPDATE
- INDEX

Miscellaneous Syntax:
- SHOW COLUMNS
- DESCRIBE
- SHOW TABLES
- SHOW INDEX

## Appendix
<details>
  <summary>Solutions Hiding Here</summary>
  
#### Practice 1
```SQL
SELECT County, Day, Deaths,
    CASE
        WHEN Deaths = 0 THEN -1
        WHEN Deaths = 1 THEN 0
        ELSE LOG(Deaths)
    END AS deathIndex
FROM Covid
ORDER BY deathIndex DESC
```

#### Practice 2
```SQL
SELECT IF(GROUPING(County), 'Total', County) as County,
    SUM(Deaths) AS Total
FROM Covid
GROUP BY County WITH ROLLUP
```

#### Practice 2b  
```SQL
SELECT 
    IF(GROUPING(County),'Michigan Total', IF(GROUPING(CP), 'County Total', County)) AS COUNTY,
    SUM(Deaths) AS DeathTotal,
    CP
FROM Covid
GROUP BY County, CP WITH ROLLUP
```

#### Practice 3
```SQL
SELECT REPLACE(County, "St", "Saint") AS County, 
    Day,
    Cases, 
    RANK() OVER (PARTITION BY Day ORDER BY Cases DESC) AS 'Rank'
FROM Covid
WHERE Day BETWEEN '2020-09-24' AND '2020-09-30'
AND County LIKE 'S%'
AND CP = 'Confirmed'
```

#### Practice 3b
```SQL
SELECT County, Day, Cases,
    LAG(Cases, 7) OVER (ORDER BY Day) As 'WeekAgo' 
FROM Covid
WHERE County = 'Wayne' AND CP = 'Confirmed'
ORDER BY Day DESC
```

#### Practice 4
```SQL
WITH cte AS
(
    SELECT Day, 
    WEEK(Day) AS Week,
    CP, 
    SUM(Cases) as Total
    FROM Covid
    GROUP BY Day, CP
)

SELECT Week, MAX(Total)
FROM cte
GROUP BY Week
```

#### Practice A
```SQL
CREATE TABLE Michigan (
    Category VARCHAR(6),
    Value VARCHAR(7),
    `Cases` INTEGER,
    `Deaths` INTEGER,
    `CaseFatalityRatio` FLOAT
);

INSERT INTO Michigan
    (Category, `Value`, Cases, `Deaths`, `CaseFatalityRatio`)
VALUES
    ('Gender', 'Female', '61390', '3212', '0.051'),
    ('Gender', 'Male', '57956', '3511', '0.061'),
    ('Gender', 'Unknown', '281', null, null);
```

#### Practice B
```SQL
CREATE TABLE MI (
    ID INT AUTO_INCREMENT,
    `Day` VARCHAR(3),
    `Category` VARCHAR(9),
    `Value` VARCHAR(19) NOT NULL,
    `Pct of Cases` FLOAT,
    `Pct of Deaths` FLOAT,
    PRIMARY KEY (ID)
);

INSERT INTO MI
    (`Day`, `Category`, `Value`, `Pct of Cases`, `Pct of Deaths`)
VALUES
    ('Sat', 'Ethnicity', 'Hispanic/Latino', '0.08', '0.03'),
    ('Sat', 'Ethnicity', 'Non-Hispanic Latino', '0.69', '0.85'),
    ('Sat', 'Ethnicity', 'Unknown', '0.23', '0.12');
```

#### Practice B2
```SQL
INSERT INTO MI 
    (Day, Value)
VALUES
    ('Sun', null);

INSERT INTO MI
    (ID, Day, Value)
VALUES
    (3, 'Sun', 'Unknown');
```

#### Practice C
```SQL
CREATE TABLE mi (
    `Category` VARCHAR(3),
    `Value` VARCHAR(8) UNIQUE,
    `Cases` INTEGER,
    `Deaths` INTEGER DEFAULT 0,
    `CaseFatalityRatio` FLOAT DEFAULT 0
);

INSERT INTO mi
    (`Category`, `Value`, `Cases`)
VALUES
    ('Age', '0 to 19', '13342'),
    ('Age', 'Unknown', '109');
  
INSERT INTO mi
VALUES
    ('Age', '20 to 29', '23038', '29', '0.001'),
    ('Age', '30 to 39', '16858', '71', '0.004'),
    ('Age', '40 to 49', '17345', '219', '0.013'),
    ('Age', '50 to 59', '18393', '541', '0.029'),
    ('Age', '60 to 69', '14656', '1188', '0.081'),
    ('Age', '70 to 79', '9374', '1808', '0.193'),
    ('Age', '80+', '8312', '2864', '0.345');
```

#### Practice D
```SQL
-- Schema SQL window
CREATE TABLE mi (
    `Category` VARCHAR(3),
    `Value` VARCHAR(8),
    `Cases` INTEGER,
    `Deaths` INTEGER,
    `CaseFatalityRatio` FLOAT
);

-- Query SQL window
ALTER TABLE mi
ADD COLUMN day VARCHAR(10);

ALTER TABLE mi
DROP COLUMN Category,
DROP COLUMN CaseFatalityRatio;

ALTER TABLE mi
MODIFY COLUMN Cases VARCHAR(6);

DESCRIBE mi;
```

#### Practice E
```SQL
-- Schema SQL window
CREATE TABLE mi (
    `Category` VARCHAR(3),
    `Value` VARCHAR(8),
    `Cases` INTEGER,
    `Deaths` INTEGER,
    `CaseFatalityRatio` FLOAT,
    INDEX(Cases)
);

INSERT INTO mi
    (`Category`, `Value`, `Cases`)
VALUES
    ('Age', '0 to 19', '13342'),
    ('Age', 'Unknown', '109');
  
INSERT INTO mi
VALUES
    ('Age', '20 to 29', '23038', '29', '0.001'),
    ('Age', '30 to 39', '16858', '71', '0.004'),
    ('Age', '40 to 49', '17345', '219', '0.013'),
    ('Age', '50 to 59', '18393', '541', '0.029'),
    ('Age', '60 to 69', '14656', '1188', '0.081'),
    ('Age', '70 to 79', '9374', '1808', '0.193'),
    ('Age', '80+', '8312', '2864', '0.345');

UPDATE mi
SET Cases = 1400
WHERE Deaths IS NULL;

UPDATE mi
SET Deaths = 5, CaseFatalityRatio = 5
WHERE Deaths IS NULL;

-- Query SQL window
SELECT * FROM mi; 

DESCRIBE mi;
SHOW INDEX FROM mi; -- Alternatively
```
</details>
