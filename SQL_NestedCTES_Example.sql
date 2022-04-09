
---///* This is an example of a query written in T-SQL to create a table containing the cumulative daily compliance rate (the % of metrics that meet a target goal of 
        ---95% compliant) and compliance status (Compliant = "YES", Uncompliant = "NO") of each metric for each day of the month for the past two years, up to most 
		---current day of the current month (in this case, the previous day for the sake of data completeness): *///---

---///* Solution *///---

///* 1. Create the tbl_Metrics_Compliance_Daily_Status table with an incremental primary key *///

	USE Metrics_Database
	GO

	CREATE TABLE tbl_Metrics_Compliance_Daily_Status (
	PK INT NOT NULL IDENTITY PRIMARY KEY,
	AddDate DATE NULL, 
	[Date] DATE NULL, 
	Metric_Code  NVARCHAR(255) NULL, 
	Metric_Description NVARCHAR(MAX) NULL, 
	Compliance_Rate FLOAT NULL, 
	Goal FLOAT NULL, 
	Compliance_Status NVARCHAR(50) NULL, 
	Report_Link NVARCHAR(MAX) NULL)

---///* 2. Create query to truncate and load tblComplianceDailyStatus table *///---

	Truncate table tbl_Metrics_Compliance_Daily_Status

	;WITH CTE1 AS (SELECT DISTINCT Metric, [Date], CompliantQTY, NoncompliantQTY, DialQTY,
	SUM(CompliantQTY) OVER (PARTITION BY Metric, YEAR(Date), MONTH([Date])) AS 'Total_Compliant',
	SUM(NoncompliantQTY) OVER (PARTITION BY Metric, YEAR(Date), MONTH([Date])) AS 'Total_NonCompliant',
	SUM(DialQTY) OVER (PARTITION BY Metric, YEAR(Date), MONTH([Date])) AS 'Total_Metrics'
	FROM[dbo].[Metrics_RealTime_Dashboard]),   ---[CTE1. A SELECT statement aggregates (SUMs) the total compliant, total noncompliant, and total metrics columns,
										          ---partitioned over each metric, month and year row, from the [Metrics_RealTime_Dashboard] source 
											      ---table to generate the numerator and denominator for the final cumulative daily compliance rate (%) formula].
	CTE2 AS (
	SELECT DISTINCT GETDATE() AS [AddDate], [Date], a.Metric AS Metric_Code, 
	c.MetricDescription AS Metric_Description, CAST(ROUND(Total_Compliant * 100 / (NULLIF(Total_Metrics,0)), 2) AS DECIMAL(5,2)) AS Compliance_Rate, 
	b.Goal, b.Link AS Report_Link
	FROM CTE1 a
	LEFT JOIN tbl_Compliance_Metrics_Info_Table b
	ON a.[Metric] = b.Metric
	LEFT JOIN tblAll_ReportOwner c
	ON a.[Metric] = c. Metric)     ---[CTE2. SELECT statement calculates the cumulative daily compliance rate (%, in rounded, decimal format) from the totals stored in 
								   ---the CTE1 table (if values in the Metric_Totals column are '0' Compliance_Rate then values will be treated as NULLS to prevent a 
								   ---divide by zero error) and appendS metric info data (department ownership and report link info) from the tbl_Compliance_Dials_Info_Table 
								   ---and All_ReportOwner tables via two LEFT JOINS.]
	INSERT INTO tbl_Metrics_Compliance_Daily_Status (AddDate, [Date], Metric_Code, Metric_Description, Compliance_Rate, Goal, Compliance_Status, Report_Link)
	SELECT DISTINCT AddDate, [Date], Metric_Code, Metric_Description, Compliance_Rate, Goal, 
	CASE WHEN Compliance_Rate >= Goal THEN 'YES'
	WHEN Goal IS NULL THEN '-'
	WHEN Compliance_Rate IS NULL THEN '-'
	ELSE 'NO'
	END AS Compliance_Status, Report_Link
	FROM CTE2
	WHERE DATE >= CONVERT(VARCHAR(10), DATEADD(YEAR,-2,GETDATE()), 120) 
	ORDER BY Metric_Code, [Date] DESC   ---[An INSERT INTO statment inserts the data into the final tbl_Metrics_Compliance_Daily_Status table by selecting data from 
										---the CTE2 table. The select statement creates a new Compliance Status column from the Compliance_Rate column via a CASE statement 
										---(a Compliance Status column value will be recorded as 'YES' if a value in the Compliance_Rate column is greater than the 95% goal 
										---value, 'NO' if the Compliance_Rate value is less than the 95% goal value, and '-' if values in the Goal or Compliance_Rate column 
										---are NULL), appends metric information to each output record from two tables via a LEFT JOIN, restricts the data to the last 
										---two years via a WHERE clause, and sorts the final results set via an ORDER BY function by the Metric_Code and Date columns in 
										---descending order.]

---///* 3. Query the final table *///---

	SELECT * FROM tbl_Metrics_Compliance_Daily_Status
