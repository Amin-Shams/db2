USE DataWarehouse;
GO

DECLARE @SchemaName SYSNAME = N'Dim';

SELECT 
    t.name        AS TableName,
    SUM(p.rows)   AS [RowCount]
FROM 
    sys.tables    AS t
    INNER JOIN sys.schemas    AS s  ON t.schema_id = s.schema_id
    INNER JOIN sys.partitions AS p  ON t.object_id = p.object_id
WHERE 
    s.name          = @SchemaName
    AND p.index_id  IN (0,1)           -- heap or clustered index
GROUP BY 
    t.name
ORDER BY 
    [RowCount] DESC;

--select * from DataWarehouse.Dim.DimDate

--select * from DataWarehouse.dbo.ETLLog;
--select * from DataWarehouse.Dim.DimServiceType;


-- فقط دایمنشن‌ها
SELECT
    t.name   AS DimTable,
    SUM(p.rows) AS [RowCount]
FROM 
    sys.schemas AS s
    INNER JOIN sys.tables AS t
        ON t.schema_id = s.schema_id
    INNER JOIN sys.partitions AS p
        ON p.object_id = t.object_id
        AND p.index_id IN (0,1)
WHERE
    s.name = 'Dim'
GROUP BY
    t.name
ORDER BY
    t.name;

-- فقط فکت‌ها
SELECT
    t.name   AS FactTable,
    SUM(p.rows) AS [RowCount]
FROM 
    sys.schemas AS s
    INNER JOIN sys.tables AS t
        ON t.schema_id = s.schema_id
    INNER JOIN sys.partitions AS p
        ON p.object_id = t.object_id
        AND p.index_id IN (0,1)
WHERE
    s.name = 'Fact'
GROUP BY
    t.name
ORDER BY
    t.name;
