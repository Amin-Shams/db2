USE TradePortDB;
GO

DECLARE @SchemaName NVARCHAR(128) = 'Finance';

SELECT 
    t.name AS TableName,
    SUM(p.rows) AS [RowCount]  
FROM 
    sys.tables AS t
INNER JOIN 
    sys.schemas AS s ON t.schema_id = s.schema_id
INNER JOIN 
    sys.partitions AS p ON t.object_id = p.object_id
WHERE 
    p.index_id IN (0, 1) 
    AND s.name = @SchemaName
GROUP BY 
    t.name
ORDER BY 
    [RowCount] DESC; 
