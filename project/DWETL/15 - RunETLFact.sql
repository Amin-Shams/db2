USE DataWarehouse;
GO

--------------------------------------------------------------------------------
-- Fact.ExecuteAllFactUpdates
-- Executes all incremental update procedures for fact tables with ETL logging
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.ExecuteAllFactUpdates
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Message   NVARCHAR(2000),
        @FactName  NVARCHAR(128);

    BEGIN TRY
        BEGIN TRAN;

        ------------------------------------------------------------------------
        -- FactInvoiceLineTransaction
        ------------------------------------------------------------------------
        SET @FactName = 'Fact.FactInvoiceLineTransaction';
        SET @StepStart = GETDATE();
        EXEC Fact.UpdateFactInvoiceLineTransactionIncremental;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
        VALUES (@FactName, 'Update', @StepStart, @StepEnd, 'Executed FactInvoiceLineTransaction incremental update');

        ------------------------------------------------------------------------
        -- FactCustomerPaymentTransaction
        ------------------------------------------------------------------------
        SET @FactName = 'Fact.FactCustomerPaymentTransaction';
        SET @StepStart = GETDATE();
        EXEC Fact.UpdateFactCustomerPaymentTransactionIncremental;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
        VALUES (@FactName, 'Update', @StepStart, @StepEnd, 'Executed FactCustomerPaymentTransaction incremental update');

        ------------------------------------------------------------------------
        -- FactCustomerBillingMonthlySnapshot
        ------------------------------------------------------------------------
        SET @FactName = 'Fact.FactCustomerBillingMonthlySnapshot';
        SET @StepStart = GETDATE();
        EXEC Fact.UpdateFactCustomerBillingMonthlySnapshotIncremental;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
        VALUES (@FactName, 'Update', @StepStart, @StepEnd, 'Executed FactCustomerBillingMonthlySnapshot incremental update');

        ------------------------------------------------------------------------
        -- FactInvoiceLifecycleAccumulating
        ------------------------------------------------------------------------
        SET @FactName = 'Fact.FactInvoiceLifecycleAccumulating';
        SET @StepStart = GETDATE();
        EXEC Fact.UpdateFactInvoiceLifecycleAccumulatingIncremental;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
        VALUES (@FactName, 'Update', @StepStart, @StepEnd, 'Executed FactInvoiceLifecycleAccumulating incremental update');

        ------------------------------------------------------------------------
        -- FactCustomerContractActivationFactless
        ------------------------------------------------------------------------
        SET @FactName = 'Fact.FactCustomerContractActivationFactless';
        SET @StepStart = GETDATE();
        EXEC Fact.UpdateFactCustomerContractActivationIncremental;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
        VALUES (@FactName, 'Update', @StepStart, @StepEnd, 'Executed FactCustomerContractActivationFactless incremental update');

        ------------------------------------------------------------------------
        -- Commit
        ------------------------------------------------------------------------
        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;

        SET @StepEnd = GETDATE();
        SET @Message = CONCAT('ExecuteAllFactUpdates failed: ', ERROR_MESSAGE());
        INSERT INTO dbo.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
        VALUES ('ALL_FACT_UPDATES', 'Error', @StepStart, @StepEnd, @Message);

        THROW;
    END CATCH
END;
GO


EXEC Fact.ExecuteAllFactUpdates;
GO


-- 1. FactInvoiceLineTransaction
SELECT TOP 100 * 
FROM DataWarehouse.Fact.FactInvoiceLineTransaction;

-- 2. FactCustomerPaymentTransaction
SELECT TOP 100 * 
FROM DataWarehouse.Fact.FactCustomerPaymentTransaction;

-- 3. FactCustomerBillingMonthlySnapshot
SELECT TOP 100 * 
FROM DataWarehouse.Fact.FactCustomerBillingMonthlySnapshot;

-- 4. FactInvoiceLifecycleAccumulating
SELECT TOP 100 * 
FROM DataWarehouse.Fact.FactInvoiceLifecycleAccumulating;

-- 5. FactCustomerContractActivationFactless
SELECT TOP 100 * 
FROM DataWarehouse.Fact.FactCustomerContractActivationFactless;


-- ›ﬁÿ ›ò ùÂ«
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


--------------------------------------------------------------------------------
-- 6) UpdateAllFacts (Master Procedure for Incremental Load)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.UpdateAllFacts
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @MasterProcName NVARCHAR(128) = 'Fact.UpdateAllFacts';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);
    DECLARE @Today DATE = GETDATE();

    INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, Status, [Message])
    VALUES (@MasterProcName, 'Start Master Update', 'All Fact Tables', @StartTime, GETDATE(), 'In Progress', 'Master fact update process started.');
    
    BEGIN TRY
        EXEC Fact.UpdateFactTermination;
        EXEC Fact.UpdateFactSalaryPayment;
        EXEC Fact.UpdateFactEmployeeAttendance;
        EXEC Fact.UpdateFactMonthlyEmployeePerformance;
        EXEC Fact.UpdateFactEmployeeLifecycle;
        
        -- After all procedures succeed, update the control date for the next run
        UPDATE Audit.ETL_Control SET LastLoadDate = @Today WHERE ProcessName = 'FactTables';

        SET @Message = 'Master fact update process completed successfully.';
        UPDATE Audit.DW_ETL_Log 
        SET Status = 'Success', [Message] = @Message, EndTime = GETDATE()
        WHERE ProcessName = @MasterProcName AND Status = 'In Progress';
    END TRY
    BEGIN CATCH
        SET @Message = CONCAT('Master fact update process failed. Error: ', ERROR_MESSAGE());
        UPDATE Audit.DW_ETL_Log 
        SET Status = 'Failed', [Message] = @Message, EndTime = GETDATE()
        WHERE ProcessName = @MasterProcName AND Status = 'In Progress';
        THROW;
    END CATCH;
END;
GO

exec Fact.UpdateAllFacts;