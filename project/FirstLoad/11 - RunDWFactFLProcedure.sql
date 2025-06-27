USE DataWarehouse;
GO

--------------------------------------------------------------------------------
-- Wrapper: LoadAllFactInitialLoads
--   1) Disable all FKs referencing Fact schema
--   2) Execute each FirstLoad fact proc (with per-proc logging)
--   3) Re-enable those FKs WITH CHECK
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.LoadAllFactInitialLoads
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE 
        @TableName NVARCHAR(128),
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @sql       NVARCHAR(MAX);

    BEGIN TRY
        BEGIN TRAN;

        ------------------------------------------------------------------------
        -- STEP 1: Run each Fact.*InitialLoad proc and log it
        ------------------------------------------------------------------------
        -- 2-1) InvoiceLineTransaction
        SET @TableName = 'Fact.FactInvoiceLineTransaction';
        SET @StepStart = GETDATE();
        EXEC Fact.LoadFactInvoiceLineTransactionInitialLoad;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName,'Procedure',@StepStart,@StepEnd,'Initial Load by LoadAllFactInitialLoads');

        -- 2-2) CustomerPaymentTransaction
        SET @TableName = 'Fact.FactCustomerPaymentTransaction';
        SET @StepStart = GETDATE();
        EXEC Fact.LoadFactCustomerPaymentTransactionInitialLoad;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName,'Procedure',@StepStart,@StepEnd,'Initial Load by LoadAllFactInitialLoads');

        -- 2-3) CustomerBillingMonthlySnapshot
        SET @TableName = 'Fact.FactCustomerBillingMonthlySnapshot';
        SET @StepStart = GETDATE();
        EXEC Fact.LoadFactCustomerBillingMonthlySnapshotInitialLoad;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName,'Procedure',@StepStart,@StepEnd,'Initial Load by LoadAllFactInitialLoads');

        -- 2-4) InvoiceLifecycleAccumulating
        SET @TableName = 'Fact.FactInvoiceLifecycleAccumulating';
        SET @StepStart = GETDATE();
        EXEC Fact.LoadFactInvoiceLifecycleAccumulatingInitialLoad;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName,'Procedure',@StepStart,@StepEnd,'Initial Load by LoadAllFactInitialLoads');

        -- 2-5) CustomerContractActivationFactless
        SET @TableName = 'Fact.FactCustomerContractActivationFactless';
        SET @StepStart = GETDATE();
        EXEC Fact.LoadFactCustomerContractActivationInitialLoad;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName,'Procedure',@StepStart,@StepEnd,'Initial Load by LoadAllFactInitialLoads');

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK;
        THROW;
    END CATCH;
END;
GO

-- To run all fact initial loads (with FK handling and logging):
EXEC Fact.LoadAllFactInitialLoads;
GO


--------------------------------------------------------------------------------
-- LoadAllFacts (Master Procedure for First Load)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.LoadAllFacts
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @MasterProcName NVARCHAR(128) = 'Fact.LoadAllFacts';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);

    INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, Status, [Message])
    VALUES (@MasterProcName, 'Start Master Load', 'All Fact Tables', @StartTime, GETDATE(), 'In Progress', 'Master fact initial load process started.');
    
    BEGIN TRY
        EXEC Fact.LoadFactTermination;
        EXEC Fact.LoadFactSalaryPayment;
        EXEC Fact.LoadFactEmployeeAttendance;
        EXEC Fact.LoadFactMonthlyEmployeePerformance;
        EXEC Fact.LoadFactEmployeeLifecycle;
        --EXEC Fact.LoadFactYearlyHeadcount;

        SET @Message = 'Master fact initial load process completed successfully.';
        UPDATE Audit.DW_ETL_Log 
        SET Status = 'Success', [Message] = @Message, EndTime = GETDATE()
        WHERE ProcessName = @MasterProcName AND Status = 'In Progress';
    END TRY
    BEGIN CATCH
        SET @Message = CONCAT('Master fact initial load process failed. Error: ', ERROR_MESSAGE());
        UPDATE Audit.DW_ETL_Log 
        SET Status = 'Failed', [Message] = @Message, EndTime = GETDATE()
        WHERE ProcessName = @MasterProcName AND Status = 'In Progress';
        THROW;
    END CATCH;
END;
GO

exec Fact.LoadAllFacts;