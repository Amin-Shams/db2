USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE Dim.ExecuteAllDimensionUpdates
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Message   NVARCHAR(2000),
        @DimName   NVARCHAR(128);

    BEGIN TRY   
        BEGIN TRAN;

        ------------------------------------------------------------------------
        -- DimDate
        ------------------------------------------------------------------------
        SET @DimName = 'Dim.DimDate';
        SET @StepStart = GETDATE();
        EXEC Dim.UpdateDimDateIncremental;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
        VALUES (@DimName, 'Update', @StepStart, @StepEnd, 'Executed DimDate incremental update');

        ------------------------------------------------------------------------
        -- DimCustomer
        ------------------------------------------------------------------------
        SET @DimName = 'Dim.DimCustomer';
        SET @StepStart = GETDATE();
        EXEC Dim.UpdateDimCustomerIncremental;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
        VALUES (@DimName, 'Update', @StepStart, @StepEnd, 'Executed DimCustomer incremental update');

        ------------------------------------------------------------------------
        -- DimContract
        ------------------------------------------------------------------------
        SET @DimName = 'Dim.DimContract';
        SET @StepStart = GETDATE();
        EXEC Dim.UpdateDimContractIncremental;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
        VALUES (@DimName, 'Update', @StepStart, @StepEnd, 'Executed DimContract incremental update');

        ------------------------------------------------------------------------
        -- DimInvoice
        ------------------------------------------------------------------------
        SET @DimName = 'Dim.DimInvoice';
        SET @StepStart = GETDATE();
        EXEC Dim.UpdateDimInvoiceIncremental;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
        VALUES (@DimName, 'Update', @StepStart, @StepEnd, 'Executed DimInvoice incremental update');

        ------------------------------------------------------------------------
        -- DimBillingCycle
        ------------------------------------------------------------------------
        SET @DimName = 'Dim.DimBillingCycle';
        SET @StepStart = GETDATE();
        EXEC Dim.UpdateDimBillingCycleIncremental;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
        VALUES (@DimName, 'Update', @StepStart, @StepEnd, 'Executed DimBillingCycle incremental update');

        ------------------------------------------------------------------------
        -- DimServiceType
        ------------------------------------------------------------------------
        SET @DimName = 'Dim.DimServiceType';
        SET @StepStart = GETDATE();
        EXEC Dim.UpdateDimServiceTypeIncremental;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
        VALUES (@DimName, 'Update', @StepStart, @StepEnd, 'Executed DimServiceType incremental update');

        ------------------------------------------------------------------------
        -- DimTax
        ------------------------------------------------------------------------
        SET @DimName = 'Dim.DimTax';
        SET @StepStart = GETDATE();
        EXEC Dim.UpdateDimTaxIncremental;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
        VALUES (@DimName, 'Update', @StepStart, @StepEnd, 'Executed DimTax incremental update');

        ------------------------------------------------------------------------
        -- DimPaymentMethod
        ------------------------------------------------------------------------
        SET @DimName = 'Dim.DimPaymentMethod';
        SET @StepStart = GETDATE();
        EXEC Dim.UpdateDimPaymentMethodIncremental;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
        VALUES (@DimName, 'Update', @StepStart, @StepEnd, 'Executed DimPaymentMethod incremental update');

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;

        SET @StepEnd = GETDATE();
        SET @Message = CONCAT('ExecuteAllDimensionUpdates failed: ', ERROR_MESSAGE());
        INSERT INTO dbo.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
        VALUES ('ALL_DIM_UPDATES', 'Error', @StepStart, @StepEnd, @Message);

        THROW;
    END CATCH
END;
GO

-- Execute 
EXEC DataWarehouse.Dim.ExecuteAllDimensionUpdates;
GO

select * from DataWarehouse.Dim.DimDate;
select * from DataWarehouse.Dim.DimCustomer;
select * from DataWarehouse.Dim.DimContract;
select * from DataWarehouse.Dim.DimServiceType;
select * from DataWarehouse.Dim.DimTax;



--------------------------------------------------------------------------------
-- UpdateAllDimensions (Master Update Procedure)
-- Executes all dimension update procedures for incremental loading.
-- This procedure should be run daily after the first load.
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.UpdateAllDimensions
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @MasterProcName NVARCHAR(128) = 'Dim.UpdateAllDimensions';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);

    INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, Status, [Message])
    VALUES (@MasterProcName, 'Start Master Update', 'All Dimensions', @StartTime, GETDATE(), 'In Progress', 'Master dimension update process started.');
    
    BEGIN TRY
        EXEC Dim.UpdateDimLeaveType;
        EXEC Dim.UpdateDimTerminationReason;
        EXEC Dim.UpdateDimDepartment;
        EXEC Dim.UpdateDimJobTitle;
        EXEC Dim.UpdateDimEmployee;

        SET @Message = 'Master dimension update process completed successfully.';
        UPDATE Audit.DW_ETL_Log 
        SET Status = 'Success', [Message] = @Message, EndTime = GETDATE()
        WHERE ProcessName = @MasterProcName AND Status = 'In Progress';
    END TRY
    BEGIN CATCH
        SET @Message = CONCAT('Master dimension update process failed. Error: ', ERROR_MESSAGE());
        UPDATE Audit.DW_ETL_Log 
        SET Status = 'Failed', [Message] = @Message, EndTime = GETDATE()
        WHERE ProcessName = @MasterProcName AND Status = 'In Progress';
        THROW;
    END CATCH;
END;
GO

exec Dim.UpdateAllDimensions;