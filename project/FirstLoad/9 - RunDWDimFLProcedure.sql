USE DataWarehouse;
GO

--------------------------------------------------------------------------------
-- Wrapper: LoadAllDimInitialLoads
--   1) Disable FKs → 2) Run each FirstLoad dim proc (with per-proc logging) 
--   3) Re-enable FKs
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.LoadAllDimInitialLoads
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
        -- STEP 1: Execute each Dim.*InitialLoad and log duration
        ------------------------------------------------------------------------
        -- 1) Calendar dates
        SET @TableName = 'Dim.DimDate';
        SET @StepStart  = GETDATE();
        EXEC Dim.LoadDimDateInitialLoad;
        SET @StepEnd    = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES (@TableName, 'Procedure', @StepStart, @StepEnd, 'Initial Load by LoadAllDimInitialLoads');

        -- 2) Customers (SCD2)
        SET @TableName = 'Dim.DimCustomer';
        SET @StepStart = GETDATE();
        EXEC Dim.LoadDimCustomerInitialLoad;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES (@TableName, 'Procedure', @StepStart, @StepEnd, 'Initial Load by LoadAllDimInitialLoads');

        -- 3) Service types (SCD1)
        SET @TableName = 'Dim.DimServiceType';
        SET @StepStart = GETDATE();
        EXEC Dim.LoadDimServiceTypeInitialLoad;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES (@TableName, 'Procedure', @StepStart, @StepEnd, 'Initial Load by LoadAllDimInitialLoads');

        -- 4) Taxes (SCD2)
        SET @TableName = 'Dim.DimTax';
        SET @StepStart = GETDATE();
        EXEC Dim.LoadDimTaxInitialLoad;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES (@TableName, 'Procedure', @StepStart, @StepEnd, 'Initial Load by LoadAllDimInitialLoads');

        -- 5) Billing cycles (SCD1)
        SET @TableName = 'Dim.DimBillingCycle';
        SET @StepStart = GETDATE();
        EXEC Dim.LoadDimBillingCycleInitialLoad;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES (@TableName, 'Procedure', @StepStart, @StepEnd, 'Initial Load by LoadAllDimInitialLoads');

        -- 6) Payment methods (SCD1)
        SET @TableName = 'Dim.DimPaymentMethod';
        SET @StepStart = GETDATE();
        EXEC Dim.LoadDimPaymentMethodInitialLoad;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES (@TableName, 'Procedure', @StepStart, @StepEnd, 'Initial Load by LoadAllDimInitialLoads');

        -- 7) Contracts (SCD3)
        SET @TableName = 'Dim.DimContract';
        SET @StepStart = GETDATE();
        EXEC Dim.LoadDimContractInitialLoad;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES (@TableName, 'Procedure', @StepStart, @StepEnd, 'Initial Load by LoadAllDimInitialLoads');

        -- 8) Invoices (Static)
        SET @TableName = 'Dim.DimInvoice';
        SET @StepStart = GETDATE();
        EXEC Dim.LoadDimInvoiceInitialLoad;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES (@TableName, 'Procedure', @StepStart, @StepEnd, 'Initial Load by LoadAllDimInitialLoads');

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK;
        THROW;
    END CATCH;
END;
GO


EXEC Dim.LoadAllDimInitialLoads;
Go


--------------------------------------------------------------------------------
-- LoadAllDimensions (Master Procedure for First Load)
-- Executes all dimension load procedures. This version is non-destructive.
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.LoadAllDimensions
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @MasterProcName NVARCHAR(128) = 'Dim.LoadAllDimensions';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);

    INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, Status, [Message])
    VALUES (@MasterProcName, 'Start Master Load', 'All Dimensions', @StartTime, GETDATE(), 'In Progress', 'Master dimension initial load process started.');
    
    BEGIN TRY
        PRINT 'Executing non-destructive dimension loads...';
        EXEC Dim.LoadDimDate;
        EXEC Dim.LoadDimDepartment;
        EXEC Dim.LoadDimJobTitle;
        EXEC Dim.LoadDimLeaveType;
        EXEC Dim.LoadDimTerminationReason;
        EXEC Dim.LoadDimEmployee;

        SET @Message = 'Master dimension initial load process completed successfully.';
        UPDATE Audit.DW_ETL_Log 
        SET Status = 'Success', [Message] = @Message, EndTime = GETDATE()
        WHERE ProcessName = @MasterProcName AND Status = 'In Progress';
    END TRY
    BEGIN CATCH
        SET @Message = CONCAT('Master dimension initial load process failed. Error: ', ERROR_MESSAGE());
        UPDATE Audit.DW_ETL_Log 
        SET Status = 'Failed', [Message] = @Message, EndTime = GETDATE()
        WHERE ProcessName = @MasterProcName AND Status = 'In Progress';
        THROW;
    END CATCH;
END;
GO

EXEC Dim.LoadAllDimensions;



USE DataWarehouse;
GO

--------------------------------------------------------------------------------
-- Wrapper: LoadAllPortOpsDimInitialLoads
--   Simply invokes each Dim.Load…InitialLoad proc in sequence
--   (no FK-disable/enable because we use DELETE+RESEED in each proc)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.LoadAllPortOpsDimInitialLoads
AS
BEGIN
    SET NOCOUNT, XACT_ABORT ON;
    DECLARE 
        @TableName NVARCHAR(128),
        @StepStart DATETIME,
        @StepEnd   DATETIME;

    BEGIN TRY
        BEGIN TRAN;
		DELETE FROM Fact.FactEquipmentAssignment;
		DELETE FROM Fact.FactCargoOperationTransactional;
		DELETE FROM Fact.FactContainerMovementsAcc;
		DELETE FROM Fact.FactPortCallPeriodicSnapshot;

        -------------------------------------------------
        -- -- 1) Date
        -- SET @TableName = 'Dim.DimDate';
        -- SET @StepStart = GETDATE();
        -- EXEC Dim.LoadDimDateInitialLoad;
        -- SET @StepEnd = GETDATE();
        -- INSERT INTO dbo.ETLLog(TableName,OperationType,StartTime,EndTime,Message)
        -- VALUES(@TableName,'Procedure',@StepStart,@StepEnd,'Initial load');
        -------------------------------------------------
        -- 2) Ship (SCD2)
        SET @TableName = 'Dim.DimShip';
        SET @StepStart = GETDATE();
        EXEC Dim.LoadDimShipInitialLoad;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog VALUES(@TableName,'Procedure',@StepStart,@StepEnd,'Initial load');

        -------------------------------------------------
        -- 3) Port (SCD1)
        SET @TableName = 'Dim.DimPort';
        SET @StepStart = GETDATE();
        EXEC Dim.LoadDimPortInitialLoad;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog VALUES(@TableName,'Procedure',@StepStart,@StepEnd,'Initial load');

        -------------------------------------------------
        -- 4) Container (SCD3)
        SET @TableName = 'Dim.DimContainer';
        SET @StepStart = GETDATE();
        EXEC Dim.LoadDimContainerInitialLoad;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog VALUES(@TableName,'Procedure',@StepStart,@StepEnd,'Initial load');

        -------------------------------------------------
        -- 5) Equipment (SCD1)
        SET @TableName = 'Dim.DimEquipment';
        SET @StepStart = GETDATE();
        EXEC Dim.LoadDimEquipmentInitialLoad;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog VALUES(@TableName,'Procedure',@StepStart,@StepEnd,'Initial load');

        -------------------------------------------------
        -- 6) Employee (SCD2)
        SET @TableName = 'Dim.DimEmployee';
        SET @StepStart = GETDATE();
        EXEC Dim.LoadDimEmployeeInitialLoad;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog VALUES(@TableName,'Procedure',@StepStart,@StepEnd,'Initial load');

        -------------------------------------------------
        -- 7) YardSlot (SCD1)
        SET @TableName = 'Dim.DimYardSlot';
        SET @StepStart = GETDATE();
        EXEC Dim.LoadDimYardSlotInitialLoad;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog VALUES(@TableName,'Procedure',@StepStart,@StepEnd,'Initial load');

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE()<>0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO


EXEC Dim.LoadAllPortOpsDimInitialLoads;
GO

