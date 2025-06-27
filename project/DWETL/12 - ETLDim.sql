USE DataWarehouse;
GO

--------------------------------------------------------------------------------
-- 2-1) UpdateDimDateIncremental
-- Incremental populate of Dim.DimDate: only new dates from yesterday+1 through today
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.UpdateDimDateIncremental
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE
        @TableName NVARCHAR(128) = 'Dim.DimDate',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Msg       NVARCHAR(2000),
        @LastDate  DATE,
        @StartDate DATE,
        @EndDate   DATE = CONVERT(DATE, GETDATE()),
        @DayCount  INT;

    BEGIN TRY
        BEGIN TRAN;

        -- 1. Find last existing date
        SELECT @LastDate = MAX(FullDate) FROM Dim.DimDate;
        IF @LastDate IS NULL
            SET @LastDate = DATEADD(DAY, -1, @EndDate);

        SET @StartDate = DATEADD(DAY, 1, @LastDate);
        SET @DayCount  = DATEDIFF(DAY, @StartDate, @EndDate) + 1;

        IF @DayCount > 0
        BEGIN
            -- 2. Generate new dates and insert
            SET @StepStart = GETDATE();
            ;WITH NewDates AS
            (
                SELECT TOP(@DayCount)
                    DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1, @StartDate) AS FullDate
                FROM sys.all_objects a
                CROSS JOIN sys.all_objects b
            )
            INSERT INTO Dim.DimDate
            (
                FullDate, [Year], [Quarter], [Month], MonthName,
                [Day], DayOfWeek, DayName, WeekOfYear, IsWeekend
            )
            SELECT
                d.FullDate,
                YEAR(d.FullDate),
                DATEPART(QUARTER, d.FullDate),
                MONTH(d.FullDate),
                DATENAME(MONTH, d.FullDate),
                DAY(d.FullDate),
                DATEPART(WEEKDAY, d.FullDate),
                DATENAME(WEEKDAY, d.FullDate),
                DATEPART(WEEK, d.FullDate),
                CASE WHEN DATEPART(WEEKDAY, d.FullDate) IN (6,7) THEN 1 ELSE 0 END
            FROM NewDates AS d
            ORDER BY d.FullDate;

            SET @StepEnd = GETDATE();
            INSERT INTO dbo.ETLLog
            (TableName, OperationType, StartTime, EndTime, Message)
            VALUES
            (
                @TableName,
                'Insert',
                @StepStart,
                @StepEnd,
                CONCAT('Incremental: Inserted ', @@ROWCOUNT, ' new dates')
            );
        END
        ELSE
        BEGIN
            -- nothing to do
            SET @StepStart = GETDATE();
            SET @StepEnd   = GETDATE();
            INSERT INTO dbo.ETLLog
            (TableName, OperationType, StartTime, EndTime, Message)
            VALUES
            (
                @TableName,
                'Skip',
                @StepStart,
                @StepEnd,
                'Incremental: No new dates found'
            );
        END

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Msg     = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog
        (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Error',
            @StepStart,
            @StepEnd,
            CONCAT('Incremental: ', @Msg)
        );
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- 2-2) UpdateDimCustomerIncremental
-- SCD2 incremental for Dim.DimCustomer
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.UpdateDimCustomerIncremental
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE
        @TableName NVARCHAR(128) = 'Dim.DimCustomer',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Msg       NVARCHAR(2000),
        @Today     DATE = CONVERT(DATE, GETDATE()),
        @RowsAffected INT;

    BEGIN TRY
        BEGIN TRAN;

        -- 1) Detect changes in staging by business key
        SET @StepStart = GETDATE();
        ;WITH Changed AS
        (
            SELECT
                C.CustomerCode,
                C.CustomerName,
                C.CustomerType,
                (SELECT CountryName FROM TradePortDB.Common.Country WHERE CountryID = C.CountryID) AS CountryName,
                C.VATNumber,
                C.Address,
                C.Email,
                C.Phone
            FROM StagingDB.Finance.Customer AS C
        )
        -- 2) Expire existing versions
        UPDATE D
        SET 
            D.EndDate   = @Today,
            D.IsCurrent = 0
        FROM Dim.DimCustomer AS D
        INNER JOIN Changed AS X
            ON D.CustomerCode = X.CustomerCode
        WHERE D.IsCurrent = 1
          AND (
              ISNULL(D.CustomerName,'')    <> ISNULL(X.CustomerName,'')
           OR ISNULL(D.CustomerType,'')    <> ISNULL(X.CustomerType,'')
           OR ISNULL(D.CountryName,'')     <> ISNULL(X.CountryName,'')
           OR ISNULL(D.VATNumber,'')       <> ISNULL(X.VATNumber,'')
           OR ISNULL(D.Address,'')         <> ISNULL(X.Address,'')
           OR ISNULL(D.Email,'')           <> ISNULL(X.Email,'')
           OR ISNULL(D.Phone,'')           <> ISNULL(X.Phone,'')
        );

        SET @RowsAffected = @@ROWCOUNT;
        SET @StepEnd      = GETDATE();
        INSERT INTO dbo.ETLLog
        (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Expire',
            @StepStart,
            @StepEnd,
            CONCAT('Incremental: Expired ', @RowsAffected, ' old versions')
        );

        -- 3) Insert new/current versions
        SET @StepStart = GETDATE();
        INSERT INTO Dim.DimCustomer
        (
            CustomerCode, CustomerName, CustomerType,
            CountryName, VATNumber, Address, Email, Phone,
            StartDate, EndDate, IsCurrent
        )
        SELECT
            C.CustomerCode,
            C.CustomerName,
            C.CustomerType,
            (SELECT CountryName FROM TradePortDB.Common.Country WHERE CountryID = C.CountryID),
            C.VATNumber,
            C.Address,
            C.Email,
            C.Phone,
            @Today,
            NULL,
            1
        FROM StagingDB.Finance.Customer AS C
        LEFT JOIN Dim.DimCustomer AS D
            ON C.CustomerCode = D.CustomerCode AND D.IsCurrent = 1
        WHERE
            D.DimCustomerID IS NULL  -- brand-new
         OR (
              ISNULL(D.CustomerName,'')  <> ISNULL(C.CustomerName,'')
           OR ISNULL(D.CustomerType,'')  <> ISNULL(C.CustomerType,'')
           OR ISNULL(D.VATNumber,'')     <> ISNULL(C.VATNumber,'')
           OR ISNULL(D.Address,'')       <> ISNULL(C.Address,'')
           OR ISNULL(D.Email,'')         <> ISNULL(C.Email,'')
           OR ISNULL(D.Phone,'')         <> ISNULL(C.Phone,'')
        );

        SET @RowsAffected = @@ROWCOUNT;
        SET @StepEnd      = GETDATE();
        INSERT INTO dbo.ETLLog
        (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Insert',
            @StepStart,
            @StepEnd,
            CONCAT('Incremental: Inserted ', @RowsAffected, ' new/current versions')
        );

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Msg     = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog
        (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Error',
            @StepStart,
            @StepEnd,
            CONCAT('Incremental: ', @Msg)
        );
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- 2-3) UpdateDimServiceTypeIncremental
-- SCD1 incremental for Dim.DimServiceType
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.UpdateDimServiceTypeIncremental
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE
        @TableName NVARCHAR(128) = 'Dim.DimServiceType',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Msg       NVARCHAR(2000),
        @RowsAffected INT;

    BEGIN TRY
        BEGIN TRAN;

        -- Update existing records when business attributes change
        SET @StepStart = GETDATE();
        UPDATE D
        SET
            D.ServiceName     = S.ServiceName,
            D.ServiceCategory = S.ServiceCategory,
            D.UnitOfMeasure   = S.UnitOfMeasure,
            D.Taxable         = S.Taxable,
            D.IsActive        = S.IsActive
        FROM Dim.DimServiceType AS D
        INNER JOIN StagingDB.Finance.ServiceType AS S
            ON D.SourceServiceTypeID = S.ServiceTypeID
        WHERE
            D.ServiceName     <> S.ServiceName
         OR D.ServiceCategory <> S.ServiceCategory
         OR D.UnitOfMeasure   <> S.UnitOfMeasure
         OR D.Taxable         <> S.Taxable
         OR D.IsActive        <> S.IsActive;

        SET @RowsAffected = @@ROWCOUNT;
        SET @StepEnd      = GETDATE();
        INSERT INTO dbo.ETLLog
        (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Update',
            @StepStart,
            @StepEnd,
            CONCAT('Incremental: Updated ', @RowsAffected, ' service types')
        );

        -- Insert brand-new service types
        SET @StepStart = GETDATE();
        INSERT INTO Dim.DimServiceType
        (SourceServiceTypeID, ServiceName, ServiceCategory, UnitOfMeasure, Taxable, IsActive)
        SELECT
            S.ServiceTypeID,
            S.ServiceName,
            S.ServiceCategory,
            S.UnitOfMeasure,
            S.Taxable,
            S.IsActive
        FROM StagingDB.Finance.ServiceType AS S
        LEFT JOIN Dim.DimServiceType AS D
            ON D.SourceServiceTypeID = S.ServiceTypeID
        WHERE D.DimServiceTypeID IS NULL;

        SET @RowsAffected = @@ROWCOUNT;
        SET @StepEnd      = GETDATE();
        INSERT INTO dbo.ETLLog
        (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Insert',
            @StepStart,
            @StepEnd,
            CONCAT('Incremental: Inserted ', @RowsAffected, ' new service types')
        );

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Msg     = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog
        (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Error',
            @StepStart,
            @StepEnd,
            CONCAT('Incremental: ', @Msg)
        );
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- 2-4) UpdateDimTaxIncremental
-- SCD2 incremental for Dim.DimTax
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.UpdateDimTaxIncremental
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE
        @TableName NVARCHAR(128) = 'Dim.DimTax',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Msg       NVARCHAR(2000),
        @Today     DATE = CONVERT(DATE, GETDATE()),
        @RowsExp   INT,
        @RowsIns   INT;

    BEGIN TRY
        BEGIN TRAN;

        -- Expire old versions when rate or type changes
        SET @StepStart = GETDATE();
        UPDATE D
        SET
            D.EndDate   = @Today,
            D.IsCurrent = 0
        FROM Dim.DimTax AS D
        INNER JOIN StagingDB.Finance.Tax AS S
            ON D.TaxName = S.TaxName
           AND D.TaxType = S.TaxType
           AND D.EffectiveFrom = S.EffectiveFrom
        WHERE D.IsCurrent = 1
          AND (D.TaxRate <> S.TaxRate);
        SET @RowsExp = @@ROWCOUNT;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog
        (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Expire',
            @StepStart,
            @StepEnd,
            CONCAT('Incremental: Expired ', @RowsExp, ' old tax versions')
        );

        -- Insert new/current versions
        SET @StepStart = GETDATE();
        INSERT INTO Dim.DimTax
        (
            TaxName, TaxRate, TaxType, EffectiveFrom, EffectiveTo,
            StartDate, EndDate, IsCurrent
        )
        SELECT
            S.TaxName,
            S.TaxRate,
            S.TaxType,
            S.EffectiveFrom,
            S.EffectiveTo,
            @Today,
            NULL,
            1
        FROM StagingDB.Finance.Tax AS S
        LEFT JOIN Dim.DimTax AS D
            ON D.TaxName = S.TaxName
           AND D.TaxType = S.TaxType
           AND D.EffectiveFrom = S.EffectiveFrom
           AND D.IsCurrent = 1
        WHERE D.DimTaxID IS NULL
           OR D.TaxRate <> S.TaxRate;

        SET @RowsIns = @@ROWCOUNT;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog
        (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Insert',
            @StepStart,
            @StepEnd,
            CONCAT('Incremental: Inserted ', @RowsIns, ' new tax versions')
        );

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Msg     = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog
        (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Error',
            @StepStart,
            @StepEnd,
            CONCAT('Incremental: ', @Msg)
        );
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- 2-5) UpdateDimBillingCycleIncremental
-- SCD1 incremental for Dim.DimBillingCycle
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.UpdateDimBillingCycleIncremental
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE
        @TableName NVARCHAR(128) = 'Dim.DimBillingCycle',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Msg       NVARCHAR(2000),
        @RowsUpd   INT,
        @RowsIns   INT;

    BEGIN TRY
        BEGIN TRAN;

        -- Update existing when length changes
        SET @StepStart = GETDATE();
        UPDATE D
        SET D.CycleLengthInDays = S.CycleLengthInDays
        FROM Dim.DimBillingCycle AS D
        INNER JOIN StagingDB.Finance.BillingCycle AS S
            ON D.CycleName = S.CycleName
        WHERE D.CycleLengthInDays <> S.CycleLengthInDays;
        SET @RowsUpd = @@ROWCOUNT;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Update',
            @StepStart,
            @StepEnd,
            CONCAT('Incremental: Updated ', @RowsUpd, ' billing cycles')
        );

        -- Insert new cycles
        SET @StepStart = GETDATE();
        INSERT INTO Dim.DimBillingCycle (CycleName, CycleLengthInDays)
        SELECT
            S.CycleName,
            S.CycleLengthInDays
        FROM StagingDB.Finance.BillingCycle AS S
        LEFT JOIN Dim.DimBillingCycle AS D
            ON D.CycleName = S.CycleName
        WHERE D.DimBillingCycleID IS NULL;
        SET @RowsIns = @@ROWCOUNT;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Insert',
            @StepStart,
            @StepEnd,
            CONCAT('Incremental: Inserted ', @RowsIns, ' new billing cycles')
        );

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Msg     = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Error',
            @StepStart,
            @StepEnd,
            CONCAT('Incremental: ', @Msg)
        );
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- 2-6) UpdateDimPaymentMethodIncremental
-- SCD1 incremental for Dim.DimPaymentMethod
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.UpdateDimPaymentMethodIncremental
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE
        @TableName NVARCHAR(128) = 'Dim.DimPaymentMethod',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Msg       NVARCHAR(2000),
        @RowsIns   INT;

    BEGIN TRY
        BEGIN TRAN;

        -- Insert any new payment methods
        SET @StepStart = GETDATE();
        INSERT INTO Dim.DimPaymentMethod (PaymentMethodName)
        SELECT DISTINCT src.PaymentMethod
        FROM StagingDB.Finance.Payment AS src
        LEFT JOIN Dim.DimPaymentMethod AS D
            ON src.PaymentMethod = D.PaymentMethodName
        WHERE D.DimPaymentMethodID IS NULL;
        SET @RowsIns = @@ROWCOUNT;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Insert',
            @StepStart,
            @StepEnd,
            CONCAT('Incremental: Inserted ', @RowsIns, ' new payment methods')
        );

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Msg     = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Error',
            @StepStart,
            @StepEnd,
            CONCAT('Incremental: ', @Msg)
        );
        THROW;
    END CATCH;
END;
GO


-- 2-7) UpdateDimContractIncremental
-- SCD3 incremental for Dim.DimContract
CREATE OR ALTER PROCEDURE Dim.UpdateDimContractIncremental
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE
        @TableName NVARCHAR(128) = 'Dim.DimContract',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Msg       NVARCHAR(2000),
        @Today     DATE = CONVERT(DATE, GETDATE());

    BEGIN TRY
        BEGIN TRAN;

        -- STEP 1: Update existing with previous status shift
        SET @StepStart = GETDATE();
        UPDATE D
        SET
            D.PrevContractStatus2 = D.PrevContractStatus1,
            D.PrevContractStatus1 = D.ContractStatus,
            D.ContractStatus      = S.ContractStatus,
            D.StartDateActual     = S.StartDate,
            D.EndDateActual       = S.EndDate
        FROM Dim.DimContract AS D
        INNER JOIN StagingDB.Finance.Contract AS S
            ON D.ContractNumber = S.ContractNumber
        WHERE D.ContractStatus <> S.ContractStatus
           OR ISNULL(D.StartDateActual,'') <> ISNULL(S.StartDate,'')
           OR ISNULL(D.EndDateActual,'')   <> ISNULL(S.EndDate,'');

        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName, 'Update', @StepStart, @StepEnd, 'Incremental: Updated contracts with shifted history');

        -- STEP 2: Insert new contracts
        SET @StepStart = GETDATE();
        INSERT INTO Dim.DimContract
        (
            ContractNumber, ContractStatus,
            PrevContractStatus1, PrevContractStatus2,
            PaymentTerms, StartDateActual, EndDateActual
        )
        SELECT
            S.ContractNumber, S.ContractStatus,
            NULL, NULL,
            S.PaymentTerms, S.StartDate, S.EndDate
        FROM StagingDB.Finance.Contract AS S
        LEFT JOIN Dim.DimContract AS D
            ON S.ContractNumber = D.ContractNumber
        WHERE D.DimContractID IS NULL;

        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName, 'Insert', @StepStart, @StepEnd, CONCAT('Incremental: Inserted ', @@ROWCOUNT, ' new contracts'));

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Msg     = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName, 'Error', @StepStart, @StepEnd, CONCAT('Incremental: ', @Msg));
        THROW;
    END CATCH;
END;
GO

-- 2-8) UpdateDimInvoiceIncremental
-- Static incremental for Dim.DimInvoice
CREATE OR ALTER PROCEDURE Dim.UpdateDimInvoiceIncremental
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE
        @TableName NVARCHAR(128) = 'Dim.DimInvoice',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Msg       NVARCHAR(2000);

    BEGIN TRY
        BEGIN TRAN;

        -- STEP 1: Update changed invoices
        SET @StepStart = GETDATE();
        UPDATE D
        SET
            D.InvoiceStatus = S.[Status],
            D.CreatedBy     = S.CreatedBy,
            D.CreatedDate   = S.CreatedDate
        FROM Dim.DimInvoice AS D
        INNER JOIN StagingDB.Finance.Invoice AS S
            ON D.InvoiceNumber = S.InvoiceNumber
        WHERE D.InvoiceStatus <> S.[Status]
           OR D.CreatedBy     <> S.CreatedBy
           OR D.CreatedDate   <> S.CreatedDate;

        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName, 'Update', @StepStart, @StepEnd, 'Incremental: Updated invoice metadata');

        -- STEP 2: Insert new invoice records
        SET @StepStart = GETDATE();
        INSERT INTO Dim.DimInvoice (InvoiceNumber, InvoiceStatus, CreatedBy, CreatedDate)
        SELECT S.InvoiceNumber, S.[Status], S.CreatedBy, S.CreatedDate
        FROM StagingDB.Finance.Invoice AS S
        LEFT JOIN Dim.DimInvoice AS D
            ON S.InvoiceNumber = D.InvoiceNumber
        WHERE D.DimInvoiceID IS NULL;

        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName, 'Insert', @StepStart, @StepEnd, CONCAT('Incremental: Inserted ', @@ROWCOUNT, ' new invoices'));

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Msg     = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName, 'Error', @StepStart, @StepEnd, CONCAT('Incremental: ', @Msg));
        THROW;
    END CATCH;
END;
GO


USE DataWarehouse;
GO

--------------------------------------------------------------------------------
-- 1) UpdateDimDepartment (SCD Type 1)
-- Handles incremental updates for DimDepartment.
-- It inserts new departments and overwrites existing ones if changes are detected.
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.UpdateDimDepartment
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessName NVARCHAR(128) = 'Dim.UpdateDimDepartment';
    DECLARE @TargetTable NVARCHAR(128) = 'Dim.DimDepartment';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);
    DECLARE @RecordsAffected INT = 0;

    -- Create a temporary table to hold the final source data after joins
    SELECT 
        s.DepartmentID,
        s.DepartmentName,
        e.FullName AS ManagerName,
        s.IsActive
    INTO #SourceDepartments
    FROM StagingDB.HumanResources.Department s
    LEFT JOIN StagingDB.HumanResources.Employee e ON s.ManagerID = e.EmployeeID;

    BEGIN TRY
        -- Use MERGE for efficient Insert/Update logic (SCD1)
        MERGE Dim.DimDepartment AS Target
        USING #SourceDepartments AS Source
        ON (Target.DepartmentID = Source.DepartmentID)
        WHEN MATCHED AND (
            Target.DepartmentName <> Source.DepartmentName OR
            ISNULL(Target.ManagerName, '') <> ISNULL(Source.ManagerName, '') OR
            Target.IsActive <> Source.IsActive
        ) THEN
            UPDATE SET
                DepartmentName = Source.DepartmentName,
                ManagerName = Source.ManagerName,
                IsActive = Source.IsActive
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (DepartmentID, DepartmentName, ManagerName, IsActive)
            VALUES (Source.DepartmentID, Source.DepartmentName, Source.ManagerName, Source.IsActive);
        
        SET @RecordsAffected = @@ROWCOUNT;
        SET @Message = 'DimDepartment update process completed successfully.';
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, RecordsUpdated, Status, [Message])
        VALUES (@ProcessName, 'Incremental Load', @TargetTable, @StartTime, GETDATE(), @RecordsAffected, 'Success', @Message);

        DROP TABLE IF EXISTS #SourceDepartments;
    END TRY
    BEGIN CATCH
        DROP TABLE IF EXISTS #SourceDepartments;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, Status, [Message])
        VALUES (@ProcessName, 'Incremental Load', @TargetTable, @StartTime, GETDATE(), 'Failed', @Message);
        THROW;
    END CATCH
END;
GO

--------------------------------------------------------------------------------
-- 2) UpdateDimJobTitle (SCD Type 3)
-- Handles incremental updates for DimJobTitle.
-- It inserts new job titles and updates existing ones for SCD3 changes.
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.UpdateDimJobTitle
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessName NVARCHAR(128) = 'Dim.UpdateDimJobTitle';
    DECLARE @TargetTable NVARCHAR(128) = 'Dim.DimJobTitle';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);
    DECLARE @RecordsInserted INT = 0;
    DECLARE @RecordsUpdated INT = 0;

    BEGIN TRY
        INSERT INTO Dim.DimJobTitle (JobTitleID, JobTitleName, CurrentJobCategory, PreviousJobCategory)
        SELECT
            s.JobTitleID,
            s.JobTitleName,
            s.JobCategory,
            NULL
        FROM StagingDB.HumanResources.JobTitle s
        WHERE NOT EXISTS (
            SELECT 1 FROM Dim.DimJobTitle d WHERE d.JobTitleID = s.JobTitleID
        );
        SET @RecordsInserted = @@ROWCOUNT;

        UPDATE Dim.DimJobTitle
        SET 
            PreviousJobCategory = CurrentJobCategory,
            CurrentJobCategory = s.JobCategory
        FROM Dim.DimJobTitle d
        JOIN StagingDB.HumanResources.JobTitle s ON d.JobTitleID = s.JobTitleID
        WHERE 
            ISNULL(d.CurrentJobCategory, '') <> ISNULL(s.JobCategory, '');
        
        SET @RecordsUpdated = @@ROWCOUNT;
        
        SET @Message = 'DimJobTitle update process completed successfully.';
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, RecordsInserted, RecordsUpdated, Status, [Message])
        VALUES (@ProcessName, 'Incremental Load', @TargetTable, @StartTime, GETDATE(), @RecordsInserted, @RecordsUpdated, 'Success', @Message);
    END TRY
    BEGIN CATCH
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, Status, [Message])
        VALUES (@ProcessName, 'Incremental Load', @TargetTable, @StartTime, GETDATE(), 'Failed', @Message);
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- 3) UpdateDimEmployee (SCD Type 2)
-- Handles the most complex update, expiring old records and inserting new ones.
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.UpdateDimEmployee
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessName NVARCHAR(128) = 'Dim.UpdateDimEmployee';
    DECLARE @TargetTable NVARCHAR(128) = 'Dim.DimEmployee';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);
    DECLARE @RecordsInserted INT = 0;
    DECLARE @RecordsUpdated INT = 0;
    DECLARE @Today DATE = GETDATE();

    BEGIN TRY
        SELECT 
            s.*,
            d.EmployeeKey AS CurrentEmployeeKey
        INTO #EmployeeChanges
        FROM StagingDB.HumanResources.Employee s
        JOIN Dim.DimEmployee d ON s.EmployeeID = d.EmployeeID AND d.IsCurrent = 1
        WHERE 
            ISNULL(d.Position, '') <> ISNULL(s.Position, '') OR
            ISNULL(d.Address, '') <> ISNULL(s.Address, '') OR
            ISNULL(d.Phone, '') <> ISNULL(s.Phone, '') OR
            ISNULL(d.MaritalStatus, '') <> ISNULL(s.MaritalStatus, '') OR
            ISNULL(d.EmploymentStatus, '') <> ISNULL(s.EmploymentStatus, '');

        UPDATE Dim.DimEmployee
        SET 
            EndDate = DATEADD(DAY, -1, @Today),
            IsCurrent = 0
        WHERE EmployeeKey IN (SELECT CurrentEmployeeKey FROM #EmployeeChanges);
        SET @RecordsUpdated = @@ROWCOUNT;

        INSERT INTO Dim.DimEmployee (EmployeeID, FullName, Gender, MaritalStatus, Position, Address, Phone, EmploymentStatus, StartDate, EndDate, IsCurrent)
        SELECT 
            c.EmployeeID, c.FullName, c.Gender, c.MaritalStatus, c.Position,
            c.Address, c.Phone, c.EmploymentStatus, @Today AS StartDate,
            NULL AS EndDate, 1 AS IsCurrent
        FROM #EmployeeChanges c;
        SET @RecordsInserted = @@ROWCOUNT;

        INSERT INTO Dim.DimEmployee (EmployeeID, FullName, Gender, MaritalStatus, Position, Address, Phone, EmploymentStatus, StartDate, EndDate, IsCurrent)
        SELECT 
            s.EmployeeID, s.FullName, s.Gender, s.MaritalStatus, s.Position,
            s.Address, s.Phone, s.EmploymentStatus, s.HireDate AS StartDate,
            NULL AS EndDate, 1 AS IsCurrent
        FROM StagingDB.HumanResources.Employee s
        WHERE NOT EXISTS (SELECT 1 FROM Dim.DimEmployee d WHERE d.EmployeeID = s.EmployeeID);
        SET @RecordsInserted = @RecordsInserted + @@ROWCOUNT;
        
        SET @Message = 'DimEmployee update process completed successfully.';
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, RecordsInserted, RecordsUpdated, Status, [Message])
        VALUES (@ProcessName, 'Incremental Load', @TargetTable, @StartTime, GETDATE(), @RecordsInserted, @RecordsUpdated, 'Success', @Message);

        DROP TABLE IF EXISTS #EmployeeChanges;
    END TRY
    BEGIN CATCH
        DROP TABLE IF EXISTS #EmployeeChanges;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, Status, [Message])
        VALUES (@ProcessName, 'Incremental Load', @TargetTable, @StartTime, GETDATE(), 'Failed', @Message);
        THROW;
    END CATCH
END;
GO

--------------------------------------------------------------------------------
-- 4) UpdateDimLeaveType
-- Inserts new leave types if they do not exist.
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.UpdateDimLeaveType
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessName NVARCHAR(128) = 'Dim.UpdateDimLeaveType';
    DECLARE @TargetTable NVARCHAR(128) = 'Dim.DimLeaveType';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);

    BEGIN TRY
        INSERT INTO Dim.DimLeaveType(LeaveTypeID, LeaveTypeName, IsPaid)
        SELECT
            s.LeaveTypeID,
            s.LeaveTypeName,
            s.IsPaid
        FROM StagingDB.HumanResources.LeaveType s
        WHERE NOT EXISTS (
            SELECT 1 FROM Dim.DimLeaveType d WHERE d.LeaveTypeID = s.LeaveTypeID
        );
        
        SET @Message = 'DimLeaveType update process completed successfully.';
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, RecordsInserted, Status, [Message])
        VALUES (@ProcessName, 'Incremental Load', @TargetTable, @StartTime, GETDATE(), @@ROWCOUNT, 'Success', @Message);
    END TRY
    BEGIN CATCH
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, Status, [Message])
        VALUES (@ProcessName, 'Incremental Load', @TargetTable, @StartTime, GETDATE(), 'Failed', @Message);
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- 5) UpdateDimTerminationReason
-- Inserts new termination reasons if they do not exist.
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.UpdateDimTerminationReason
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessName NVARCHAR(128) = 'Dim.UpdateDimTerminationReason';
    DECLARE @TargetTable NVARCHAR(128) = 'Dim.DimTerminationReason';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);

    BEGIN TRY
        INSERT INTO Dim.DimTerminationReason(TerminationReason)
        SELECT DISTINCT s.TerminationReason
        FROM StagingDB.HumanResources.Termination s
        WHERE s.TerminationReason IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM Dim.DimTerminationReason d WHERE d.TerminationReason = s.TerminationReason
        );

        SET @Message = 'DimTerminationReason update process completed successfully.';
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, RecordsInserted, Status, [Message])
        VALUES (@ProcessName, 'Incremental Load', @TargetTable, @StartTime, GETDATE(), @@ROWCOUNT, 'Success', @Message);
    END TRY
    BEGIN CATCH
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, Status, [Message])
        VALUES (@ProcessName, 'Incremental Load', @TargetTable, @StartTime, GETDATE(), 'Failed', @Message);
        THROW;
    END CATCH;
END;
GO

