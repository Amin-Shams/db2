USE DataWarehouse;
GO

--------------------------------------------------------------------------------
-- 1-1) LoadDimDateInitialLoad
-- Full initial populate of Dim.DimDate (from 2025-06-22 to today)
--------------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE Dim.LoadDimDateInitialLoad
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE 
        @TableName NVARCHAR(128) = 'Dim.DimDate',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Message   NVARCHAR(2000),
        @StartDate DATE = '2025-06-22',
        @EndDate   DATE = CONVERT(DATE, GETDATE()),
        @DayCount  INT;

    BEGIN TRY
        BEGIN TRAN;

        --------------------------------------------------------------------
        -- STEP 1: Delete all existing rows (DELETE + reseed)
        --------------------------------------------------------------------
        SET @StepStart = GETDATE();

        DELETE FROM Dim.DimDate;
        -- Reset identity so next insert starts at 1
        DBCC CHECKIDENT('Dim.DimDate', RESEED, 0);

        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Delete',
            @StepStart,
            @StepEnd,
            'FirstLoad: Deleted all rows and reseeded DimDate'
        );

        --------------------------------------------------------------------
        -- STEP 2: Compute number of days to generate
        --------------------------------------------------------------------
        SET @DayCount = DATEDIFF(DAY, @StartDate, @EndDate) + 1;

        --------------------------------------------------------------------
        -- STEP 3: Generate and Insert date rows
        --------------------------------------------------------------------
        SET @StepStart = GETDATE();
        SET DATEFIRST 1;  -- ensure Monday = 1

        ;WITH DateSeq AS
        (
            SELECT TOP (@DayCount)
                DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1, @StartDate) AS FullDate
            FROM sys.all_objects AS s1
            CROSS JOIN sys.all_objects AS s2
        )
        INSERT INTO Dim.DimDate
        (
            FullDate,
            [Year],
            [Quarter],
            [Month],
            MonthName,
            [Day],
            DayOfWeek,
            DayName,
            WeekOfYear,
            IsWeekend
        )
        SELECT
            FullDate,
            YEAR(FullDate),
            DATEPART(QUARTER, FullDate),
            MONTH(FullDate),
            DATENAME(MONTH, FullDate),
            DAY(FullDate),
            DATEPART(WEEKDAY, FullDate),
            DATENAME(WEEKDAY, FullDate),
            DATEPART(WEEK, FullDate),
            CASE WHEN DATEPART(WEEKDAY, FullDate) IN (6,7) THEN 1 ELSE 0 END
        FROM DateSeq
        ORDER BY FullDate
        OPTION (MAXDOP 1);  -- محدود کردن موازی‌سازی برای ثبات

        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Insert',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: Inserted ', @@ROWCOUNT, ' rows into Dim.DimDate')
        );

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Error',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: ', @Message)
        );
        THROW;
    END CATCH;
END;
GO


--------------------------------------------------------------------------------
-- 1-2) LoadDimCustomerInitialLoad
-- Full initial populate of Dim.DimCustomer (SCD Type 2)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.LoadDimCustomerInitialLoad
AS
BEGIN
    DECLARE
        @TableName   NVARCHAR(128) = 'Dim.DimCustomer',
        @StepStart   DATETIME,
        @StepEnd     DATETIME,
        @Message     NVARCHAR(2000),
        @NullCount   INT,
        @DupCount    INT;

    BEGIN TRY
        BEGIN TRAN;

        ------------------------------------------------------------------------
        -- STEP 1: Truncate target dimension
        ------------------------------------------------------------------------
        SET @StepStart = GETDATE();
        DELETE FROM Dim.DimCustomer;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Truncate',
            @StepStart,
            @StepEnd,
            'FirstLoad: Truncated Dim.DimCustomer'
        );

        ------------------------------------------------------------------------
        -- STEP 2: Validate source for NULLs & duplicates on business key
        ------------------------------------------------------------------------
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM StagingDB.Finance.Customer
        WHERE CustomerCode    IS NULL
           OR CustomerName    IS NULL
           OR CustomerType    IS NULL;
        IF @NullCount > 0
            THROW 51002, 'Validation failed: NULLs in StagingDB.Finance.Customer', 1;

        SELECT @DupCount = COUNT(*)
        FROM (
            SELECT CustomerCode, COUNT(*) AS Cnt
            FROM StagingDB.Finance.Customer
            GROUP BY CustomerCode
            HAVING COUNT(*) > 1
        ) AS D;
        IF @DupCount > 0
            THROW 51003, 'Validation failed: Duplicates in StagingDB.Finance.Customer', 1;

        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Validate',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: Found ', @NullCount, ' NULLs & ', @DupCount, ' duplicates')
        );

        ------------------------------------------------------------------------
        -- STEP 3: Insert initial SCD2 records from staging
        ------------------------------------------------------------------------
        SET @StepStart = GETDATE();
        INSERT INTO Dim.DimCustomer
        (
            CustomerCode,
            CustomerName,
            CustomerType,
            CountryName,
            VATNumber,
            Address,
            Email,
            Phone,
            StartDate,
            EndDate,
            IsCurrent
        )
        SELECT
            CustomerCode,
            CustomerName,
            CustomerType,
            (SELECT CountryName 
               FROM TradePortDB.Common.Country
              WHERE CountryID = C.CountryID),
            VATNumber,
            [Address],
            Email,
            Phone,
            CONVERT(DATE, GETDATE()),  -- start as today
            NULL,                      -- no end date on initial load
            1                          -- current flag
        FROM StagingDB.Finance.Customer AS C;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Insert',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: Inserted ', @@ROWCOUNT, ' rows into Dim.DimCustomer')
        );

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Error',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: ', @Message)
        );
        THROW;
    END CATCH;
END;
GO



--------------------------------------------------------------------------------
-- 1-3) LoadDimServiceTypeInitialLoad
-- Full initial populate of Dim.DimServiceType (SCD Type 1)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.LoadDimServiceTypeInitialLoad
AS
BEGIN
    DECLARE
        @TableName   NVARCHAR(128) = 'Dim.DimServiceType',
        @StepStart   DATETIME,
        @StepEnd     DATETIME,
        @Message     NVARCHAR(2000),
        @NullCount   INT,
        @DupCount    INT;

    BEGIN TRY
        BEGIN TRAN;

        ------------------------------------------------------------------------
        -- STEP 1: Truncate target dimension
        ------------------------------------------------------------------------
        SET @StepStart = GETDATE();
        DELETE FROM Dim.DimServiceType;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Truncate',
            @StepStart,
            @StepEnd,
            'FirstLoad: Truncated Dim.DimServiceType'
        );

        ------------------------------------------------------------------------
        -- STEP 2: Validate source for NULLs & duplicates on business key
        ------------------------------------------------------------------------
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*)
        FROM StagingDB.Finance.ServiceType
        WHERE ServiceTypeID IS NULL
           OR ServiceName   IS NULL
           OR UnitOfMeasure IS NULL;
        IF @NullCount > 0
            THROW 53000, 'Validation failed: NULLs in StagingDB.Finance.ServiceType', 1;

        SELECT @DupCount = COUNT(*)
        FROM (
            SELECT ServiceTypeID, COUNT(*) AS Cnt
            FROM StagingDB.Finance.ServiceType
            GROUP BY ServiceTypeID
            HAVING COUNT(*) > 1
        ) AS D;
        IF @DupCount > 0
            THROW 53001, 'Validation failed: Duplicates in StagingDB.Finance.ServiceType', 1;

        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Validate',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: Found ', @NullCount, ' NULLs & ', @DupCount, ' duplicates')
        );

        ------------------------------------------------------------------------
        -- STEP 3: Insert initial SCD1 records from staging
        ------------------------------------------------------------------------
        SET @StepStart = GETDATE();
        INSERT INTO Dim.DimServiceType
        (
            SourceServiceTypeID,  -- map staging.ServiceTypeID
            ServiceName,
            ServiceCategory,
            UnitOfMeasure,
            Taxable,
            IsActive
        )
        SELECT
            ServiceTypeID,
            ServiceName,
            ServiceCategory,
            UnitOfMeasure,
            Taxable,
            IsActive
        FROM StagingDB.Finance.ServiceType;
        SET @StepEnd = GETDATE();

        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Insert',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: Inserted ', @@ROWCOUNT, ' rows into Dim.DimServiceType')
        );

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Error',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: ', @Message)
        );
        THROW;
    END CATCH;
END;
GO





--------------------------------------------------------------------------------
-- 1-4) LoadDimTaxInitialLoad
-- Full initial populate of Dim.DimTax (SCD Type 2)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.LoadDimTaxInitialLoad
AS
BEGIN
    DECLARE
        @TableName   NVARCHAR(128) = 'Dim.DimTax',
        @StepStart   DATETIME,
        @StepEnd     DATETIME,
        @Message     NVARCHAR(2000),
        @NullCount   INT,
        @DupCount    INT;

    BEGIN TRY
        BEGIN TRAN;

        ------------------------------------------------------------------------
        -- STEP 1: Truncate target dimension
        ------------------------------------------------------------------------
        SET @StepStart = GETDATE();
        DELETE FROM Dim.DimTax;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Truncate',
            @StepStart,
            @StepEnd,
            'FirstLoad: Truncated Dim.DimTax'
        );

        ------------------------------------------------------------------------
        -- STEP 2: Validate staging for NULLs & duplicates on business key
        ------------------------------------------------------------------------
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*)
        FROM StagingDB.Finance.Tax
        WHERE TaxName       IS NULL
           OR TaxRate       IS NULL
           OR TaxType       IS NULL
           OR EffectiveFrom IS NULL;
        IF @NullCount > 0
            THROW 54000, 'Validation failed: NULLs in StagingDB.Finance.Tax', 1;

        SELECT @DupCount = COUNT(*)
        FROM (
            SELECT TaxName, TaxType, EffectiveFrom, COUNT(*) AS Cnt
            FROM StagingDB.Finance.Tax
            GROUP BY TaxName, TaxType, EffectiveFrom
            HAVING COUNT(*) > 1
        ) AS D;
        IF @DupCount > 0
            THROW 54001, 'Validation failed: Duplicates in StagingDB.Finance.Tax', 1;

        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Validate',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: Found ', @NullCount, ' NULLs & ', @DupCount, ' duplicates')
        );

        ------------------------------------------------------------------------
        -- STEP 3: Insert initial SCD2 records from staging
        ------------------------------------------------------------------------
        SET @StepStart = GETDATE();
        INSERT INTO Dim.DimTax
        (
            TaxName,
            TaxRate,
            TaxType,
            EffectiveFrom,
            EffectiveTo,
            StartDate,
            EndDate,
            IsCurrent
        )
        SELECT
            TaxName,
            TaxRate,
            TaxType,
            EffectiveFrom,
            EffectiveTo,
            CONVERT(DATE, GETDATE()), -- start as today
            NULL,                     -- no end date on initial load
            1                         -- current flag
        FROM StagingDB.Finance.Tax;
        SET @StepEnd = GETDATE();

        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Insert',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: Inserted ', @@ROWCOUNT, ' rows into Dim.DimTax')
        );

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Error',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: ', @Message)
        );
        THROW;
    END CATCH;
END;
GO



--------------------------------------------------------------------------------
-- 1-5) LoadDimBillingCycleInitialLoad
-- Full initial populate of Dim.DimBillingCycle (SCD Type 1)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.LoadDimBillingCycleInitialLoad
AS
BEGIN
    DECLARE
        @TableName   NVARCHAR(128) = 'Dim.DimBillingCycle',
        @StepStart   DATETIME,
        @StepEnd     DATETIME,
        @Message     NVARCHAR(2000),
        @NullCount   INT,
        @DupCount    INT;

    BEGIN TRY
        BEGIN TRAN;

        ------------------------------------------------------------------------
        -- STEP 1: Truncate target dimension
        ------------------------------------------------------------------------
        SET @StepStart = GETDATE();
        DELETE FROM Dim.DimBillingCycle;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Truncate',
            @StepStart,
            @StepEnd,
            'FirstLoad: Truncated Dim.DimBillingCycle'
        );

        ------------------------------------------------------------------------
        -- STEP 2: Validate staging for NULLs & duplicates on CycleName
        ------------------------------------------------------------------------
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*)
        FROM StagingDB.Finance.BillingCycle
        WHERE CycleName         IS NULL
           OR CycleLengthInDays IS NULL;
        IF @NullCount > 0
            THROW 55000, 'Validation failed: NULLs in StagingDB.Finance.BillingCycle', 1;

        SELECT @DupCount = COUNT(*)
        FROM (
            SELECT CycleName, COUNT(*) AS Cnt
            FROM StagingDB.Finance.BillingCycle
            GROUP BY CycleName
            HAVING COUNT(*) > 1
        ) AS D;
        IF @DupCount > 0
            THROW 55001, 'Validation failed: Duplicates in StagingDB.Finance.BillingCycle', 1;

        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Validate',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: Found ', @NullCount, ' NULLs & ', @DupCount, ' duplicates')
        );

        ------------------------------------------------------------------------
        -- STEP 3: Insert initial SCD1 records from staging
        ------------------------------------------------------------------------
        SET @StepStart = GETDATE();
        INSERT INTO Dim.DimBillingCycle
        (
            CycleName,
            CycleLengthInDays
        )
        SELECT
            CycleName,
            CycleLengthInDays
        FROM StagingDB.Finance.BillingCycle;
        SET @StepEnd = GETDATE();

        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Insert',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: Inserted ', @@ROWCOUNT, ' rows into Dim.DimBillingCycle')
        );

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Error',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: ', @Message)
        );
        THROW;
    END CATCH;
END;
GO


--------------------------------------------------------------------------------
-- 1-6) LoadDimPaymentMethodInitialLoad
-- Full initial populate of Dim.DimPaymentMethod (SCD Type 1)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.LoadDimPaymentMethodInitialLoad
AS
BEGIN
    DECLARE
        @TableName NVARCHAR(128) = 'Dim.DimPaymentMethod',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Message   NVARCHAR(2000),
        @NullCount INT;

    BEGIN TRY
        BEGIN TRAN;

        ------------------------------------------------------------------------
        -- STEP 1: Truncate target dimension
        ------------------------------------------------------------------------
        SET @StepStart = GETDATE();
        DELETE FROM Dim.DimPaymentMethod;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Truncate',
            @StepStart,
            @StepEnd,
            'FirstLoad: Truncated Dim.DimPaymentMethod'
        );

        ------------------------------------------------------------------------
        -- STEP 2: Validate staging for NULLs in PaymentMethod
        ------------------------------------------------------------------------
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*)
        FROM StagingDB.Finance.Payment
        WHERE PaymentMethod IS NULL;
        IF @NullCount > 0
            THROW 56000, 'Validation failed: NULL PaymentMethod in StagingDB.Finance.Payment', 1;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Validate',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: Found ', @NullCount, ' NULL PaymentMethod values')
        );

        ------------------------------------------------------------------------
        -- STEP 3: Insert distinct PaymentMethod values
        ------------------------------------------------------------------------
        SET @StepStart = GETDATE();
        INSERT INTO Dim.DimPaymentMethod (PaymentMethodName)
        SELECT DISTINCT PaymentMethod
        FROM StagingDB.Finance.Payment;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Insert',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: Inserted ', @@ROWCOUNT, ' rows into Dim.DimPaymentMethod')
        );

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Error',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: ', @Message)
        );
        THROW;
    END CATCH;
END;
GO


--------------------------------------------------------------------------------
-- 1-7) LoadDimContractInitialLoad
-- Full initial populate of Dim.DimContract (SCD Type 3)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.LoadDimContractInitialLoad
AS
BEGIN
    DECLARE
        @TableName NVARCHAR(128) = 'Dim.DimContract',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Message   NVARCHAR(2000),
        @NullCount INT,
        @DupCount  INT;

    BEGIN TRY
        BEGIN TRAN;

        ------------------------------------------------------------------------
        -- STEP 1: Truncate target dimension
        ------------------------------------------------------------------------
        SET @StepStart = GETDATE();
        DELETE FROM Dim.DimContract;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Truncate',
            @StepStart,
            @StepEnd,
            'FirstLoad: Truncated Dim.DimContract'
        );

        ------------------------------------------------------------------------
        -- STEP 2: Validate staging for NULLs & duplicates on ContractNumber
        ------------------------------------------------------------------------
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*)
        FROM StagingDB.Finance.Contract
        WHERE ContractNumber  IS NULL
           OR ContractStatus  IS NULL
           OR StartDate       IS NULL;
        IF @NullCount > 0
            THROW 57000, 'Validation failed: NULLs in StagingDB.Finance.Contract', 1;

        SELECT @DupCount = COUNT(*)
        FROM (
            SELECT ContractNumber, COUNT(*) AS Cnt
            FROM StagingDB.Finance.Contract
            GROUP BY ContractNumber
            HAVING COUNT(*) > 1
        ) AS D;
        IF @DupCount > 0
            THROW 57001, 'Validation failed: Duplicates in StagingDB.Finance.Contract', 1;

        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Validate',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: Found ', @NullCount, ' NULLs & ', @DupCount, ' duplicates')
        );

        ------------------------------------------------------------------------
        -- STEP 3: Insert initial SCD3 records from staging
        ------------------------------------------------------------------------
        SET @StepStart = GETDATE();
        INSERT INTO Dim.DimContract
        (
            ContractNumber,
            ContractStatus,
            PrevContractStatus1,
            PrevContractStatus2,
            PaymentTerms,
            StartDateActual,
            EndDateActual
        )
        SELECT
            ContractNumber,
            ContractStatus,
            NULL,           -- no previous status on initial load
            NULL,           -- no second previous status
            PaymentTerms,
            StartDate,
            EndDate
        FROM StagingDB.Finance.Contract;
        SET @StepEnd = GETDATE();

        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Insert',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: Inserted ', @@ROWCOUNT, ' rows into Dim.DimContract')
        );

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Error',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: ', @Message)
        );
        THROW;
    END CATCH;
END;
GO





--------------------------------------------------------------------------------
-- 1-8) LoadDimInvoiceInitialLoad
-- Full initial populate of Dim.DimInvoice (Static)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.LoadDimInvoiceInitialLoad
AS
BEGIN
    DECLARE
        @TableName NVARCHAR(128) = 'Dim.DimInvoice',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Message   NVARCHAR(2000),
        @NullCount INT,
        @DupCount  INT;

    BEGIN TRY
        BEGIN TRAN;

        ------------------------------------------------------------------------
        -- STEP 1: Truncate target dimension
        ------------------------------------------------------------------------
        SET @StepStart = GETDATE();
        DELETE FROM Dim.DimInvoice;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Truncate',
            @StepStart,
            @StepEnd,
            'FirstLoad: Truncated Dim.DimInvoice'
        );

        ------------------------------------------------------------------------
        -- STEP 2: Validate staging for NULLs & duplicates on InvoiceNumber
        ------------------------------------------------------------------------
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*)
        FROM StagingDB.Finance.Invoice
        WHERE InvoiceNumber IS NULL
           OR [Status]      IS NULL    -- use source column name
           OR CreatedDate  IS NULL;
        IF @NullCount > 0
            THROW 58000, 'Validation failed: NULLs in StagingDB.Finance.Invoice', 1;

        SELECT @DupCount = COUNT(*)
        FROM (
            SELECT InvoiceNumber, COUNT(*) AS Cnt
            FROM StagingDB.Finance.Invoice
            GROUP BY InvoiceNumber
            HAVING COUNT(*) > 1
        ) AS D;
        IF @DupCount > 0
            THROW 58001, 'Validation failed: Duplicates in StagingDB.Finance.Invoice', 1;

        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Validate',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: Found ', @NullCount, ' NULLs & ', @DupCount, ' duplicates')
        );

        ------------------------------------------------------------------------
        -- STEP 3: Insert initial static records from staging
        ------------------------------------------------------------------------
        SET @StepStart = GETDATE();
        INSERT INTO Dim.DimInvoice
        (
            InvoiceNumber,
            InvoiceStatus,  -- maps to staging.[Status]
            CreatedBy,
            CreatedDate
        )
        SELECT
            InvoiceNumber,
            [Status],       -- source column
            CreatedBy,
            CreatedDate
        FROM StagingDB.Finance.Invoice;
        SET @StepEnd = GETDATE();

        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Insert',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: Inserted ', @@ROWCOUNT, ' rows into Dim.DimInvoice')
        );

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Error',
            @StepStart,
            @StepEnd,
            CONCAT('FirstLoad: ', @Message)
        );
        THROW;
    END CATCH;
END;
GO


USE DataWarehouse;
GO

--------------------------------------------------------------------------------
-- 1) LoadDimDate
-- Populates the Date Dimension if it's not already populated.
-- This procedure is safe to re-run.
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.LoadDimDate
    @StartDate DATE = '2015-01-01',
    @EndDate   DATE = '2030-12-31'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessName NVARCHAR(128) = 'Dim.LoadDimDate';
    DECLARE @TargetTable NVARCHAR(128) = 'Dim.DimDate';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);
    DECLARE @RecordsInserted INT = 0;

    BEGIN TRY
        -- Step 1: Add the Unknown Member if it does not exist
        IF NOT EXISTS (SELECT 1 FROM Dim.DimDate WHERE DateKey = -1)
        BEGIN
            SET IDENTITY_INSERT Dim.DimDate ON;
            INSERT INTO Dim.DimDate (DateKey, FullDate, [Year], [Quarter], [Month], MonthName, [Day], DayOfWeek, DayName, WeekOfYear, IsWeekend)
            VALUES (-1, '1900-01-01', 1900, 0, 0, 'Unknown', 0, 0, 'Unknown', 0, 0);
            SET IDENTITY_INSERT Dim.DimDate OFF;
        END

        -- Step 2: Populate the dimension with the date range
        DECLARE @CurrentDate DATE = @StartDate;
        WHILE @CurrentDate <= @EndDate
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM Dim.DimDate WHERE FullDate = @CurrentDate)
            BEGIN
                INSERT INTO Dim.DimDate (FullDate, [Year], [Quarter], [Month], MonthName, [Day], DayOfWeek, DayName, WeekOfYear, IsWeekend)
                VALUES (
                    @CurrentDate, YEAR(@CurrentDate), DATEPART(QUARTER, @CurrentDate), MONTH(@CurrentDate),
                    FORMAT(@CurrentDate, 'MMMM', 'en-US'), DAY(@CurrentDate), DATEPART(WEEKDAY, @CurrentDate),
                    FORMAT(@CurrentDate, 'dddd', 'en-US'), DATEPART(WEEK, @CurrentDate),
                    CASE WHEN DATEPART(WEEKDAY, @CurrentDate) IN (6, 7) THEN 1 ELSE 0 END
                );
                SET @RecordsInserted = @RecordsInserted + 1;
            END
            SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
        END;

        SET @Message = 'DimDate load process completed.';
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, RecordsInserted, Status, [Message])
        VALUES (@ProcessName, 'Initial Load', @TargetTable, @StartTime, GETDATE(), @RecordsInserted, 'Success', @Message);
    END TRY
    BEGIN CATCH
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, Status, [Message])
        VALUES (@ProcessName, 'Initial Load', @TargetTable, @StartTime, GETDATE(), 'Failed', @Message);
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- 2) LoadDimDepartment (First Load - No Truncate)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.LoadDimDepartment
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessName NVARCHAR(128) = 'Dim.LoadDimDepartment';
    DECLARE @TargetTable NVARCHAR(128) = 'Dim.DimDepartment';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);

    BEGIN TRY
        IF NOT EXISTS (SELECT 1 FROM Dim.DimDepartment WHERE DepartmentKey = -1)
        BEGIN
            SET IDENTITY_INSERT Dim.DimDepartment ON;
            INSERT INTO Dim.DimDepartment (DepartmentKey, DepartmentID, DepartmentName, ManagerName, IsActive)
            VALUES (-1, -1, 'Unknown', 'Unknown', 0);
            SET IDENTITY_INSERT Dim.DimDepartment OFF;
        END

        INSERT INTO Dim.DimDepartment (DepartmentID, DepartmentName, ManagerName, IsActive)
        SELECT 
            s.DepartmentID, s.DepartmentName, e.FullName, s.IsActive
        FROM StagingDB.HumanResources.Department s
        LEFT JOIN StagingDB.HumanResources.Employee e ON s.ManagerID = e.EmployeeID
        WHERE NOT EXISTS (SELECT 1 FROM Dim.DimDepartment d WHERE d.DepartmentID = s.DepartmentID);

        SET @Message = 'DimDepartment load process completed successfully.';
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, RecordsInserted, Status, [Message])
        VALUES (@ProcessName, 'Initial Load', @TargetTable, @StartTime, GETDATE(), @@ROWCOUNT, 'Success', @Message);
    END TRY
    BEGIN CATCH
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, Status, [Message])
        VALUES (@ProcessName, 'Initial Load', @TargetTable, @StartTime, GETDATE(), 'Failed', @Message);
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- 3) LoadDimJobTitle (First Load - No Truncate)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.LoadDimJobTitle
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessName NVARCHAR(128) = 'Dim.LoadDimJobTitle';
    DECLARE @TargetTable NVARCHAR(128) = 'Dim.DimJobTitle';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);

    BEGIN TRY
        IF NOT EXISTS (SELECT 1 FROM Dim.DimJobTitle WHERE JobTitleKey = -1)
        BEGIN
            SET IDENTITY_INSERT Dim.DimJobTitle ON;
            INSERT INTO Dim.DimJobTitle (JobTitleKey, JobTitleID, JobTitleName, CurrentJobCategory, PreviousJobCategory)
            VALUES (-1, -1, 'Unknown', 'Unknown', 'Unknown');
            SET IDENTITY_INSERT Dim.DimJobTitle OFF;
        END

        INSERT INTO Dim.DimJobTitle (JobTitleID, JobTitleName, CurrentJobCategory, PreviousJobCategory)
        SELECT s.JobTitleID, s.JobTitleName, s.JobCategory, NULL
        FROM StagingDB.HumanResources.JobTitle s
        WHERE NOT EXISTS (SELECT 1 FROM Dim.DimJobTitle d WHERE d.JobTitleID = s.JobTitleID);

        SET @Message = 'DimJobTitle load process completed successfully.';
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, RecordsInserted, Status, [Message])
        VALUES (@ProcessName, 'Initial Load', @TargetTable, @StartTime, GETDATE(), @@ROWCOUNT, 'Success', @Message);
    END TRY
    BEGIN CATCH
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, Status, [Message])
        VALUES (@ProcessName, 'Initial Load', @TargetTable, @StartTime, GETDATE(), 'Failed', @Message);
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- 4) LoadDimLeaveType (First Load - No Truncate)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.LoadDimLeaveType
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessName NVARCHAR(128) = 'Dim.LoadDimLeaveType';
    DECLARE @TargetTable NVARCHAR(128) = 'Dim.DimLeaveType';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);

    BEGIN TRY
        IF NOT EXISTS (SELECT 1 FROM Dim.DimLeaveType WHERE LeaveTypeKey = -1)
        BEGIN
            SET IDENTITY_INSERT Dim.DimLeaveType ON;
            INSERT INTO Dim.DimLeaveType (LeaveTypeKey, LeaveTypeID, LeaveTypeName, IsPaid)
            VALUES (-1, -1, 'Unknown', 0);
            SET IDENTITY_INSERT Dim.DimLeaveType OFF;
        END

        INSERT INTO Dim.DimLeaveType (LeaveTypeID, LeaveTypeName, IsPaid)
        SELECT s.LeaveTypeID, s.LeaveTypeName, s.IsPaid
        FROM StagingDB.HumanResources.LeaveType s
        WHERE NOT EXISTS (SELECT 1 FROM Dim.DimLeaveType d WHERE d.LeaveTypeID = s.LeaveTypeID);
        
        SET @Message = 'DimLeaveType load process completed successfully.';
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, RecordsInserted, Status, [Message])
        VALUES (@ProcessName, 'Initial Load', @TargetTable, @StartTime, GETDATE(), @@ROWCOUNT, 'Success', @Message);
    END TRY
    BEGIN CATCH
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, Status, [Message])
        VALUES (@ProcessName, 'Initial Load', @TargetTable, @StartTime, GETDATE(), 'Failed', @Message);
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- 5) LoadDimTerminationReason (First Load - No Truncate)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.LoadDimTerminationReason
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessName NVARCHAR(128) = 'Dim.LoadDimTerminationReason';
    DECLARE @TargetTable NVARCHAR(128) = 'Dim.DimTerminationReason';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);

    BEGIN TRY
        IF NOT EXISTS (SELECT 1 FROM Dim.DimTerminationReason WHERE TerminationReasonKey = -1)
        BEGIN
            SET IDENTITY_INSERT Dim.DimTerminationReason ON;
            INSERT INTO Dim.DimTerminationReason (TerminationReasonKey, TerminationReason)
            VALUES (-1, 'Unknown');
            SET IDENTITY_INSERT Dim.DimTerminationReason OFF;
        END

        INSERT INTO Dim.DimTerminationReason (TerminationReason)
        SELECT DISTINCT s.TerminationReason
        FROM StagingDB.HumanResources.Termination s
        WHERE s.TerminationReason IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM Dim.DimTerminationReason d WHERE d.TerminationReason = s.TerminationReason);

        SET @Message = 'DimTerminationReason load process completed successfully.';
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, RecordsInserted, Status, [Message])
        VALUES (@ProcessName, 'Initial Load', @TargetTable, @StartTime, GETDATE(), @@ROWCOUNT, 'Success', @Message);
    END TRY
    BEGIN CATCH
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, Status, [Message])
        VALUES (@ProcessName, 'Initial Load', @TargetTable, @StartTime, GETDATE(), 'Failed', @Message);
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- 6) LoadDimEmployee (First Load - No Truncate)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Dim.LoadDimEmployee
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessName NVARCHAR(128) = 'Dim.LoadDimEmployee';
    DECLARE @TargetTable NVARCHAR(128) = 'Dim.DimEmployee';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);

    BEGIN TRY
        IF NOT EXISTS (SELECT 1 FROM Dim.DimEmployee WHERE EmployeeKey = -1)
        BEGIN
            SET IDENTITY_INSERT Dim.DimEmployee ON;
            INSERT INTO Dim.DimEmployee (EmployeeKey, EmployeeID, FullName, Gender, MaritalStatus, Position, Address, Phone, EmploymentStatus, StartDate, EndDate, IsCurrent)
            VALUES (-1, -1, 'Unknown', 'N/A', 'N/A', 'Unknown', 'Unknown', 'Unknown', 'Unknown', '1900-01-01', '9999-12-31', 0);
            SET IDENTITY_INSERT Dim.DimEmployee OFF;
        END

        INSERT INTO Dim.DimEmployee (EmployeeID, FullName, Gender, MaritalStatus, Position, Address, Phone, EmploymentStatus, StartDate, EndDate, IsCurrent)
        SELECT 
            s.EmployeeID, s.FullName, s.Gender, s.MaritalStatus, s.Position, s.Address, s.Phone,
            s.EmploymentStatus, s.HireDate AS StartDate, NULL AS EndDate, 1 AS IsCurrent
        FROM StagingDB.HumanResources.Employee s
        WHERE NOT EXISTS (SELECT 1 FROM Dim.DimEmployee d WHERE d.EmployeeID = s.EmployeeID);
        
        SET @Message = 'DimEmployee load process completed successfully.';
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, RecordsInserted, Status, [Message])
        VALUES (@ProcessName, 'Initial Load', @TargetTable, @StartTime, GETDATE(), @@ROWCOUNT, 'Success', @Message);
    END TRY
    BEGIN CATCH
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, Status, [Message])
        VALUES (@ProcessName, 'Initial Load', @TargetTable, @StartTime, GETDATE(), 'Failed', @Message);
        THROW;
    END CATCH;
END;
GO

