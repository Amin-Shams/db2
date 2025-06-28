USE StagingDB;
go

-------------------------------------------------------------------------------
-- 1) LoadFinanceCustomer
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Finance.LoadFinanceCustomer
AS
BEGIN
    DECLARE @TableName NVARCHAR(128) = 'StagingDB.Finance.Customer',
            @StepStart DATETIME, @StepEnd DATETIME, @Message NVARCHAR(1000),
            @NullCount INT, @DupCount INT;

    BEGIN TRY
        BEGIN TRAN;

        -- STEP 1: TRUNCATE TARGET
        SET @StepStart = GETDATE();
        TRUNCATE TABLE StagingDB.Finance.Customer;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
            VALUES (@TableName,'Truncate',@StepStart,@StepEnd,'Truncated Finance.Customer');

        -- STEP 2: VALIDATE SOURCE
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.Finance.Customer AS src
        WHERE src.CustomerID   IS NULL
           OR src.CustomerCode IS NULL
           OR src.CustomerName IS NULL
           OR src.CustomerType IS NULL;
        IF @NullCount > 0
            THROW 51000, 'Validation failed: NULLs in Finance.Customer', 1;

        SELECT @DupCount = COUNT(*) 
        FROM (
            SELECT CustomerID FROM Finance.Customer
            GROUP BY CustomerID
            HAVING COUNT(*) > 1
        ) AS d;
        IF @DupCount > 0
            THROW 51001, 'Validation failed: Duplicates in Finance.Customer', 1;

        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog VALUES(@TableName,'Validate',@StepStart,@StepEnd,
            CONCAT('Null=',@NullCount,',Dup=',@DupCount));

        -- STEP 3: INSERT DATA
        SET @StepStart = GETDATE();
        INSERT INTO StagingDB.Finance.Customer (
            CustomerID, CustomerCode, CustomerName, CustomerType,
            TIN, VATNumber, Phone, Email, [Address], CountryID, LoadDate
        )
        SELECT
            CustomerID, CustomerCode, CustomerName, CustomerType,
            TIN, VATNumber, Phone, Email, [Address], CountryID, GETDATE()
        FROM TradePortDB.Finance.Customer;  -- فرض جدول منبع Finance.Customer_Source

        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog VALUES(@TableName,'Insert',@StepStart,@StepEnd,
            CONCAT('Inserted ',@@ROWCOUNT,' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO StagingDB.Finance.ETLLog VALUES(@TableName,'Error',@StepStart,@StepEnd,@Message);
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- 2) LoadFinanceBillingCycle
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Finance.LoadFinanceBillingCycle
AS
BEGIN
    DECLARE @TableName NVARCHAR(128) = 'StagingDB.Finance.BillingCycle',
            @StepStart DATETIME, @StepEnd DATETIME, @Message NVARCHAR(1000),
            @NullCount INT, @DupCount INT;
    BEGIN TRY
        BEGIN TRAN;
        SET @StepStart = GETDATE();
        TRUNCATE TABLE StagingDB.Finance.BillingCycle;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog VALUES (@TableName, 'Truncate', @StepStart, @StepEnd, 'Truncated');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) FROM TradePortDB.Finance.BillingCycle
        WHERE BillingCycleID IS NULL OR CycleName IS NULL;
        SELECT @DupCount = COUNT(*) FROM (SELECT BillingCycleID FROM TradePortDB.Finance.BillingCycle GROUP BY BillingCycleID HAVING COUNT(*) > 1) x;
        IF @NullCount > 0 THROW 52000, 'Validation failed: NULLs in BillingCycle', 1;
        IF @DupCount > 0 THROW 52001, 'Validation failed: Duplicates in BillingCycle', 1;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog VALUES (@TableName, 'Validate', @StepStart, @StepEnd, CONCAT('Null=', @NullCount, ', Dup=', @DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO StagingDB.Finance.BillingCycle
        SELECT *, GETDATE() FROM TradePortDB.Finance.BillingCycle;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog VALUES (@TableName, 'Insert', @StepStart, @StepEnd, CONCAT('Inserted ', @@ROWCOUNT, ' rows'));
        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE(); SET @Message = ERROR_MESSAGE();
        INSERT INTO StagingDB.Finance.ETLLog VALUES (@TableName, 'Error', @StepStart, @StepEnd, @Message);
        THROW;
    END CATCH;
END;
GO


--------------------------------------------------------------------------------
-- 3) LoadFinanceServiceType
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Finance.LoadFinanceServiceType
AS
BEGIN
    DECLARE @TableName NVARCHAR(128) = 'StagingDB.Finance.ServiceType',
            @StepStart DATETIME, @StepEnd DATETIME, @Message NVARCHAR(1000),
            @NullCount INT, @DupCount INT;
    BEGIN TRY
        BEGIN TRAN;
        SET @StepStart = GETDATE();
        TRUNCATE TABLE StagingDB.Finance.ServiceType;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog VALUES (@TableName, 'Truncate', @StepStart, @StepEnd, 'Truncated');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) FROM TradePortDB.Finance.ServiceType
        WHERE ServiceTypeID IS NULL OR ServiceName IS NULL;
        SELECT @DupCount = COUNT(*) FROM (SELECT ServiceTypeID FROM TradePortDB.Finance.ServiceType GROUP BY ServiceTypeID HAVING COUNT(*) > 1) x;
        IF @NullCount > 0 THROW 53000, 'Validation failed: NULLs in ServiceType', 1;
        IF @DupCount > 0 THROW 53001, 'Validation failed: Duplicates in ServiceType', 1;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog VALUES (@TableName, 'Validate', @StepStart, @StepEnd, CONCAT('Null=', @NullCount, ', Dup=', @DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO StagingDB.Finance.ServiceType
        SELECT *, GETDATE() FROM TradePortDB.Finance.ServiceType;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog VALUES (@TableName, 'Insert', @StepStart, @StepEnd, CONCAT('Inserted ', @@ROWCOUNT, ' rows'));
        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE(); SET @Message = ERROR_MESSAGE();
        INSERT INTO StagingDB.Finance.ETLLog VALUES (@TableName, 'Error', @StepStart, @StepEnd, @Message);
        THROW;
    END CATCH;
END;
GO


--------------------------------------------------------------------------------
-- 4) LoadFinanceTax
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Finance.LoadFinanceTax
AS
BEGIN
    DECLARE @TableName NVARCHAR(128) = 'StagingDB.Finance.Tax',
            @StepStart DATETIME, @StepEnd DATETIME, @Message NVARCHAR(1000),
            @NullCount INT, @DupCount INT;
    BEGIN TRY
        BEGIN TRAN;
        SET @StepStart = GETDATE();
        TRUNCATE TABLE StagingDB.Finance.Tax;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog VALUES (@TableName, 'Truncate', @StepStart, @StepEnd, 'Truncated');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) FROM TradePortDB.Finance.Tax
        WHERE TaxID IS NULL OR TaxName IS NULL OR TaxRate IS NULL OR TaxType IS NULL OR EffectiveFrom IS NULL;
        SELECT @DupCount = COUNT(*) FROM (SELECT TaxID FROM TradePortDB.Finance.Tax GROUP BY TaxID HAVING COUNT(*) > 1) x;
        IF @NullCount > 0 THROW 54000, 'Validation failed: NULLs in Tax', 1;
        IF @DupCount > 0 THROW 54001, 'Validation failed: Duplicates in Tax', 1;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog VALUES (@TableName, 'Validate', @StepStart, @StepEnd, CONCAT('Null=', @NullCount, ', Dup=', @DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO StagingDB.Finance.Tax
        SELECT *, GETDATE() FROM TradePortDB.Finance.Tax;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog VALUES (@TableName, 'Insert', @StepStart, @StepEnd, CONCAT('Inserted ', @@ROWCOUNT, ' rows'));
        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE(); SET @Message = ERROR_MESSAGE();
        INSERT INTO StagingDB.Finance.ETLLog VALUES (@TableName, 'Error', @StepStart, @StepEnd, @Message);
        THROW;
    END CATCH;
END;
GO


--------------------------------------------------------------------------------
-- 5) LoadFinanceTariff
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Finance.LoadFinanceTariff
AS
BEGIN
    DECLARE @TableName NVARCHAR(128) = 'StagingDB.Finance.Tariff',
            @StepStart DATETIME, @StepEnd DATETIME, @Message NVARCHAR(1000),
            @NullCount INT, @DupCount INT;

    BEGIN TRY
        BEGIN TRAN;

        -- Step 1: Truncate
        SET @StepStart = GETDATE();
        TRUNCATE TABLE StagingDB.Finance.Tariff;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Truncate', @StepStart, @StepEnd, 'Truncated');

        -- Step 2: Validate
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) FROM TradePortDB.Finance.Tariff
        WHERE TariffID IS NULL OR ServiceTypeID IS NULL OR ValidFrom IS NULL OR UnitRate IS NULL;

        SELECT @DupCount = COUNT(*) FROM (
            SELECT TariffID FROM TradePortDB.Finance.Tariff GROUP BY TariffID HAVING COUNT(*) > 1
        ) x;

        IF @NullCount > 0 THROW 55000, 'Validation failed: NULLs in Tariff', 1;
        IF @DupCount > 0 THROW 55001, 'Validation failed: Duplicates in Tariff', 1;

        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Validate', @StepStart, @StepEnd, CONCAT('Null=', @NullCount, ', Dup=', @DupCount));

        -- Step 3: Insert
        SET @StepStart = GETDATE();
        INSERT INTO StagingDB.Finance.Tariff
        SELECT *, GETDATE() FROM TradePortDB.Finance.Tariff;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Insert', @StepStart, @StepEnd, CONCAT('Inserted ', @@ROWCOUNT, ' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Error', @StepStart, @StepEnd, @Message);
        THROW;
    END CATCH;
END;
GO


--------------------------------------------------------------------------------
-- 6) LoadFinanceContract
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Finance.LoadFinanceContract
AS
BEGIN
    DECLARE @TableName NVARCHAR(128) = 'StagingDB.Finance.Contract',
            @StepStart DATETIME, @StepEnd DATETIME, @Message NVARCHAR(1000),
            @NullCount INT, @DupCount INT;

    BEGIN TRY
        BEGIN TRAN;

        SET @StepStart = GETDATE();
        TRUNCATE TABLE StagingDB.Finance.Contract;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Truncate', @StepStart, @StepEnd, 'Truncated');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) FROM TradePortDB.Finance.Contract
        WHERE ContractID IS NULL OR CustomerID IS NULL OR ContractNumber IS NULL;

        SELECT @DupCount = COUNT(*) FROM (
            SELECT ContractID FROM TradePortDB.Finance.Contract GROUP BY ContractID HAVING COUNT(*) > 1
        ) x;

        IF @NullCount > 0 THROW 56000, 'Validation failed: NULLs in Contract', 1;
        IF @DupCount > 0 THROW 56001, 'Validation failed: Duplicates in Contract', 1;

        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Validate', @StepStart, @StepEnd, CONCAT('Null=', @NullCount, ', Dup=', @DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO StagingDB.Finance.Contract
        SELECT *, GETDATE() FROM TradePortDB.Finance.Contract;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Insert', @StepStart, @StepEnd, CONCAT('Inserted ', @@ROWCOUNT, ' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Error', @StepStart, @StepEnd, @Message);
        THROW;
    END CATCH;
END;
GO


--------------------------------------------------------------------------------
-- 7) LoadFinanceInvoice
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Finance.LoadFinanceInvoice
AS
BEGIN
    DECLARE @TableName NVARCHAR(128) = 'StagingDB.Finance.Invoice',
            @StepStart DATETIME, @StepEnd DATETIME, @Message NVARCHAR(1000),
            @NullCount INT, @DupCount INT;

    BEGIN TRY
        BEGIN TRAN;
        SET @StepStart = GETDATE();
        TRUNCATE TABLE StagingDB.Finance.Invoice;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Truncate', @StepStart, @StepEnd, 'Truncated');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) FROM TradePortDB.Finance.Invoice
        WHERE InvoiceID IS NULL OR ContractID IS NULL OR InvoiceNumber IS NULL OR InvoiceDate IS NULL OR DueDate IS NULL OR TotalAmount IS NULL OR TaxAmount IS NULL;

        SELECT @DupCount = COUNT(*) FROM (
            SELECT InvoiceID FROM TradePortDB.Finance.Invoice GROUP BY InvoiceID HAVING COUNT(*) > 1
        ) x;

        IF @NullCount > 0 THROW 57000, 'Validation failed: NULLs in Invoice', 1;
        IF @DupCount > 0 THROW 57001, 'Validation failed: Duplicates in Invoice', 1;

        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Validate', @StepStart, @StepEnd, CONCAT('Null=', @NullCount, ', Dup=', @DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO StagingDB.Finance.Invoice
        SELECT *, GETDATE() FROM TradePortDB.Finance.Invoice;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Insert', @StepStart, @StepEnd, CONCAT('Inserted ', @@ROWCOUNT, ' rows'));
        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Error', @StepStart, @StepEnd, @Message);
        THROW;
    END CATCH;
END;
GO


--------------------------------------------------------------------------------
-- 8) LoadFinanceInvoiceLine
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Finance.LoadFinanceInvoiceLine
AS
BEGIN
    DECLARE @TableName NVARCHAR(128) = 'StagingDB.Finance.InvoiceLine',
            @StepStart DATETIME, @StepEnd DATETIME, @Message NVARCHAR(1000),
            @NullCount INT, @DupCount INT;

    BEGIN TRY
        BEGIN TRAN;
        SET @StepStart = GETDATE();
        TRUNCATE TABLE StagingDB.Finance.InvoiceLine;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Truncate', @StepStart, @StepEnd, 'Truncated');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) FROM TradePortDB.Finance.InvoiceLine
        WHERE InvoiceLineID IS NULL OR InvoiceID IS NULL OR ServiceTypeID IS NULL OR Quantity IS NULL OR UnitPrice IS NULL OR TaxAmount IS NULL OR NetAmount IS NULL;

        SELECT @DupCount = COUNT(*) FROM (
            SELECT InvoiceLineID FROM TradePortDB.Finance.InvoiceLine GROUP BY InvoiceLineID HAVING COUNT(*) > 1
        ) x;

        IF @NullCount > 0 THROW 58000, 'Validation failed: NULLs in InvoiceLine', 1;
        IF @DupCount > 0 THROW 58001, 'Validation failed: Duplicates in InvoiceLine', 1;

        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Validate', @StepStart, @StepEnd, CONCAT('Null=', @NullCount, ', Dup=', @DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO StagingDB.Finance.InvoiceLine
        SELECT *, GETDATE() FROM TradePortDB.Finance.InvoiceLine;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Insert', @StepStart, @StepEnd, CONCAT('Inserted ', @@ROWCOUNT, ' rows'));
        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Error', @StepStart, @StepEnd, @Message);
        THROW;
    END CATCH;
END;
GO


--------------------------------------------------------------------------------
-- 9) LoadFinancePayment
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Finance.LoadFinancePayment
AS
BEGIN
    DECLARE @TableName NVARCHAR(128) = 'StagingDB.Finance.Payment',
            @StepStart DATETIME, @StepEnd DATETIME, @Message NVARCHAR(1000),
            @NullCount INT, @DupCount INT;

    BEGIN TRY
        BEGIN TRAN;
        SET @StepStart = GETDATE();
        TRUNCATE TABLE StagingDB.Finance.Payment;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Truncate', @StepStart, @StepEnd, 'Truncated');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) FROM TradePortDB.Finance.Payment
        WHERE PaymentID IS NULL OR InvoiceID IS NULL OR PaymentDate IS NULL OR Amount IS NULL OR PaymentMethod IS NULL;

        SELECT @DupCount = COUNT(*) FROM (
            SELECT PaymentID FROM TradePortDB.Finance.Payment GROUP BY PaymentID HAVING COUNT(*) > 1
        ) x;

        IF @NullCount > 0 THROW 59000, 'Validation failed: NULLs in Payment', 1;
        IF @DupCount > 0 THROW 59001, 'Validation failed: Duplicates in Payment', 1;

        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Validate', @StepStart, @StepEnd, CONCAT('Null=', @NullCount, ', Dup=', @DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO StagingDB.Finance.Payment
        SELECT *, GETDATE() FROM TradePortDB.Finance.Payment;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Insert', @StepStart, @StepEnd, CONCAT('Inserted ', @@ROWCOUNT, ' rows'));
        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Error', @StepStart, @StepEnd, @Message);
        THROW;
    END CATCH;
END;
GO


--------------------------------------------------------------------------------
-- 10) LoadFinanceRevenueRecognition
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Finance.LoadFinanceRevenueRecognition
AS
BEGIN
    DECLARE @TableName NVARCHAR(128) = 'StagingDB.Finance.RevenueRecognition',
            @StepStart DATETIME, @StepEnd DATETIME, @Message NVARCHAR(1000),
            @NullCount INT, @DupCount INT;

    BEGIN TRY
        BEGIN TRAN;
        SET @StepStart = GETDATE();
        TRUNCATE TABLE StagingDB.Finance.RevenueRecognition;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Truncate', @StepStart, @StepEnd, 'Truncated');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) FROM TradePortDB.Finance.RevenueRecognition
        WHERE RecognitionID IS NULL OR InvoiceID IS NULL OR DateRecognized IS NULL OR Amount IS NULL;

        SELECT @DupCount = COUNT(*) FROM (
            SELECT RecognitionID FROM TradePortDB.Finance.RevenueRecognition GROUP BY RecognitionID HAVING COUNT(*) > 1
        ) x;

        IF @NullCount > 0 THROW 60000, 'Validation failed: NULLs in RevenueRecognition', 1;
        IF @DupCount > 0 THROW 60001, 'Validation failed: Duplicates in RevenueRecognition', 1;

        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Validate', @StepStart, @StepEnd, CONCAT('Null=', @NullCount, ', Dup=', @DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO StagingDB.Finance.RevenueRecognition
        SELECT *, GETDATE() FROM TradePortDB.Finance.RevenueRecognition;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Insert', @StepStart, @StepEnd, CONCAT('Inserted ', @@ROWCOUNT, ' rows'));
        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO StagingDB.Finance.ETLLog 
        VALUES (@TableName, 'Error', @StepStart, @StepEnd, @Message);
        THROW;
    END CATCH;
END;
GO




--------------------------------------------------------------------------------
-- 1) LoadSAEmployee
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE HumanResources.LoadSAEmployee
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @TableName NVARCHAR(128) = 'HumanResources.Employee';
    DECLARE @StepStart DATETIME;
    DECLARE @StepEnd DATETIME;
    DECLARE @Message NVARCHAR(1000);
    DECLARE @NullCount INT;
    DECLARE @DupCount INT;

    BEGIN TRY
        BEGIN TRAN;

        -- STEP 1: TRUNCATE TARGET
        SET @StepStart = GETDATE();
        TRUNCATE TABLE HumanResources.Employee;
        SET @StepEnd = GETDATE();
        SET @Message = 'Truncated HumanResources.Employee';
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Truncate', @StepStart, @StepEnd, @Message);

        -- STEP 2: VALIDATE SOURCE
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.HumanResources.Employee
        WHERE EmployeeID IS NULL OR FullName IS NULL;
        IF @NullCount > 0
            THROW 51000, 'Validation failed: NULLs found in critical columns of HumanResources.Employee', 1;
        
        SELECT @DupCount = COUNT(*) 
        FROM (
            SELECT EmployeeID, COUNT(*) cnt 
            FROM TradePortDB.HumanResources.Employee 
            GROUP BY EmployeeID 
            HAVING COUNT(*) > 1
        ) d;
        IF @DupCount > 0
            THROW 51001, 'Validation failed: Duplicate EmployeeIDs found in HumanResources.Employee', 1;
        
        SET @StepEnd = GETDATE();
        SET @Message = CONCAT('Validation successful. Nulls found: ', @NullCount, ', Duplicates found: ', @DupCount);
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Validate', @StepStart, @StepEnd, @Message);

        -- STEP 3: INSERT DATA
        SET @StepStart = GETDATE();
        INSERT INTO HumanResources.Employee (
            EmployeeID, FullName, Position, NationalID, HireDate, BirthDate, 
            Gender, MaritalStatus, Address, Phone, Email, EmploymentStatus, LoadDate
        )
        SELECT
            EmployeeID, FullName, Position, NationalID, HireDate, BirthDate, 
            Gender, MaritalStatus, Address, Phone, Email, EmploymentStatus, GETDATE()
        FROM TradePortDB.HumanResources.Employee;
        
        SET @StepEnd = GETDATE();
        SET @Message = CONCAT('Inserted ', @@ROWCOUNT, ' rows');
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Insert', @StepStart, @StepEnd, @Message);

        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Error', GETDATE(), GETDATE(), @Message);
        THROW;
    END CATCH
END;
GO

--------------------------------------------------------------------------------
-- 2) LoadSADepartment
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE HumanResources.LoadSADepartment
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @TableName NVARCHAR(128) = 'HumanResources.Department';
    DECLARE @StepStart DATETIME;
    DECLARE @StepEnd DATETIME;
    DECLARE @Message NVARCHAR(1000);
    DECLARE @NullCount INT;
    DECLARE @DupCount INT;

    BEGIN TRY
        BEGIN TRAN;
        SET @StepStart = GETDATE();
        TRUNCATE TABLE HumanResources.Department;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Truncate', @StepStart, @StepEnd, 'Truncated HumanResources.Department');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.HumanResources.Department 
        WHERE DepartmentID IS NULL OR DepartmentName IS NULL;
        IF @NullCount > 0 
            THROW 52000, 'NULLs found in critical columns of HumanResources.Department', 1;
        SELECT @DupCount = COUNT(*) 
        FROM (
            SELECT DepartmentID 
            FROM TradePortDB.HumanResources.Department 
            GROUP BY DepartmentID 
            HAVING COUNT(*) > 1
        ) d;
        IF @DupCount > 0 
            THROW 52001, 'Duplicate DepartmentIDs found in HumanResources.Department', 1;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Validate', @StepStart, @StepEnd, CONCAT('Validation successful. Nulls: ', @NullCount, ', Duplicates: ', @DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO HumanResources.Department (DepartmentID, DepartmentName, ManagerID, IsActive, LoadDate)
        SELECT DepartmentID, DepartmentName, ManagerID, IsActive, GETDATE()
        FROM TradePortDB.HumanResources.Department;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Insert', @StepStart, @StepEnd, CONCAT('Inserted ', @@ROWCOUNT, ' rows'));
        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Error', GETDATE(), GETDATE(), @Message);
        THROW;
    END CATCH
END;
GO

--------------------------------------------------------------------------------
-- 3) LoadSAJobTitle
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE HumanResources.LoadSAJobTitle
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @TableName NVARCHAR(128) = 'HumanResources.JobTitle';
    DECLARE @StepStart DATETIME;
    DECLARE @StepEnd DATETIME;
    DECLARE @Message NVARCHAR(1000);
    DECLARE @NullCount INT;
    DECLARE @DupCount INT;

    BEGIN TRY
        BEGIN TRAN;
        SET @StepStart = GETDATE();
        TRUNCATE TABLE HumanResources.JobTitle;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Truncate', @StepStart, @StepEnd, 'Truncated HumanResources.JobTitle');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.HumanResources.JobTitle 
        WHERE JobTitleID IS NULL OR JobTitleName IS NULL;
        IF @NullCount > 0 
            THROW 53000, 'NULLs found in critical columns of HumanResources.JobTitle', 1;
        SELECT @DupCount = COUNT(*) 
        FROM (
            SELECT JobTitleID 
            FROM TradePortDB.HumanResources.JobTitle 
            GROUP BY JobTitleID 
            HAVING COUNT(*) > 1
        ) d;
        IF @DupCount > 0 
            THROW 53001, 'Duplicate JobTitleIDs found in HumanResources.JobTitle', 1;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Validate', @StepStart, @StepEnd, CONCAT('Validation successful. Nulls: ', @NullCount, ', Duplicates: ', @DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO HumanResources.JobTitle (JobTitleID, JobTitleName, JobCategory, BaseSalary, IsActive, LoadDate)
        SELECT JobTitleID, JobTitleName, JobCategory, BaseSalary, IsActive, GETDATE()
        FROM TradePortDB.HumanResources.JobTitle;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Insert', @StepStart, @StepEnd, CONCAT('Inserted ', @@ROWCOUNT, ' rows'));
        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Error', GETDATE(), GETDATE(), @Message);
        THROW;
    END CATCH
END;
GO

--------------------------------------------------------------------------------
-- 4) LoadSAEmploymentHistory
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE HumanResources.LoadSAEmploymentHistory
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @TableName NVARCHAR(128) = 'HumanResources.EmploymentHistory';
    DECLARE @StepStart DATETIME;
    DECLARE @StepEnd DATETIME;
    DECLARE @Message NVARCHAR(1000);
    DECLARE @NullCount INT;
    DECLARE @DupCount INT;

    BEGIN TRY
        BEGIN TRAN;
        SET @StepStart = GETDATE();
        TRUNCATE TABLE HumanResources.EmploymentHistory;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Truncate', @StepStart, @StepEnd, 'Truncated HumanResources.EmploymentHistory');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.HumanResources.EmploymentHistory 
        WHERE EmploymentHistoryID IS NULL OR EmployeeID IS NULL OR JobTitleID IS NULL OR DepartmentID IS NULL OR StartDate IS NULL;
        IF @NullCount > 0 
            THROW 54000, 'NULLs found in critical columns of HumanResources.EmploymentHistory', 1;
        SELECT @DupCount = COUNT(*) 
        FROM (
            SELECT EmploymentHistoryID 
            FROM TradePortDB.HumanResources.EmploymentHistory 
            GROUP BY EmploymentHistoryID 
            HAVING COUNT(*) > 1
        ) d;
        IF @DupCount > 0 
            THROW 54001, 'Duplicate EmploymentHistoryIDs found', 1;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Validate', @StepStart, @StepEnd, CONCAT('Validation successful. Nulls: ', @NullCount, ', Duplicates: ', @DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO HumanResources.EmploymentHistory (EmploymentHistoryID, EmployeeID, JobTitleID, DepartmentID, StartDate, EndDate, Salary, EmploymentType, LoadDate)
        SELECT EmploymentHistoryID, EmployeeID, JobTitleID, DepartmentID, StartDate, EndDate, Salary, EmploymentType, GETDATE()
        FROM TradePortDB.HumanResources.EmploymentHistory;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Insert', @StepStart, @StepEnd, CONCAT('Inserted ', @@ROWCOUNT, ' rows'));
        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Error', GETDATE(), GETDATE(), @Message);
        THROW;
    END CATCH
END;
GO

--------------------------------------------------------------------------------
-- 5) LoadSATermination
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE HumanResources.LoadSATermination
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @TableName NVARCHAR(128) = 'HumanResources.Termination';
    DECLARE @StepStart DATETIME;
    DECLARE @StepEnd DATETIME;
    DECLARE @Message NVARCHAR(1000);
    DECLARE @NullCount INT;
    DECLARE @DupCount INT;

    BEGIN TRY
        BEGIN TRAN;
        SET @StepStart = GETDATE();
        TRUNCATE TABLE HumanResources.Termination;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Truncate', @StepStart, @StepEnd, 'Truncated HumanResources.Termination');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.HumanResources.Termination 
        WHERE TerminationID IS NULL OR EmployeeID IS NULL OR TerminationDate IS NULL OR TerminationReason IS NULL;
        IF @NullCount > 0 
            THROW 55000, 'NULLs found in critical columns of HumanResources.Termination', 1;
        SELECT @DupCount = COUNT(*) 
        FROM (
            SELECT TerminationID 
            FROM TradePortDB.HumanResources.Termination 
            GROUP BY TerminationID 
            HAVING COUNT(*) > 1
        ) d;
        IF @DupCount > 0 
            THROW 55001, 'Duplicate TerminationIDs found', 1;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Validate', @StepStart, @StepEnd, CONCAT('Validation successful. Nulls: ', @NullCount, ', Duplicates: ', @DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO HumanResources.Termination (TerminationID, EmployeeID, TerminationDate, TerminationReason, Notes, LoadDate)
        SELECT TerminationID, EmployeeID, TerminationDate, TerminationReason, Notes, GETDATE()
        FROM TradePortDB.HumanResources.Termination;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Insert', @StepStart, @StepEnd, CONCAT('Inserted ', @@ROWCOUNT, ' rows'));
        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Error', GETDATE(), GETDATE(), @Message);
        THROW;
    END CATCH
END;
GO

--------------------------------------------------------------------------------
-- 6) LoadSAAttendance
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE HumanResources.LoadSAAttendance
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @TableName NVARCHAR(128) = 'HumanResources.Attendance';
    DECLARE @StepStart DATETIME;
    DECLARE @StepEnd DATETIME;
    DECLARE @Message NVARCHAR(1000);
    DECLARE @NullCount INT;
    DECLARE @DupCount INT;

    BEGIN TRY
        BEGIN TRAN;
        SET @StepStart = GETDATE();
        TRUNCATE TABLE HumanResources.Attendance;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Truncate', @StepStart, @StepEnd, 'Truncated HumanResources.Attendance');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.HumanResources.Attendance 
        WHERE AttendanceID IS NULL OR EmployeeID IS NULL OR AttendanceDate IS NULL OR Status IS NULL;
        IF @NullCount > 0 
            THROW 56000, 'NULLs found in critical columns of HumanResources.Attendance', 1;
        SELECT @DupCount = COUNT(*) 
        FROM (
            SELECT AttendanceID 
            FROM TradePortDB.HumanResources.Attendance 
            GROUP BY AttendanceID 
            HAVING COUNT(*) > 1
        ) d;
        IF @DupCount > 0 
            THROW 56001, 'Duplicate AttendanceIDs found', 1;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Validate', @StepStart, @StepEnd, CONCAT('Validation successful. Nulls: ', @NullCount, ', Duplicates: ', @DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO HumanResources.Attendance (AttendanceID, EmployeeID, AttendanceDate, Status, CheckInTime, CheckOutTime, HoursWorked, Notes, LoadDate)
        SELECT AttendanceID, EmployeeID, AttendanceDate, Status, CheckInTime, CheckOutTime, HoursWorked, Notes, GETDATE()
        FROM TradePortDB.HumanResources.Attendance;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Insert', @StepStart, @StepEnd, CONCAT('Inserted ', @@ROWCOUNT, ' rows'));
        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Error', GETDATE(), GETDATE(), @Message);
        THROW;
    END CATCH
END;
GO

--------------------------------------------------------------------------------
-- 7) LoadSASalaryPayment
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE HumanResources.LoadSASalaryPayment
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @TableName NVARCHAR(128) = 'HumanResources.SalaryPayment';
    DECLARE @StepStart DATETIME;
    DECLARE @StepEnd DATETIME;
    DECLARE @Message NVARCHAR(1000);
    DECLARE @NullCount INT;
    DECLARE @DupCount INT;

    BEGIN TRY
        BEGIN TRAN;
        SET @StepStart = GETDATE();
        TRUNCATE TABLE HumanResources.SalaryPayment;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Truncate', @StepStart, @StepEnd, 'Truncated HumanResources.SalaryPayment');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.HumanResources.SalaryPayment 
        WHERE SalaryPaymentID IS NULL OR EmployeeID IS NULL OR PaymentDate IS NULL OR Amount IS NULL OR NetAmount IS NULL;
        IF @NullCount > 0 
            THROW 57000, 'NULLs found in critical columns of HumanResources.SalaryPayment', 1;
        SELECT @DupCount = COUNT(*) 
        FROM (
            SELECT SalaryPaymentID 
            FROM TradePortDB.HumanResources.SalaryPayment 
            GROUP BY SalaryPaymentID 
            HAVING COUNT(*) > 1
        ) d;
        IF @DupCount > 0 
            THROW 57001, 'Duplicate SalaryPaymentIDs found', 1;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Validate', @StepStart, @StepEnd, CONCAT('Validation successful. Nulls: ', @NullCount, ', Duplicates: ', @DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO HumanResources.SalaryPayment (SalaryPaymentID, EmployeeID, PaymentDate, Amount, Bonus, Deductions, NetAmount, PaymentMethod, ReferenceNumber, LoadDate)
        SELECT SalaryPaymentID, EmployeeID, PaymentDate, Amount, Bonus, Deductions, NetAmount, PaymentMethod, ReferenceNumber, GETDATE()
        FROM TradePortDB.HumanResources.SalaryPayment;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Insert', @StepStart, @StepEnd, CONCAT('Inserted ', @@ROWCOUNT, ' rows'));
        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Error', GETDATE(), GETDATE(), @Message);
        THROW;
    END CATCH
END;
GO

--------------------------------------------------------------------------------
-- 8) LoadSATrainingProgram
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE HumanResources.LoadSATrainingProgram
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @TableName NVARCHAR(128) = 'HumanResources.TrainingProgram';
    DECLARE @StepStart DATETIME;
    DECLARE @StepEnd DATETIME;
    DECLARE @Message NVARCHAR(1000);
    DECLARE @NullCount INT;
    DECLARE @DupCount INT;

    BEGIN TRY
        BEGIN TRAN;
        SET @StepStart = GETDATE();
        TRUNCATE TABLE HumanResources.TrainingProgram;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Truncate', @StepStart, @StepEnd, 'Truncated HumanResources.TrainingProgram');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.HumanResources.TrainingProgram 
        WHERE TrainingProgramID IS NULL OR ProgramName IS NULL;
        IF @NullCount > 0 
            THROW 58000, 'NULLs found in critical columns of HumanResources.TrainingProgram', 1;
        SELECT @DupCount = COUNT(*) 
        FROM (
            SELECT TrainingProgramID 
            FROM TradePortDB.HumanResources.TrainingProgram 
            GROUP BY TrainingProgramID 
            HAVING COUNT(*) > 1
        ) d;
        IF @DupCount > 0 
            THROW 58001, 'Duplicate TrainingProgramIDs found', 1;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Validate', @StepStart, @StepEnd, CONCAT('Validation successful. Nulls: ', @NullCount, ', Duplicates: ', @DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO HumanResources.TrainingProgram (TrainingProgramID, ProgramName, Category, DurationHours, Cost, IsActive, LoadDate)
        SELECT TrainingProgramID, ProgramName, Category, DurationHours, Cost, IsActive, GETDATE()
        FROM TradePortDB.HumanResources.TrainingProgram;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Insert', @StepStart, @StepEnd, CONCAT('Inserted ', @@ROWCOUNT, ' rows'));
        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Error', GETDATE(), GETDATE(), @Message);
        THROW;
    END CATCH
END;
GO

--------------------------------------------------------------------------------
-- 9) LoadSAEmployeeTraining
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE HumanResources.LoadSAEmployeeTraining
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @TableName NVARCHAR(128) = 'HumanResources.EmployeeTraining';
    DECLARE @StepStart DATETIME;
    DECLARE @StepEnd DATETIME;
    DECLARE @Message NVARCHAR(1000);
    DECLARE @NullCount INT;
    DECLARE @DupCount INT;

    BEGIN TRY
        BEGIN TRAN;
        SET @StepStart = GETDATE();
        TRUNCATE TABLE HumanResources.EmployeeTraining;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Truncate', @StepStart, @StepEnd, 'Truncated HumanResources.EmployeeTraining');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.HumanResources.EmployeeTraining 
        WHERE EmployeeTrainingID IS NULL OR EmployeeID IS NULL OR TrainingProgramID IS NULL OR TrainingDate IS NULL OR CompletionStatus IS NULL;
        IF @NullCount > 0 
            THROW 59000, 'NULLs found in critical columns of HumanResources.EmployeeTraining', 1;
        SELECT @DupCount = COUNT(*) 
        FROM (
            SELECT EmployeeTrainingID 
            FROM TradePortDB.HumanResources.EmployeeTraining 
            GROUP BY EmployeeTrainingID 
            HAVING COUNT(*) > 1
        ) d;
        IF @DupCount > 0 
            THROW 59001, 'Duplicate EmployeeTrainingIDs found', 1;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Validate', @StepStart, @StepEnd, CONCAT('Validation successful. Nulls: ', @NullCount, ', Duplicates: ', @DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO HumanResources.EmployeeTraining (EmployeeTrainingID, EmployeeID, TrainingProgramID, TrainingDate, Score, CompletionStatus, Notes, LoadDate)
        SELECT EmployeeTrainingID, EmployeeID, TrainingProgramID, TrainingDate, Score, CompletionStatus, Notes, GETDATE()
        FROM TradePortDB.HumanResources.EmployeeTraining;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Insert', @StepStart, @StepEnd, CONCAT('Inserted ', @@ROWCOUNT, ' rows'));
        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Error', GETDATE(), GETDATE(), @Message);
        THROW;
    END CATCH
END;
GO

--------------------------------------------------------------------------------
-- 10) LoadSALeaveType
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE HumanResources.LoadSALeaveType
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @TableName NVARCHAR(128) = 'HumanResources.LeaveType';
    DECLARE @StepStart DATETIME;
    DECLARE @StepEnd DATETIME;
    DECLARE @Message NVARCHAR(1000);
    DECLARE @NullCount INT;
    DECLARE @DupCount INT;

    BEGIN TRY
        BEGIN TRAN;
        SET @StepStart = GETDATE();
        TRUNCATE TABLE HumanResources.LeaveType;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Truncate', @StepStart, @StepEnd, 'Truncated HumanResources.LeaveType');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.HumanResources.LeaveType 
        WHERE LeaveTypeID IS NULL OR LeaveTypeName IS NULL;
        IF @NullCount > 0 
            THROW 60000, 'NULLs found in critical columns of HumanResources.LeaveType', 1;
        SELECT @DupCount = COUNT(*) 
        FROM (
            SELECT LeaveTypeID 
            FROM TradePortDB.HumanResources.LeaveType 
            GROUP BY LeaveTypeID 
            HAVING COUNT(*) > 1
        ) d;
        IF @DupCount > 0 
            THROW 60001, 'Duplicate LeaveTypeIDs found', 1;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Validate', @StepStart, @StepEnd, CONCAT('Validation successful. Nulls: ', @NullCount, ', Duplicates: ', @DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO HumanResources.LeaveType (LeaveTypeID, LeaveTypeName, IsPaid, MaxDaysPerYear, LoadDate)
        SELECT LeaveTypeID, LeaveTypeName, IsPaid, MaxDaysPerYear, GETDATE()
        FROM TradePortDB.HumanResources.LeaveType;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Insert', @StepStart, @StepEnd, CONCAT('Inserted ', @@ROWCOUNT, ' rows'));
        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Error', GETDATE(), GETDATE(), @Message);
        THROW;
    END CATCH
END;
GO

--------------------------------------------------------------------------------
-- 11) LoadSALeaveRequest
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE HumanResources.LoadSALeaveRequest
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @TableName NVARCHAR(128) = 'HumanResources.LeaveRequest';
    DECLARE @StepStart DATETIME;
    DECLARE @StepEnd DATETIME;
    DECLARE @Message NVARCHAR(1000);
    DECLARE @NullCount INT;
    DECLARE @DupCount INT;

    BEGIN TRY
        BEGIN TRAN;
        SET @StepStart = GETDATE();
        TRUNCATE TABLE HumanResources.LeaveRequest;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Truncate', @StepStart, @StepEnd, 'Truncated HumanResources.LeaveRequest');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.HumanResources.LeaveRequest 
        WHERE LeaveRequestID IS NULL OR EmployeeID IS NULL OR LeaveTypeID IS NULL OR StartDate IS NULL OR EndDate IS NULL OR Status IS NULL OR RequestDate IS NULL;
        IF @NullCount > 0 
            THROW 61000, 'NULLs found in critical columns of HumanResources.LeaveRequest', 1;
        SELECT @DupCount = COUNT(*) 
        FROM (
            SELECT LeaveRequestID 
            FROM TradePortDB.HumanResources.LeaveRequest 
            GROUP BY LeaveRequestID 
            HAVING COUNT(*) > 1
        ) d;
        IF @DupCount > 0 
            THROW 61001, 'Duplicate LeaveRequestIDs found', 1;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Validate', @StepStart, @StepEnd, CONCAT('Validation successful. Nulls: ', @NullCount, ', Duplicates: ', @DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO HumanResources.LeaveRequest (LeaveRequestID, EmployeeID, LeaveTypeID, StartDate, EndDate, Status, RequestDate, ApprovedBy, Notes, LoadDate)
        SELECT LeaveRequestID, EmployeeID, LeaveTypeID, StartDate, EndDate, Status, RequestDate, ApprovedBy, Notes, GETDATE()
        FROM TradePortDB.HumanResources.LeaveRequest;
        SET @StepEnd = GETDATE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Insert', @StepStart, @StepEnd, CONCAT('Inserted ', @@ROWCOUNT, ' rows'));
        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@TableName, 'Error', GETDATE(), GETDATE(), @Message);
        THROW;
    END CATCH
END;
GO



USE StagingDB;
go


CREATE OR ALTER PROCEDURE PortOperations.LoadContainerType
AS
BEGIN
    DECLARE 
      @TableName    NVARCHAR(128) = 'StagingDB.PortOperations.ContainerType',
      @StepStart    DATETIME,
      @StepEnd      DATETIME,
      @Message      NVARCHAR(2000),
      @NullCount    INT,
      @DupCount     INT;

    BEGIN TRY
        BEGIN TRAN;

        -- 1) TRUNCATE
        SET @StepStart = GETDATE();
        TRUNCATE TABLE StagingDB.PortOperations.ContainerType;
        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.PortOperations.ETLLog(TableName,OperationType,StartTime,EndTime,Message)
         VALUES(@TableName,'Truncate',@StepStart,@StepEnd,'Truncated ContainerType');

        -- 2) VALIDATE (NULL)
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.PortOperations.ContainerType AS src
        WHERE src.ContainerTypeID IS NULL
           OR src.Description       IS NULL OR src.Description = ''
           OR src.MaxWeightKG       IS NULL OR src.MaxWeightKG = '';
        IF @NullCount > 0
            THROW 51000, 'Validation failed: NULLs in ContainerType', 1;

        -- 3) VALIDATE (Duplicates)
        SELECT @DupCount = COUNT(*) 
        FROM (
            SELECT ContainerTypeID 
            FROM TradePortDB.PortOperations.ContainerType
            GROUP BY ContainerTypeID
            HAVING COUNT(*) > 1
        ) AS d;
        IF @DupCount > 0
            THROW 51001, 'Validation failed: Duplicates in ContainerType', 1;

        SET @StepEnd = GETDATE();
        INSERT INTO StagingDB.PortOperations.ETLLog VALUES
          (@TableName,'Validate',@StepStart,@StepEnd,CONCAT('Null=',@NullCount,',Dup=',@DupCount));

        -- 4) INSERT
        SET @StepStart = GETDATE();
        INSERT INTO PortOperations.ContainerType
          (ContainerTypeID, Description, MaxWeightKG)
        SELECT
          ContainerTypeID, Description, MaxWeightKG
        FROM TradePortDB.PortOperations.ContainerType;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Insert',@StepStart,@StepEnd,CONCAT('Inserted ',@@ROWCOUNT,' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Error',@StepStart,@StepEnd,@Message);
        THROW;
    END CATCH;
END;
GO



CREATE OR ALTER PROCEDURE PortOperations.LoadEquipmentType
AS
BEGIN
    DECLARE
        @TableName NVARCHAR(128) = 'StagingDB.PortOperations.EquipmentType',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Message   NVARCHAR(2000),
        @NullCount INT,
        @DupCount  INT;

    BEGIN TRY
        BEGIN TRAN;

        -- STEP 1: TRUNCATE TARGET
        SET @StepStart = GETDATE();
        TRUNCATE TABLE PortOperations.EquipmentType;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
            VALUES(@TableName, 'Truncate', @StepStart, @StepEnd, 'Truncated EquipmentType');

        -- STEP 2: VALIDATE SOURCE NULLS
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*)
        FROM TradePortDB.PortOperations.EquipmentType AS src
        WHERE src.EquipmentTypeID IS NULL
           OR src.Description     IS NULL OR src.Description = '';
        IF @NullCount > 0
            THROW 51000, 'Validation failed: NULLs in EquipmentType', 1;

        -- STEP 3: VALIDATE DUPLICATES
        SELECT @DupCount = COUNT(*)
        FROM (
            SELECT EquipmentTypeID
            FROM TradePortDB.PortOperations.EquipmentType
            GROUP BY EquipmentTypeID
            HAVING COUNT(*) > 1
        ) AS d;
        IF @DupCount > 0
            THROW 51001, 'Validation failed: Duplicates in EquipmentType', 1;

        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
            VALUES(@TableName, 'Validate', @StepStart, @StepEnd,
                   CONCAT('Null=', @NullCount, ', Dup=', @DupCount));

        -- STEP 4: INSERT DATA
        SET @StepStart = GETDATE();
        INSERT INTO PortOperations.EquipmentType
            (EquipmentTypeID, Description)
        SELECT
            EquipmentTypeID,
            Description
        FROM TradePortDB.PortOperations.EquipmentType;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
            VALUES(@TableName, 'Insert', @StepStart, @StepEnd,
                   CONCAT('Inserted ', @@ROWCOUNT, ' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO PortOperations.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
            VALUES(@TableName, 'Error', @StepStart, @StepEnd, @Message);
        THROW;
    END CATCH;
END;
GO


CREATE OR ALTER PROCEDURE PortOperations.LoadPort
AS
BEGIN
    DECLARE
        @TableName NVARCHAR(128) = 'StagingDB.PortOperations.Port',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Message   NVARCHAR(2000),
        @NullCount INT,
        @DupCount  INT;

    BEGIN TRY
        BEGIN TRAN;

        -- STEP 1: TRUNCATE TARGET
        SET @StepStart = GETDATE();
        TRUNCATE TABLE PortOperations.Port;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
            VALUES(@TableName, 'Truncate', @StepStart, @StepEnd, 'Truncated Port');

        -- STEP 2: VALIDATE SOURCE NULLS
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*)
        FROM TradePortDB.PortOperations.Port AS src
        WHERE src.PortID   IS NULL
           OR src.Name     IS NULL OR src.Name ='';
        IF @NullCount > 0
            THROW 51000, 'Validation failed: NULLs in Port', 1;

        -- STEP 3: VALIDATE DUPLICATES
        SELECT @DupCount = COUNT(*)
        FROM (
            SELECT PortID
            FROM TradePortDB.PortOperations.Port
            GROUP BY PortID
            HAVING COUNT(*) > 1
        ) AS d;
        IF @DupCount > 0
            THROW 51001, 'Validation failed: Duplicates in Port', 1;

        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
            VALUES(@TableName, 'Validate', @StepStart, @StepEnd,
                   CONCAT('Null=', @NullCount, ', Dup=', @DupCount));

        -- STEP 4: INSERT DATA
        SET @StepStart = GETDATE();
        INSERT INTO PortOperations.Port
            (PortID, Name, Location)
        SELECT
            PortID,
            Name,
            Location
        FROM TradePortDB.PortOperations.Port;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
            VALUES(@TableName, 'Insert', @StepStart, @StepEnd,
                   CONCAT('Inserted ', @@ROWCOUNT, ' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO PortOperations.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
            VALUES(@TableName, 'Error', @StepStart, @StepEnd, @Message);
        THROW;
    END CATCH;
END;
GO


CREATE OR ALTER PROCEDURE PortOperations.LoadShip
AS
BEGIN
    DECLARE
        @TableName NVARCHAR(128) = 'StagingDB.PortOperations.Ship',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Message   NVARCHAR(2000),
        @NullCount INT,
        @DupCount  INT;

    BEGIN TRY
        BEGIN TRAN;

        -- STEP 1: TRUNCATE TARGET
        SET @StepStart = GETDATE();
        TRUNCATE TABLE PortOperations.Ship;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
            VALUES(@TableName, 'Truncate', @StepStart, @StepEnd, 'Truncated Ship');

        -- STEP 2: VALIDATE SOURCE NULLS
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*)
        FROM TradePortDB.PortOperations.Ship AS src
        WHERE src.ShipID     IS NULL
           OR src.IMO_Number IS NULL OR src.IMO_Number=''
           OR src.Name       IS NULL OR src.Name='';
        IF @NullCount > 0
            THROW 51000, 'Validation failed: NULLs in Ship', 1;

        -- STEP 3: VALIDATE DUPLICATES
        SELECT @DupCount = COUNT(*)
        FROM (
            SELECT ShipID
            FROM TradePortDB.PortOperations.Ship
            GROUP BY ShipID
            HAVING COUNT(*) > 1
        ) AS d;
        IF @DupCount > 0
            THROW 51001, 'Validation failed: Duplicates in Ship', 1;

        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
            VALUES(@TableName, 'Validate', @StepStart, @StepEnd,
                   CONCAT('Null=', @NullCount, ', Dup=', @DupCount));

        -- STEP 4: INSERT DATA
        SET @StepStart = GETDATE();
        INSERT INTO PortOperations.Ship
            (ShipID, IMO_Number, Name, CountryID)
        SELECT
            ShipID,
            IMO_Number,
            Name,
            CountryID
        FROM TradePortDB.PortOperations.Ship;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
            VALUES(@TableName, 'Insert', @StepStart, @StepEnd,
                   CONCAT('Inserted ', @@ROWCOUNT, ' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO PortOperations.ETLLog (TableName, OperationType, StartTime, EndTime, Message)
            VALUES(@TableName, 'Error', @StepStart, @StepEnd, @Message);
        THROW;
    END CATCH;
END;
GO




CREATE OR ALTER PROCEDURE PortOperations.LoadContainerYardMovement
AS
BEGIN
    DECLARE
        @TableName NVARCHAR(128) = 'PortOperations.ContainerYardMovement',
        @StepStart DATETIME, @StepEnd DATETIME,
        @Message   NVARCHAR(2000),
        @NullCount INT, @DupCount INT;
    BEGIN TRY
        BEGIN TRAN;

        SET @StepStart = GETDATE();
        TRUNCATE TABLE PortOperations.ContainerYardMovement;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Truncate',@StepStart,@StepEnd,'Truncated ContainerYardMovement');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.PortOperations.ContainerYardMovement src
        WHERE src.MovementID       IS NULL
           OR src.ContainerID      IS NULL
           OR src.YardSlotID       IS NULL
           OR src.MovementType     IS NULL OR src.MovementType=''
           OR src.MovementDateTime IS NULL OR src.MovementDateTime='';
        IF @NullCount > 0 THROW 51000,'NULLs in ContainerYardMovement',1;

        SELECT @DupCount = COUNT(*) 
        FROM (
          SELECT MovementID 
          FROM TradePortDB.PortOperations.ContainerYardMovement
          GROUP BY MovementID
          HAVING COUNT(*)>1
        ) d;
        IF @DupCount > 0 THROW 51001,'Duplicates in ContainerYardMovement',1;

        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Validate',@StepStart,@StepEnd, CONCAT('Null=',@NullCount,',Dup=',@DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO PortOperations.ContainerYardMovement
          (MovementID,ContainerID,YardSlotID,MovementType,MovementDateTime)
        SELECT MovementID,ContainerID,YardSlotID,MovementType,MovementDateTime
        FROM TradePortDB.PortOperations.ContainerYardMovement;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Insert',@StepStart,@StepEnd, CONCAT('Inserted ',@@ROWCOUNT,' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Error',@StepStart,@StepEnd,@Message);
        THROW;
    END CATCH;
END;
GO





CREATE OR ALTER PROCEDURE PortOperations.LoadCargoOperation
AS
BEGIN
    DECLARE
        @TableName NVARCHAR(128) = 'PortOperations.CargoOperation',
        @StepStart DATETIME, @StepEnd DATETIME,
        @Message   NVARCHAR(2000),
        @NullCount INT, @DupCount INT;
    BEGIN TRY
        BEGIN TRAN;

        SET @StepStart = GETDATE();
        TRUNCATE TABLE PortOperations.CargoOperation;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Truncate',@StepStart,@StepEnd,'Truncated CargoOperation');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.PortOperations.CargoOperation src
        WHERE src.CargoOpID        IS NULL
           OR src.PortCallID       IS NULL
           OR src.ContainerID      IS NULL
           OR src.OperationType    IS NULL OR src.OperationType=''
           OR src.OperationDateTime IS NULL OR src.OperationDateTime=''
		   OR src.Quantity IS NULL OR src.Quantity=''
		   OR src.WeightKG IS NULL OR src.WeightKG='';
        IF @NullCount > 0 THROW 51000,'NULLs in CargoOperation',1;

        SELECT @DupCount = COUNT(*) 
        FROM (
          SELECT CargoOpID 
          FROM TradePortDB.PortOperations.CargoOperation
          GROUP BY CargoOpID
          HAVING COUNT(*)>1
        ) d;
        IF @DupCount > 0 THROW 51001,'Duplicates in CargoOperation',1;

        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Validate',@StepStart,@StepEnd, CONCAT('Null=',@NullCount,',Dup=',@DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO PortOperations.CargoOperation
          (CargoOpID,PortCallID,ContainerID,OperationType,OperationDateTime,Quantity,WeightKG)
        SELECT CargoOpID,PortCallID,ContainerID,OperationType,OperationDateTime,Quantity,WeightKG
        FROM TradePortDB.PortOperations.CargoOperation;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Insert',@StepStart,@StepEnd, CONCAT('Inserted ',@@ROWCOUNT,' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Error',@StepStart,@StepEnd,@Message);
        THROW;
    END CATCH;
END;
GO




CREATE OR ALTER PROCEDURE Common.LoadOperationEquipmentAssignment
AS
BEGIN
    DECLARE
        @TableName NVARCHAR(128) = 'Common.OperationEquipmentAssignment',
        @StepStart DATETIME, @StepEnd DATETIME,
        @Message   NVARCHAR(2000),
        @NullCount INT, @DupCount INT;
    BEGIN TRY
        BEGIN TRAN;

        SET @StepStart = GETDATE();
        TRUNCATE TABLE Common.OperationEquipmentAssignment;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Truncate',@StepStart,@StepEnd,'Truncated OperationEquipmentAssignment');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.Common.OperationEquipmentAssignment src
        WHERE src.AssignmentID IS NULL
           OR src.CargoOpID     IS NULL
           OR src.EquipmentID   IS NULL
		   OR src.StartTime IS NULL OR src.StartTime=''
		   OR src.EndTime IS NULL OR src.EndTime='';
        IF @NullCount > 0 THROW 51000,'NULLs in OperationEquipmentAssignment',1;

        SELECT @DupCount = COUNT(*) 
        FROM (
          SELECT AssignmentID 
          FROM TradePortDB.Common.OperationEquipmentAssignment
          GROUP BY AssignmentID
          HAVING COUNT(*)>1
        ) d;
        IF @DupCount > 0 THROW 51001,'Duplicates in OperationEquipmentAssignment',1;

        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Validate',@StepStart,@StepEnd, CONCAT('Null=',@NullCount,',Dup=',@DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO Common.OperationEquipmentAssignment
          (AssignmentID,CargoOpID,EquipmentID,EmployeeID,StartTime,EndTime)
        SELECT AssignmentID,CargoOpID,EquipmentID,EmployeeID,StartTime,EndTime
        FROM TradePortDB.Common.OperationEquipmentAssignment;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Insert',@StepStart,@StepEnd, CONCAT('Inserted ',@@ROWCOUNT,' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Error',@StepStart,@StepEnd,@Message);
        THROW;
    END CATCH;
END;
GO



--------------------------------------------------------------------------------
-- LoadContainer
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE PortOperations.LoadContainer
AS
BEGIN
    DECLARE
        @TableName  NVARCHAR(128) = 'PortOperations.Container',
        @StepStart  DATETIME,
        @StepEnd    DATETIME,
        @Message    NVARCHAR(2000),
        @NullCount  INT,
        @DupCount   INT;
    BEGIN TRY
        BEGIN TRAN;

        -- Truncate target
        SET @StepStart = GETDATE();
        TRUNCATE TABLE PortOperations.Container;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog(TableName,OperationType,StartTime,EndTime,Message)
            VALUES(@TableName,'Truncate',@StepStart,@StepEnd,'Truncated Container');

        -- Validate NULLs
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.PortOperations.Container src
        WHERE src.ContainerID     IS NULL
           OR src.ContainerNumber IS NULL
           OR src.ContainerTypeID IS NULL
		   OR src.OwnerCompany IS NULL OR src.OwnerCompany='';
        IF @NullCount > 0
            THROW 51000, 'NULLs in Container', 1;

        -- Validate duplicates
        SELECT @DupCount = COUNT(*) 
        FROM (
          SELECT ContainerID 
          FROM TradePortDB.PortOperations.Container
          GROUP BY ContainerID
          HAVING COUNT(*) > 1
        ) d;
        IF @DupCount > 0
            THROW 51001, 'Duplicates in Container', 1;

        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Validate',@StepStart,@StepEnd, CONCAT('Null=',@NullCount,',Dup=',@DupCount));

        -- Insert data
        SET @StepStart = GETDATE();
        INSERT INTO PortOperations.Container(ContainerID,ContainerNumber,ContainerTypeID,OwnerCompany)
        SELECT ContainerID,ContainerNumber,ContainerTypeID,OwnerCompany
        FROM TradePortDB.PortOperations.Container;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Insert',@StepStart,@StepEnd, CONCAT('Inserted ',@@ROWCOUNT,' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Error',@StepStart,@StepEnd,@Message);
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- LoadEquipment
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE PortOperations.LoadEquipment
AS
BEGIN
    DECLARE
        @TableName  NVARCHAR(128) = 'PortOperations.Equipment',
        @StepStart  DATETIME,
        @StepEnd    DATETIME,
        @Message    NVARCHAR(2000),
        @NullCount  INT,
        @DupCount   INT;
    BEGIN TRY
        BEGIN TRAN;

        SET @StepStart = GETDATE();
        TRUNCATE TABLE PortOperations.Equipment;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Truncate',@StepStart,@StepEnd,'Truncated Equipment');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.PortOperations.Equipment src
        WHERE src.EquipmentID     IS NULL
           OR src.EquipmentTypeID IS NULL
		   OR src.Model IS NULL OR src.Model='';
        IF @NullCount > 0 THROW 51000,'NULLs in Equipment',1;

        SELECT @DupCount = COUNT(*) 
        FROM (SELECT EquipmentID FROM TradePortDB.PortOperations.Equipment GROUP BY EquipmentID HAVING COUNT(*)>1) d;
        IF @DupCount > 0 THROW 51001,'Duplicates in Equipment',1;

        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Validate',@StepStart,@StepEnd, CONCAT('Null=',@NullCount,',Dup=',@DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO PortOperations.Equipment(EquipmentID,EquipmentTypeID,Model)
        SELECT EquipmentID,EquipmentTypeID,Model
        FROM TradePortDB.PortOperations.Equipment;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Insert',@StepStart,@StepEnd, CONCAT('Inserted ',@@ROWCOUNT,' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Error',@StepStart,@StepEnd,@Message);
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- LoadVoyage
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE PortOperations.LoadVoyage
AS
BEGIN
    DECLARE
        @TableName  NVARCHAR(128) = 'PortOperations.Voyage',
        @StepStart  DATETIME,
        @StepEnd    DATETIME,
        @Message    NVARCHAR(2000),
        @NullCount  INT,
        @DupCount   INT;
    BEGIN TRY
        BEGIN TRAN;

        SET @StepStart = GETDATE();
        TRUNCATE TABLE PortOperations.Voyage;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Truncate',@StepStart,@StepEnd,'Truncated Voyage');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.PortOperations.Voyage src
        WHERE src.VoyageID      IS NULL
           OR src.ShipID        IS NULL
           OR src.VoyageNumber  IS NULL;
        IF @NullCount > 0 THROW 51000,'NULLs in Voyage',1;

        SELECT @DupCount = COUNT(*) 
        FROM (SELECT VoyageID FROM TradePortDB.PortOperations.Voyage GROUP BY VoyageID HAVING COUNT(*)>1) d;
        IF @DupCount > 0 THROW 51001,'Duplicates in Voyage',1;

        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Validate',@StepStart,@StepEnd, CONCAT('Null=',@NullCount,',Dup=',@DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO PortOperations.Voyage(VoyageID,ShipID,VoyageNumber,DeparturePortID,ArrivalPortID)
        SELECT VoyageID,ShipID,VoyageNumber,DeparturePortID,ArrivalPortID
        FROM TradePortDB.PortOperations.Voyage;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Insert',@StepStart,@StepEnd, CONCAT('Inserted ',@@ROWCOUNT,' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Error',@StepStart,@StepEnd,@Message);
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- LoadPortCall
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE PortOperations.LoadPortCall
AS
BEGIN
    DECLARE
        @TableName  NVARCHAR(128) = 'PortOperations.PortCall',
        @StepStart  DATETIME,
        @StepEnd    DATETIME,
        @Message    NVARCHAR(2000),
        @NullCount  INT,
        @DupCount   INT;
    BEGIN TRY
        BEGIN TRAN;

        SET @StepStart = GETDATE();
        TRUNCATE TABLE PortOperations.PortCall;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Truncate',@StepStart,@StepEnd,'Truncated PortCall');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.PortOperations.PortCall src
        WHERE src.PortCallID      IS NULL
           OR src.VoyageID        IS NULL
           OR src.PortID          IS NULL
           OR src.ArrivalDateTime IS NULL;
        IF @NullCount > 0 THROW 51000,'NULLs in PortCall',1;

        SELECT @DupCount = COUNT(*) 
        FROM (SELECT PortCallID FROM TradePortDB.PortOperations.PortCall GROUP BY PortCallID HAVING COUNT(*)>1) d;
        IF @DupCount > 0 THROW 51001,'Duplicates in PortCall',1;

        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Validate',@StepStart,@StepEnd, CONCAT('Null=',@NullCount,',Dup=',@DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO PortOperations.PortCall
          (PortCallID,VoyageID,PortID,ArrivalDateTime,DepartureDateTime,Status)
        SELECT PortCallID,VoyageID,PortID,ArrivalDateTime,DepartureDateTime,Status
        FROM TradePortDB.PortOperations.PortCall;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Insert',@StepStart,@StepEnd, CONCAT('Inserted ',@@ROWCOUNT,' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Error',@StepStart,@StepEnd,@Message);
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- LoadBerth
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE PortOperations.LoadBerth
AS
BEGIN
    DECLARE
        @TableName  NVARCHAR(128) = 'PortOperations.Berth',
        @StepStart  DATETIME,
        @StepEnd    DATETIME,
        @Message    NVARCHAR(2000),
        @NullCount  INT,
        @DupCount   INT;
    BEGIN TRY
        BEGIN TRAN;

        SET @StepStart = GETDATE();
        TRUNCATE TABLE PortOperations.Berth;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Truncate',@StepStart,@StepEnd,'Truncated Berth');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.PortOperations.Berth src
        WHERE src.BerthID    IS NULL
           OR src.PortID     IS NULL
           OR src.Name       IS NULL OR src.Name='';
        IF @NullCount > 0 THROW 51000,'NULLs in Berth',1;

        SELECT @DupCount = COUNT(*) 
        FROM (SELECT BerthID FROM TradePortDB.PortOperations.Berth GROUP BY BerthID HAVING COUNT(*)>1) d;
        IF @DupCount > 0 THROW 51001,'Duplicates in Berth',1;

        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Validate',@StepStart,@StepEnd, CONCAT('Null=',@NullCount,',Dup=',@DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO PortOperations.Berth(BerthID,PortID,Name,LengthMeters)
        SELECT BerthID,PortID,Name,LengthMeters
        FROM TradePortDB.PortOperations.Berth;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Insert',@StepStart,@StepEnd, CONCAT('Inserted ',@@ROWCOUNT,' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Error',@StepStart,@StepEnd,@Message);
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- LoadBerthAllocation
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE PortOperations.LoadBerthAllocation
AS
BEGIN
    DECLARE
        @TableName  NVARCHAR(128) = 'PortOperations.BerthAllocation',
        @StepStart  DATETIME,
        @StepEnd    DATETIME,
        @Message    NVARCHAR(2000),
        @NullCount  INT,
        @DupCount   INT;
    BEGIN TRY
        BEGIN TRAN;

        SET @StepStart = GETDATE();
        TRUNCATE TABLE PortOperations.BerthAllocation;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Truncate',@StepStart,@StepEnd,'Truncated BerthAllocation');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.PortOperations.BerthAllocation src
        WHERE src.AllocationID IS NULL
           OR src.PortCallID    IS NULL
           OR src.BerthID       IS NULL;
        IF @NullCount > 0 THROW 51000,'NULLs in BerthAllocation',1;

        SELECT @DupCount = COUNT(*) 
        FROM (SELECT AllocationID FROM TradePortDB.PortOperations.BerthAllocation GROUP BY AllocationID HAVING COUNT(*)>1) d;
        IF @DupCount > 0 THROW 51001,'Duplicates in BerthAllocation',1;

        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Validate',@StepStart,@StepEnd, CONCAT('Null=',@NullCount,',Dup=',@DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO PortOperations.BerthAllocation
          (AllocationID,PortCallID,BerthID,AllocationStart,AllocationEnd,AssignedBy)
        SELECT AllocationID,PortCallID,BerthID,AllocationStart,AllocationEnd,AssignedBy
        FROM TradePortDB.PortOperations.BerthAllocation;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Insert',@StepStart,@StepEnd, CONCAT('Inserted ',@@ROWCOUNT,' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Error',@StepStart,@StepEnd,@Message);
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- LoadYard
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE PortOperations.LoadYard
AS
BEGIN
    DECLARE
        @TableName  NVARCHAR(128) = 'PortOperations.Yard',
        @StepStart  DATETIME,
        @StepEnd    DATETIME,
        @Message    NVARCHAR(2000),
        @NullCount  INT,
        @DupCount   INT;
    BEGIN TRY
        BEGIN TRAN;

        SET @StepStart = GETDATE();
        TRUNCATE TABLE PortOperations.Yard;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Truncate',@StepStart,@StepEnd,'Truncated Yard');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.PortOperations.Yard src
        WHERE src.YardID   IS NULL
           OR src.PortID   IS NULL
           OR src.Name     IS NULL OR src.Name='';
        IF @NullCount > 0 THROW 51000,'NULLs in Yard',1;

        SELECT @DupCount = COUNT(*) 
        FROM (SELECT YardID FROM TradePortDB.PortOperations.Yard GROUP BY YardID HAVING COUNT(*)>1) d;
        IF @DupCount > 0 THROW 51001,'Duplicates in Yard',1;

        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Validate',@StepStart,@StepEnd, CONCAT('Null=',@NullCount,',Dup=',@DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO PortOperations.Yard(YardID,PortID,Name,UsageType)
        SELECT YardID,PortID,Name,UsageType
        FROM TradePortDB.PortOperations.Yard;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Insert',@StepStart,@StepEnd, CONCAT('Inserted ',@@ROWCOUNT,' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Error',@StepStart,@StepEnd,@Message);
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- LoadYardSlot
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE PortOperations.LoadYardSlot
AS
BEGIN
    DECLARE
        @TableName  NVARCHAR(128) = 'PortOperations.YardSlot',
        @StepStart  DATETIME,
        @StepEnd    DATETIME,
        @Message    NVARCHAR(2000),
        @NullCount  INT,
        @DupCount   INT;
    BEGIN TRY
        BEGIN TRAN;

        SET @StepStart = GETDATE();
        TRUNCATE TABLE PortOperations.YardSlot;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Truncate',@StepStart,@StepEnd,'Truncated YardSlot');

        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*) 
        FROM TradePortDB.PortOperations.YardSlot src
        WHERE src.YardSlotID IS NULL
           OR src.YardID     IS NULL
		   OR src.Block		IS NULL OR src.Block = '';
        IF @NullCount > 0 THROW 51000,'NULLs in YardSlot',1;

        SELECT @DupCount = COUNT(*) 
        FROM (SELECT YardSlotID FROM TradePortDB.PortOperations.YardSlot GROUP BY YardSlotID HAVING COUNT(*)>1) d;
        IF @DupCount > 0 THROW 51001,'Duplicates in YardSlot',1;

        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Validate',@StepStart,@StepEnd, CONCAT('Null=',@NullCount,',Dup=',@DupCount));

        SET @StepStart = GETDATE();
        INSERT INTO PortOperations.YardSlot(YardSlotID,YardID,Block,RowNumber,TierLevel)
        SELECT YardSlotID,YardID,Block,RowNumber,TierLevel
        FROM TradePortDB.PortOperations.YardSlot;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Insert',@StepStart,@StepEnd, CONCAT('Inserted ',@@ROWCOUNT,' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO PortOperations.ETLLog VALUES
          (@TableName,'Error',@StepStart,@StepEnd,@Message);
        THROW;
    END CATCH;
END;
GO



--------------------------------------------------------------------------------
-- LoadCountry
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Common.LoadCountry
AS
BEGIN
    DECLARE
        @TableName  NVARCHAR(128) = 'Common.Country',
        @StepStart  DATETIME,
        @StepEnd    DATETIME,
        @Message    NVARCHAR(2000),
        @NullCount  INT,
        @DupCount   INT;

    BEGIN TRY
        BEGIN TRAN;

        -- STEP 1: DELETE TARGET (به جای TRUNCATE)
        SET @StepStart = GETDATE();
        DELETE FROM Common.Country;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog
            (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
            (@TableName, 'Delete', @StepStart, @StepEnd, 'Deleted all rows from Country');

        -- STEP 2: VALIDATE SOURCE NULLS
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*)
        FROM TradePortDB.Common.Country AS src
        WHERE src.CountryID   IS NULL
           OR src.CountryName IS NULL OR src.CountryName='';
        IF @NullCount > 0
            THROW 51000, 'Validation failed: NULLs in Country', 1;

        -- STEP 3: VALIDATE DUPLICATES
        SELECT @DupCount = COUNT(*)
        FROM (
            SELECT CountryID
            FROM TradePortDB.Common.Country
            GROUP BY CountryID
            HAVING COUNT(*) > 1
        ) AS d;
        IF @DupCount > 0
            THROW 51001, 'Validation failed: Duplicates in Country', 1;

        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog
            (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
            (@TableName, 'Validate', @StepStart, @StepEnd,
             CONCAT('Null=', @NullCount, ', Dup=', @DupCount));

        -- STEP 4: INSERT DATA
        SET @StepStart = GETDATE();
        INSERT INTO Common.Country
            (CountryID, CountryName, CountryCode)
        SELECT
            CountryID, CountryName, CountryCode
        FROM TradePortDB.Common.Country;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog
            (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
            (@TableName, 'Insert', @StepStart, @StepEnd,
             CONCAT('Inserted ', @@ROWCOUNT, ' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO PortOperations.ETLLog
            (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
            (@TableName, 'Error', @StepStart, @StepEnd, @Message);
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- LoadEmployee
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE HumanResources.LoadEmployee
AS
BEGIN
    DECLARE
        @TableName  NVARCHAR(128) = 'HumanResources.Employee',
        @StepStart  DATETIME,
        @StepEnd    DATETIME,
        @Message    NVARCHAR(2000),
        @NullCount  INT,
        @DupCount   INT;
    BEGIN TRY
        BEGIN TRAN;

        -- STEP 1: DELETE TARGET (به جای TRUNCATE)
        SET @StepStart = GETDATE();
        DELETE FROM HumanResources.Employee;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog
            (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
            (@TableName, 'Delete', @StepStart, @StepEnd, 'Deleted all rows from Employee');

        -- STEP 2: VALIDATE SOURCE NULLS
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*)
        FROM TradePortDB.HumanResources.Employee AS src
        WHERE src.EmployeeID IS NULL
           OR src.FullName   IS NULL OR src.FullName='';
        IF @NullCount > 0
            THROW 51000, 'Validation failed: NULLs in Employee', 1;

        -- STEP 3: VALIDATE DUPLICATES
        SELECT @DupCount = COUNT(*)
        FROM (
            SELECT EmployeeID
            FROM TradePortDB.HumanResources.Employee
            GROUP BY EmployeeID
            HAVING COUNT(*) > 1
        ) AS d;
        IF @DupCount > 0
            THROW 51001, 'Validation failed: Duplicates in Employee', 1;

        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog
            (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
            (@TableName, 'Validate', @StepStart, @StepEnd,
             CONCAT('Null=', @NullCount, ', Dup=', @DupCount));

        -- STEP 4: INSERT DATA
        SET @StepStart = GETDATE();
        INSERT INTO HumanResources.Employee
            (EmployeeID, FullName, Position, NationalID, HireDate,
             BirthDate, Gender, MaritalStatus, Address, Phone, Email, EmploymentStatus)
        SELECT
            EmployeeID, FullName, Position, NationalID, HireDate,
            BirthDate, Gender, MaritalStatus, Address, Phone, Email, EmploymentStatus
        FROM TradePortDB.HumanResources.Employee;
        SET @StepEnd = GETDATE();
        INSERT INTO PortOperations.ETLLog
            (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
            (@TableName, 'Insert', @StepStart, @StepEnd,
             CONCAT('Inserted ', @@ROWCOUNT, ' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();
        INSERT INTO PortOperations.ETLLog
            (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
            (@TableName, 'Error', @StepStart, @StepEnd, @Message);
        THROW;
    END CATCH;
END;
GO



--SELECT 
--  'Source'   = COUNT(*)
--FROM TradePortDB.PortOperations.Port

--SELECT
--  'Staging'  = COUNT(*)
--FROM StagingDB.PortOperations.Port;

--SELECT * 
--FROM PortOperations.ETLLog 
--WHERE OperationType = 'Error';


--USE StagingDB;
--GO

--SELECT 
--    LogID,
--    TableName,
--    OperationType,
--    StartTime,
--    EndTime,
--    Message
--FROM PortOperations.ETLLog
--ORDER BY LogID DESC;

