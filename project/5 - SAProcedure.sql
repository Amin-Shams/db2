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
