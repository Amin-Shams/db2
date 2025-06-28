USE DataWarehouse;
GO

--------------------------------------------------------------------------------
-- 2-1) LoadFactInvoiceLineTransactionInitialLoad
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.LoadFactInvoiceLineTransactionInitialLoad
AS
BEGIN
    DECLARE
        @TableName NVARCHAR(128) = 'Fact.FactInvoiceLineTransaction',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Message   NVARCHAR(2000),
        @NullCount INT,
        @RowCount  INT;

    BEGIN TRY
        BEGIN TRAN;

        -- STEP 1: DELETE target fact
        SET @StepStart = GETDATE();
        DELETE FROM Fact.FactInvoiceLineTransaction;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName,'Truncate',@StepStart,@StepEnd,'FirstLoad: Cleared FactInvoiceLineTransaction');

        -- STEP 2: Validate staging NULLs
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*)
        FROM StagingDB.Finance.InvoiceLine AS src
        WHERE src.InvoiceID      IS NULL
           OR src.ServiceTypeID  IS NULL
           OR src.Quantity       IS NULL
           OR src.UnitPrice      IS NULL
           OR src.TaxAmount      IS NULL
           OR src.NetAmount      IS NULL;
        IF @NullCount > 0
            THROW 62000, 'Validation failed: NULLs in StagingDB.Finance.InvoiceLine', 1;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName,'Validate',@StepStart,@StepEnd,CONCAT('FirstLoad: Found ',@NullCount,' nulls'));

        -- STEP 3: Insert mapping to dims
        SET @StepStart = GETDATE();
        INSERT INTO Fact.FactInvoiceLineTransaction
            (DimInvoiceID,DimCustomerID,DimServiceTypeID,DimTaxID,DimDateID,
             Quantity,UnitPrice,DiscountPercent,NetAmount,TaxAmount)
        SELECT
            inv.DimInvoiceID,
            cust.DimCustomerID,
            svc.DimServiceTypeID,
            tax.DimTaxID,
            dt.DimDateID,
            src.Quantity,
            src.UnitPrice,
            src.DiscountPercent,
            src.NetAmount,
            src.TaxAmount
        FROM StagingDB.Finance.InvoiceLine AS src
        INNER JOIN StagingDB.Finance.Invoice       AS i      ON src.InvoiceID     = i.InvoiceID
        INNER JOIN Dim.DimInvoice                   AS inv    ON i.InvoiceNumber  = inv.InvoiceNumber
        INNER JOIN StagingDB.Finance.Contract      AS ctr    ON i.ContractID     = ctr.ContractID
        INNER JOIN StagingDB.Finance.Customer      AS stc    ON ctr.CustomerID   = stc.CustomerID
        INNER JOIN Dim.DimCustomer                  AS cust   ON stc.CustomerCode = cust.CustomerCode
        INNER JOIN Dim.DimServiceType               AS svc    ON src.ServiceTypeID= svc.SourceServiceTypeID
        LEFT JOIN Dim.DimTax                        AS tax    ON src.TaxID        = tax.DimTaxID
        INNER JOIN Dim.DimDate                      AS dt     ON i.InvoiceDate    = dt.FullDate;
        SET @RowCount = @@ROWCOUNT;
        SET @StepEnd  = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName,'Insert',@StepStart,@StepEnd,CONCAT('FirstLoad: Inserted ',@RowCount,' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog(TableName,OperationType,StartTime,EndTime,Message)
        VALUES(@TableName,'Error',@StepStart,GETDATE(),CONCAT('FirstLoad: ',@Message));
        THROW;
    END CATCH;
END;
GO


--------------------------------------------------------------------------------
-- 2-2) LoadFactCustomerPaymentTransactionInitialLoad
-- Full initial populate of Fact.FactCustomerPaymentTransaction (Transaction Fact)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.LoadFactCustomerPaymentTransactionInitialLoad
AS
BEGIN
    DECLARE
        @TableName NVARCHAR(128) = 'Fact.FactCustomerPaymentTransaction',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Message   NVARCHAR(2000),
        @NullCount INT,
        @RowCount  INT;

    BEGIN TRY
        BEGIN TRAN;

        -- STEP 1: Clear the target fact table
        SET @StepStart = GETDATE();
        DELETE FROM Fact.FactCustomerPaymentTransaction;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName, 'Truncate', @StepStart, @StepEnd,
               'FirstLoad: Cleared FactCustomerPaymentTransaction');

        -- STEP 2: Validate that no required fields are NULL in staging
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*)
        FROM StagingDB.Finance.Payment AS src
        WHERE src.InvoiceID     IS NULL
           OR src.PaymentDate   IS NULL
           OR src.Amount        IS NULL
           OR src.PaymentMethod IS NULL;
        IF @NullCount > 0
            THROW 63000, 'Validation failed: NULLs in StagingDB.Finance.Payment', 1;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName, 'Validate', @StepStart, @StepEnd,
               CONCAT('FirstLoad: Found ', @NullCount, ' nulls in staging.Payment'));

        -- STEP 3: Insert into fact, joining through Invoice → Contract → Customer → DimCustomer
        SET @StepStart = GETDATE();
        INSERT INTO Fact.FactCustomerPaymentTransaction
        (
            DimPaymentMethodID,
            DimInvoiceID,
            DimCustomerID,
            DimDateID,
            PaymentAmount,
            DaysToPayment,
            RemainingAmount,
            IsFullPayment,
            PartialPaymentCount
        )
        SELECT
            pm.DimPaymentMethodID,
            inv.DimInvoiceID,
            cust.DimCustomerID,
            dt.DimDateID,
            src.Amount,
            DATEDIFF(DAY, invHdr.InvoiceDate, src.PaymentDate),
            invHdr.TotalAmount - src.Amount,
            CASE WHEN src.Amount >= invHdr.TotalAmount THEN 1 ELSE 0 END,
            0  -- no partial-payment count in staging
        FROM StagingDB.Finance.Payment AS src

        -- join to invoice header
        INNER JOIN StagingDB.Finance.Invoice AS invHdr
            ON src.InvoiceID = invHdr.InvoiceID
        -- surrogate invoice dimension
        INNER JOIN Dim.DimInvoice AS inv
            ON invHdr.InvoiceNumber = inv.InvoiceNumber
        -- contract for this invoice
        INNER JOIN StagingDB.Finance.Contract AS ctr
            ON invHdr.ContractID = ctr.ContractID
        -- customer from contract
        INNER JOIN StagingDB.Finance.Customer AS stc
            ON ctr.CustomerID = stc.CustomerID
        -- surrogate customer dimension
        INNER JOIN Dim.DimCustomer AS cust
            ON stc.CustomerCode = cust.CustomerCode
        -- payment-method dimension
        INNER JOIN Dim.DimPaymentMethod AS pm
            ON src.PaymentMethod = pm.PaymentMethodName
        -- date dimension on payment date
        INNER JOIN Dim.DimDate AS dt
            ON src.PaymentDate = dt.FullDate;

        SET @RowCount = @@ROWCOUNT;
        SET @StepEnd  = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName, 'Insert', @StepStart, @StepEnd,
               CONCAT('FirstLoad: Inserted ', @RowCount, ' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName, 'Error', @StepStart, GETDATE(),
               CONCAT('FirstLoad: ', @Message));
        THROW;
    END CATCH;
END;
GO




--------------------------------------------------------------------------------
-- 2-3) LoadFactCustomerBillingMonthlySnapshotInitialLoad
-- Full initial populate of Fact.FactCustomerBillingMonthlySnapshot (Periodic Snapshot)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.LoadFactCustomerBillingMonthlySnapshotInitialLoad
AS
BEGIN
    DECLARE
        @TableName NVARCHAR(128) = 'Fact.FactCustomerBillingMonthlySnapshot',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Message   NVARCHAR(2000),
        @NullCount INT,
        @RowCount  INT;

    BEGIN TRY
        BEGIN TRAN;

        -- STEP 1: Clear the target snapshot
        SET @StepStart = GETDATE();
        DELETE FROM Fact.FactCustomerBillingMonthlySnapshot;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName, 'Truncate', @StepStart, @StepEnd,
               'FirstLoad: Cleared FactCustomerBillingMonthlySnapshot');

        -- STEP 2: Validate no NULLs in critical staging.Invoice columns
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*)
        FROM StagingDB.Finance.Invoice AS inv
        WHERE inv.InvoiceDate IS NULL
           OR inv.ContractID  IS NULL;
        IF @NullCount > 0
            THROW 63000, 'Validation failed: NULLs in staging.Invoice for snapshot', 1;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName, 'Validate', @StepStart, @StepEnd,
               CONCAT('FirstLoad: Found ', @NullCount, ' invalid staging invoices'));

        -- STEP 3: Aggregate invoice lines and payments per customer-month
        WITH InvoiceLineAgg AS
        (
            SELECT
                ctr.CustomerID,
                DATEFROMPARTS(YEAR(inv.InvoiceDate), MONTH(inv.InvoiceDate), 1) AS SnapshotDate,
                COUNT(DISTINCT inv.InvoiceID)         AS TotalInvoiceCount,
                SUM(il.NetAmount)                     AS TotalNetAmount,
                SUM(il.TaxAmount)                     AS TotalTaxAmount,
                SUM(il.Quantity * il.UnitPrice * ISNULL(il.DiscountPercent,0)) AS TotalDiscount
            FROM StagingDB.Finance.InvoiceLine AS il
            INNER JOIN StagingDB.Finance.Invoice     AS inv ON il.InvoiceID = inv.InvoiceID
            INNER JOIN StagingDB.Finance.Contract    AS ctr ON inv.ContractID    = ctr.ContractID
            GROUP BY
                ctr.CustomerID,
                DATEFROMPARTS(YEAR(inv.InvoiceDate), MONTH(inv.InvoiceDate), 1)
        ),
        PaymentAgg AS
        (
            SELECT
                ctr.CustomerID,
                DATEFROMPARTS(YEAR(inv.InvoiceDate), MONTH(inv.InvoiceDate), 1) AS SnapshotDate,
                SUM(pay.Amount)                         AS TotalPaid,
                AVG(DATEDIFF(DAY, inv.InvoiceDate, pay.PaymentDate)) AS AveragePaymentDelay
            FROM StagingDB.Finance.Payment   AS pay
            INNER JOIN StagingDB.Finance.Invoice AS inv ON pay.InvoiceID = inv.InvoiceID
            INNER JOIN StagingDB.Finance.Contract  AS ctr ON inv.ContractID = ctr.ContractID
            GROUP BY
                ctr.CustomerID,
                DATEFROMPARTS(YEAR(inv.InvoiceDate), MONTH(inv.InvoiceDate), 1)
        ),
        OutstandingAgg AS
        (
            SELECT
                ila.CustomerID,
                ila.SnapshotDate,
                MAX(ila.TotalNetAmount + ila.TotalTaxAmount - ISNULL(pa.TotalPaid,0)) AS MaxOutstandingAmount
            FROM InvoiceLineAgg AS ila
            LEFT JOIN PaymentAgg AS pa
              ON ila.CustomerID   = pa.CustomerID
             AND ila.SnapshotDate = pa.SnapshotDate
            GROUP BY
                ila.CustomerID,
                ila.SnapshotDate
        )

        -- STEP 4: Insert into snapshot fact by mapping to dimensions
        INSERT INTO Fact.FactCustomerBillingMonthlySnapshot
        (
            DimCustomerID,
            DimDateID,
            DimBillingCycleID,
            TotalInvoiceCount,
            TotalNetAmount,
            TotalTaxAmount,
            TotalDiscount,
            TotalPaid,
            AveragePaymentDelay,
            MaxOutstandingAmount
        )
        SELECT
            cust.DimCustomerID,
            dt.DimDateID,
            bc.DimBillingCycleID,
            ila.TotalInvoiceCount,
            ila.TotalNetAmount,
            ila.TotalTaxAmount,
            ila.TotalDiscount,
            ISNULL(pa.TotalPaid,0),
            ISNULL(pa.AveragePaymentDelay,0),
            oa.MaxOutstandingAmount
        FROM InvoiceLineAgg AS ila
        -- map back through Contract → Customer
        INNER JOIN StagingDB.Finance.Contract AS ctr
            ON ila.CustomerID = ctr.CustomerID
        INNER JOIN StagingDB.Finance.Customer AS srcCust
            ON ctr.CustomerID = srcCust.CustomerID
        INNER JOIN Dim.DimCustomer AS cust
            ON srcCust.CustomerCode = cust.CustomerCode
        -- snapshot date to date dimension
        INNER JOIN Dim.DimDate AS dt
            ON ila.SnapshotDate = dt.FullDate
        -- billing cycle from contract
        INNER JOIN Dim.DimBillingCycle AS bc
            ON ctr.BillingCycleID = bc.DimBillingCycleID
        -- join aggregated payments and outstanding
        LEFT JOIN PaymentAgg    AS pa ON ila.CustomerID = pa.CustomerID
                                    AND ila.SnapshotDate = pa.SnapshotDate
        LEFT JOIN OutstandingAgg AS oa ON ila.CustomerID = oa.CustomerID
                                      AND ila.SnapshotDate = oa.SnapshotDate;

        SET @RowCount = @@ROWCOUNT;
        SET @StepEnd  = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName, 'Insert', @StepStart, @StepEnd,
               CONCAT('FirstLoad: Inserted ', @RowCount, ' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName, 'Error', @StepStart, GETDATE(),
               CONCAT('FirstLoad: ', @Message));
        THROW;
    END CATCH;
END;
GO




--------------------------------------------------------------------------------
-- 2-4) LoadFactInvoiceLifecycleAccumulatingInitialLoad
-- Full initial populate of Fact.FactInvoiceLifecycleAccumulating (Accumulating Snapshot)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.LoadFactInvoiceLifecycleAccumulatingInitialLoad
AS
BEGIN
    DECLARE
        @TableName NVARCHAR(128) = 'Fact.FactInvoiceLifecycleAccumulating',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Message   NVARCHAR(2000),
        @NullCount INT,
        @RowCount  INT;

    BEGIN TRY
        BEGIN TRAN;

        -- STEP 1: Clear the target accumulating snapshot
        SET @StepStart = GETDATE();
        DELETE FROM Fact.FactInvoiceLifecycleAccumulating;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName, 'Truncate', @StepStart, @StepEnd,
               'FirstLoad: Cleared FactInvoiceLifecycleAccumulating');

        -- STEP 2: Validate that invoice dates exist
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*)
        FROM StagingDB.Finance.Invoice AS i
        WHERE i.InvoiceDate IS NULL
           OR i.DueDate     IS NULL;
        IF @NullCount > 0
            THROW 65000, 'Validation failed: NULL dates in staging.Invoice', 1;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName, 'Validate', @StepStart, @StepEnd,
               CONCAT('FirstLoad: ', @NullCount, ' invalid staging invoices'));

        -- STEP 3: Build source set with aggregated payments
        WITH Src AS
        (
            SELECT
                i.InvoiceID,
                i.InvoiceNumber,
                i.InvoiceDate,
                i.DueDate,
                i.TotalAmount,
                SUM(p.Amount)        AS PaidAmount,
                COUNT(p.PaymentID)   AS PaymentCount,
                MIN(p.PaymentDate)   AS FirstPayDate,
                MAX(p.PaymentDate)   AS LastPayDate
            FROM StagingDB.Finance.Invoice AS i
            LEFT JOIN StagingDB.Finance.Payment AS p
                ON i.InvoiceID = p.InvoiceID
            GROUP BY
                i.InvoiceID,
                i.InvoiceNumber,
                i.InvoiceDate,
                i.DueDate,
                i.TotalAmount
        )

        -- STEP 4: Insert into accumulating fact
        INSERT INTO Fact.FactInvoiceLifecycleAccumulating
        (
            DimInvoiceID,
            DimCustomerID,
            DimContractID,
            InvoiceDateID,
            DueDateID,
            FirstPaymentDateID,
            FinalPaymentDateID,
            TotalAmount,
            PaidAmount,
            OutstandingAmount,
            PaymentCount,
            FirstPaymentDelay,
            FinalPaymentDelay,
            DaysToClose
        )
        SELECT
            inv.DimInvoiceID,
            cust.DimCustomerID,
            dctr.DimContractID,
            dd1.DimDateID,
            dd2.DimDateID,
            dd3.DimDateID,
            dd4.DimDateID,
            s.TotalAmount,
            s.PaidAmount,
            s.TotalAmount - s.PaidAmount AS OutstandingAmount,
            s.PaymentCount,
            DATEDIFF(DAY, s.InvoiceDate, s.FirstPayDate),
            DATEDIFF(DAY, s.InvoiceDate, s.LastPayDate),
            DATEDIFF(DAY, s.InvoiceDate, s.LastPayDate)
        FROM Src AS s
        -- map to invoice number → surrogate
        JOIN Dim.DimInvoice        AS inv ON s.InvoiceNumber = inv.InvoiceNumber
        -- find contract for this invoice
        JOIN StagingDB.Finance.Contract AS ctr ON s.InvoiceID = ctr.ContractID
        -- customer chain
        JOIN StagingDB.Finance.Customer AS srcCust ON ctr.CustomerID   = srcCust.CustomerID
        JOIN Dim.DimCustomer          AS cust    ON srcCust.CustomerCode = cust.CustomerCode
        -- contract dimension
        JOIN Dim.DimContract          AS dctr    ON ctr.ContractNumber   = dctr.ContractNumber
        -- invoice and due dates
        JOIN Dim.DimDate             AS dd1     ON s.InvoiceDate = dd1.FullDate
        JOIN Dim.DimDate             AS dd2     ON s.DueDate     = dd2.FullDate
        LEFT JOIN Dim.DimDate        AS dd3     ON s.FirstPayDate = dd3.FullDate
        LEFT JOIN Dim.DimDate        AS dd4     ON s.LastPayDate  = dd4.FullDate;

        SET @RowCount = @@ROWCOUNT;
        SET @StepEnd  = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName, 'Insert', @StepStart, @StepEnd,
               CONCAT('FirstLoad: Inserted ', @RowCount, ' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName, 'Error', @StepStart, GETDATE(),
               CONCAT('FirstLoad: ', @Message));
        THROW;
    END CATCH;
END;
GO



--------------------------------------------------------------------------------
-- 2-5) LoadFactCustomerContractActivationInitialLoad
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.LoadFactCustomerContractActivationInitialLoad
AS
BEGIN
    DECLARE
        @TableName NVARCHAR(128) = 'Fact.FactCustomerContractActivationFactless',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Message   NVARCHAR(2000),
        @RowCount  INT;

    BEGIN TRY
        BEGIN TRAN;

        -- STEP 1: پاکسازی هدف
        SET @StepStart = GETDATE();
        DELETE FROM Fact.FactCustomerContractActivationFactless;
        SET @StepEnd   = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName,'Truncate',@StepStart,@StepEnd,'FirstLoad: Cleared factless');

        -- STEP 2: درج رکوردها با جوین صحیح
        SET @StepStart = GETDATE();
        INSERT INTO Fact.FactCustomerContractActivationFactless
            (DimDateID, DimCustomerID, DimContractID)
        SELECT
            dd.DimDateID,          -- تاریخ شروع
            dc.DimCustomerID,      -- کلید مخفی مشتری
            dctr.DimContractID     -- کلید مخفی قرارداد
        FROM StagingDB.Finance.Contract AS srcCtr
        -- 1) از جدول استیج مشتری، CustomerID -> CustomerCode
        INNER JOIN StagingDB.Finance.Customer AS srcCust
            ON srcCtr.CustomerID = srcCust.CustomerID
        -- 2) به جدول دایمنش مشتری بر مبنای CustomerCode
        INNER JOIN Dim.DimCustomer AS dc
            ON srcCust.CustomerCode = dc.CustomerCode
        -- 3) به جدول دایمنش قرارداد بر مبنای ContractNumber
        INNER JOIN Dim.DimContract AS dctr
            ON srcCtr.ContractNumber = dctr.ContractNumber
        -- 4) به جدول دایمنش تاریخ بر مبنای StartDate
        INNER JOIN Dim.DimDate AS dd
            ON srcCtr.StartDate = dd.FullDate;

        SET @RowCount = @@ROWCOUNT;
        SET @StepEnd  = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName,'Insert',@StepStart,@StepEnd,CONCAT('FirstLoad: Inserted ',@RowCount,' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES(@TableName,'Error',@StepStart,GETDATE(),CONCAT('FirstLoad: ',@Message));
        THROW;
    END CATCH;
END;
GO


USE DataWarehouse;
GO

--------------------------------------------------------------------------------
-- 1) LoadFactTermination (First Load - Final Corrected Version)
-- This version uses a robust JOIN strategy to handle potential missing
-- employment history data while ensuring core data integrity.
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.LoadFactTermination
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessName NVARCHAR(128) = 'Fact.LoadFactTermination';
    DECLARE @TargetTable NVARCHAR(128) = 'Fact.FactTermination';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);

    BEGIN TRY
        -- Start with a clean table for the initial load.
        TRUNCATE TABLE Fact.FactTermination;

        -- This subquery finds the very first hire date for each employee to correctly calculate tenure.
        WITH EmployeeFirstHire AS (
            SELECT EmployeeID, MIN(StartDate) AS FirstHireDate
            FROM StagingDB.HumanResources.EmploymentHistory
            GROUP BY EmployeeID
        )
        INSERT INTO Fact.FactTermination (
            TerminationDateKey, EmployeeKey, DepartmentKey, JobTitleKey,
            TerminationReasonKey, TenureInDays, TenureInMonths, SalaryAtTermination, IsVoluntary
        )
        SELECT
            dd.DateKey,         -- INNER JOIN guarantees a valid key.
            de.EmployeeKey,     -- INNER JOIN guarantees a valid key.
            ISNULL(d.DepartmentKey, -1),
            ISNULL(jt.JobTitleKey, -1),
            ISNULL(dtr.TerminationReasonKey, -1),
            ISNULL(DATEDIFF(DAY, efh.FirstHireDate, t.TerminationDate), 0) AS TenureInDays,
            ISNULL(DATEDIFF(MONTH, efh.FirstHireDate, t.TerminationDate), 0) AS TenureInMonths,
            ISNULL(eh.Salary, 0) AS SalaryAtTermination,
            CASE WHEN t.TerminationReason IN ('Resignation', 'Retirement') THEN 1 ELSE 0 END AS IsVoluntary
        FROM 
            StagingDB.HumanResources.Termination t
        -- CORE FIX 1: Use INNER JOIN for essential dimensions to ensure data integrity.
        -- A fact record is meaningless without knowing WHO was terminated and WHEN.
        INNER JOIN 
            Dim.DimDate dd ON t.TerminationDate = dd.FullDate
        INNER JOIN 
            Dim.DimEmployee de ON t.EmployeeID = de.EmployeeID 
                               AND t.TerminationDate BETWEEN de.StartDate AND ISNULL(de.EndDate, '9999-12-31')
        -- CORE FIX 2: Use LEFT JOIN for descriptive attributes (Department, Job Title, Salary).
        -- This ensures that if an employee's final history record is missing,
        -- the termination fact is still loaded, and the missing attributes are marked as 'Unknown' (-1).
        LEFT JOIN 
            StagingDB.HumanResources.EmploymentHistory eh ON t.EmployeeID = eh.EmployeeID 
                                                   AND t.TerminationDate >= eh.StartDate 
                                                   AND (eh.EndDate IS NULL OR t.TerminationDate <= eh.EndDate)
        LEFT JOIN 
            EmployeeFirstHire efh ON t.EmployeeID = efh.EmployeeID
        LEFT JOIN 
            Dim.DimDepartment d ON eh.DepartmentID = d.DepartmentID
        LEFT JOIN 
            Dim.DimJobTitle jt ON eh.JobTitleID = jt.JobTitleID
        LEFT JOIN 
            Dim.DimTerminationReason dtr ON t.TerminationReason = dtr.TerminationReason;

        SET @Message = 'FactTermination initial load completed successfully.';
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
-- 2) LoadFactSalaryPayment (First Load - Final Corrected Version)
-- This procedure uses a robust OUTER APPLY logic to find the last known
-- department and job title, correctly handling gaps in employment history.
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.LoadFactSalaryPayment
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessName NVARCHAR(128) = 'Fact.LoadFactSalaryPayment';
    DECLARE @TargetTable NVARCHAR(128) = 'Fact.FactSalaryPayment';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);
    DECLARE @EmployerContributionRate DECIMAL(5, 2) = 0.23; -- Business rule for employer costs

    BEGIN TRY
        -- Start with a clean table for the initial load.
        TRUNCATE TABLE Fact.FactSalaryPayment;

        INSERT INTO Fact.FactSalaryPayment (
            PaymentDateKey, EmployeeKey, DepartmentKey, JobTitleKey,
            GrossPayAmount, BaseAmount, BonusAmount, DeductionsAmount, NetAmount, SalaryCostToCompany
        )
        SELECT
            dd.DateKey,
            de.EmployeeKey,
            ISNULL(d.DepartmentKey, -1),
            ISNULL(jt.JobTitleKey, -1),
            sp.Amount + ISNULL(sp.Bonus, 0) AS GrossPayAmount,
            sp.Amount,
            ISNULL(sp.Bonus, 0) AS BonusAmount,
            ISNULL(sp.Deductions, 0) AS DeductionsAmount,
            sp.NetAmount,
            (sp.Amount + ISNULL(sp.Bonus, 0)) * (1 + @EmployerContributionRate) AS SalaryCostToCompany
        FROM 
            StagingDB.HumanResources.SalaryPayment sp
        -- INNER JOIN to core dimensions to ensure data integrity
        INNER JOIN 
            Dim.DimDate dd ON sp.PaymentDate = dd.FullDate
        INNER JOIN 
            Dim.DimEmployee de ON sp.EmployeeID = de.EmployeeID 
                               AND sp.PaymentDate BETWEEN de.StartDate AND ISNULL(de.EndDate, '9999-12-31')
        -- CORE FIX: Use OUTER APPLY to find the most recent employment history record
        -- for each salary payment. This is robust against data gaps.
        OUTER APPLY (
            SELECT TOP 1 
                eh.DepartmentID,
                eh.JobTitleID
            FROM 
                StagingDB.HumanResources.EmploymentHistory eh
            WHERE 
                eh.EmployeeID = sp.EmployeeID
                AND eh.StartDate <= sp.PaymentDate -- Find all history records up to the payment date
            ORDER BY 
                eh.StartDate DESC, -- Order by the most recent StartDate
                eh.EmploymentHistoryID DESC -- Tie-breaker for same-day changes
        ) AS last_eh
        -- Now, LEFT JOIN to the dimensions based on the IDs found by OUTER APPLY
        LEFT JOIN 
            Dim.DimDepartment d ON last_eh.DepartmentID = d.DepartmentID
        LEFT JOIN 
            Dim.DimJobTitle jt ON last_eh.JobTitleID = jt.JobTitleID;

        SET @Message = 'FactSalaryPayment initial load completed successfully.';
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
-- 3) LoadFactEmployeeAttendance (First Load - Final Corrected Version)
-- This procedure uses a robust JOIN logic to find the last known
-- valid employee record, correctly handling potential gaps in employment history.
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.LoadFactEmployeeAttendance
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessName NVARCHAR(128) = 'Fact.LoadFactEmployeeAttendance';
    DECLARE @TargetTable NVARCHAR(128) = 'Fact.FactEmployeeAttendance';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);

    BEGIN TRY
        TRUNCATE TABLE Fact.FactEmployeeAttendance;

        INSERT INTO Fact.FactEmployeeAttendance (AttendanceDateKey, EmployeeKey, AttendanceStatus)
        SELECT
            ISNULL(dd.DateKey, -1),
            ISNULL(de.EmployeeKey, -1),
            a.Status
        FROM StagingDB.HumanResources.Attendance a
        JOIN Dim.DimEmployee de ON a.EmployeeID = de.EmployeeID 
        JOIN Dim.DimDate dd ON a.AttendanceDate = dd.FullDate
        WHERE a.AttendanceDate BETWEEN de.StartDate AND ISNULL(de.EndDate, '9999-12-31');

        SET @Message = 'FactEmployeeAttendance initial load completed successfully.';
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
-- 4) LoadFactMonthlyEmployeePerformance (First Load)
-- This procedure loads the periodic snapshot fact table by aggregating
-- daily attendance data into monthly performance metrics.
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.LoadFactMonthlyEmployeePerformance
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessName NVARCHAR(128) = 'Fact.LoadFactMonthlyEmployeePerformance';
    DECLARE @TargetTable NVARCHAR(128) = 'Fact.FactMonthlyEmployeePerformance';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);

    BEGIN TRY
        -- Start with a clean table for the initial load.
        TRUNCATE TABLE Fact.FactMonthlyEmployeePerformance;

        -- Step 1: Pre-aggregate daily attendance data to a monthly level.
        WITH MonthlyMetrics AS (
            SELECT
                EOMONTH(a.AttendanceDate) AS MonthEndDate,
                a.EmployeeID,
                SUM(ISNULL(a.HoursWorked, 0)) AS TotalHoursWorked,
                AVG(ISNULL(a.HoursWorked, 0)) AS AverageDailyHoursWorked,
                MIN(a.HoursWorked) AS MinDailyHoursWorked,
                MAX(a.HoursWorked) AS MaxDailyHoursWorked,
                SUM(CASE WHEN a.Status = 'Late' THEN 1 ELSE 0 END) AS LateDaysCount,
                SUM(CASE WHEN a.Status = 'Absent' THEN 1 ELSE 0 END) AS AbsentDaysCount,
                COUNT(a.AttendanceID) AS WorkDaysCount
            FROM 
                StagingDB.HumanResources.Attendance a
            GROUP BY 
                EOMONTH(a.AttendanceDate), a.EmployeeID
        )
        -- Step 2: Insert the aggregated data into the fact table, joining with dimensions.
        INSERT INTO Fact.FactMonthlyEmployeePerformance (
            MonthDateKey, EmployeeKey, DepartmentKey, TotalHoursWorked, 
            AverageDailyHoursWorked, MinDailyHoursWorked, MaxDailyHoursWorked,
            LateDaysCount, AbsentDaysCount, WorkDaysCount, OvertimeAsPercentage
        )
        SELECT
            dd.DateKey,
            de.EmployeeKey,
            ISNULL(d.DepartmentKey, -1) AS DepartmentKey,
            m.TotalHoursWorked,
            m.AverageDailyHoursWorked,
            m.MinDailyHoursWorked,
            m.MaxDailyHoursWorked,
            m.LateDaysCount,
            m.AbsentDaysCount,
            m.WorkDaysCount,
            -- Calculate overtime as a percentage of total hours worked (assuming 8 hours/day standard)
            CASE 
                WHEN m.TotalHoursWorked > 0 THEN 
                    (m.TotalHoursWorked - (m.WorkDaysCount * 8.0)) / m.TotalHoursWorked 
                ELSE 0 
            END AS OvertimeAsPercentage
        FROM 
            MonthlyMetrics m
        -- Use INNER JOIN to ensure integrity with core dimensions.
        INNER JOIN 
            Dim.DimDate dd ON m.MonthEndDate = dd.FullDate
        INNER JOIN 
            Dim.DimEmployee de ON m.EmployeeID = de.EmployeeID 
                               AND m.MonthEndDate BETWEEN de.StartDate AND ISNULL(de.EndDate, '9999-12-31')
        -- Use LEFT JOIN for descriptive attributes that might have gaps in history.
        LEFT JOIN 
            StagingDB.HumanResources.EmploymentHistory eh ON m.EmployeeID = eh.EmployeeID 
                                                   AND m.MonthEndDate BETWEEN eh.StartDate AND ISNULL(eh.EndDate, '9999-12-31')
        LEFT JOIN 
            Dim.DimDepartment d ON eh.DepartmentID = d.DepartmentID;

        SET @Message = 'FactMonthlyEmployeePerformance initial load completed successfully.';
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
-- 5) LoadFactEmployeeLifecycle (First Load - Final Corrected Version)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.LoadFactEmployeeLifecycle
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessName NVARCHAR(128) = 'Fact.LoadFactEmployeeLifecycle';
    DECLARE @TargetTable NVARCHAR(128) = 'Fact.FactEmployeeLifecycle';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);

    BEGIN TRY
        TRUNCATE TABLE Fact.FactEmployeeLifecycle;

        WITH TrainingHistory AS (
            SELECT EmployeeID, COUNT(*) AS TotalTrainings, MAX(TrainingDate) AS LastTrainingDate
            FROM StagingDB.HumanResources.EmployeeTraining GROUP BY EmployeeID
        ),
        PromotionHistory AS (
            SELECT EmployeeID, CASE WHEN COUNT(*) > 0 THEN COUNT(*) - 1 ELSE 0 END AS TotalPromotions
            FROM StagingDB.HumanResources.EmploymentHistory GROUP BY EmployeeID
        ),
        TerminationInfo AS (
            SELECT EmployeeID, TerminationDate, TerminationReason FROM StagingDB.HumanResources.Termination
        ),
        FirstHireDate AS (
            SELECT EmployeeID, MIN(StartDate) as Date FROM StagingDB.HumanResources.EmploymentHistory GROUP BY EmployeeID
        ),
        LastKnownSalary AS (
            SELECT EmployeeID, Salary, ROW_NUMBER() OVER(PARTITION BY EmployeeID ORDER BY StartDate DESC, EmploymentHistoryID DESC) as rn
            FROM StagingDB.HumanResources.EmploymentHistory
        )
        INSERT INTO Fact.FactEmployeeLifecycle (
            EmployeeKey, HireDateKey, TerminationDateKey, --FirstPromotionDateKey,
            LastTrainingDateKey, TerminationReasonKey, DaysToTermination,
            --DaysToFirstPromotion,
			TotalPromotionsCount, TotalTrainingsCompleted, FinalSalary
        )
        SELECT
            de.EmployeeKey,
            ISNULL(hire_date.DateKey, -1),
            ISNULL(term_date.DateKey, -1),
            --1, -- Logic for FirstPromotionDateKey is complex and deferred
            ISNULL(last_training_date.DateKey, -1),
            ISNULL(dtr.TerminationReasonKey, -1),
            CASE WHEN term.TerminationDate IS NOT NULL AND fhd.Date IS NOT NULL AND term.TerminationDate > fhd.Date THEN DATEDIFF(DAY, fhd.Date, term.TerminationDate) ELSE NULL END,
            --NULL, -- Logic for DaysToFirstPromotion is complex and deferred
            ISNULL(promo.TotalPromotions, 0),
            ISNULL(train.TotalTrainings, 0),
            lks.Salary
        FROM StagingDB.HumanResources.Employee e
        INNER JOIN Dim.DimEmployee de ON e.EmployeeID = de.EmployeeID AND de.IsCurrent = 1
        LEFT JOIN Dim.DimDate hire_date ON e.HireDate = hire_date.FullDate
        LEFT JOIN FirstHireDate fhd ON e.EmployeeID = fhd.EmployeeID
        LEFT JOIN TrainingHistory train ON e.EmployeeID = train.EmployeeID
        LEFT JOIN Dim.DimDate last_training_date ON train.LastTrainingDate = last_training_date.FullDate
        LEFT JOIN PromotionHistory promo ON e.EmployeeID = promo.EmployeeID
        LEFT JOIN TerminationInfo term ON e.EmployeeID = term.EmployeeID
        LEFT JOIN Dim.DimDate term_date ON term.TerminationDate = term_date.FullDate
        LEFT JOIN Dim.DimTerminationReason dtr ON term.TerminationReason = dtr.TerminationReason
        LEFT JOIN LastKnownSalary lks ON e.EmployeeID = lks.EmployeeID AND lks.rn = 1;

        SET @Message = 'FactEmployeeLifecycle initial load completed successfully with full history.';
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

/*
--------------------------------------------------------------------------------
-- 6) LoadFactYearlyHeadcount (First Load - Full Implementation)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.LoadFactYearlyHeadcount
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessName NVARCHAR(128) = 'Fact.LoadFactYearlyHeadcount';
    DECLARE @TargetTable NVARCHAR(128) = 'Fact.FactYearlyHeadcount';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);

    BEGIN TRY
        TRUNCATE TABLE Fact.FactYearlyHeadcount;

        DECLARE @Years TABLE (YearValue INT PRIMARY KEY);
        INSERT INTO @Years (YearValue)
        SELECT DISTINCT [Year] FROM Dim.DimDate WHERE [Year] >= (SELECT MIN(YEAR(HireDate)) FROM StagingDB.HumanResources.Employee) AND [Year] <= YEAR(GETDATE());

        SELECT 
            eh.EmployeeID, eh.DepartmentID, d.DepartmentKey,
            YEAR(eh.StartDate) AS StartYear, ISNULL(YEAR(eh.EndDate), 9999) AS EndYear
        INTO #EmpDeptHistory
        FROM StagingDB.HumanResources.EmploymentHistory eh
        INNER JOIN Dim.DimDepartment d ON eh.DepartmentID = d.DepartmentID;

        DECLARE @CurrentYear INT;
        DECLARE YearCursor CURSOR FOR SELECT YearValue FROM @Years;
        OPEN YearCursor;
        FETCH NEXT FROM YearCursor INTO @CurrentYear;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            INSERT INTO Fact.FactYearlyHeadcount (
                YearDateKey, DepartmentKey, HeadcountStartOfYear, HeadcountEndOfYear,
                HiresCount, TerminationsCount, VoluntaryTerminationsCount,
                InvoluntaryTerminationsCount, AverageTenureInMonths, TurnoverRate,
                AverageAgeOfEmployees
            )
            SELECT
                ISNULL(dd.DateKey, -1),
                d.DepartmentKey,
                (SELECT COUNT(DISTINCT EmployeeID) FROM #EmpDeptHistory WHERE DepartmentKey = d.DepartmentKey AND StartYear < @CurrentYear AND EndYear >= @CurrentYear),
                (SELECT COUNT(DISTINCT EmployeeID) FROM #EmpDeptHistory WHERE DepartmentKey = d.DepartmentKey AND StartYear <= @CurrentYear AND EndYear > @CurrentYear),
                (SELECT COUNT(DISTINCT e.EmployeeID) FROM StagingDB.HumanResources.Employee e INNER JOIN #EmpDeptHistory h ON e.EmployeeID = h.EmployeeID WHERE YEAR(e.HireDate) = @CurrentYear AND h.DepartmentKey = d.DepartmentKey),
                (SELECT COUNT(DISTINCT t.EmployeeID) FROM StagingDB.HumanResources.Termination t INNER JOIN #EmpDeptHistory h ON t.EmployeeID = h.EmployeeID WHERE YEAR(t.TerminationDate) = @CurrentYear AND h.DepartmentKey = d.DepartmentKey),
                (SELECT COUNT(DISTINCT t.EmployeeID) FROM StagingDB.HumanResources.Termination t INNER JOIN #EmpDeptHistory h ON t.EmployeeID = h.EmployeeID WHERE YEAR(t.TerminationDate) = @CurrentYear AND t.TerminationReason IN ('Resignation', 'Retirement') AND h.DepartmentKey = d.DepartmentKey),
                (SELECT COUNT(DISTINCT t.EmployeeID) FROM StagingDB.HumanResources.Termination t INNER JOIN #EmpDeptHistory h ON t.EmployeeID = h.EmployeeID WHERE YEAR(t.TerminationDate) = @CurrentYear AND t.TerminationReason NOT IN ('Resignation', 'Retirement') AND h.DepartmentKey = d.DepartmentKey),
                ISNULL(AVG(DATEDIFF(MONTH, e.HireDate, EOMONTH(DATEFROMPARTS(@CurrentYear, 12, 31)))), 0),
                0, -- TurnoverRate is complex, left as 0 for this implementation
                ISNULL(AVG(DATEDIFF(YEAR, e.BirthDate, EOMONTH(DATEFROMPARTS(@CurrentYear, 12, 31)))), 0)
            FROM Dim.DimDepartment d
            LEFT JOIN Dim.DimDate dd ON dd.FullDate = EOMONTH(DATEFROMPARTS(@CurrentYear, 12, 31))
            LEFT JOIN #EmpDeptHistory edh ON d.DepartmentKey = edh.DepartmentKey AND @CurrentYear >= edh.StartYear AND @CurrentYear <= edh.EndYear
            LEFT JOIN StagingDB.HumanResources.Employee e ON edh.EmployeeID = e.EmployeeID
            WHERE d.DepartmentKey <> -1
            GROUP BY d.DepartmentKey, dd.DateKey;

            FETCH NEXT FROM YearCursor INTO @CurrentYear;
        END;

        CLOSE YearCursor; DEALLOCATE YearCursor;
        DROP TABLE #EmpDeptHistory;
        
        SET @Message = 'FactYearlyHeadcount initial load completed successfully.';
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, RecordsInserted, Status, [Message])
        VALUES (@ProcessName, 'Initial Load', @TargetTable, @StartTime, GETDATE(), @@ROWCOUNT, 'Success', @Message);
    END TRY
    BEGIN CATCH
        IF CURSOR_STATUS('global','YearCursor') >= -1 DEALLOCATE YearCursor;
        DROP TABLE IF EXISTS #EmpDeptHistory;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, Status, [Message])
        VALUES (@ProcessName, 'Initial Load', @TargetTable, @StartTime, GETDATE(), 'Failed', @Message);
        THROW;
    END CATCH;
END;
GO
*/



USE DataWarehouse;
GO
--------------------------------------------------------------------------------
-- 2.1) LoadFactCargoOperationInitialLoad
-- Full initial populate of Fact.FactCargoOperationTransactional
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.LoadFactCargoOperationInitialLoad
AS
BEGIN
    SET NOCOUNT, XACT_ABORT ON;
    DECLARE 
      @TableName NVARCHAR(128) = 'Fact.FactCargoOperationTransactional',
      @StepStart DATETIME,
      @StepEnd   DATETIME,
      @Message   NVARCHAR(2000),
      @NullCount INT,
      @DupCount  INT;

    BEGIN TRY
        BEGIN TRAN;

        -- STEP 1: Clear target and reseed
        SET @StepStart = GETDATE();
        DELETE FROM fact.FactCargoOperationTransactional;
        DBCC CHECKIDENT('fact.FactCargoOperationTransactional', RESEED, 0);
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog
          (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
          (@TableName,'Delete',@StepStart,@StepEnd,'FirstLoad: Cleared fact');

        -- STEP 2: Validate source
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*)
        FROM StagingDB.PortOperations.CargoOperation AS co
        WHERE co.CargoOpID        IS NULL
           OR co.PortCallID       IS NULL
           OR co.ContainerID      IS NULL
           OR co.OperationType    IS NULL
           OR co.OperationDateTime IS NULL;
        IF @NullCount>0 THROW 70000,'Validation failed: NULLs in CargoOperation',1;

        SELECT @DupCount = COUNT(*)
        FROM (
          SELECT CargoOpID, COUNT(*) AS Cnt
          FROM StagingDB.PortOperations.CargoOperation
          GROUP BY CargoOpID
          HAVING COUNT(*)>1
        ) AS d;
        IF @DupCount>0 THROW 70001,'Validation failed: Duplicates in CargoOperation',1;

        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog
          (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
          (@TableName,'Validate',@StepStart,@StepEnd,
           CONCAT('FirstLoad: Null=',@NullCount,',Dup=',@DupCount));

        -- STEP 3: Insert with lookups including Equipment & Employee
        SET @StepStart = GETDATE();
        INSERT INTO fact.FactCargoOperationTransactional
          (DateKey,FullDate,ShipSK, PortSK, ContainerSK, EquipmentSK, EmployeeSK,
           OperationType, Quantity, WeightKG, OperationDateTime)
        SELECT
          dd.DimDateID,
		  dd.FullDate,
          ds.ShipSK,
          dp.PortSK,
          dc.ContainerSK,
          deq.EquipmentSK,
          dem.EmployeeSK,
          co.OperationType,
          co.Quantity,
          co.WeightKG,
          co.OperationDateTime
        FROM StagingDB.PortOperations.CargoOperation AS co
        JOIN StagingDB.PortOperations.PortCall     AS pc
          ON co.PortCallID = pc.PortCallID
        JOIN StagingDB.PortOperations.Voyage       AS v
          ON pc.VoyageID = v.VoyageID
        JOIN dim.DimShip                           AS ds
          ON v.ShipID = ds.ShipID
        JOIN dim.DimPort                           AS dp
          ON pc.PortID = dp.PortID
        JOIN dim.DimContainer                      AS dc
          ON co.ContainerID = dc.ContainerID
        JOIN StagingDB.Common.OperationEquipmentAssignment AS oea
          ON co.CargoOpID = oea.CargoOpID
        JOIN StagingDB.PortOperations.Equipment    AS e
          ON oea.EquipmentID = e.EquipmentID
        JOIN dim.DimEquipment                     AS deq
          ON e.EquipmentID = deq.EquipmentID
        JOIN StagingDB.HumanResources.Employee    AS emp
          ON oea.EmployeeID = emp.EmployeeID
        JOIN dim.DimEmployee                      AS dem
          ON emp.EmployeeID = dem.EmployeeID
        JOIN dim.DimDate                           AS dd
          ON CAST(co.OperationDateTime AS DATE) = dd.FullDate;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog
          (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
          (@TableName,'Insert',@StepStart,@StepEnd,
           CONCAT('FirstLoad: Inserted ',@@ROWCOUNT,' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE()<>0 ROLLBACK;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog
          (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
          (@TableName,'Error',@StepStart,GETDATE(),CONCAT('FirstLoad: ',@Message));
        THROW;
    END CATCH;
END;
GO



--------------------------------------------------------------------------------
-- 2.2) LoadFactPortCallSnapshotInitialLoad
-- Full initial populate of Fact.FactPortCallPeriodicSnapshot
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.LoadFactPortCallSnapshotInitialLoad
AS
BEGIN
    SET NOCOUNT, XACT_ABORT ON;
    DECLARE 
      @TableName NVARCHAR(128) = 'Fact.FactPortCallPeriodicSnapshot',
      @StepStart DATETIME,
      @StepEnd   DATETIME,
      @Message   NVARCHAR(2000);

    BEGIN TRY
        BEGIN TRAN;

        -- STEP 1: Clear target and reseed
        SET @StepStart=GETDATE();
        DELETE FROM Fact.FactPortCallPeriodicSnapshot;
        DBCC CHECKIDENT('Fact.FactPortCallPeriodicSnapshot',RESEED,0);
        SET @StepEnd=GETDATE();
        INSERT INTO dbo.ETLLog VALUES
          (@TableName,'Delete',@StepStart,@StepEnd,'FirstLoad: Cleared fact');

		-- STEP 2: Build snapshot per PortCall × Berth × Date
		SET @StepStart = GETDATE();

		INSERT INTO Fact.FactPortCallPeriodicSnapshot
		  (DateKey,FullDate, PortCallID, VoyageID, PortSK, BerthID, Status, AllocationCount, TotalOps)
		SELECT
		  ddate.DimDateID,
		  ddate.FullDate,
		  pc.PortCallID,
		  pc.VoyageID,
		  dp.PortSK,
		  ba.BerthID,
		  pc.Status,
		  COUNT(DISTINCT ba.AllocationID),
		  COUNT(DISTINCT co.CargoOpID)
		FROM StagingDB.PortOperations.PortCall AS pc
		JOIN Dim.DimDate AS ddate 
		  ON CAST(pc.ArrivalDateTime AS DATE) = ddate.FullDate
		JOIN Dim.DimPort AS dp 
		  ON pc.PortID = dp.PortID
		LEFT JOIN StagingDB.PortOperations.BerthAllocation AS ba
		  ON pc.PortCallID = ba.PortCallID
		LEFT JOIN StagingDB.PortOperations.CargoOperation AS co
		  ON pc.PortCallID = co.PortCallID   -- فقط بر اساس PortCallID
		GROUP BY
		  ddate.DimDateID,
		  ddate.FullDate,
		  pc.PortCallID,
		  pc.VoyageID,
		  dp.PortSK,
		  ba.BerthID,
		  pc.Status;

		SET @StepEnd = GETDATE();

		INSERT INTO dbo.ETLLog
		  (TableName, OperationType, StartTime, EndTime, Message)
		VALUES
		  (@TableName, 'Insert', @StepStart, @StepEnd,
		   CONCAT('FirstLoad: Inserted ', @@ROWCOUNT, ' rows'));


        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE()<>0 ROLLBACK;
        SET @Message=ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog VALUES
          (@TableName,'Error',@StepStart,GETDATE(),CONCAT('FirstLoad: ',@Message));
        THROW;
    END CATCH;
END;
GO


USE DataWarehouse;
GO

--------------------------------------------------------------------------------
-- 2.3) LoadFactContainerMovementsAccInitialLoad
-- Full initial populate of Fact.FactContainerMovementsAcc (Accumulating Fact)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.LoadFactContainerMovementsAccInitialLoad
AS
BEGIN
    SET NOCOUNT, XACT_ABORT ON;
    DECLARE
        @TableName  NVARCHAR(128) = 'Fact.FactContainerMovementsAcc',
        @StepStart  DATETIME,
        @StepEnd    DATETIME,
        @Message    NVARCHAR(2000),
        @NullCount  INT;

    BEGIN TRY
        BEGIN TRAN;

        --------------------------------------------------------------------
        -- STEP 1: Clear target and reseed identity
        --------------------------------------------------------------------
        SET @StepStart = GETDATE();
        DELETE FROM Fact.FactContainerMovementsAcc;
        DBCC CHECKIDENT('Fact.FactContainerMovementsAcc', RESEED, 0);
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog
          (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
          (@TableName, 'Delete', @StepStart, @StepEnd, 'FirstLoad: Cleared Accumulating fact');

        --------------------------------------------------------------------
        -- STEP 2: Validate staging for required fields
        --------------------------------------------------------------------
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*)
        FROM StagingDB.PortOperations.CargoOperation AS co
        LEFT JOIN StagingDB.PortOperations.PortCall AS pc
          ON co.PortCallID = pc.PortCallID
        LEFT JOIN StagingDB.PortOperations.Container AS c
          ON co.ContainerID = c.ContainerID
        WHERE co.CargoOpID      IS NULL
           OR co.OperationType  IS NULL
           OR co.Quantity       IS NULL
           OR pc.PortID         IS NULL
           OR c.ContainerTypeID IS NULL;
        IF @NullCount > 0
            THROW 72000, 'Validation failed: NULLs in staging CargoOperation/related tables', 1;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog
          (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
          (@TableName, 'Validate', @StepStart, @StepEnd,
           CONCAT('FirstLoad: Found ', @NullCount, ' nulls in source data'));

        --------------------------------------------------------------------
        -- STEP 3: Insert aggregated metrics
        --------------------------------------------------------------------
        SET @StepStart = GETDATE();
        INSERT INTO Fact.FactContainerMovementsAcc
          (PortSK, ContainerTypeID, TotalLoads, TotalUnloads, TotalTEU)
        SELECT
          dp.PortSK,
          c.ContainerTypeID,
          SUM(CASE WHEN co.OperationType = 'LOAD'   THEN co.Quantity ELSE 0 END) AS TotalLoads,
          SUM(CASE WHEN co.OperationType = 'UNLOAD' THEN co.Quantity ELSE 0 END) AS TotalUnloads,
          SUM(CASE WHEN co.OperationType = 'LOAD'   THEN co.Quantity ELSE 0 END) AS TotalTEU
        FROM StagingDB.PortOperations.CargoOperation AS co
        JOIN StagingDB.PortOperations.PortCall     AS pc  ON co.PortCallID  = pc.PortCallID
        JOIN StagingDB.PortOperations.Container    AS c   ON co.ContainerID = c.ContainerID
        JOIN Dim.DimPort                           AS dp  ON pc.PortID      = dp.PortID
        GROUP BY
          dp.PortSK,
          c.ContainerTypeID;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog
          (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
          (@TableName, 'Insert', @StepStart, @StepEnd,
           CONCAT('FirstLoad: Inserted ', @@ROWCOUNT, ' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog
          (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
          (@TableName, 'Error', @StepStart, GETDATE(), CONCAT('FirstLoad: ', @Message));
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- 2.4) LoadFactEquipmentAssignmentInitialLoad
-- Full initial populate of Fact.FactEquipmentAssignment (Factless)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.LoadFactEquipmentAssignmentInitialLoad
AS
BEGIN
    SET NOCOUNT, XACT_ABORT ON;
    DECLARE
        @TableName NVARCHAR(128) = 'Fact.FactEquipmentAssignment',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Message   NVARCHAR(2000),
        @NullCount INT,
        @DupCount  INT;

    BEGIN TRY
        BEGIN TRAN;

        --------------------------------------------------------------------
        -- STEP 1: Clear target and reseed identity
        --------------------------------------------------------------------
        SET @StepStart = GETDATE();
        DELETE FROM Fact.FactEquipmentAssignment;
        DBCC CHECKIDENT('Fact.FactEquipmentAssignment', RESEED, 0);
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog
          (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
          (@TableName, 'Delete', @StepStart, @StepEnd, 'FirstLoad: Cleared factless fact');

        --------------------------------------------------------------------
        -- STEP 2: Validate staging for required fields & duplicates
        --------------------------------------------------------------------
        SET @StepStart = GETDATE();
        SELECT @NullCount = COUNT(*)
        FROM StagingDB.Common.OperationEquipmentAssignment AS o
        WHERE o.AssignmentID IS NULL
           OR o.CargoOpID    IS NULL
           OR o.EquipmentID  IS NULL
           OR o.EmployeeID   IS NULL
           OR o.StartTime    IS NULL;
        IF @NullCount > 0
            THROW 73000, 'Validation failed: NULLs in staging OperationEquipmentAssignment', 1;

        SELECT @DupCount = COUNT(*)
        FROM (
          SELECT AssignmentID, COUNT(*) AS Cnt
          FROM StagingDB.Common.OperationEquipmentAssignment
          GROUP BY AssignmentID
          HAVING COUNT(*) > 1
        ) AS d;
        IF @DupCount > 0
            THROW 73001, 'Validation failed: Duplicate AssignmentID in staging', 1;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog
          (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
          (@TableName, 'Validate', @StepStart, @StepEnd,
           CONCAT('FirstLoad: Nulls=', @NullCount, ',Dup=', @DupCount));

        --------------------------------------------------------------------
        -- STEP 3: Insert factless records with lookups
        --------------------------------------------------------------------
        SET @StepStart = GETDATE();
        INSERT INTO Fact.FactEquipmentAssignment
          (DateKey, EquipmentSK, EmployeeSK, PortSK, ContainerTypeID)
        SELECT
          ddate.DimDateID,
          deq.EquipmentSK,
          dem.EmployeeSK,
          dp.PortSK,
          dc.ContainerTypeID
        FROM StagingDB.Common.OperationEquipmentAssignment AS o
        JOIN StagingDB.PortOperations.CargoOperation AS co
          ON o.CargoOpID = co.CargoOpID
        JOIN StagingDB.PortOperations.PortCall     AS pc
          ON co.PortCallID = pc.PortCallID
        JOIN Dim.DimDate       AS ddate
          ON CAST(o.StartTime AS DATE) = ddate.FullDate
        JOIN Dim.DimEquipment   AS deq
          ON o.EquipmentID = deq.EquipmentID
        JOIN Dim.DimEmployee    AS dem
          ON o.EmployeeID  = dem.EmployeeID
        JOIN Dim.DimPort        AS dp
          ON pc.PortID = dp.PortID
        JOIN Dim.DimContainer   AS dc
          ON co.ContainerID = dc.ContainerID;
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog
          (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
          (@TableName, 'Insert', @StepStart, @StepEnd,
           CONCAT('FirstLoad: Inserted ', @@ROWCOUNT, ' rows'));

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO dbo.ETLLog
          (TableName, OperationType, StartTime, EndTime, Message)
        VALUES
          (@TableName, 'Error', @StepStart, GETDATE(), CONCAT('FirstLoad: ', @Message));
        THROW;
    END CATCH;
END;
GO
