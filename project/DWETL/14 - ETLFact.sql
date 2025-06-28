USE DataWarehouse;
GO

--------------------------------------------------------------------------------
-- 1) UpdateFactInvoiceLineTransactionIncremental
--    Maintains Fact.FactInvoiceLineTransaction (Transaction Fact)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.UpdateFactInvoiceLineTransactionIncremental
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE
        @TableName NVARCHAR(128) = 'Fact.FactInvoiceLineTransaction',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Message   NVARCHAR(2000),
        @Inserted  INT;

    BEGIN TRY
        BEGIN TRAN;

        SET @StepStart = GETDATE();

        -- Insert any invoice‐line rows in staging not yet in the fact
        INSERT INTO Fact.FactInvoiceLineTransaction
        (
            DimInvoiceID,
            DimCustomerID,
            DimServiceTypeID,
            DimTaxID,
            DimDateID,
            Quantity,
            UnitPrice,
            DiscountPercent,
            NetAmount,
            TaxAmount
        )
        SELECT
            di.DimInvoiceID,
            dc.DimCustomerID,
            dst.DimServiceTypeID,
            dtax.DimTaxID,
            ddate.DimDateID,
            src.Quantity,
            src.UnitPrice,
            ISNULL(src.DiscountPercent, 0),
            src.NetAmount,
            src.TaxAmount
        FROM StagingDB.Finance.InvoiceLine AS src
        INNER JOIN StagingDB.Finance.Invoice        AS inv  ON src.InvoiceID     = inv.InvoiceID
        INNER JOIN Dim.DimInvoice                   AS di   ON inv.InvoiceNumber = di.InvoiceNumber
        INNER JOIN StagingDB.Finance.Contract       AS ctr  ON inv.ContractID    = ctr.ContractID
        INNER JOIN StagingDB.Finance.Customer       AS stc  ON ctr.CustomerID    = stc.CustomerID
        INNER JOIN Dim.DimCustomer                  AS dc   ON stc.CustomerCode  = dc.CustomerCode
        INNER JOIN Dim.DimServiceType               AS dst  ON src.ServiceTypeID = dst.SourceServiceTypeID
        LEFT  JOIN Dim.DimTax                       AS dtax ON src.TaxID         = dtax.DimTaxID
        INNER JOIN Dim.DimDate                      AS ddate ON inv.InvoiceDate   = ddate.FullDate
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM Fact.FactInvoiceLineTransaction AS f
            WHERE f.DimInvoiceID     = di.DimInvoiceID
              AND f.DimCustomerID    = dc.DimCustomerID
              AND f.DimServiceTypeID = dst.DimServiceTypeID
              AND f.Quantity         = src.Quantity
              AND f.UnitPrice        = src.UnitPrice
              AND f.NetAmount        = src.NetAmount
        );

        SET @Inserted = @@ROWCOUNT;
        SET @StepEnd  = GETDATE();

        -- Log ETL activity
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'IncrementalInsert',
            @StepStart,
            @StepEnd,
            CONCAT('Inserted ', @Inserted, ' new invoice‐line rows')
        );

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();

        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Error',
            @StepStart,
            @StepEnd,
            CONCAT('Error: ', @Message)
        );
        THROW;
    END CATCH;
END;
GO


--------------------------------------------------------------------------------
-- 2) UpdateFactCustomerPaymentTransactionIncremental
--    Maintains Fact.FactCustomerPaymentTransaction (Transaction Fact)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.UpdateFactCustomerPaymentTransactionIncremental
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE
        @TableName NVARCHAR(128) = 'Fact.FactCustomerPaymentTransaction',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Message   NVARCHAR(2000),
        @Inserted  INT;

    BEGIN TRY
        BEGIN TRAN;

        SET @StepStart = GETDATE();

        -- Insert any payment rows in staging not yet in the fact
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
            dpm.DimPaymentMethodID,
            di.DimInvoiceID,
            dc.DimCustomerID,
            ddate.DimDateID,
            src.Amount,
            DATEDIFF(DAY, inv.InvoiceDate, src.PaymentDate),
            inv.TotalAmount - src.Amount,
            CASE WHEN src.Amount >= inv.TotalAmount THEN 1 ELSE 0 END,
            0
        FROM StagingDB.Finance.Payment     AS src
        INNER JOIN StagingDB.Finance.Invoice AS inv  ON src.InvoiceID = inv.InvoiceID
        INNER JOIN Dim.DimInvoice            AS di   ON inv.InvoiceNumber = di.InvoiceNumber
        INNER JOIN StagingDB.Finance.Contract AS ctr ON inv.ContractID    = ctr.ContractID
        INNER JOIN StagingDB.Finance.Customer AS stc  ON ctr.CustomerID    = stc.CustomerID
        INNER JOIN Dim.DimCustomer           AS dc   ON stc.CustomerCode  = dc.CustomerCode
        INNER JOIN Dim.DimPaymentMethod      AS dpm  ON src.PaymentMethod = dpm.PaymentMethodName
        INNER JOIN Dim.DimDate               AS ddate ON src.PaymentDate    = ddate.FullDate
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM Fact.FactCustomerPaymentTransaction AS f
            WHERE f.DimInvoiceID   = di.DimInvoiceID
              AND f.DimCustomerID  = dc.DimCustomerID
              AND f.PaymentAmount  = src.Amount
              AND f.DimDateID      = ddate.DimDateID
        );

        SET @Inserted = @@ROWCOUNT;
        SET @StepEnd  = GETDATE();

        -- Log ETL activity
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'IncrementalInsert',
            @StepStart,
            @StepEnd,
            CONCAT('Inserted ', @Inserted, ' new payment rows')
        );

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();

        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Error',
            @StepStart,
            @StepEnd,
            CONCAT('Error: ', @Message)
        );
        THROW;
    END CATCH;
END;
GO


--------------------------------------------------------------------------------
-- 3) UpdateFactCustomerBillingMonthlySnapshotIncremental
--    Maintains Fact.FactCustomerBillingMonthlySnapshot (Periodic Snapshot)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.UpdateFactCustomerBillingMonthlySnapshotIncremental
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE
        @TableName NVARCHAR(128) = 'Fact.FactCustomerBillingMonthlySnapshot',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Message   NVARCHAR(2000),
        @Inserted  INT;

    BEGIN TRY
        BEGIN TRAN;

        SET @StepStart = GETDATE();

        WITH InvoiceAgg AS
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
            INNER JOIN StagingDB.Finance.Contract    AS ctr ON inv.ContractID = ctr.ContractID
            GROUP BY
                ctr.CustomerID,
                DATEFROMPARTS(YEAR(inv.InvoiceDate), MONTH(inv.InvoiceDate), 1)
        ),
        PaymentAgg AS
        (
            SELECT
                ctr.CustomerID,
                DATEFROMPARTS(YEAR(inv.InvoiceDate), MONTH(inv.InvoiceDate), 1) AS SnapshotDate,
                SUM(pay.Amount) AS TotalPaid,
                AVG(DATEDIFF(DAY, inv.InvoiceDate, pay.PaymentDate)) AS AveragePaymentDelay
            FROM StagingDB.Finance.Payment AS pay
            INNER JOIN StagingDB.Finance.Invoice AS inv ON pay.InvoiceID = inv.InvoiceID
            INNER JOIN StagingDB.Finance.Contract AS ctr ON inv.ContractID = ctr.ContractID
            GROUP BY
                ctr.CustomerID,
                DATEFROMPARTS(YEAR(inv.InvoiceDate), MONTH(inv.InvoiceDate), 1)
        ),
        OutstandingAgg AS
        (
            SELECT
                ia.CustomerID,
                ia.SnapshotDate,
                MAX(ia.TotalNetAmount + ia.TotalTaxAmount - ISNULL(pa.TotalPaid,0)) AS MaxOutstandingAmount
            FROM InvoiceAgg AS ia
            LEFT JOIN PaymentAgg AS pa
              ON ia.CustomerID   = pa.CustomerID
             AND ia.SnapshotDate = pa.SnapshotDate
            GROUP BY
                ia.CustomerID,
                ia.SnapshotDate
        )
        -- Insert only new month‐snapshots
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
            dc.DimCustomerID,
            dd.DimDateID,
            bc.DimBillingCycleID,
            ia.TotalInvoiceCount,
            ia.TotalNetAmount,
            ia.TotalTaxAmount,
            ia.TotalDiscount,
            ISNULL(pa.TotalPaid,0),
            ISNULL(pa.AveragePaymentDelay,0),
            oa.MaxOutstandingAmount
        FROM InvoiceAgg AS ia
        INNER JOIN StagingDB.Finance.Contract       AS ctr ON ia.CustomerID = ctr.CustomerID
        INNER JOIN StagingDB.Finance.Customer       AS stc ON ctr.CustomerID = stc.CustomerID
        INNER JOIN Dim.DimCustomer                  AS dc  ON stc.CustomerCode = dc.CustomerCode
        INNER JOIN Dim.DimDate                      AS dd  ON ia.SnapshotDate = dd.FullDate
        INNER JOIN Dim.DimBillingCycle              AS bc  ON ctr.BillingCycleID = bc.DimBillingCycleID
        LEFT JOIN PaymentAgg                        AS pa  ON ia.CustomerID = pa.CustomerID
                                                      AND ia.SnapshotDate = pa.SnapshotDate
        LEFT JOIN OutstandingAgg                    AS oa  ON ia.CustomerID = oa.CustomerID
                                                      AND ia.SnapshotDate = oa.SnapshotDate
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM Fact.FactCustomerBillingMonthlySnapshot AS f
            WHERE f.DimCustomerID = dc.DimCustomerID
              AND f.DimDateID     = dd.DimDateID
        );

        SET @Inserted = @@ROWCOUNT;
        SET @StepEnd  = GETDATE();

        -- Log ETL activity
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'IncrementalInsert',
            @StepStart,
            @StepEnd,
            CONCAT('Inserted ', @Inserted, ' new monthly snapshots')
        );

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();

        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Error',
            @StepStart,
            @StepEnd,
            CONCAT('Error: ', @Message)
        );
        THROW;
    END CATCH;
END;
GO


--------------------------------------------------------------------------------
-- 4) UpdateFactInvoiceLifecycleAccumulatingIncremental
--    Maintains Fact.FactInvoiceLifecycleAccumulating (Accumulating Snapshot)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.UpdateFactInvoiceLifecycleAccumulatingIncremental
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE
        @TableName NVARCHAR(128) = 'Fact.FactInvoiceLifecycleAccumulating',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Message   NVARCHAR(2000);

    BEGIN TRY
        BEGIN TRAN;

        SET @StepStart = GETDATE();

        -- Aggregate payments per invoice into one row
        WITH PayAgg AS
        (
            SELECT
                p.InvoiceID,
                SUM(p.Amount)      AS PaidAmount,
                COUNT(*)           AS PaymentCount,
                MIN(p.PaymentDate) AS FirstPayDate,
                MAX(p.PaymentDate) AS LastPayDate
            FROM StagingDB.Finance.Payment AS p
            GROUP BY p.InvoiceID
        ),
        -- Build single‐row source per invoice, deduplicated via ROW_NUMBER
        SrcRaw AS
        (
            SELECT
                inv.InvoiceID,
                di.DimInvoiceID,
                dc.DimCustomerID,
                dctr.DimContractID,
                ddInv.DimDateID   AS InvoiceDateID,
                ddDue.DimDateID   AS DueDateID,
                ddFirst.DimDateID AS FirstPaymentDateID,
                ddLast.DimDateID  AS FinalPaymentDateID,
                inv.TotalAmount,
                ISNULL(pa.PaidAmount,0)   AS PaidAmount,
                ISNULL(pa.PaymentCount,0) AS PaymentCount,
                ROW_NUMBER() OVER(PARTITION BY di.DimInvoiceID ORDER BY inv.InvoiceID) AS rn
            FROM StagingDB.Finance.Invoice       AS inv
            INNER JOIN Dim.DimInvoice            AS di   ON inv.InvoiceNumber = di.InvoiceNumber
            INNER JOIN StagingDB.Finance.Contract AS ctr ON inv.ContractID      = ctr.ContractID
            INNER JOIN StagingDB.Finance.Customer AS stc ON ctr.CustomerID      = stc.CustomerID
            INNER JOIN Dim.DimCustomer           AS dc   ON stc.CustomerCode    = dc.CustomerCode
            INNER JOIN Dim.DimContract           AS dctr ON ctr.ContractNumber  = dctr.ContractNumber
            INNER JOIN Dim.DimDate               AS ddInv ON inv.InvoiceDate    = ddInv.FullDate
            INNER JOIN Dim.DimDate               AS ddDue ON inv.DueDate         = ddDue.FullDate
            LEFT JOIN PayAgg                     AS pa   ON inv.InvoiceID       = pa.InvoiceID
            LEFT JOIN Dim.DimDate                AS ddFirst ON pa.FirstPayDate  = ddFirst.FullDate
            LEFT JOIN Dim.DimDate                AS ddLast  ON pa.LastPayDate   = ddLast.FullDate
        ),
        Src AS
        (
            SELECT * FROM SrcRaw WHERE rn = 1
        )

        -- Merge into accumulating snapshot
        MERGE INTO Fact.FactInvoiceLifecycleAccumulating AS tgt
        USING Src AS src
          ON tgt.DimInvoiceID = src.DimInvoiceID

        WHEN MATCHED THEN
          UPDATE SET
            PaidAmount        = src.PaidAmount,
            OutstandingAmount = src.TotalAmount - src.PaidAmount,
            PaymentCount      = src.PaymentCount,
            FirstPaymentDelay = DATEDIFF(DAY, src.InvoiceDateID, src.FirstPaymentDateID),
            FinalPaymentDelay = DATEDIFF(DAY, src.InvoiceDateID, src.FinalPaymentDateID),
            DaysToClose       = DATEDIFF(DAY, src.InvoiceDateID, src.FinalPaymentDateID)

        WHEN NOT MATCHED BY TARGET THEN
          INSERT
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
          VALUES
          (
            src.DimInvoiceID,
            src.DimCustomerID,
            src.DimContractID,
            src.InvoiceDateID,
            src.DueDateID,
            src.FirstPaymentDateID,
            src.FinalPaymentDateID,
            src.TotalAmount,
            src.PaidAmount,
            src.TotalAmount - src.PaidAmount,
            src.PaymentCount,
            DATEDIFF(DAY, src.InvoiceDateID, src.FirstPaymentDateID),
            DATEDIFF(DAY, src.InvoiceDateID, src.FinalPaymentDateID),
            DATEDIFF(DAY, src.InvoiceDateID, src.FinalPaymentDateID)
          );

        -- Log via MERGE’s @@ROWCOUNT not reliable; use ETLLog step instead
        SET @StepEnd = GETDATE();
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'IncrementalMerge',
            @StepStart,
            @StepEnd,
            'Completed incremental MERGE for invoice‐lifecycle facts'
        );

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();

        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Error',
            @StepStart,
            @StepEnd,
            CONCAT('Error: ', @Message)
        );
        THROW;
    END CATCH;
END;
GO


--------------------------------------------------------------------------------
-- 5) UpdateFactCustomerContractActivationIncremental
--    Maintains Fact.FactCustomerContractActivationFactless (Factless)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.UpdateFactCustomerContractActivationIncremental
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE
        @TableName NVARCHAR(128) = 'Fact.FactCustomerContractActivationFactless',
        @StepStart DATETIME,
        @StepEnd   DATETIME,
        @Message   NVARCHAR(2000),
        @Inserted  INT;

    BEGIN TRY
        BEGIN TRAN;

        SET @StepStart = GETDATE();

        -- Insert any new activation events not yet in factless table
        INSERT INTO Fact.FactCustomerContractActivationFactless
        (
            DimDateID,
            DimCustomerID,
            DimContractID
        )
        SELECT
            dd.DimDateID,
            dc.DimCustomerID,
            dctr.DimContractID
        FROM StagingDB.Finance.Contract       AS srcCtr
        INNER JOIN Dim.DimDate                AS dd   ON srcCtr.StartDate       = dd.FullDate
        INNER JOIN StagingDB.Finance.Customer AS stc  ON srcCtr.CustomerID      = stc.CustomerID
        INNER JOIN Dim.DimCustomer            AS dc   ON stc.CustomerCode      = dc.CustomerCode
        INNER JOIN Dim.DimContract            AS dctr ON srcCtr.ContractNumber = dctr.ContractNumber
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM Fact.FactCustomerContractActivationFactless AS f
            WHERE f.DimDateID     = dd.DimDateID
              AND f.DimCustomerID = dc.DimCustomerID
              AND f.DimContractID = dctr.DimContractID
        );

        SET @Inserted = @@ROWCOUNT;
        SET @StepEnd  = GETDATE();

        -- Log ETL activity
        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'IncrementalInsert',
            @StepStart,
            @StepEnd,
            CONCAT('Inserted ', @Inserted, ' new activation facts')
        );

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        SET @StepEnd = GETDATE();
        SET @Message = ERROR_MESSAGE();

        INSERT INTO dbo.ETLLog(TableName, OperationType, StartTime, EndTime, Message)
        VALUES
        (
            @TableName,
            'Error',
            @StepStart,
            @StepEnd,
            CONCAT('Error: ', @Message)
        );
        THROW;
    END CATCH;
END;
GO



-- USE DataWarehouse;
-- GO

-- --------------------------------------------------------------------------------
-- -- Drop all five incremental‐update procedures in the Fact schema, if they exist
-- --------------------------------------------------------------------------------

--IF OBJECT_ID('Fact.UpdateFactInvoiceLineTransactionIncremental', 'P') IS NOT NULL
--   DROP PROCEDURE Fact.UpdateFactInvoiceLineTransactionIncremental;
--GO

--IF OBJECT_ID('Fact.UpdateFactCustomerPaymentTransactionIncremental', 'P') IS NOT NULL
--   DROP PROCEDURE Fact.UpdateFactCustomerPaymentTransactionIncremental;
--GO

--IF OBJECT_ID('Fact.UpdateFactCustomerBillingMonthlySnapshotIncremental', 'P') IS NOT NULL
--   DROP PROCEDURE Fact.UpdateFactCustomerBillingMonthlySnapshotIncremental;
--GO

--IF OBJECT_ID('Fact.UpdateFactInvoiceLifecycleAccumulatingIncremental', 'P') IS NOT NULL
--   DROP PROCEDURE Fact.UpdateFactInvoiceLifecycleAccumulatingIncremental;
--GO

--IF OBJECT_ID('Fact.UpdateFactCustomerContractActivationIncremental', 'P') IS NOT NULL
--   DROP PROCEDURE Fact.UpdateFactCustomerContractActivationIncremental;
--GO


USE DataWarehouse;
GO



--------------------------------------------------------------------------------
-- 1) UpdateFactTermination (High-Performance Incremental Load)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.UpdateFactTermination
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessName NVARCHAR(128) = 'Fact.UpdateFactTermination';
    DECLARE @TargetTable NVARCHAR(128) = 'Fact.FactTermination';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);
    DECLARE @LastLoadDate DATE = (SELECT LastLoadDate FROM Audit.ETL_Control WHERE ProcessName = 'FactTables');
    DECLARE @EndDate DATE = CONVERT(DATE, GETDATE());

    BEGIN TRY
        -- Step 1: Select only the new records into a temp table for fast processing.
        SELECT *
        INTO #NewTerminations
        FROM StagingDB.HumanResources.Termination
        WHERE TerminationDate > @LastLoadDate AND TerminationDate < @EndDate;

        -- Step 2: Insert into the fact table by joining the small temp table with dimensions.
        INSERT INTO Fact.FactTermination (
            TerminationDateKey, EmployeeKey, DepartmentKey, JobTitleKey,
            TerminationReasonKey, TenureInDays, TenureInMonths, SalaryAtTermination, IsVoluntary
        )
        SELECT
            dd.DateKey,
            de.EmployeeKey,
            ISNULL(d.DepartmentKey, -1),
            ISNULL(jt.JobTitleKey, -1),
            ISNULL(dtr.TerminationReasonKey, -1),
            ISNULL(DATEDIFF(DAY, efh.FirstHireDate, t.TerminationDate), 0),
            ISNULL(DATEDIFF(MONTH, efh.FirstHireDate, t.TerminationDate), 0),
            ISNULL(last_eh.Salary, 0),
            CASE WHEN t.TerminationReason IN ('Resignation', 'Retirement') THEN 1 ELSE 0 END
        FROM #NewTerminations t
        INNER JOIN Dim.DimDate dd ON t.TerminationDate = dd.FullDate
        INNER JOIN Dim.DimEmployee de ON t.EmployeeID = de.EmployeeID AND t.TerminationDate BETWEEN de.StartDate AND ISNULL(de.EndDate, '9999-12-31')
        OUTER APPLY (
            SELECT TOP 1 eh.DepartmentID, eh.JobTitleID, eh.Salary
            FROM StagingDB.HumanResources.EmploymentHistory eh
            WHERE eh.EmployeeID = t.EmployeeID AND eh.StartDate <= t.TerminationDate
            ORDER BY eh.StartDate DESC, eh.EmploymentHistoryID DESC
        ) AS last_eh
        LEFT JOIN (SELECT EmployeeID, MIN(StartDate) as FirstHireDate FROM StagingDB.HumanResources.EmploymentHistory GROUP BY EmployeeID) efh ON t.EmployeeID = efh.EmployeeID
        LEFT JOIN Dim.DimDepartment d ON last_eh.DepartmentID = d.DepartmentID
        LEFT JOIN Dim.DimJobTitle jt ON last_eh.JobTitleID = jt.JobTitleID
        LEFT JOIN Dim.DimTerminationReason dtr ON t.TerminationReason = dtr.TerminationReason;

        SET @Message = 'FactTermination incremental load completed successfully.';
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, RecordsInserted, Status, [Message])
        VALUES (@ProcessName, 'Incremental Load', @TargetTable, @StartTime, GETDATE(), @@ROWCOUNT, 'Success', @Message);

        DROP TABLE #NewTerminations;
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#NewTerminations') IS NOT NULL DROP TABLE #NewTerminations;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, Status, [Message])
        VALUES (@ProcessName, 'Incremental Load', @TargetTable, @StartTime, GETDATE(), 'Failed', @Message);
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- 2) UpdateFactSalaryPayment (High-Performance Incremental Load)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.UpdateFactSalaryPayment
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessName NVARCHAR(128) = 'Fact.UpdateFactSalaryPayment';
    DECLARE @TargetTable NVARCHAR(128) = 'Fact.FactSalaryPayment';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);
    DECLARE @EmployerContributionRate DECIMAL(5, 2) = 0.23;
    DECLARE @LastLoadDate DATE = (SELECT LastLoadDate FROM Audit.ETL_Control WHERE ProcessName = 'FactTables');
    DECLARE @EndDate DATE = CONVERT(DATE, GETDATE());

    BEGIN TRY
        SELECT * INTO #NewPayments FROM StagingDB.HumanResources.SalaryPayment
        WHERE PaymentDate > @LastLoadDate AND PaymentDate < @EndDate;

        INSERT INTO Fact.FactSalaryPayment (
            PaymentDateKey, EmployeeKey, DepartmentKey, JobTitleKey,
            GrossPayAmount, BaseAmount, BonusAmount, DeductionsAmount, NetAmount, SalaryCostToCompany
        )
        SELECT
            dd.DateKey, de.EmployeeKey, ISNULL(d.DepartmentKey, -1),
            ISNULL(jt.JobTitleKey, -1), sp.Amount + ISNULL(sp.Bonus, 0), sp.Amount,
            ISNULL(sp.Bonus, 0), ISNULL(sp.Deductions, 0), sp.NetAmount,
            (sp.Amount + ISNULL(sp.Bonus, 0)) * (1 + @EmployerContributionRate)
        FROM #NewPayments sp
        INNER JOIN Dim.DimDate dd ON sp.PaymentDate = dd.FullDate
        INNER JOIN Dim.DimEmployee de ON sp.EmployeeID = de.EmployeeID AND sp.PaymentDate BETWEEN de.StartDate AND ISNULL(de.EndDate, '9999-12-31')
        OUTER APPLY (
            SELECT TOP 1 eh.DepartmentID, eh.JobTitleID
            FROM StagingDB.HumanResources.EmploymentHistory eh
            WHERE eh.EmployeeID = sp.EmployeeID AND eh.StartDate <= sp.PaymentDate
            ORDER BY eh.StartDate DESC, eh.EmploymentHistoryID DESC
        ) AS last_eh
        LEFT JOIN Dim.DimDepartment d ON last_eh.DepartmentID = d.DepartmentID
        LEFT JOIN Dim.DimJobTitle jt ON last_eh.JobTitleID = jt.JobTitleID;

        SET @Message = 'FactSalaryPayment incremental load completed successfully.';
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, RecordsInserted, Status, [Message])
        VALUES (@ProcessName, 'Incremental Load', @TargetTable, @StartTime, GETDATE(), @@ROWCOUNT, 'Success', @Message);

        DROP TABLE #NewPayments;
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#NewPayments') IS NOT NULL DROP TABLE #NewPayments;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, Status, [Message])
        VALUES (@ProcessName, 'Incremental Load', @TargetTable, @StartTime, GETDATE(), 'Failed', @Message);
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- 3) UpdateFactEmployeeAttendance (High-Performance Incremental Load)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.UpdateFactEmployeeAttendance
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessName NVARCHAR(128) = 'Fact.UpdateFactEmployeeAttendance';
    DECLARE @TargetTable NVARCHAR(128) = 'Fact.FactEmployeeAttendance';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);
    DECLARE @LastLoadDate DATE = (SELECT LastLoadDate FROM Audit.ETL_Control WHERE ProcessName = 'FactTables');
    DECLARE @EndDate DATE = CONVERT(DATE, GETDATE());

    BEGIN TRY
        SELECT * INTO #NewAttendance FROM StagingDB.HumanResources.Attendance
        WHERE AttendanceDate > @LastLoadDate AND AttendanceDate < @EndDate;
        
        INSERT INTO Fact.FactEmployeeAttendance (AttendanceDateKey, EmployeeKey, AttendanceStatus)
        SELECT dd.DateKey, ISNULL(last_known_employee.EmployeeKey, -1), a.Status
        FROM #NewAttendance a
        INNER JOIN Dim.DimDate dd ON a.AttendanceDate = dd.FullDate
        OUTER APPLY (
            SELECT TOP 1 de.EmployeeKey
            FROM Dim.DimEmployee de
            WHERE de.EmployeeID = a.EmployeeID AND de.StartDate <= a.AttendanceDate
            ORDER BY de.StartDate DESC, de.EmployeeKey DESC
        ) AS last_known_employee;

        SET @Message = 'FactEmployeeAttendance incremental load completed successfully.';
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, RecordsInserted, Status, [Message])
        VALUES (@ProcessName, 'Incremental Load', @TargetTable, @StartTime, GETDATE(), @@ROWCOUNT, 'Success', @Message);

        DROP TABLE #NewAttendance;
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#NewAttendance') IS NOT NULL DROP TABLE #NewAttendance;
        SET @Message = ERROR_MESSAGE();
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, Status, [Message])
        VALUES (@ProcessName, 'Incremental Load', @TargetTable, @StartTime, GETDATE(), 'Failed', @Message);
        THROW;
    END CATCH;
END;
GO

--------------------------------------------------------------------------------
-- 4) UpdateFactMonthlyEmployeePerformance (Resilient Incremental Load)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.UpdateFactMonthlyEmployeePerformance
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessName NVARCHAR(128) = 'Fact.UpdateFactMonthlyEmployeePerformance';
    DECLARE @TargetTable NVARCHAR(128) = 'Fact.FactMonthlyEmployeePerformance';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);
    DECLARE @FirstDayOfCurrentMonth DATE = DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1);

    BEGIN TRY
        DELETE FROM Fact.FactMonthlyEmployeePerformance
        WHERE MonthDateKey >= (SELECT MIN(DateKey) FROM Dim.DimDate WHERE FullDate >= @FirstDayOfCurrentMonth);

        ;WITH MonthlyMetrics AS (
            SELECT EOMONTH(a.AttendanceDate) AS MonthEndDate, a.EmployeeID,
                SUM(ISNULL(a.HoursWorked, 0)) AS TotalHours, AVG(ISNULL(a.HoursWorked, 0)) AS AvgHours,
                MIN(a.HoursWorked) AS MinHours, MAX(a.HoursWorked) AS MaxHours,
                SUM(CASE WHEN a.Status = 'Late' THEN 1 ELSE 0 END) AS LateDays,
                SUM(CASE WHEN a.Status = 'Absent' THEN 1 ELSE 0 END) AS AbsentDays,
                COUNT(a.AttendanceID) AS WorkDays
            FROM StagingDB.HumanResources.Attendance a
            WHERE a.AttendanceDate >= @FirstDayOfCurrentMonth
            GROUP BY EOMONTH(a.AttendanceDate), a.EmployeeID
        )
        INSERT INTO Fact.FactMonthlyEmployeePerformance (
            MonthDateKey, EmployeeKey, DepartmentKey, TotalHoursWorked, 
            AverageDailyHoursWorked, MinDailyHoursWorked, MaxDailyHoursWorked,
            LateDaysCount, AbsentDaysCount, WorkDaysCount, OvertimeAsPercentage
        )
        SELECT
            dd.DateKey, ISNULL(de.EmployeeKey, -1), ISNULL(d.DepartmentKey, -1),
            m.TotalHours, m.AvgHours, m.MinHours, m.MaxHours, m.LateDays, m.AbsentDays, m.WorkDays,
            CASE WHEN m.TotalHours > 0 THEN (m.TotalHours - (m.WorkDays * 8.0)) / m.TotalHours ELSE 0 END
        FROM MonthlyMetrics m
        INNER JOIN Dim.DimDate dd ON m.MonthEndDate = dd.FullDate
        OUTER APPLY (
            SELECT TOP 1 de.EmployeeKey FROM Dim.DimEmployee de
            WHERE de.EmployeeID = m.EmployeeID AND de.StartDate <= m.MonthEndDate ORDER BY de.StartDate DESC
        ) AS de
        OUTER APPLY (
            SELECT TOP 1 eh.DepartmentID FROM StagingDB.HumanResources.EmploymentHistory eh
            WHERE eh.EmployeeID = m.EmployeeID AND eh.StartDate <= m.MonthEndDate ORDER BY eh.StartDate DESC
        ) AS last_eh
        LEFT JOIN Dim.DimDepartment d ON last_eh.DepartmentID = d.DepartmentID;

        SET @Message = 'FactMonthlyEmployeePerformance update completed successfully.';
        INSERT INTO Audit.DW_ETL_Log (ProcessName, OperationType, TargetTable, StartTime, EndTime, RecordsUpdated, Status, [Message])
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
-- 5) UpdateFactEmployeeLifecycle (Resilient Incremental Load)
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Fact.UpdateFactEmployeeLifecycle
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProcessName NVARCHAR(128) = 'Fact.UpdateFactEmployeeLifecycle';
    DECLARE @TargetTable NVARCHAR(128) = 'Fact.FactEmployeeLifecycle';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Message NVARCHAR(MAX);
    DECLARE @RecordsInserted INT = 0;
    DECLARE @RecordsUpdated INT = 0;

    BEGIN TRY
        INSERT INTO Fact.FactEmployeeLifecycle (EmployeeKey, HireDateKey, TotalPromotionsCount, TotalTrainingsCompleted)
        SELECT de.EmployeeKey, ISNULL(dd.DateKey, -1), 0, 0
        FROM Dim.DimEmployee de
        INNER JOIN Dim.DimDate dd ON de.StartDate = dd.FullDate
        WHERE de.IsCurrent = 1 AND NOT EXISTS (
            SELECT 1 FROM Fact.FactEmployeeLifecycle fel WHERE fel.EmployeeKey = de.EmployeeKey
        );
        SET @RecordsInserted = @@ROWCOUNT;

        UPDATE fel
        SET 
            fel.TerminationDateKey = ISNULL(dd.DateKey, -1),
            fel.TerminationReasonKey = ISNULL(dtr.TerminationReasonKey, -1),
            fel.DaysToTermination = DATEDIFF(DAY, hire_date.FullDate, t.TerminationDate),
            fel.FinalSalary = last_eh.Salary
        FROM Fact.FactEmployeeLifecycle fel
        JOIN Dim.DimEmployee de ON fel.EmployeeKey = de.EmployeeKey
        JOIN Dim.DimDate hire_date ON fel.HireDateKey = hire_date.DateKey
        JOIN StagingDB.HumanResources.Termination t ON de.EmployeeID = t.EmployeeID
        LEFT JOIN Dim.DimDate dd ON t.TerminationDate = dd.FullDate
        LEFT JOIN Dim.DimTerminationReason dtr ON t.TerminationReason = dtr.TerminationReason
        OUTER APPLY (
            SELECT TOP 1 eh.Salary FROM StagingDB.HumanResources.EmploymentHistory eh
            WHERE eh.EmployeeID = t.EmployeeID AND eh.StartDate <= t.TerminationDate ORDER BY eh.StartDate DESC
        ) AS last_eh
        WHERE fel.TerminationDateKey IS NULL;
        SET @RecordsUpdated = @RecordsUpdated + @@ROWCOUNT;

        ;WITH LatestTraining AS (
            SELECT EmployeeID, COUNT(*) AS TrainingCount, MAX(TrainingDate) AS LastDate
            FROM StagingDB.HumanResources.EmployeeTraining GROUP BY EmployeeID
        )
        UPDATE fel
        SET
            fel.LastTrainingDateKey = ISNULL(dd.DateKey, -1),
            fel.TotalTrainingsCompleted = lt.TrainingCount
        FROM Fact.FactEmployeeLifecycle fel
        JOIN Dim.DimEmployee de ON fel.EmployeeKey = de.EmployeeKey
        JOIN LatestTraining lt ON de.EmployeeID = lt.EmployeeID
        LEFT JOIN Dim.DimDate dd ON lt.LastDate = dd.FullDate
        WHERE ISNULL(fel.LastTrainingDateKey, -1) <> ISNULL(dd.DateKey, -1) OR ISNULL(fel.TotalTrainingsCompleted, 0) <> lt.TrainingCount;
        SET @RecordsUpdated = @RecordsUpdated + @@ROWCOUNT;
        
        SET @Message = 'FactEmployeeLifecycle update completed successfully.';
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


USE DataWarehouse;
GO

/*------------------------------------------------------------------------------
  1) UpdateFactCargoOperationIncremental
------------------------------------------------------------------------------*/
CREATE OR ALTER PROCEDURE Fact.UpdateFactCargoOperationIncremental
AS
BEGIN
    SET NOCOUNT, XACT_ABORT ON;
    DECLARE
      @StepStart DATETIME,
      @StepEnd   DATETIME;

    BEGIN TRY
      BEGIN TRAN;
      SET @StepStart = GETDATE();

      INSERT INTO Fact.FactCargoOperationTransactional
      (
        DateKey, FullDate, ShipSK, PortSK, ContainerSK,
        EquipmentSK, EmployeeSK, OperationType,
        Quantity, WeightKG, OperationDateTime
      )
      SELECT
        dd.DimDateID,
        CAST(co.OperationDateTime AS DATE),
        ds.ShipSK,
        dp.PortSK,
        dc.ContainerSK,
        deq.EquipmentSK,
        COALESCE(dem.EmployeeSK,0),
        co.OperationType,
        co.Quantity,
        co.WeightKG,
        co.OperationDateTime
      FROM StagingDB.PortOperations.CargoOperation AS co
      JOIN StagingDB.PortOperations.PortCall       AS pc  ON co.PortCallID = pc.PortCallID
      JOIN StagingDB.PortOperations.Voyage         AS v   ON pc.VoyageID   = v.VoyageID
      JOIN Dim.DimDate                             AS dd  ON CAST(co.OperationDateTime AS DATE) = dd.FullDate
      JOIN Dim.DimShip                             AS ds  ON v.ShipID = ds.ShipID AND ds.IsCurrent=1
      JOIN Dim.DimPort                             AS dp  ON pc.PortID = dp.PortID
      JOIN Dim.DimContainer                        AS dc  ON co.ContainerID = dc.ContainerID
      INNER JOIN StagingDB.Common.OperationEquipmentAssignment AS oea
        ON co.CargoOpID = oea.CargoOpID
      INNER JOIN Dim.DimEquipment                   AS deq
        ON oea.EquipmentID = deq.EquipmentID
      LEFT JOIN Dim.DimEmployee                    AS dem
        ON oea.EmployeeID  = dem.EmployeeID AND dem.IsCurrent=1
      WHERE NOT EXISTS
      (
        SELECT 1
        FROM Fact.FactCargoOperationTransactional AS f
        WHERE f.OperationDateTime = co.OperationDateTime
          AND f.ShipSK      = ds.ShipSK
          AND f.PortSK      = dp.PortSK
          AND f.ContainerSK = dc.ContainerSK
      );

      SET @StepEnd = GETDATE();
      INSERT INTO dbo.ETLLog
      (TableName, OperationType, StartTime, EndTime, Message)
      VALUES
      (
        'Fact.FactCargoOperationTransactional',
        'IncrementalInsert',
        @StepStart,
        @StepEnd,
        CONCAT('Inserted ', @@ROWCOUNT, ' new cargo‐operations')
      );

      COMMIT;
    END TRY
    BEGIN CATCH
      IF XACT_STATE()<>0 ROLLBACK;
      SET @StepEnd = GETDATE();
      INSERT INTO dbo.ETLLog
      (TableName, OperationType, StartTime, EndTime, Message)
      VALUES
      (
        'Fact.FactCargoOperationTransactional',
        'Error',
        @StepStart,
        @StepEnd,
        ERROR_MESSAGE()
      );
      THROW;
    END CATCH;
END;
GO


/*------------------------------------------------------------------------------
  2) UpdateFactEquipmentAssignmentIncremental
------------------------------------------------------------------------------*/

CREATE OR ALTER PROCEDURE Fact.UpdateFactEquipmentAssignmentIncremental
AS
BEGIN
    SET NOCOUNT, XACT_ABORT ON;

    DECLARE
      @TableName NVARCHAR(128) = 'Fact.FactEquipmentAssignment',
      @StepStart DATETIME,
      @StepEnd   DATETIME;

    BEGIN TRY
      BEGIN TRAN;
      SET @StepStart = GETDATE();

      INSERT INTO Fact.FactEquipmentAssignment
      (
        DateKey,
        EquipmentSK,
        EmployeeSK,
        PortSK,
        ContainerTypeID
      )
      SELECT
        dd.DimDateID,
        deq.EquipmentSK,
        dem.EmployeeSK,
        dp.PortSK,
        c.ContainerTypeID
      FROM StagingDB.Common.OperationEquipmentAssignment AS oea
      JOIN Dim.DimDate                             AS dd  
        ON CAST(oea.StartTime AS DATE) = dd.FullDate
      INNER JOIN Dim.DimEquipment                  AS deq 
        ON oea.EquipmentID = deq.EquipmentID
      LEFT JOIN Dim.DimEmployee                    AS dem 
        ON oea.EmployeeID  = dem.EmployeeID 
       AND dem.IsCurrent = 1
      JOIN StagingDB.PortOperations.CargoOperation AS co 
        ON oea.CargoOpID = co.CargoOpID
      JOIN StagingDB.PortOperations.PortCall       AS pc  
        ON co.PortCallID = pc.PortCallID
      JOIN Dim.DimPort                             AS dp  
        ON pc.PortID = dp.PortID
      JOIN StagingDB.PortOperations.Container      AS c   
        ON co.ContainerID = c.ContainerID
      WHERE NOT EXISTS
      (
        SELECT 1
        FROM Fact.FactEquipmentAssignment AS f
        WHERE f.DateKey         = dd.DimDateID
          AND f.EquipmentSK     = deq.EquipmentSK
          AND f.EmployeeSK      = dem.EmployeeSK
          AND f.PortSK          = dp.PortSK
          AND f.ContainerTypeID = c.ContainerTypeID
      );

      SET @StepEnd = GETDATE();
      INSERT INTO dbo.ETLLog
      (TableName, OperationType, StartTime, EndTime, Message)
      VALUES
      (
        @TableName,
        'IncrementalInsert',
        @StepStart,
        @StepEnd,
        CONCAT('Inserted ', @@ROWCOUNT, ' new equipment‐assignments')
      );

      COMMIT;
    END TRY
    BEGIN CATCH
      IF XACT_STATE()<>0 ROLLBACK;
      SET @StepEnd = GETDATE();
      INSERT INTO dbo.ETLLog
      (TableName, OperationType, StartTime, EndTime, Message)
      VALUES
      (
        @TableName,
        'Error',
        @StepStart,
        @StepEnd,
        ERROR_MESSAGE()
      );
      THROW;
    END CATCH;
END;
GO



/*------------------------------------------------------------------------------
  3) UpdateFactContainerMovementsAcc (Aggregate full‐reload)
------------------------------------------------------------------------------*/
CREATE OR ALTER PROCEDURE Fact.UpdateFactContainerMovementsAcc
AS
BEGIN
    SET NOCOUNT, XACT_ABORT ON;
    DECLARE
      @TableName NVARCHAR(128) = 'Fact.FactContainerMovementsAcc',
      @StepStart DATETIME,
      @StepEnd   DATETIME;

    BEGIN TRY
      BEGIN TRAN;
      SET @StepStart = GETDATE();

      TRUNCATE TABLE Fact.FactContainerMovementsAcc;

      INSERT INTO Fact.FactContainerMovementsAcc
      (PortSK, ContainerTypeID, TotalLoads, TotalUnloads, TotalTEU)
      SELECT
        dp.PortSK,
        cty.ContainerTypeID,
        SUM(CASE WHEN co.OperationType='LOAD'   THEN co.Quantity ELSE 0 END),
        SUM(CASE WHEN co.OperationType='UNLOAD' THEN co.Quantity ELSE 0 END),
        SUM(co.Quantity)
      FROM StagingDB.PortOperations.CargoOperation AS co
      JOIN StagingDB.PortOperations.PortCall       AS pc  ON co.PortCallID = pc.PortCallID
      JOIN Dim.DimPort                             AS dp  ON pc.PortID = dp.PortID
      JOIN StagingDB.PortOperations.Container      AS c   ON co.ContainerID = c.ContainerID
      JOIN StagingDB.PortOperations.ContainerType  AS cty ON c.ContainerTypeID = cty.ContainerTypeID
      GROUP BY dp.PortSK, cty.ContainerTypeID;

      SET @StepEnd = GETDATE();
      INSERT INTO dbo.ETLLog
      (TableName, OperationType, StartTime, EndTime, Message)
      VALUES
      (
        @TableName,
        'Rebuild',
        @StepStart,
        @StepEnd,
        CONCAT('Rebuilt container‐movements aggregate: ', @@ROWCOUNT, ' rows')
      );

      COMMIT;
    END TRY
    BEGIN CATCH
      IF XACT_STATE()<>0 ROLLBACK;
      SET @StepEnd = GETDATE();
      INSERT INTO dbo.ETLLog
      (TableName, OperationType, StartTime, EndTime, Message)
      VALUES
      (
        @TableName,
        'Error',
        @StepStart,
        @StepEnd,
        ERROR_MESSAGE()
      );
      THROW;
    END CATCH;
END;
GO


/*------------------------------------------------------------------------------
  4) UpdateFactPortCallSnapshotIncremental
------------------------------------------------------------------------------*/
CREATE OR ALTER PROCEDURE Fact.UpdateFactPortCallSnapshotIncremental
AS
BEGIN
    SET NOCOUNT, XACT_ABORT ON;
    DECLARE
      @TableName   NVARCHAR(128) = 'Fact.FactPortCallPeriodicSnapshot',
      @StepStart   DATETIME,
      @StepEnd     DATETIME;

    BEGIN TRY
      BEGIN TRAN;
      -- find last loaded DateKey
      DECLARE @LastDateKey INT =
        ISNULL((SELECT MAX(DateKey) FROM Fact.FactPortCallPeriodicSnapshot), 0);

      SET @StepStart = GETDATE();

      INSERT INTO Fact.FactPortCallPeriodicSnapshot
      (DateKey, PortCallID, VoyageID, PortSK, Status, AllocationCount, TotalOps)
      SELECT
        dd.DimDateID,
        pc.PortCallID,
        pc.VoyageID,
        dp.PortSK,
        pc.Status,
        COUNT(DISTINCT ba.AllocationID),
        COUNT(DISTINCT co.CargoOpID)
      FROM StagingDB.PortOperations.PortCall AS pc
      JOIN Dim.DimDate                          AS dd  ON CAST(pc.ArrivalDateTime AS DATE)=dd.FullDate
      JOIN Dim.DimPort                          AS dp  ON pc.PortID = dp.PortID
      LEFT JOIN StagingDB.PortOperations.BerthAllocation AS ba ON pc.PortCallID = ba.PortCallID
      LEFT JOIN StagingDB.PortOperations.CargoOperation     AS co ON pc.PortCallID = co.PortCallID
      WHERE dd.DimDateID > @LastDateKey
      GROUP BY dd.DimDateID, pc.PortCallID, pc.VoyageID, dp.PortSK, pc.Status;

      SET @StepEnd = GETDATE();
      INSERT INTO dbo.ETLLog
      (TableName, OperationType, StartTime, EndTime, Message)
      VALUES
      (
        @TableName,
        'IncrementalInsert',
        @StepStart,
        @StepEnd,
        CONCAT('Inserted ', @@ROWCOUNT, ' new port‐call snapshots')
      );

      COMMIT;
    END TRY
    BEGIN CATCH
      IF XACT_STATE()<>0 ROLLBACK;
	  SET @StepEnd = GETDATE();
      INSERT INTO dbo.ETLLog
      (TableName, OperationType, StartTime, EndTime, Message)
      VALUES
      (
        @TableName,
        'Error',
        @StepStart,
        @StepEnd,
        ERROR_MESSAGE()
      );
      THROW;
    END CATCH;
END;
GO

