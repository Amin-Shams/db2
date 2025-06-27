USE StagingDB;
GO

--------------------------------------------------------------------------------
-- Wrapper: LoadAllFinanceStagingTables
-- Description: Executes all individual Finance staging-load procedures in sequence
--              within a single transaction and in the correct dependency order.
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Finance.LoadAllFinanceStagingTables
AS
BEGIN
    BEGIN TRY
        BEGIN TRAN;

        -- Step 1: Load Customer
        EXEC Finance.LoadFinanceCustomer;

        -- Step 2: Load BillingCycle
        EXEC Finance.LoadFinanceBillingCycle;

        -- Step 3: Load ServiceType
        EXEC Finance.LoadFinanceServiceType;

        -- Step 4: Load Tax
        EXEC Finance.LoadFinanceTax;

        -- Step 5: Load Tariff
        EXEC Finance.LoadFinanceTariff;

        -- Step 6: Load Contract
        EXEC Finance.LoadFinanceContract;

        -- Step 7: Load Invoice
        EXEC Finance.LoadFinanceInvoice;

        -- Step 8: Load InvoiceLine
        EXEC Finance.LoadFinanceInvoiceLine;

        -- Step 9: Load Payment
        EXEC Finance.LoadFinancePayment;

        -- Step 10: Load RevenueRecognition
        EXEC Finance.LoadFinanceRevenueRecognition;

        COMMIT;
    END TRY
    BEGIN CATCH
        ROLLBACK;
        THROW; -- Reraise for external logging
    END CATCH;
END;
GO



--------------------------------------------------------------------------------
-- To execute all ETL loads:
EXEC Finance.LoadAllFinanceStagingTables;
--------------------------------------------------------------------------------



--------------------------------------------------------------------------------
-- LoadSAAllHumanResources
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE HumanResources.LoadSAAllHumanResources
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @MasterProcName NVARCHAR(128) = 'HumanResources.LoadSAAllHumanResources';
    DECLARE @StepStart DATETIME;
    DECLARE @Message NVARCHAR(1000);

    SET @StepStart = GETDATE();
    SET @Message = 'Master procedure started.';
    INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
        VALUES (@MasterProcName, 'Start', @StepStart, @StepStart, @Message);

    BEGIN TRY
        EXEC HumanResources.LoadSAEmployee;
        EXEC HumanResources.LoadSADepartment;
        EXEC HumanResources.LoadSAJobTitle;
        EXEC HumanResources.LoadSAEmploymentHistory;
        EXEC HumanResources.LoadSATermination;
        EXEC HumanResources.LoadSAAttendance;
        EXEC HumanResources.LoadSASalaryPayment;
        EXEC HumanResources.LoadSATrainingProgram;
        EXEC HumanResources.LoadSAEmployeeTraining;
        EXEC HumanResources.LoadSALeaveType;
        EXEC HumanResources.LoadSALeaveRequest;

        SET @Message = 'Master procedure finished successfully.';
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@MasterProcName, 'End', @StepStart, GETDATE(), @Message);
    END TRY
    BEGIN CATCH
        SET @Message = CONCAT('Master procedure failed. Error: ', ERROR_MESSAGE());
        INSERT INTO Audit.ETLLog (TableName, OperationType, StartTime, EndTime, [Message])
            VALUES (@MasterProcName, 'Error', @StepStart, GETDATE(), @Message);
        THROW;
    END CATCH
END;
GO



exec HumanResources.LoadSAAllHumanResources;