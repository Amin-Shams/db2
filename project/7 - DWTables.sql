-- 1) Create DW database if it does not already exist
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    CREATE DATABASE DataWarehouse;
END
GO

-- 2) Switch context to the DW database
USE DataWarehouse;
GO

/*------------------------------------------
    DW Schemas
------------------------------------------*/
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Dim')
    EXEC('CREATE SCHEMA Dim;');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Fact')
    EXEC('CREATE SCHEMA Fact;');
GO

/*====================================================================
  1.  DIMENSION TABLES
====================================================================*/

/*-------------- 1-1  DimDate (Calendar) --------------*/
CREATE TABLE Dim.DimDate (
    DimDateID     INT IDENTITY(1,1) PRIMARY KEY,
    FullDate      DATE            NOT NULL,
    [Year]        SMALLINT        NOT NULL,
    [Quarter]     TINYINT         NOT NULL,
    [Month]       TINYINT         NOT NULL,
    MonthName     NVARCHAR(50),                 -- نام ماه
    [Day]         TINYINT         NOT NULL,
    DayOfWeek     TINYINT         NOT NULL,    -- 1 = Monday
    DayName       NVARCHAR(50),                 -- نام روز هفته
    WeekOfYear    TINYINT         NOT NULL,
    IsWeekend     BIT             NOT NULL
);
GO

/*-------------- 1-2  DimCustomer  (SCD2) --------------*/
CREATE TABLE Dim.DimCustomer (
    DimCustomerID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerCode  VARCHAR(50)      NOT NULL,    -- کد یکتا مشتری
    CustomerName  NVARCHAR(200)    NOT NULL,    -- نام مشتری
    CustomerType  NVARCHAR(100)    NOT NULL,    -- نوع مشتری
    CountryName   NVARCHAR(200),                  -- نام کشور
    VATNumber     VARCHAR(50),                    -- شماره VAT (به «شماره مالیات بر ارزش افزوده» (Value-Added Tax Number) مشتری اشاره دارد.)
    Address       NVARCHAR(500),                  -- آدرس
    Email         NVARCHAR(200),                  -- ایمیل
    Phone         VARCHAR(50),                    -- تلفن

    -- SCD2 tracking
    StartDate     DATE             NOT NULL,    -- شروع دوره جاری
    EndDate       DATE,                           -- پایان دوره قبلی
    IsCurrent     BIT              NOT NULL DEFAULT 1  -- ۱=جاری، ۰=قدیمی
);
GO

/*-------------- 1-3  DimServiceType (SCD1) --------------*/
CREATE TABLE Dim.DimServiceType (
    DimServiceTypeID    INT IDENTITY(1,1) PRIMARY KEY,
    SourceServiceTypeID INT             NOT NULL,     -- کلید طبیعی
    ServiceName         NVARCHAR(200)   NOT NULL,     -- نام خدمت
    ServiceCategory     NVARCHAR(100),                  -- دسته‌بندی خدمت
    UnitOfMeasure       NVARCHAR(50)    NOT NULL,     -- واحد اندازه‌گیری
    Taxable             BIT             NOT NULL,     -- مشمول مالیات
    IsActive            BIT             NOT NULL      -- فعال/غیرفعال
);
GO

/*-------------- 1-4  DimTax  (SCD2) --------------*/
CREATE TABLE Dim.DimTax (
    DimTaxID      INT IDENTITY(1,1) PRIMARY KEY,
    TaxName       NVARCHAR(100)   NOT NULL,           -- نام مالیات
    TaxRate       DECIMAL(18,4)   NOT NULL,           -- نرخ مالیات
    TaxType       NVARCHAR(100)   NOT NULL,           -- نوع مالیات
    EffectiveFrom DATE            NOT NULL,           -- از تاریخ
    EffectiveTo   DATE,                              -- تا تاریخ

    -- SCD2 tracking
    StartDate     DATE            NOT NULL,           -- شروع دوره جاری
    EndDate       DATE,                              -- پایان دوره قبلی
    IsCurrent     BIT             NOT NULL DEFAULT 1  -- ۱=جاری، ۰=قدیمی
);
GO

/*-------------- 1-5  DimBillingCycle (SCD1) --------------*/
CREATE TABLE Dim.DimBillingCycle (
    DimBillingCycleID INT IDENTITY(1,1) PRIMARY KEY,
    CycleName         NVARCHAR(100)  NOT NULL,       -- نام دوره
    CycleLengthInDays INT            NOT NULL        -- طول دوره (روز)
);
GO

/*-------------- 1-6  DimPaymentMethod (SCD1) --------------*/
CREATE TABLE Dim.DimPaymentMethod (
    DimPaymentMethodID INT IDENTITY(1,1) PRIMARY KEY,
    PaymentMethodName  NVARCHAR(100)  NOT NULL        -- نام روش پرداخت
);
GO

/*-------------- 1-7  DimContract  (SCD3) --------------*/
CREATE TABLE Dim.DimContract (
    DimContractID       INT IDENTITY(1,1) PRIMARY KEY,
    ContractNumber      VARCHAR(100)   NOT NULL,     -- شماره قرارداد
    ContractStatus      NVARCHAR(100)  NOT NULL,     -- وضعیت فعلی
    PrevContractStatus1 NVARCHAR(100),                -- وضعیت قبلی اول
    PrevContractStatus2 NVARCHAR(100),                -- وضعیت قبلی دوم
    PaymentTerms        NVARCHAR(200),                -- شرایط پرداخت
    StartDateActual     DATE            NOT NULL,     -- تاریخ شروع معتبر
    EndDateActual       DATE                          -- تاریخ پایان معتبر
);
GO

/*-------------- 1-8  DimInvoice (Static) --------------*/
CREATE TABLE Dim.DimInvoice (
    DimInvoiceID  INT IDENTITY(1,1) PRIMARY KEY,
    InvoiceNumber VARCHAR(100)     NOT NULL,         -- شماره فاکتور
    InvoiceStatus NVARCHAR(100)    NOT NULL,         -- وضعیت
    CreatedBy     NVARCHAR(150),                     -- ایجادکننده
    CreatedDate   DATETIME        NOT NULL           -- زمان ایجاد
);
GO

/*====================================================================
  2.  FACT TABLES  (بدون PK)
====================================================================*/

/*-------------- 2-1  FactInvoiceLineTransaction  (Transaction Fact) --------------*/
CREATE TABLE Fact.FactInvoiceLineTransaction (
    DimInvoiceID       INT          NOT NULL,        -- FK → Dim.DimInvoice
    DimCustomerID      INT          NOT NULL,        -- FK → Dim.DimCustomer
    DimServiceTypeID   INT          NOT NULL,        -- FK → Dim.DimServiceType
    DimTaxID           INT,                             -- FK → Dim.DimTax
    DimDateID          INT          NOT NULL,        -- FK → Dim.DimDate (تاریخ صدور)

    Quantity           INT          NOT NULL,        -- تعداد
    UnitPrice          DECIMAL(18,4) NOT NULL,       -- قیمت واحد
    GrossAmount        AS (Quantity * UnitPrice) PERSISTED,  -- مبلغ ناخالص
    DiscountPercent    DECIMAL(6,4),                   -- درصد تخفیف
    NetAmount          DECIMAL(18,4) NOT NULL,       -- مبلغ خالص
    TaxAmount          DECIMAL(18,4) NOT NULL,       -- مبلغ مالیات
    EffectiveRate      AS (CASE WHEN Quantity = 0 THEN 0 ELSE NetAmount/Quantity END) PERSISTED,

    CONSTRAINT FK_FactInvLine_DimInvoice     FOREIGN KEY(DimInvoiceID)     REFERENCES Dim.DimInvoice(DimInvoiceID),
    CONSTRAINT FK_FactInvLine_DimCustomer    FOREIGN KEY(DimCustomerID)    REFERENCES Dim.DimCustomer(DimCustomerID),
    CONSTRAINT FK_FactInvLine_DimServiceType FOREIGN KEY(DimServiceTypeID) REFERENCES Dim.DimServiceType(DimServiceTypeID),
    CONSTRAINT FK_FactInvLine_DimTax         FOREIGN KEY(DimTaxID)         REFERENCES Dim.DimTax(DimTaxID),
    CONSTRAINT FK_FactInvLine_DimDate        FOREIGN KEY(DimDateID)        REFERENCES Dim.DimDate(DimDateID)
);
GO

/*-------------- 2-2  FactCustomerPaymentTransaction  (Transaction Fact) --------------*/
CREATE TABLE Fact.FactCustomerPaymentTransaction (
    DimPaymentMethodID INT            NOT NULL,     -- FK → Dim.DimPaymentMethod
    DimInvoiceID       INT            NOT NULL,     -- FK → Dim.DimInvoice
    DimCustomerID      INT            NOT NULL,     -- FK → Dim.DimCustomer
    DimDateID          INT            NOT NULL,     -- FK → Dim.DimDate (تاریخ پرداخت)

    PaymentAmount      DECIMAL(18,4)  NOT NULL,     -- مبلغ پرداخت
    DaysToPayment      INT,                            -- روز تا پرداخت
    RemainingAmount    DECIMAL(18,4),                  -- مانده
    IsFullPayment      BIT            NOT NULL,     -- ۱=تمام‌پرداخت
    PartialPaymentCount INT,                           -- تعداد پرداخت‌های جزئی

    CONSTRAINT FK_FactPay_DimPaymentMethod FOREIGN KEY(DimPaymentMethodID) REFERENCES Dim.DimPaymentMethod(DimPaymentMethodID),
    CONSTRAINT FK_FactPay_DimInvoice       FOREIGN KEY(DimInvoiceID)       REFERENCES Dim.DimInvoice(DimInvoiceID),
    CONSTRAINT FK_FactPay_DimCustomer      FOREIGN KEY(DimCustomerID)      REFERENCES Dim.DimCustomer(DimCustomerID),
    CONSTRAINT FK_FactPay_DimDate          FOREIGN KEY(DimDateID)          REFERENCES Dim.DimDate(DimDateID)
);
GO

/*-------------- 2-3  FactCustomerBillingMonthlySnapshot  (Periodic Snapshot) --------------*/
CREATE TABLE Fact.FactCustomerBillingMonthlySnapshot (
    DimCustomerID        INT            NOT NULL,  -- FK → Dim.DimCustomer
    DimDateID            INT            NOT NULL,  -- FK → Dim.DimDate (اولین روز ماه)
    DimBillingCycleID    INT                     ,-- FK → Dim.DimBillingCycle

    TotalInvoiceCount    INT            NOT NULL,  -- تعداد فاکتور
    TotalNetAmount       DECIMAL(18,4)  NOT NULL,  -- مجموع مبلغ خالص
    TotalTaxAmount       DECIMAL(18,4)  NOT NULL,  -- مجموع مالیات
    TotalDiscount        DECIMAL(18,4),             -- مجموع تخفیف
    TotalPaid            DECIMAL(18,4)  NOT NULL,  -- مجموع پرداخت
    AveragePaymentDelay  DECIMAL(10,2),             -- میانگین تأخیر
    MaxOutstandingAmount DECIMAL(18,4),             -- حداکثر مانده

    CONSTRAINT FK_FactMonBill_DimCustomer     FOREIGN KEY(DimCustomerID)     REFERENCES Dim.DimCustomer(DimCustomerID),
    CONSTRAINT FK_FactMonBill_DimDate         FOREIGN KEY(DimDateID)         REFERENCES Dim.DimDate(DimDateID),
    CONSTRAINT FK_FactMonBill_DimBillingCycle FOREIGN KEY(DimBillingCycleID) REFERENCES Dim.DimBillingCycle(DimBillingCycleID)
);
GO

/*-------------- 2-4  FactInvoiceLifecycleAccumulating  (Accumulating Snapshot) --------------*/
CREATE TABLE Fact.FactInvoiceLifecycleAccumulating (
    DimInvoiceID        INT            NOT NULL,  -- FK → Dim.DimInvoice
    DimCustomerID       INT            NOT NULL,  -- FK → Dim.DimCustomer
    DimContractID       INT                     ,-- FK → Dim.DimContract
    InvoiceDateID       INT            NOT NULL,  -- FK → Dim.DimDate
    DueDateID           INT            NOT NULL,  -- FK → Dim.DimDate
    FirstPaymentDateID  INT                     ,-- FK → Dim.DimDate
    FinalPaymentDateID  INT                     ,-- FK → Dim.DimDate

    TotalAmount         DECIMAL(18,4)  NOT NULL,  -- مبلغ کل
    PaidAmount          DECIMAL(18,4)  NOT NULL,  -- مجموع پرداخت
    OutstandingAmount   DECIMAL(18,4)  NOT NULL,  -- مانده
    PaymentCount        INT            NOT NULL,  -- تعداد پرداخت
    FirstPaymentDelay   INT                     ,-- روز تأخیر اول
    FinalPaymentDelay   INT                     ,-- روز تأخیر نهایی
    DaysToClose         INT                     ,-- روز تا بسته شدن

    CONSTRAINT FK_FactLife_DimInvoice  FOREIGN KEY(DimInvoiceID)  REFERENCES Dim.DimInvoice(DimInvoiceID),
    CONSTRAINT FK_FactLife_DimCustomer FOREIGN KEY(DimCustomerID) REFERENCES Dim.DimCustomer(DimCustomerID),
    CONSTRAINT FK_FactLife_DimContract FOREIGN KEY(DimContractID) REFERENCES Dim.DimContract(DimContractID),
    CONSTRAINT FK_FactLife_DimDate1    FOREIGN KEY(InvoiceDateID) REFERENCES Dim.DimDate(DimDateID),
    CONSTRAINT FK_FactLife_DimDate2    FOREIGN KEY(DueDateID)     REFERENCES Dim.DimDate(DimDateID),
    CONSTRAINT FK_FactLife_DimDate3    FOREIGN KEY(FirstPaymentDateID) REFERENCES Dim.DimDate(DimDateID),
    CONSTRAINT FK_FactLife_DimDate4    FOREIGN KEY(FinalPaymentDateID) REFERENCES Dim.DimDate(DimDateID)
);
GO

/*-------------- 2-5  FactCustomerContractActivationFactless  (Factless) --------------*/
CREATE TABLE Fact.FactCustomerContractActivationFactless (
    DimDateID     INT NOT NULL,  -- FK → Dim.DimDate
    DimCustomerID INT NOT NULL,  -- FK → Dim.DimCustomer
    DimContractID INT NOT NULL,  -- FK → Dim.DimContract

    CONSTRAINT FK_FactAct_DimDate     FOREIGN KEY(DimDateID)     REFERENCES Dim.DimDate(DimDateID),
    CONSTRAINT FK_FactAct_DimCustomer FOREIGN KEY(DimCustomerID) REFERENCES Dim.DimCustomer(DimCustomerID),
    CONSTRAINT FK_FactAct_DimContract FOREIGN KEY(DimContractID) REFERENCES Dim.DimContract(DimContractID)
);
GO


-- جدول Finance.ETLLog: لاگ مراحل ETL
-- DW
CREATE TABLE ETLLog (
    [LogID]         INT IDENTITY(1,1) PRIMARY KEY,  -- کلید لاگ
    [TableName]     NVARCHAR(128)     NOT NULL,     -- نام جدول هدف
    [OperationType] NVARCHAR(50)      NOT NULL,     -- 'Truncate'/'Validate'/'Insert'/'Error'
    [StartTime]     DATETIME          NOT NULL DEFAULT GETDATE(),  -- زمان شروع
    [EndTime]       DATETIME,                       -- زمان پایان
    [Message]       NVARCHAR(2000)                  -- پیام وضعیت یا خطا
);
GO




-- DimDate: معمولاً بر اساس FullDate و DimDateID جستجو می‌شود
CREATE NONCLUSTERED INDEX IX_DimDate_FullDate ON Dim.DimDate(FullDate);

-- DimCustomer: چون SCD2 است، معمولاً CustomerCode و IsCurrent برای یافتن نسخه جاری استفاده می‌شود
CREATE NONCLUSTERED INDEX IX_DimCustomer_CustomerCode_IsCurrent ON Dim.DimCustomer(CustomerCode, IsCurrent);

-- DimServiceType
CREATE NONCLUSTERED INDEX IX_DimServiceType_SourceID ON Dim.DimServiceType(SourceServiceTypeID);

-- DimTax: ممکن است بر اساس TaxName یا TaxType فیلتر شود
CREATE NONCLUSTERED INDEX IX_DimTax_TaxName ON Dim.DimTax(TaxName);
CREATE NONCLUSTERED INDEX IX_DimTax_TaxType ON Dim.DimTax(TaxType);

-- DimBillingCycle
CREATE NONCLUSTERED INDEX IX_DimBillingCycle_CycleLengthInDays ON Dim.DimBillingCycle(CycleLengthInDays);

-- DimPaymentMethod
CREATE NONCLUSTERED INDEX IX_DimPaymentMethod_Name ON Dim.DimPaymentMethod(PaymentMethodName);

-- DimContract: برای join بر اساس ContractNumber
CREATE NONCLUSTERED INDEX IX_DimContract_ContractNumber ON Dim.DimContract(ContractNumber);

-- DimInvoice
CREATE NONCLUSTERED INDEX IX_DimInvoice_InvoiceNumber ON Dim.DimInvoice(InvoiceNumber);


-- FactInvoiceLineTransaction
CREATE NONCLUSTERED INDEX IX_FactInvoiceLine_AllFKs
ON Fact.FactInvoiceLineTransaction(DimInvoiceID, DimCustomerID, DimServiceTypeID, DimTaxID, DimDateID);

-- FactCustomerPaymentTransaction
CREATE NONCLUSTERED INDEX IX_FactCustomerPayment_AllFKs
ON Fact.FactCustomerPaymentTransaction(DimCustomerID, DimInvoiceID, DimDateID, DimPaymentMethodID);

-- FactCustomerBillingMonthlySnapshot
CREATE NONCLUSTERED INDEX IX_FactMonthlyBilling_Composite
ON Fact.FactCustomerBillingMonthlySnapshot(DimCustomerID, DimDateID);

-- FactInvoiceLifecycleAccumulating
CREATE NONCLUSTERED INDEX IX_FactLifecycle_Invoice
ON Fact.FactInvoiceLifecycleAccumulating(DimInvoiceID, InvoiceDateID, DueDateID);

-- FactCustomerContractActivationFactless
CREATE NONCLUSTERED INDEX IX_FactContractActivation_Composite
ON Fact.FactCustomerContractActivationFactless(DimCustomerID, DimContractID, DimDateID);











--USE DataWarehouse;
--GO

-- Drop all tables if they exist
--DROP TABLE IF EXISTS Fact.FactCustomerContractActivationFactless;
--DROP TABLE IF EXISTS Fact.FactInvoiceLifecycleAccumulating;
--DROP TABLE IF EXISTS Fact.FactCustomerBillingMonthlySnapshot;
--DROP TABLE IF EXISTS Fact.FactCustomerPaymentTransaction;
--DROP TABLE IF EXISTS Fact.FactInvoiceLineTransaction;

--DROP TABLE IF EXISTS Dim.DimInvoice;
--DROP TABLE IF EXISTS Dim.DimContract;
--DROP TABLE IF EXISTS Dim.DimPaymentMethod;
--DROP TABLE IF EXISTS Dim.DimBillingCycle;
--DROP TABLE IF EXISTS Dim.DimTax;
--DROP TABLE IF EXISTS Dim.DimServiceType;
--DROP TABLE IF EXISTS Dim.DimCustomer;
--DROP TABLE IF EXISTS Dim.DimDate;

--DROP TABLE IF EXISTS ETLLog;


-- 1) Create DW database if it does not already exist
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    CREATE DATABASE DataWarehouse;
    PRINT 'Database DataWarehouse created.';
END
GO

-- 2) Switch context to the DW database
USE DataWarehouse;
GO

/*------------------------------------------
  DW SCHEMAS
------------------------------------------*/
-- Create schemas if they do not exist
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Dim') BEGIN EXEC('CREATE SCHEMA Dim;'); PRINT 'Schema Dim created.'; END
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Fact') BEGIN EXEC('CREATE SCHEMA Fact;'); PRINT 'Schema Fact created.'; END
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Audit') BEGIN EXEC('CREATE SCHEMA Audit;'); PRINT 'Schema Audit created.'; END
GO

/*--------------------------------------------------------------------------------*/
/*--------------------------- 1. DIMENSION TABLES ------------------------------*/
/*--------------------------------------------------------------------------------*/

-------------------------------------------------
-- 1.1 DimDate (Calendar)
-------------------------------------------------
PRINT 'Creating Dim.DimDate...';
IF OBJECT_ID('Dim.DimDate', 'U') IS NOT NULL DROP TABLE Dim.DimDate;
CREATE TABLE Dim.DimDate (
    DateKey         INT IDENTITY(1,1) PRIMARY KEY,
    FullDate        DATE          NOT NULL UNIQUE,
    [Year]          SMALLINT      NOT NULL,
    [Quarter]       TINYINT       NOT NULL,
    [Month]         TINYINT       NOT NULL,
    MonthName       NVARCHAR(20)  NOT NULL,
    [Day]           TINYINT       NOT NULL,
    DayOfWeek       TINYINT       NOT NULL,
    DayName         NVARCHAR(20)  NOT NULL,
    WeekOfYear      TINYINT       NOT NULL,
    IsWeekend       BIT           NOT NULL
);
GO

-------------------------------------------------
-- 1.2 DimEmployee (SCD Type 2: Add New Row)
-- Maintains a full history of key employee attribute changes (including contact info).
-------------------------------------------------
PRINT 'Creating Dim.DimEmployee...';
IF OBJECT_ID('Dim.DimEmployee', 'U') IS NOT NULL DROP TABLE Dim.DimEmployee;
CREATE TABLE Dim.DimEmployee (
    EmployeeKey         INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID          INT NOT NULL,           -- Natural key from the source system
    FullName            NVARCHAR(100) NOT NULL,
    Gender              NVARCHAR(10),
    MaritalStatus       NVARCHAR(20),
    Position            NVARCHAR(50),           -- Tracked Attribute
    Address             NVARCHAR(200),          -- Tracked Attribute
    Phone               VARCHAR(20),            -- Tracked Attribute
    EmploymentStatus    NVARCHAR(50),
    StartDate           DATE NOT NULL,          -- The date this record version becomes effective
    EndDate             DATE NULL,              -- The date this record version expires
    IsCurrent           BIT NOT NULL            -- Flag to indicate if this is the current, active record
);
GO

-------------------------------------------------
-- 1.3 DimDepartment (SCD Type 1: Overwrite)
-- Stores department information, overwriting on change.
-------------------------------------------------
PRINT 'Creating Dim.DimDepartment...';
IF OBJECT_ID('Dim.DimDepartment', 'U') IS NOT NULL DROP TABLE Dim.DimDepartment;
CREATE TABLE Dim.DimDepartment (
    DepartmentKey       INT IDENTITY(1,1) PRIMARY KEY,
    DepartmentID        INT NOT NULL,           -- Natural key from the source system
    DepartmentName      NVARCHAR(100) NOT NULL,
    ManagerName         NVARCHAR(100),
    IsActive            BIT NOT NULL
);
GO

-------------------------------------------------
-- 1.4 DimJobTitle (SCD Type 3: Add New Attribute)
-- Tracks changes to job category by adding a new attribute column.
-------------------------------------------------
PRINT 'Creating Dim.DimJobTitle...';
IF OBJECT_ID('Dim.DimJobTitle', 'U') IS NOT NULL DROP TABLE Dim.DimJobTitle;
CREATE TABLE Dim.DimJobTitle (
    JobTitleKey         INT IDENTITY(1,1) PRIMARY KEY,
    JobTitleID          INT NOT NULL,           -- Natural key from the source system
    JobTitleName        NVARCHAR(100) NOT NULL,
    CurrentJobCategory  NVARCHAR(50),           -- The current job category
    PreviousJobCategory NVARCHAR(50)            -- The previous job category (to track the change)
);
GO

-------------------------------------------------
-- 1.5 DimTerminationReason (Simple Dimension)
-- A simple dimension for termination reasons.
-------------------------------------------------
PRINT 'Creating Dim.DimTerminationReason...';
IF OBJECT_ID('Dim.DimTerminationReason', 'U') IS NOT NULL DROP TABLE Dim.DimTerminationReason;
CREATE TABLE Dim.DimTerminationReason (
    TerminationReasonKey    INT IDENTITY(1,1) PRIMARY KEY,
    TerminationReason       NVARCHAR(100) NOT NULL
);
GO

-------------------------------------------------
-- 1.6 DimLeaveType (Simple Dimension)
-- Dimension for different types of leave.
-------------------------------------------------
PRINT 'Creating Dim.DimLeaveType...';
IF OBJECT_ID('Dim.DimLeaveType', 'U') IS NOT NULL DROP TABLE Dim.DimLeaveType;
CREATE TABLE Dim.DimLeaveType (
    LeaveTypeKey        INT IDENTITY(1,1) PRIMARY KEY,
    LeaveTypeID         INT NOT NULL,           -- Natural key from the source system
    LeaveTypeName       NVARCHAR(50) NOT NULL,
    IsPaid              BIT NOT NULL
);
GO

/*--------------------------------------------------------------------------------*/
/*------------------- 2. FINAL SELECTED FACT TABLES (REVISED) ------------------*/
/*--------------------------------------------------------------------------------*/

-------------------------------------------------
-- 2.1 FactTermination
-------------------------------------------------
PRINT 'Creating Fact.FactTermination...';
IF OBJECT_ID('Fact.FactTermination', 'U') IS NOT NULL DROP TABLE Fact.FactTermination;
CREATE TABLE Fact.FactTermination (
    TerminationDateKey      INT NOT NULL FOREIGN KEY REFERENCES Dim.DimDate(DateKey),
    EmployeeKey             INT NOT NULL FOREIGN KEY REFERENCES Dim.DimEmployee(EmployeeKey),
    DepartmentKey           INT NOT NULL FOREIGN KEY REFERENCES Dim.DimDepartment(DepartmentKey),
    JobTitleKey             INT NOT NULL FOREIGN KEY REFERENCES Dim.DimJobTitle(JobTitleKey),
    TerminationReasonKey    INT NOT NULL FOREIGN KEY REFERENCES Dim.DimTerminationReason(TerminationReasonKey),
    TenureInDays            INT NOT NULL,
    TenureInMonths          INT NOT NULL,
    SalaryAtTermination     DECIMAL(15, 2) NOT NULL,
    IsVoluntary             BIT NOT NULL
);
GO

-------------------------------------------------
-- 2.2 FactYearlyHeadcount
-------------------------------------------------
--PRINT 'Creating Fact.FactYearlyHeadcount...';
--IF OBJECT_ID('Fact.FactYearlyHeadcount', 'U') IS NOT NULL DROP TABLE Fact.FactYearlyHeadcount;
--CREATE TABLE Fact.FactYearlyHeadcount (
--    YearDateKey                 INT NOT NULL FOREIGN KEY REFERENCES Dim.DimDate(DateKey),
--    DepartmentKey               INT NOT NULL FOREIGN KEY REFERENCES Dim.DimDepartment(DepartmentKey),
--    HeadcountStartOfYear        INT NOT NULL,
--    HeadcountEndOfYear          INT NOT NULL,
--    HiresCount                  INT NOT NULL,
--    TerminationsCount           INT NOT NULL,
--    VoluntaryTerminationsCount  INT NOT NULL,
--    InvoluntaryTerminationsCount INT NOT NULL,
--    AverageTenureInMonths       DECIMAL(10, 2) NOT NULL,
--    TurnoverRate                DECIMAL(5, 4) NOT NULL,
--    AverageAgeOfEmployees       DECIMAL(10, 2) NOT NULL
--);
--GO

-------------------------------------------------
-- 2.3 FactMonthlyEmployeePerformance
-------------------------------------------------
PRINT 'Creating Fact.FactMonthlyEmployeePerformance...';
IF OBJECT_ID('Fact.FactMonthlyEmployeePerformance', 'U') IS NOT NULL DROP TABLE Fact.FactMonthlyEmployeePerformance;
CREATE TABLE Fact.FactMonthlyEmployeePerformance (
    MonthDateKey                INT NOT NULL FOREIGN KEY REFERENCES Dim.DimDate(DateKey),
    EmployeeKey                 INT NOT NULL FOREIGN KEY REFERENCES Dim.DimEmployee(EmployeeKey),
    DepartmentKey               INT NOT NULL FOREIGN KEY REFERENCES Dim.DimDepartment(DepartmentKey),
    TotalHoursWorked            DECIMAL(10, 2) NOT NULL,
    AverageDailyHoursWorked     DECIMAL(10, 2) NOT NULL,
    MinDailyHoursWorked         DECIMAL(10, 2),
    MaxDailyHoursWorked         DECIMAL(10, 2),
    LateDaysCount               INT NOT NULL,
    AbsentDaysCount             INT NOT NULL,
    WorkDaysCount               INT NOT NULL,
    OvertimeAsPercentage        DECIMAL(5, 4) NOT NULL
);
GO

-------------------------------------------------
-- 2.4 FactEmployeeLifecycle
-------------------------------------------------
PRINT 'Creating Fact.FactEmployeeLifecycle...';
IF OBJECT_ID('Fact.FactEmployeeLifecycle', 'U') IS NOT NULL DROP TABLE Fact.FactEmployeeLifecycle;
CREATE TABLE Fact.FactEmployeeLifecycle (
    EmployeeKey             INT NOT NULL PRIMARY KEY FOREIGN KEY REFERENCES Dim.DimEmployee(EmployeeKey),
    HireDateKey             INT NOT NULL FOREIGN KEY REFERENCES Dim.DimDate(DateKey),
    TerminationDateKey      INT,
    FirstPromotionDateKey   INT,
    LastTrainingDateKey     INT,
    TerminationReasonKey    INT,
    DaysToTermination       INT,
    DaysToFirstPromotion    INT,
    TotalPromotionsCount    INT,
    TotalTrainingsCompleted INT,
    FinalSalary             DECIMAL(15, 2)
);
GO

-------------------------------------------------
-- 2.5 FactSalaryPayment
-------------------------------------------------
PRINT 'Creating Fact.FactSalaryPayment...';
IF OBJECT_ID('Fact.FactSalaryPayment', 'U') IS NOT NULL DROP TABLE Fact.FactSalaryPayment;
CREATE TABLE Fact.FactSalaryPayment (
    PaymentDateKey      INT NOT NULL FOREIGN KEY REFERENCES Dim.DimDate(DateKey),
    EmployeeKey         INT NOT NULL FOREIGN KEY REFERENCES Dim.DimEmployee(EmployeeKey),
    DepartmentKey       INT NOT NULL FOREIGN KEY REFERENCES Dim.DimDepartment(DepartmentKey),
    JobTitleKey         INT NOT NULL FOREIGN KEY REFERENCES Dim.DimJobTitle(JobTitleKey),
    GrossPayAmount      DECIMAL(15, 2) NOT NULL,
    BaseAmount          DECIMAL(15, 2) NOT NULL,
    BonusAmount         DECIMAL(15, 2) NOT NULL,
    DeductionsAmount    DECIMAL(15, 2) NOT NULL,
    NetAmount           DECIMAL(15, 2) NOT NULL,
    SalaryCostToCompany DECIMAL(15, 2) NOT NULL
);
GO

-------------------------------------------------
-- 2.6 FactEmployeeAttendance
-------------------------------------------------
PRINT 'Creating Fact.FactEmployeeAttendance...';
IF OBJECT_ID('Fact.FactEmployeeAttendance', 'U') IS NOT NULL DROP TABLE Fact.FactEmployeeAttendance;
CREATE TABLE Fact.FactEmployeeAttendance (
    AttendanceDateKey   INT NOT NULL FOREIGN KEY REFERENCES Dim.DimDate(DateKey),
    EmployeeKey         INT NOT NULL FOREIGN KEY REFERENCES Dim.DimEmployee(EmployeeKey),
    AttendanceStatus    NVARCHAR(20) NOT NULL
);
GO

/*--------------------------------------------------------------------------------*/
/*----------------------------- 3. AUDIT LOG TABLE -----------------------------*/
/*--------------------------------------------------------------------------------*/
PRINT 'Creating Audit.DW_ETL_Log...';
IF OBJECT_ID('Audit.DW_ETL_Log', 'U') IS NOT NULL DROP TABLE Audit.DW_ETL_Log;
CREATE TABLE Audit.DW_ETL_Log (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    ProcessName NVARCHAR(128) NOT NULL,
    OperationType NVARCHAR(50) NOT NULL,
    TargetTable NVARCHAR(128) NOT NULL,
    StartTime DATETIME NOT NULL,
    EndTime DATETIME NOT NULL,
    RecordsInserted INT,
    RecordsUpdated INT,
    Status NVARCHAR(20) NOT NULL,
    [Message] NVARCHAR(MAX)
);
GO

--------------------------------------------------------------------------------
-- Create/Verify the Control table to manage incremental loads
--------------------------------------------------------------------------------
IF OBJECT_ID('Audit.ETL_Control', 'U') IS NULL
BEGIN
    CREATE TABLE Audit.ETL_Control (
        ProcessName NVARCHAR(128) PRIMARY KEY,
        LastLoadDate DATE NOT NULL
    );
    INSERT INTO Audit.ETL_Control (ProcessName, LastLoadDate) VALUES ('FactTables', '1900-01-01');
    PRINT 'Table Audit.ETL_Control created and initialized.';
END
GO

PRINT 'Data Warehouse creation script completed successfully.';
