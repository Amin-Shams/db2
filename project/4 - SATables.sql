-- 1) Create Staging database if it does not already exist
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'StagingDB')
BEGIN
    CREATE DATABASE StagingDB;
END
GO

-- 2) Switch context to the Staging database
USE StagingDB;
GO

-- 3) Create Finance schema if not exists
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Finance')
BEGIN
    EXEC('CREATE SCHEMA Finance;');
END
GO


-- جدول Finance.Customer: داده‌ مشتریان (Staging)
CREATE TABLE Finance.Customer (
    [CustomerID]    INT              NOT NULL,  -- کلید طبیعی مشتری
    [CustomerCode]  VARCHAR(50)      NOT NULL,  -- کد یکتا
    [CustomerName]  NVARCHAR(200)    NOT NULL,  -- نام مشتری
    [CustomerType]  NVARCHAR(100)    NOT NULL,  -- نوع مشتری
    [TIN]           VARCHAR(50),                -- شناسه مالیاتی
    [VATNumber]     VARCHAR(50),                -- شماره VAT
    [Phone]         VARCHAR(50),                -- تلفن تماس
    [Email]         NVARCHAR(200),              -- پست الکترونیک
    [Address]       NVARCHAR(500),              -- آدرس
    [CountryID]     INT,                        -- FK → Common.Country.CountryID
    [LoadDate]      DATETIME         NOT NULL DEFAULT GETDATE()  -- زمان بارگذاری
);
GO

-- جدول Finance.BillingCycle: دوره‌های صدور فاکتور (Staging)
CREATE TABLE Finance.BillingCycle (
    [BillingCycleID]    INT              NOT NULL,  -- کلید دوره
    [CycleName]         NVARCHAR(100)    NOT NULL,  -- نام دوره
    [CycleLengthInDays] INT              NOT NULL,  -- طول دوره به روز
    [LoadDate]          DATETIME         NOT NULL DEFAULT GETDATE()  -- زمان بارگذاری
);
GO

-- جدول Finance.ServiceType: انواع خدمات (Staging)
CREATE TABLE Finance.ServiceType (
    [ServiceTypeID]   INT              NOT NULL,  -- کلید خدمت
    [ServiceName]     NVARCHAR(200)    NOT NULL,  -- نام خدمت
    [ServiceCategory] NVARCHAR(100),              -- دسته‌بندی
    [BaseRate]        DECIMAL(18,4)    NOT NULL,  -- نرخ پایه
    [UnitOfMeasure]   NVARCHAR(50)     NOT NULL,  -- واحد اندازه‌گیری
    [Taxable]         BIT              NOT NULL,  -- مشمول مالیات؟
    [IsActive]        BIT              NOT NULL,  -- فعال/غیرفعال
    [LoadDate]        DATETIME         NOT NULL DEFAULT GETDATE()  -- زمان بارگذاری
);
GO

-- جدول Finance.Tax: مالیات‌ها (Staging)
CREATE TABLE Finance.Tax (
    [TaxID]         INT              NOT NULL,  -- کلید مالیات
    [TaxName]       NVARCHAR(100)    NOT NULL,  -- نام مالیات
    [TaxRate]       DECIMAL(6,3)     NOT NULL,  -- نرخ مالیات
    [TaxType]       NVARCHAR(100)    NOT NULL,  -- نوع مالیات
    [EffectiveFrom] DATE             NOT NULL,  -- از تاریخ
    [EffectiveTo]   DATE,                       -- تا تاریخ
    [LoadDate]      DATETIME         NOT NULL DEFAULT GETDATE()  -- زمان بارگذاری
);
GO

-- جدول Finance.Tariff: تعرفه‌ها (Staging)
CREATE TABLE Finance.Tariff (
    [TariffID]      INT              NOT NULL,  -- کلید تعرفه
    [ServiceTypeID] INT              NOT NULL,  -- FK → Finance.ServiceType
    [ValidFrom]     DATE             NOT NULL,  -- از تاریخ
    [ValidTo]       DATE,                       -- تا تاریخ
    [UnitRate]      DECIMAL(18,4)     NOT NULL,  -- نرخ واحد
    [LoadDate]      DATETIME         NOT NULL DEFAULT GETDATE()  -- زمان بارگذاری
);
GO

-- جدول Finance.Contract: قراردادها (Staging)
CREATE TABLE Finance.Contract (
    [ContractID]     INT              NOT NULL,  -- کلید قرارداد
    [CustomerID]     INT              NOT NULL,  -- FK → Finance.Customer
    [ContractNumber] VARCHAR(100)     NOT NULL,  -- شماره قرارداد
    [StartDate]      DATE             NOT NULL,  -- از تاریخ
    [EndDate]        DATE,                       -- تا تاریخ
    [BillingCycleID] INT              NOT NULL,  -- FK → Finance.BillingCycle
    [PaymentTerms]   NVARCHAR(200),             -- شرایط پرداخت
    [ContractStatus] NVARCHAR(100)     NOT NULL,  -- وضعیت قرارداد
    [CreatedDate]    DATETIME         NOT NULL,  -- زمان ایجاد
    [LoadDate]       DATETIME         NOT NULL DEFAULT GETDATE()  -- زمانa بارگذاری
);
GO

-- جدول Finance.Invoice: سربرگ فاکتورها (Staging)
CREATE TABLE Finance.Invoice (
    [InvoiceID]     INT              NOT NULL,  -- کلید فاکتور
    [ContractID]    INT              NOT NULL,  -- FK → Finance.Contract
    [InvoiceNumber] VARCHAR(100)     NOT NULL,  -- شماره فاکتور
    [InvoiceDate]   DATE             NOT NULL,  -- تاریخ صدور
    [DueDate]       DATE             NOT NULL,  -- تاریخ سررسید
    [Status]        NVARCHAR(100)    NOT NULL,  -- وضعیت
    [TotalAmount]   DECIMAL(18,4)    NOT NULL,  -- مبلغ کل
    [TaxAmount]     DECIMAL(18,4)    NOT NULL,  -- مبلغ مالیات
    [CreatedBy]     NVARCHAR(150),              -- ایجادکننده
    [CreatedDate]   DATETIME         NOT NULL,  -- زمان ایجاد
    [LoadDate]      DATETIME         NOT NULL DEFAULT GETDATE()  -- زمان بارگذاری
);
GO

-- جدول Finance.InvoiceLine: جزئیات فاکتور (Staging)
CREATE TABLE Finance.InvoiceLine (
    [InvoiceLineID]   INT              NOT NULL,  -- کلید سطر فاکتور
    [InvoiceID]       INT              NOT NULL,  -- FK → Finance.Invoice
    [ServiceTypeID]   INT              NOT NULL,  -- FK → Finance.ServiceType
    [TaxID]           INT,                        -- FK → Finance.Tax
    [Quantity]        INT              NOT NULL,  -- تعداد
    [UnitPrice]       DECIMAL(18,4)    NOT NULL,  -- قیمت واحد
    [DiscountPercent] DECIMAL(6,4),              -- درصد تخفیف
    [TaxAmount]       DECIMAL(18,4)    NOT NULL,  -- مبلغ مالیات
    [NetAmount]       DECIMAL(18,4)    NOT NULL,  -- مبلغ خالص
    [LoadDate]        DATETIME         NOT NULL DEFAULT GETDATE()  -- زمان بارگذاری
);
GO

-- جدول Finance.Payment: پرداخت‌ها (Staging)
CREATE TABLE Finance.Payment (
    [PaymentID]       INT              NOT NULL,  -- کلید پرداخت
    [InvoiceID]       INT              NOT NULL,  -- FK → Finance.Invoice
    [PaymentDate]     DATE             NOT NULL,  -- تاریخ پرداخت
    [Amount]          DECIMAL(18,4)    NOT NULL,  -- مبلغ پرداخت
    [PaymentMethod]   NVARCHAR(100)    NOT NULL,  -- روش پرداخت
    [ConfirmedBy]     NVARCHAR(150),              -- تأییدکننده
    [ReferenceNumber] VARCHAR(100),               -- شماره ارجاع
    [Notes]           NVARCHAR(1000),             -- توضیحات
    [LoadDate]        DATETIME         NOT NULL DEFAULT GETDATE()  -- زمان بارگذاری
);
GO

-- جدول Finance.RevenueRecognition: شناسایی درآمد (Staging)
CREATE TABLE Finance.RevenueRecognition (
    [RecognitionID]   INT              NOT NULL,  -- کلید شناسایی
    [InvoiceID]       INT              NOT NULL,  -- FK → Finance.Invoice
    [DateRecognized]  DATE             NOT NULL,  -- تاریخ شناسایی
    [Amount]          DECIMAL(18,4)    NOT NULL,  -- مبلغ شناسایی‌شده
    [Notes]           NVARCHAR(1000),             -- توضیحات
    [LoadDate]        DATETIME         NOT NULL DEFAULT GETDATE()  -- زمان بارگذاری
);
GO

-- جدول Finance.ETL_Log: لاگ مراحل ETL
CREATE TABLE Finance.ETLLog (
    [LogID]         INT IDENTITY(1,1) PRIMARY KEY,  -- کلید لاگ
    [TableName]     NVARCHAR(128)     NOT NULL,     -- نام جدول هدف
    [OperationType] NVARCHAR(50)      NOT NULL,     -- 'Truncate'/'Validate'/'Insert'/'Error'
    [StartTime]     DATETIME          NOT NULL DEFAULT GETDATE(),  -- زمان شروع
    [EndTime]       DATETIME,                       -- زمان پایان
    [Message]       NVARCHAR(2000)                  -- پیام وضعیت یا خطا
);
GO



-- Create the HumanResources schema if it does not already exist
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'HumanResources')
BEGIN
    EXEC('CREATE SCHEMA HumanResources;');
    PRINT 'Schema HumanResources created.';
END
GO

-- Create the Audit schema for the log table
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Audit')
BEGIN
    EXEC('CREATE SCHEMA Audit;');
    PRINT 'Schema Audit created.';
END
GO

--------------------------------------------------------------------------------
-- Staging Tables under the HumanResources Schema
--------------------------------------------------------------------------------

-- Staging table for Employee
CREATE TABLE HumanResources.Employee (
    EmployeeID INT,
    FullName NVARCHAR(100),
    Position NVARCHAR(50),
    NationalID VARCHAR(20),
    HireDate DATE,
    BirthDate DATE,
    Gender NVARCHAR(10),
    MaritalStatus NVARCHAR(20),
    Address NVARCHAR(200),
    Phone VARCHAR(20),
    Email VARCHAR(100),
    EmploymentStatus NVARCHAR(50),
    LoadDate DATETIME NOT NULL DEFAULT GETDATE()
);
GO

-- Staging table for Department
CREATE TABLE HumanResources.Department (
    DepartmentID INT,
    DepartmentName NVARCHAR(100),
    ManagerID INT,
    IsActive BIT,
    LoadDate DATETIME NOT NULL DEFAULT GETDATE()
);
GO

-- Staging table for JobTitle
CREATE TABLE HumanResources.JobTitle (
    JobTitleID INT,
    JobTitleName NVARCHAR(100),
    JobCategory NVARCHAR(50),
    BaseSalary DECIMAL(15, 2),
    IsActive BIT,
    LoadDate DATETIME NOT NULL DEFAULT GETDATE()
);
GO

-- Staging table for EmploymentHistory
CREATE TABLE HumanResources.EmploymentHistory (
    EmploymentHistoryID INT,
    EmployeeID INT,
    JobTitleID INT,
    DepartmentID INT,
    StartDate DATE,
    EndDate DATE,
    Salary DECIMAL(15, 2),
    EmploymentType NVARCHAR(50),
    LoadDate DATETIME NOT NULL DEFAULT GETDATE()
);
GO

-- Staging table for Termination
CREATE TABLE HumanResources.Termination (
    TerminationID INT,
    EmployeeID INT,
    TerminationDate DATE,
    TerminationReason NVARCHAR(100),
    Notes NVARCHAR(500),
    LoadDate DATETIME NOT NULL DEFAULT GETDATE()
);
GO

-- Staging table for Attendance
CREATE TABLE HumanResources.Attendance (
    AttendanceID INT,
    EmployeeID INT,
    AttendanceDate DATE,
    Status NVARCHAR(50),
    CheckInTime TIME,
    CheckOutTime TIME,
    HoursWorked DECIMAL(5, 2),
    Notes NVARCHAR(200),
    LoadDate DATETIME NOT NULL DEFAULT GETDATE()
);
GO

-- Staging table for SalaryPayment
CREATE TABLE HumanResources.SalaryPayment (
    SalaryPaymentID INT,
    EmployeeID INT,
    PaymentDate DATE,
    Amount DECIMAL(15, 2),
    Bonus DECIMAL(15, 2),
    Deductions DECIMAL(15, 2),
    NetAmount DECIMAL(15, 2),
    PaymentMethod NVARCHAR(50),
    ReferenceNumber VARCHAR(50),
    LoadDate DATETIME NOT NULL DEFAULT GETDATE()
);
GO

-- Staging table for TrainingProgram
CREATE TABLE HumanResources.TrainingProgram (
    TrainingProgramID INT,
    ProgramName NVARCHAR(100),
    Category NVARCHAR(50),
    DurationHours DECIMAL(5, 2),
    Cost DECIMAL(15, 2),
    IsActive BIT,
    LoadDate DATETIME NOT NULL DEFAULT GETDATE()
);
GO

-- Staging table for EmployeeTraining
CREATE TABLE HumanResources.EmployeeTraining (
    EmployeeTrainingID INT,
    EmployeeID INT,
    TrainingProgramID INT,
    TrainingDate DATE,
    Score DECIMAL(5, 2),
    CompletionStatus NVARCHAR(50),
    Notes NVARCHAR(200),
    LoadDate DATETIME NOT NULL DEFAULT GETDATE()
);
GO

-- Staging table for LeaveType
CREATE TABLE HumanResources.LeaveType (
    LeaveTypeID INT,
    LeaveTypeName NVARCHAR(50),
    IsPaid BIT,
    MaxDaysPerYear INT,
    LoadDate DATETIME NOT NULL DEFAULT GETDATE()
);
GO

-- Staging table for LeaveRequest
CREATE TABLE HumanResources.LeaveRequest (
    LeaveRequestID INT,
    EmployeeID INT,
    LeaveTypeID INT,
    StartDate DATE,
    EndDate DATE,
    Status NVARCHAR(50),
    RequestDate DATE,
    ApprovedBy INT,
    Notes NVARCHAR(200),
    LoadDate DATETIME NOT NULL DEFAULT GETDATE()
);
GO

--------------------------------------------------------------------------------
-- ETL Log Table under the Audit Schema
--------------------------------------------------------------------------------
CREATE TABLE Audit.ETLLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    TableName NVARCHAR(128) NOT NULL,
    OperationType NVARCHAR(50) NOT NULL,
    StartTime DATETIME NOT NULL,
    EndTime DATETIME NOT NULL,
    [Message] NVARCHAR(MAX)
);
GO


-- 1) Create Staging database if it does not already exist
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'StagingDB')
BEGIN
    CREATE DATABASE StagingDB;
END
GO

-- 2) Switch context to the Staging database
USE StagingDB;
GO

-- 3) Create Finance schema if not exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Common')  
    EXEC('CREATE SCHEMA Common');
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'HumanResources')
    EXEC('CREATE SCHEMA HumanResources');
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'PortOperations')
    EXEC('CREATE SCHEMA PortOperations');
GO
--------------------------------------------------------------------------------
-- 1) Common.Country
--------------------------------------------------------------------------------
CREATE TABLE Common.Country (
    CountryID    INT          NOT NULL,
    CountryName  NVARCHAR(100) NOT NULL,
    CountryCode  VARCHAR(10)   NULL
);
--------------------------------------------------------------------------------
-- 2) Common.OperationEquipmentAssignment
--------------------------------------------------------------------------------
CREATE TABLE Common.OperationEquipmentAssignment (
    AssignmentID INT           NOT NULL,
    CargoOpID    INT           NOT NULL,
    EquipmentID  INT           NOT NULL,
    EmployeeID   INT           NULL,
    StartTime    DATETIME      NULL,
    EndTime      DATETIME      NULL,
);
--------------------------------------------------------------------------------
-- 3) HumanResources.Employee
--------------------------------------------------------------------------------
CREATE TABLE HumanResources.Employee (
    EmployeeID       INT           NOT NULL,
    FullName         NVARCHAR(100) NOT NULL,
    Position         NVARCHAR(50)  NULL,
    NationalID       NVARCHAR(20)  NULL,
    HireDate         DATE          NULL,
    BirthDate        DATE          NULL,
    Gender           NVARCHAR(10)  NULL,
    MaritalStatus    NVARCHAR(20)  NULL,
    Address          NVARCHAR(200) NULL,
    Phone            NVARCHAR(20)  NULL,
    Email            NVARCHAR(100) NULL,
    EmploymentStatus NVARCHAR(20)  NULL
);
--------------------------------------------------------------------------------
-- 4) PortOperations.ContainerType
--------------------------------------------------------------------------------
CREATE TABLE PortOperations.ContainerType (
    ContainerTypeID INT           NOT NULL ,
    Description     NVARCHAR(50)  NOT NULL,
    MaxWeightKG     FLOAT         NOT NULL
);
--------------------------------------------------------------------------------
-- 5) PortOperations.EquipmentType
--------------------------------------------------------------------------------
CREATE TABLE PortOperations.EquipmentType (
    EquipmentTypeID INT           NOT NULL ,
    Description     NVARCHAR(50)  NOT NULL
);
--------------------------------------------------------------------------------
-- 6) PortOperations.Port
--------------------------------------------------------------------------------
CREATE TABLE PortOperations.Port (
    PortID    INT           NOT NULL ,
    Name      NVARCHAR(100) NOT NULL,
    Location  NVARCHAR(200) NULL
);
--------------------------------------------------------------------------------
-- 7) PortOperations.Ship
--------------------------------------------------------------------------------
CREATE TABLE PortOperations.Ship (
    ShipID      INT           NOT NULL ,
    IMO_Number  VARCHAR(20)   NOT NULL,
    Name        NVARCHAR(100) NOT NULL,
    CountryID   INT           NULL
);
--------------------------------------------------------------------------------
-- 8) PortOperations.Container
--------------------------------------------------------------------------------
CREATE TABLE PortOperations.Container (
    ContainerID     INT           NOT NULL ,
    ContainerNumber VARCHAR(20)   NOT NULL,
    ContainerTypeID INT           NOT NULL,
    OwnerCompany    NVARCHAR(100) NULL
);
--------------------------------------------------------------------------------
-- 9) PortOperations.Equipment
--------------------------------------------------------------------------------
CREATE TABLE PortOperations.Equipment (
    EquipmentID     INT           NOT NULL ,
    EquipmentTypeID INT           NOT NULL,
    Model           NVARCHAR(50)  NULL
);
--------------------------------------------------------------------------------
-- 10) PortOperations.Voyage
--------------------------------------------------------------------------------
CREATE TABLE PortOperations.Voyage (
    VoyageID        INT           NOT NULL ,
    ShipID          INT           NOT NULL,
    VoyageNumber    VARCHAR(50)   NOT NULL,
    DeparturePortID INT           NULL,
    ArrivalPortID   INT           NULL
);
--------------------------------------------------------------------------------
-- 11) PortOperations.PortCall
--------------------------------------------------------------------------------
CREATE TABLE PortOperations.PortCall (
    PortCallID        INT           NOT NULL ,
    VoyageID          INT           NOT NULL,
    PortID            INT           NOT NULL,
    ArrivalDateTime   DATETIME      NOT NULL,
    DepartureDateTime DATETIME      NULL,
    Status            NVARCHAR(50)  NULL
);
--------------------------------------------------------------------------------
-- 12) PortOperations.Berth
--------------------------------------------------------------------------------
CREATE TABLE PortOperations.Berth (
    BerthID       INT           NOT NULL ,
    PortID        INT           NOT NULL,
    Name          NVARCHAR(50)  NOT NULL,
    LengthMeters  FLOAT         NULL
);
--------------------------------------------------------------------------------
-- 13) PortOperations.BerthAllocation
--------------------------------------------------------------------------------
CREATE TABLE PortOperations.BerthAllocation (
    AllocationID    INT           NOT NULL ,
    PortCallID      INT           NOT NULL,
    BerthID         INT           NOT NULL,
    AllocationStart DATETIME      NULL,
    AllocationEnd   DATETIME      NULL,
    AssignedBy      NVARCHAR(100) NULL
);
--------------------------------------------------------------------------------
-- 14) PortOperations.Yard
--------------------------------------------------------------------------------
CREATE TABLE PortOperations.Yard (
    YardID    INT           NOT NULL ,
    PortID    INT           NOT NULL,
    Name      NVARCHAR(100) NOT NULL,
    UsageType NVARCHAR(50)  NULL
);
--------------------------------------------------------------------------------
-- 15) PortOperations.YardSlot
--------------------------------------------------------------------------------
CREATE TABLE PortOperations.YardSlot (
    YardSlotID INT           NOT NULL ,
    YardID     INT           NOT NULL,
    Block      VARCHAR(10)   NULL,
    RowNumber  INT           NULL,
    TierLevel  INT           NULL
);
--------------------------------------------------------------------------------
-- 16) PortOperations.ContainerYardMovement
--------------------------------------------------------------------------------
CREATE TABLE PortOperations.ContainerYardMovement (
    MovementID       INT       NOT NULL ,
    ContainerID      INT       NOT NULL,
    YardSlotID       INT       NOT NULL,
    MovementType     NVARCHAR(20) NOT NULL,
    MovementDateTime DATETIME  NOT NULL
);
--------------------------------------------------------------------------------
-- 17) PortOperations.CargoOperation
--------------------------------------------------------------------------------
CREATE TABLE PortOperations.CargoOperation (
    CargoOpID       INT           NOT NULL ,
    PortCallID      INT           NOT NULL,
    ContainerID     INT           NOT NULL,
    OperationType   NVARCHAR(20)  NOT NULL,
    OperationDateTime DATETIME     NOT NULL,
    Quantity        INT           NULL,
    WeightKG        FLOAT         NULL
);
GO

CREATE TABLE PortOperations.ETLLog ( 
    LogID          INT IDENTITY(1,1) ,
    TableName      NVARCHAR(128) NOT NULL,
    OperationType  NVARCHAR(50)  NOT NULL,
    StartTime      DATETIME      NOT NULL DEFAULT GETDATE(),
    EndTime        DATETIME      NULL,
    Message        NVARCHAR(2000) NULL
);



USE StagingDB;
GO

-- ----------------------------------------
-- Common.OperationEquipmentAssignment
-- FK: CargoOpID, EquipmentID, EmployeeID
-- ----------------------------------------
CREATE NONCLUSTERED INDEX IX_OEA_CargoOpID
  ON Common.OperationEquipmentAssignment(CargoOpID);
GO
CREATE NONCLUSTERED INDEX IX_OEA_EquipmentID
  ON Common.OperationEquipmentAssignment(EquipmentID);
GO
CREATE NONCLUSTERED INDEX IX_OEA_EmployeeID
  ON Common.OperationEquipmentAssignment(EmployeeID);
GO

-- ----------------------------------------
-- HumanResources.Employee
-- ----------------------------------------
CREATE NONCLUSTERED INDEX IX_Employee_NationalID
  ON HumanResources.Employee(NationalID);
GO
-- ----------------------------------------
-- PortOperations.Ship
-- FK: CountryID
-- ----------------------------------------
CREATE NONCLUSTERED INDEX IX_Ship_CountryID
  ON PortOperations.Ship(CountryID);
GO

-- ----------------------------------------
-- PortOperations.Container
-- FK: ContainerTypeID
-- ----------------------------------------
CREATE NONCLUSTERED INDEX IX_Container_ContainerTypeID
  ON PortOperations.Container(ContainerTypeID);
GO

-- ----------------------------------------
-- PortOperations.Equipment
-- FK: EquipmentTypeID
-- ----------------------------------------
CREATE NONCLUSTERED INDEX IX_Equipment_EquipmentTypeID
  ON PortOperations.Equipment(EquipmentTypeID);
GO

-- ----------------------------------------
-- PortOperations.Voyage
-- FK: ShipID, DeparturePortID, ArrivalPortID
-- ----------------------------------------
CREATE NONCLUSTERED INDEX IX_Voyage_ShipID
  ON PortOperations.Voyage(ShipID);
GO
CREATE NONCLUSTERED INDEX IX_Voyage_DeparturePortID
  ON PortOperations.Voyage(DeparturePortID);
GO
CREATE NONCLUSTERED INDEX IX_Voyage_ArrivalPortID
  ON PortOperations.Voyage(ArrivalPortID);
GO

-- ----------------------------------------
-- PortOperations.PortCall
-- FK: VoyageID, PortID
-- ----------------------------------------
CREATE NONCLUSTERED INDEX IX_PortCall_VoyageID
  ON PortOperations.PortCall(VoyageID);
GO
CREATE NONCLUSTERED INDEX IX_PortCall_PortID
  ON PortOperations.PortCall(PortID);
GO

-- ----------------------------------------
-- PortOperations.Berth
-- FK: PortID
-- ----------------------------------------
CREATE NONCLUSTERED INDEX IX_Berth_PortID
  ON PortOperations.Berth(PortID);
GO

-- ----------------------------------------
-- PortOperations.BerthAllocation
-- FK: PortCallID, BerthID
-- ----------------------------------------
CREATE NONCLUSTERED INDEX IX_BerthAlloc_PortCallID
  ON PortOperations.BerthAllocation(PortCallID);
GO
CREATE NONCLUSTERED INDEX IX_BerthAlloc_BerthID
  ON PortOperations.BerthAllocation(BerthID);
GO

-- ----------------------------------------
-- PortOperations.Yard
-- FK: PortID
-- ----------------------------------------
CREATE NONCLUSTERED INDEX IX_Yard_PortID
  ON PortOperations.Yard(PortID);
GO

-- ----------------------------------------
-- PortOperations.YardSlot
-- FK: YardID
-- ----------------------------------------
CREATE NONCLUSTERED INDEX IX_YardSlot_YardID
  ON PortOperations.YardSlot(YardID);
GO

-- ----------------------------------------
-- PortOperations.ContainerYardMovement
-- FK: ContainerID, YardSlotID
-- ----------------------------------------
CREATE NONCLUSTERED INDEX IX_CYM_ContainerID
  ON PortOperations.ContainerYardMovement(ContainerID);
GO
CREATE NONCLUSTERED INDEX IX_CYM_YardSlotID
  ON PortOperations.ContainerYardMovement(YardSlotID);
GO

-- ----------------------------------------
-- PortOperations.CargoOperation
-- FK: PortCallID, ContainerID
-- ----------------------------------------
CREATE NONCLUSTERED INDEX IX_CO_PortCallID
  ON PortOperations.CargoOperation(PortCallID);
GO
CREATE NONCLUSTERED INDEX IX_CO_ContainerID
  ON PortOperations.CargoOperation(ContainerID);
GO

-- ----------------------------------------
-- ETL Log (Common or PortOperations.ETLLog)
-- ----------------------------------------
IF OBJECT_ID('PortOperations.ETLLog','U') IS NOT NULL
BEGIN
  CREATE NONCLUSTERED INDEX IX_ETLLog_TableName
    ON PortOperations.ETLLog(TableName);
  CREATE NONCLUSTERED INDEX IX_ETLLog_OperationType
    ON PortOperations.ETLLog(OperationType);
END
GO



-- Indexes for Finance.Customer
CREATE NONCLUSTERED INDEX IX_Customer_CustomerID ON Finance.Customer(CustomerID);
CREATE NONCLUSTERED INDEX IX_Customer_CustomerCode ON Finance.Customer(CustomerCode);

-- Indexes for Finance.BillingCycle
CREATE NONCLUSTERED INDEX IX_BillingCycleID ON Finance.BillingCycle(BillingCycleID);

-- Indexes for Finance.ServiceType
CREATE NONCLUSTERED INDEX IX_ServiceTypeID ON Finance.ServiceType(ServiceTypeID);
CREATE NONCLUSTERED INDEX IX_ServiceType_IsActive ON Finance.ServiceType(IsActive);

-- Indexes for Finance.Tax
CREATE NONCLUSTERED INDEX IX_Tax_TaxID ON Finance.Tax(TaxID);
CREATE NONCLUSTERED INDEX IX_Tax_TaxType ON Finance.Tax(TaxType);

-- Indexes for Finance.Tariff
CREATE NONCLUSTERED INDEX IX_Tariff_ServiceTypeID ON Finance.Tariff(ServiceTypeID);

-- Indexes for Finance.Contract
CREATE NONCLUSTERED INDEX IX_Contract_ContractID ON Finance.Contract(ContractID);
CREATE NONCLUSTERED INDEX IX_Contract_CustomerID ON Finance.Contract(CustomerID);
CREATE NONCLUSTERED INDEX IX_Contract_BillingCycleID ON Finance.Contract(BillingCycleID);

-- Indexes for Finance.Invoice
CREATE NONCLUSTERED INDEX IX_Invoice_InvoiceID ON Finance.Invoice(InvoiceID);
CREATE NONCLUSTERED INDEX IX_Invoice_ContractID ON Finance.Invoice(ContractID);
CREATE NONCLUSTERED INDEX IX_Invoice_InvoiceNumber ON Finance.Invoice(InvoiceNumber);

-- Indexes for Finance.InvoiceLine
CREATE NONCLUSTERED INDEX IX_InvoiceLine_InvoiceID ON Finance.InvoiceLine(InvoiceID);
CREATE NONCLUSTERED INDEX IX_InvoiceLine_ServiceTypeID ON Finance.InvoiceLine(ServiceTypeID);
CREATE NONCLUSTERED INDEX IX_InvoiceLine_TaxID ON Finance.InvoiceLine(TaxID);

-- Indexes for Finance.Payment
CREATE NONCLUSTERED INDEX IX_Payment_InvoiceID ON Finance.Payment(InvoiceID);
CREATE NONCLUSTERED INDEX IX_Payment_ReferenceNumber ON Finance.Payment(ReferenceNumber);

-- Indexes for Finance.RevenueRecognition
CREATE NONCLUSTERED INDEX IX_RevenueRecognition_InvoiceID ON Finance.RevenueRecognition(InvoiceID);

-- Index for Finance.ETLLog
CREATE NONCLUSTERED INDEX IX_ETLLog_TableName ON Finance.ETLLog(TableName);
