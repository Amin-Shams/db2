-- 1) Create TradePortDB database if it does not already exist
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'TradePortDB')
BEGIN
    CREATE DATABASE TradePortDB;
END
GO

-- 2) Switch context to the TradePortDB database
USE TradePortDB;
GO

/*------------------------------------------
    Source Schemas
------------------------------------------*/
-- Create Finance schema if not exists
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Finance')
BEGIN
    EXEC('CREATE SCHEMA Finance;');
END
GO

-- Create PortOperations schema if not exists
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'PortOperations')
BEGIN
    EXEC('CREATE SCHEMA PortOperations;');
END
GO

-- Create HumanResources schema if not exists
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'HumanResources')
BEGIN
    EXEC('CREATE SCHEMA HumanResources;');
END
GO

-- Create Common schema if not exists
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Common')
BEGIN
    EXEC('CREATE SCHEMA Common;');
END
GO

-- ----------------------------------
-- جداول مرجع بدون وابستگی
-- ----------------------------------

-- جدول Common.Country
-- توضیح: اطلاعات کشورها برای Customer و Ship
-- دسته‌بندی: مشترک (مدیریت امور مالی و عملیات بندر)
CREATE TABLE Common.Country (
    CountryID        INT            PRIMARY KEY,
    CountryName      NVARCHAR(100)  NOT NULL,
    CountryCode      VARCHAR(10)    NULL
);
GO

-- جدول Finance.ServiceType
-- توضیح: انواع خدمات مالی قابل فاکتور شدن
-- دسته‌بندی: مدیریت امور مالی
CREATE TABLE Finance.ServiceType (
    ServiceTypeID    INT            PRIMARY KEY,
    ServiceName      NVARCHAR(100)  NOT NULL,
    ServiceCategory  NVARCHAR(50)   NULL, -- e.g., 'Unloading', 'Storage', 'Transport'
    BaseRate         DECIMAL(15,2)  NOT NULL,
    UnitOfMeasure    NVARCHAR(50)   NOT NULL, -- e.g., 'TEU', 'Hour'
    Taxable          BIT            NOT NULL DEFAULT 1,
    IsActive         BIT            NOT NULL DEFAULT 1
);
GO

-- جدول Finance.BillingCycle
-- توضیح: انواع دوره‌های صدور فاکتور
-- دسته‌بندی: مدیریت امور مالی
CREATE TABLE Finance.BillingCycle (
    BillingCycleID    INT            PRIMARY KEY,
    CycleName         NVARCHAR(50)   NOT NULL, -- e.g., 'Monthly', 'Quarterly'
    CycleLengthInDays INT            NOT NULL
);
GO

-- جدول Finance.Tax
-- توضیح: تعریف انواع مالیات و نرخ‌های آنها برای محاسبه مالیات در فاکتورها
-- دسته‌بندی: مدیریت امور مالی
CREATE TABLE Finance.Tax (
    TaxID            INT            PRIMARY KEY,
    TaxName          NVARCHAR(50)   NOT NULL, -- e.g., 'VAT'
    TaxRate          DECIMAL(5,2)   NOT NULL, -- e.g., 0.09 for 9% VAT
    TaxType          NVARCHAR(50)   NOT NULL, -- e.g., 'National', 'Service'
    EffectiveFrom    DATE           NOT NULL,
    EffectiveTo      DATE           NULL
);
GO

-- جدول PortOperations.ContainerType
-- توضیح: انواع کانتینرها
-- دسته‌بندی: مدیریت عملیات بندر
CREATE TABLE PortOperations.ContainerType (
    ContainerTypeID  INT            PRIMARY KEY,
    Description      NVARCHAR(50)   NOT NULL,
    MaxWeightKG      FLOAT          NOT NULL
);
GO

-- جدول PortOperations.EquipmentType
-- توضیح: انواع تجهیزات (مثل جرثقیل، لیفتراک)
-- دسته‌بندی: مدیریت عملیات بندر
CREATE TABLE PortOperations.EquipmentType (
    EquipmentTypeID  INT            PRIMARY KEY,
    Description      NVARCHAR(50)   NOT NULL
);
GO

-- جدول PortOperations.Port
-- توضیح: اطلاعات بنادر
-- دسته‌بندی: مدیریت عملیات بندر
CREATE TABLE PortOperations.Port (
    PortID           INT            PRIMARY KEY,
    Name             NVARCHAR(100)  NOT NULL,
    Location         NVARCHAR(200)  NULL
);
GO

-- جدول PortOperations.Ship
-- توضیح: اطلاعات کشتی‌ها
-- دسته‌بندی: مدیریت عملیات بندر
CREATE TABLE PortOperations.Ship (
    ShipID           INT            PRIMARY KEY,
    IMO_Number       VARCHAR(20)    NOT NULL UNIQUE,
    Name             NVARCHAR(100)  NOT NULL,
    CountryID        INT            NULL
        FOREIGN KEY REFERENCES Common.Country(CountryID)
);
GO

-- جدول HumanResources.JobTitle
-- توضیح: عنوان‌های شغلی
-- دسته‌بندی: مدیریت منابع انسانی
CREATE TABLE HumanResources.JobTitle (
    JobTitleID        INT            PRIMARY KEY,
    JobTitleName      NVARCHAR(100)  NOT NULL,
    JobCategory       NVARCHAR(50)   NULL, -- e.g., 'Operational', 'Administrative'
    BaseSalary        DECIMAL(15,2)  NULL,
    IsActive          BIT            NOT NULL DEFAULT 1
);
GO

-- جدول HumanResources.LeaveType
-- توضیح: انواع مرخصی
-- دسته‌بندی: مدیریت منابع انسانی
CREATE TABLE HumanResources.LeaveType (
    LeaveTypeID        INT            PRIMARY KEY,
    LeaveTypeName      NVARCHAR(50)   NOT NULL,
    IsPaid             BIT            NOT NULL DEFAULT 1,
    MaxDaysPerYear     INT            NULL
);
GO

-- جدول HumanResources.TrainingProgram
-- توضیح: برنامه‌های آموزشی
-- دسته‌بندی: مدیریت منابع انسانی
CREATE TABLE HumanResources.TrainingProgram (
    TrainingProgramID  INT            PRIMARY KEY,
    ProgramName        NVARCHAR(100)  NOT NULL,
    Category           NVARCHAR(50)   NULL, -- e.g., 'Security', 'Technical'
    DurationHours      DECIMAL(5,2)   NOT NULL,
    Cost               DECIMAL(15,2)  NULL,
    IsActive           BIT            NOT NULL DEFAULT 1
);
GO

-- ----------------------------------
-- جداول مرجع با وابستگی
-- ----------------------------------

-- جدول HumanResources.Employee
-- Responsibility: اطلاعات کامل کارکنان (ادغام Worker و Employee قبلی)
-- دسته‌بندی: مدیریت منابع انسانی
CREATE TABLE HumanResources.Employee (
    EmployeeID        INT            PRIMARY KEY,
    FullName         NVARCHAR(100)  NOT NULL,
    Position         NVARCHAR(50)   NULL,
    NationalID       VARCHAR(20)    NOT NULL UNIQUE,
    HireDate         DATE           NOT NULL,
    BirthDate        DATE           NULL,
    Gender           NVARCHAR(10)   NULL,
    MaritalStatus    NVARCHAR(20)   NULL,
    Address          NVARCHAR(200)  NULL,
    Phone            VARCHAR(20)    NULL,
    Email            VARCHAR(100)   NULL,
    EmploymentStatus NVARCHAR(50)   NOT NULL -- e.g., 'Active', 'Terminated', 'OnLeave'
);
GO

-- جدول HumanResources.Department
-- توضیح: دپارتمان‌های سازمانی
-- دسته‌بندی: مدیریت منابع انسانی
CREATE TABLE HumanResources.Department (
    DepartmentID      INT            PRIMARY KEY,
    DepartmentName    NVARCHAR(100)  NOT NULL,
    ManagerID         INT            NULL
        FOREIGN KEY REFERENCES HumanResources.Employee(EmployeeID),
    IsActive          BIT            NOT NULL DEFAULT 1
);
GO

-- ----------------------------------
-- جداول عملیاتی و تراکنشی
-- ----------------------------------

-- جدول Finance.Customer
-- توضیح: اطلاعات مشتریان (شرکت‌ها، خطوط کشتیرانی، نمایندگان)
-- دسته‌بندی: مدیریت امور مالی
CREATE TABLE Finance.Customer (
    CustomerID       INT            PRIMARY KEY,
    CustomerCode     VARCHAR(20)    NOT NULL UNIQUE,
    CustomerName     NVARCHAR(100)  NOT NULL,
    CustomerType     NVARCHAR(50)   NOT NULL, -- e.g., 'Individual', 'Company', 'Domestic', 'Foreign'
    TIN              VARCHAR(20)    NULL,
    VATNumber        VARCHAR(20)    NULL,
    Phone            VARCHAR(20)    NULL,
    Email            VARCHAR(100)   NULL,
    Address          NVARCHAR(200)  NULL,
    CountryID        INT            NULL
        FOREIGN KEY REFERENCES Common.Country(CountryID)
);
GO

-- جدول Finance.Contract
-- توضیح: قراردادهای تجاری بین بندر و مشتریان
-- دسته‌بندی: مدیریت امور مالی
CREATE TABLE Finance.Contract (
    ContractID       INT            PRIMARY KEY,
    CustomerID       INT            NOT NULL
        FOREIGN KEY REFERENCES Finance.Customer(CustomerID),
    ContractNumber   VARCHAR(50)    NOT NULL UNIQUE,
    StartDate        DATE           NOT NULL,
    EndDate          DATE           NULL,
    BillingCycleID   INT            NOT NULL
        FOREIGN KEY REFERENCES Finance.BillingCycle(BillingCycleID),
    PaymentTerms     NVARCHAR(100)  NULL, -- e.g., 'Net 30 Days'
    ContractStatus   NVARCHAR(50)   NOT NULL, -- e.g., 'Active', 'Expired'
    CreatedDate      DATETIME       NOT NULL DEFAULT GETDATE()
);
GO

-- جدول Finance.Invoice
-- توضیح: فاکتورهای صادر شده به مشتری
-- دسته‌بندی: مدیریت امور مالی
CREATE TABLE Finance.Invoice (
    InvoiceID        INT            PRIMARY KEY,
    ContractID       INT            NOT NULL
        FOREIGN KEY REFERENCES Finance.Contract(ContractID),
    InvoiceNumber    VARCHAR(50)    NOT NULL UNIQUE,
    InvoiceDate      DATE           NOT NULL,
    DueDate          DATE           NOT NULL,
    Status           NVARCHAR(50)   NOT NULL, -- e.g., 'Paid', 'Overdue', 'Cancelled'
    TotalAmount      DECIMAL(15,2)  NOT NULL,
    TaxAmount        DECIMAL(15,2)  NOT NULL,
    CreatedBy         NVARCHAR(100)  NULL,
    CreatedDate      DATETIME       NOT NULL DEFAULT GETDATE()
);
GO

-- جدول Finance.InvoiceLine
-- توضیح: جزئیات آیتم‌های فاکتور
-- دسته‌بندی: مدیریت امور مالی
CREATE TABLE Finance.InvoiceLine (
    InvoiceLineID    INT            PRIMARY KEY,
    InvoiceID        INT            NOT NULL
        FOREIGN KEY REFERENCES Finance.Invoice(InvoiceID),
    ServiceTypeID    INT            NOT NULL
        FOREIGN KEY REFERENCES Finance.ServiceType(ServiceTypeID),
    TaxID            INT            NULL
        FOREIGN KEY REFERENCES Finance.Tax(TaxID),
    Quantity         INT            NOT NULL,
    UnitPrice        DECIMAL(15,2)  NOT NULL,
    DiscountPercent  DECIMAL(5,2)   NULL,
    TaxAmount        DECIMAL(15,2)  NOT NULL,
    NetAmount        DECIMAL(15,2)  NOT NULL
);
GO

-- جدول Finance.Payment
-- توضیح: پرداخت‌های مشتری برای فاکتورها
-- دسته‌بندی: مدیریت امور مالی
CREATE TABLE Finance.Payment (
    PaymentID        INT            PRIMARY KEY,
    InvoiceID        INT            NOT NULL
        FOREIGN KEY REFERENCES Finance.Invoice(InvoiceID),
    PaymentDate      DATE           NOT NULL,
    Amount           DECIMAL(15,2)  NOT NULL,
    PaymentMethod    NVARCHAR(50)   NOT NULL, -- e.g., 'Cash', 'Card', 'Transfer'
    ConfirmedBy      NVARCHAR(100)  NULL,
    ReferenceNumber  VARCHAR(50)    NULL,
    Notes            NVARCHAR(500)  NULL
);
GO

-- جدول Finance.RevenueRecognition
-- توضیح: ثبت درآمد واقعی و زمان‌بندی آن
-- دسته‌بندی: مدیریت امور مالی
CREATE TABLE Finance.RevenueRecognition (
    RecognitionID    INT            PRIMARY KEY,
    InvoiceID        INT NOT NULL
        FOREIGN KEY REFERENCES Finance.Invoice(InvoiceID),
    DateRecognized   DATE           NOT NULL,
    Amount           DECIMAL(15,2)  NOT NULL,
    Notes            NVARCHAR(500)  NULL
);
GO

-- جدول Finance.Tariff
-- توضیح: تعرفه‌های خدمات در بازه‌های زمانی
-- دسته‌بندی: مدیریت امور مالی
CREATE TABLE Finance.Tariff (
    TariffID         INT            PRIMARY KEY,
    ServiceTypeID    INT            NOT NULL
        FOREIGN KEY REFERENCES Finance.ServiceType(ServiceTypeID),
    ValidFrom        DATE           NOT NULL,
    ValidTo          DATE           NULL,
    UnitRate         DECIMAL(15,2)  NOT NULL
);
GO

-- جدول PortOperations.Voyage
-- توضیح: سفرهای دریایی
-- دسته‌بندی: مدیریت عملیات بندر
CREATE TABLE PortOperations.Voyage (
    VoyageID         INT            PRIMARY KEY,
    ShipID           INT            NOT NULL
        FOREIGN KEY REFERENCES PortOperations.Ship(ShipID),
    VoyageNumber     VARCHAR(50)    NOT NULL,
    DeparturePortID  INT            NULL
        FOREIGN KEY REFERENCES PortOperations.Port(PortID),
    ArrivalPortID    INT            NULL
        FOREIGN KEY REFERENCES PortOperations.Port(PortID)
);
GO

-- جدول PortOperations.PortCall
-- توضیح: نوبت ورود/خروج در هر بندر
-- دسته‌بندی: مدیریت عملیات بندر
CREATE TABLE PortOperations.PortCall (
    PortCallID       INT            PRIMARY KEY,
    VoyageID         INT            NOT NULL
        FOREIGN KEY REFERENCES PortOperations.Voyage(VoyageID),
    PortID           INT            NOT NULL
        FOREIGN KEY REFERENCES PortOperations.Port(PortID),
    ArrivalDateTime  DATETIME       NOT NULL,
    DepartureDateTime DATETIME      NULL,
    Status           NVARCHAR(50)   NULL -- e.g., 'Docked', 'Departed'
);
GO

-- جدول PortOperations.Berth
-- توضیح: اسکله‌های هر بندر
-- دسته‌بندی: مدیریت عملیات بندر
CREATE TABLE PortOperations.Berth (
    BerthID          INT            PRIMARY KEY,
    PortID           INT            NOT NULL
        FOREIGN KEY REFERENCES PortOperations.Port(PortID),
    Name             NVARCHAR(50)   NOT NULL,
    LengthMeters     FLOAT          NULL
);
GO

-- جدول PortOperations.BerthAllocation
-- توضیح: تخصیص اسکله به یک PortCall
-- دسته‌بندی: مدیریت عملیات بندر
CREATE TABLE PortOperations.BerthAllocation (
    AllocationID     INT            PRIMARY KEY,
    PortCallID       INT            NOT NULL
        FOREIGN KEY REFERENCES PortOperations.PortCall(PortCallID),
    BerthID          INT            NOT NULL
        FOREIGN KEY REFERENCES PortOperations.Berth(BerthID),
    AllocationStart  DATETIME       NOT NULL,
    AllocationEnd    DATETIME       NULL,
    AssignedBy       NVARCHAR(100)  NULL
);
GO

-- جدول PortOperations.Container
-- توضیح: اطلاعات کانتینرها
-- دسته‌بندی: مدیریت عملیات بندر
CREATE TABLE PortOperations.Container (
    ContainerID      INT            PRIMARY KEY,
    ContainerNumber  VARCHAR(20)    NOT NULL UNIQUE,
    ContainerTypeID  INT            NOT NULL
        FOREIGN KEY REFERENCES PortOperations.ContainerType(ContainerTypeID),
    OwnerCompany     NVARCHAR(100)  NULL
);
GO

-- جدول PortOperations.CargoOperation
-- توضیح: عملیات بارگیری یا تخلیه
-- دسته‌بندی: مدیریت عملیات بندر
CREATE TABLE PortOperations.CargoOperation (
    CargoOpID        INT            PRIMARY KEY,
    PortCallID       INT            NOT NULL
        FOREIGN KEY REFERENCES PortOperations.PortCall(PortCallID),
    ContainerID      INT            NOT NULL
        FOREIGN KEY REFERENCES PortOperations.Container(ContainerID),
    OperationType    NVARCHAR(20)   NOT NULL, -- e.g., 'LOAD', 'UNLOAD'
    OperationDateTime DATETIME       NOT NULL,
    Quantity         INT            NULL,
    WeightKG         FLOAT          NULL
);
GO

-- جدول PortOperations.Equipment
-- توضیح: تجهیزات بندری (مثل جرثقیل، لیفتراک)
-- دسته‌بندی: مدیریت عملیات بندر
CREATE TABLE PortOperations.Equipment (
    EquipmentID      INT            PRIMARY KEY,
    EquipmentTypeID  INT            NOT NULL
        FOREIGN KEY REFERENCES PortOperations.EquipmentType(EquipmentTypeID),
    Model            NVARCHAR(50)   NULL
);
GO

-- جدول PortOperations.Yard
-- توضیح: محوطه‌های یارد در بندر
-- دسته‌بندی: مدیریت عملیات بندر
CREATE TABLE PortOperations.Yard (
    YardID           INT            PRIMARY KEY,
    PortID           INT            NOT NULL
        FOREIGN KEY REFERENCES PortOperations.Port(PortID),
    Name             NVARCHAR(100)  NOT NULL,
    UsageType        NVARCHAR(50)   NULL -- e.g., 'IMPORT', 'EXPORT', 'EMPTY'
);
GO

-- جدول PortOperations.YardSlot
-- توضیح: خانه‌های تعریف‌شده در یارد
-- دسته‌بندی: مدیریت عملیات بندر
CREATE TABLE PortOperations.YardSlot (
    YardSlotID       INT            PRIMARY KEY,
    YardID           INT            NOT NULL
        FOREIGN KEY REFERENCES PortOperations.Yard(YardID),
    Block            VARCHAR(10)    NULL,
    RowNumber        INT            NULL,
    TierLevel        INT            NULL
);
GO

-- جدول PortOperations.ContainerYardMovement
-- توضیح: جابجایی کانتینرها در یارد
-- دسته‌بندی: مدیریت عملیات
CREATE TABLE PortOperations.ContainerYardMovement (
    MovementID       INT            PRIMARY KEY,
    ContainerID      INT            NOT NULL
        FOREIGN KEY REFERENCES PortOperations.Container(ContainerID),
    YardSlotID       INT            NOT NULL
        FOREIGN KEY REFERENCES PortOperations.YardSlot(YardSlotID),
    MovementType     NVARCHAR(20)   NOT NULL, -- e.g., 'IN', 'OUT', 'RELOCATE'
    MovementDateTime DATETIME       NOT NULL
);
GO

-- جدول HumanResources.EmploymentHistory
-- توضیح: سابقه کار کارکنان
-- دسته‌بندی: مدیریت منابع انسانی
CREATE TABLE HumanResources.EmploymentHistory (
    EmploymentHistoryID INT PRIMARY KEY,
    EmployeeID INT NOT NULL
        FOREIGN KEY REFERENCES HumanResources.Employee(EmployeeID),
    JobTitleID INT NOT NULL
        FOREIGN KEY REFERENCES HumanResources.JobTitle(JobTitleID),
    DepartmentID INT NOT NULL
        FOREIGN KEY REFERENCES HumanResources.Department(DepartmentID),
    StartDate DATE NOT NULL,
    EndDate DATE NULL,
    Salary DECIMAL(15,2) NOT NULL,
    EmploymentType NVARCHAR(50) NOT NULL -- e.g., 'Permanent', 'Contract', 'PartTime'
);
GO

-- جدول HumanResources.Termination
-- توضیح: ثبت خاتمه کار کارکنان
-- دسته‌بندی: مدیریت منابع انسانی
CREATE TABLE HumanResources.Termination (
    TerminationID INT PRIMARY KEY,
    EmployeeID INT NOT NULL
        FOREIGN KEY REFERENCES HumanResources.Employee(EmployeeID),
    TerminationDate DATE NOT NULL,
    TerminationReason NVARCHAR(100) NOT NULL, -- e.g., 'Resignation', 'Termination'
    Notes NVARCHAR(500) NULL
);
GO

-- جدول HumanResources.Attendance
-- توضیح: حضور و غیاب کارکنان
-- دسته‌بندی: اسناد رسمی
CREATE TABLE HumanResources.Attendance (
    AttendanceID INT PRIMARY KEY,
    EmployeeID INT NOT NULL
        FOREIGN KEY REFERENCES HumanResources.Employee(EmployeeID),
    AttendanceDate DATE NOT NULL,
    Status NVARCHAR(50) NOT NULL, -- e.g., 'Present', 'Absent', 'Late'
    CheckInTime TIME NULL,
    CheckOutTime TIME NULL,
    HoursWorked DECIMAL(5,2) NULL,
    Notes NVARCHAR(500) NULL
);
GO

-- جدول HumanResources.SalaryPayment
-- توضیح: پرداخت حقوق و دستمزد
-- دسته‌بندی: مدیریت منابع انسانی
CREATE TABLE HumanResources.SalaryPayment (
    SalaryPaymentID INT PRIMARY KEY,
    EmployeeID INT NOT NULL
        FOREIGN KEY REFERENCES HumanResources.Employee(EmployeeID),
    PaymentDate DATE NOT NULL,
    Amount DECIMAL(15,2) NOT NULL,
    Bonus DECIMAL(15,2) NULL,
    Deductions DECIMAL(15,2) NULL,
    NetAmount DECIMAL(15,2) NOT NULL,
    PaymentMethod NVARCHAR(50) NOT NULL, -- e.g., 'BankTransfer', 'Cash'
    ReferenceNumber VARCHAR(50) NULL
);
GO

-- جدول HumanResources.EmployeeTraining
-- توضیح: آموزش‌های کارکنان
-- دسته‌بندی: مدیریت منابع انسانی
CREATE TABLE HumanResources.EmployeeTraining (
    EmployeeTrainingID INT PRIMARY KEY,
    EmployeeID INT NOT NULL
        FOREIGN KEY REFERENCES HumanResources.Employee(EmployeeID),
    TrainingProgramID INT NOT NULL
        FOREIGN KEY REFERENCES HumanResources.TrainingProgram(TrainingProgramID),
    TrainingDate DATE NOT NULL,
    Score DECIMAL(5,2) NULL,
    CompletionStatus NVARCHAR(50) NOT NULL, -- e.g., 'Completed', 'Failed'
    Notes NVARCHAR(200) NULL,
);
GO

-- جدول HumanResources.LeaveRequest
-- توضیح: درخواست‌های مرخصی کارکنان
-- دسته‌بندی: مدیریت منابع انسانی
CREATE TABLE HumanResources.LeaveRequest (
    LeaveRequestID INT PRIMARY KEY,
    EmployeeID INT NOT NULL
        FOREIGN KEY REFERENCES HumanResources.Employee(EmployeeID),
    LeaveTypeID INT NOT NULL
        FOREIGN KEY REFERENCES HumanResources.LeaveType(LeaveTypeID),
    StartDate DATE NOT NULL,
    EndDate DATE NOT NULL,
    Status NVARCHAR(50) NOT NULL, -- e.g., 'Approved', 'Rejected', 'Pending'
    RequestDate DATE NOT NULL,
    ApprovedBy INT NULL
        FOREIGN KEY REFERENCES HumanResources.Employee(EmployeeID),
    Notes NVARCHAR(200) NULL
);
GO

-- ----------------------------------

-- جداول مشترک
-- ----------------------------------

-- جدول Common.OperationEquipmentAssignment
-- توضیح: تخصیص تجهیزات و اپراتورها به عملیات بارگیری/تخلیه
-- دسته‌بندی: مشترک (مدیریت عملیات بندر و منابع انسانی)
CREATE TABLE Common.OperationEquipmentAssignment (
    AssignmentID INT PRIMARY KEY,
    CargoOpID INT NOT NULL
        FOREIGN KEY REFERENCES PortOperations.CargoOperation(CargoOpID),
    EquipmentID INT NOT NULL
        FOREIGN KEY REFERENCES PortOperations.Equipment(EquipmentID),
    EmployeeID INT NULL
        FOREIGN KEY REFERENCES HumanResources.Employee(EmployeeID),
    StartTime DATETIME NULL,
    EndTime DATETIME NULL
);
GO
