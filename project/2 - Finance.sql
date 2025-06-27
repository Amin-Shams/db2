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
    [Address]          NVARCHAR(200)  NULL,
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
    [Status]           NVARCHAR(50)   NOT NULL, -- e.g., 'Paid', 'Overdue', 'Cancelled'
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
