USE TradePortDB;
GO

UPDATE TradePortDB.Finance.Customer
SET Phone = '09121234567'
WHERE CustomerCode = 'CUST0001';
-- 1	CUST0001	Stewart-Gallegos	Foreign	jb9261TP	VAT553ct	858.365.9792	ccarr@bennett.biz	17760 Michael Bypass Apt. 281, Harrisshire, AZ 16934	12

UPDATE TradePortDB.Finance.Tax
SET TaxRate = 0.18
WHERE TaxName = 'Tax001';
-- 1	Tax001	0.02	Service	2025-06-25	2025-06-23

UPDATE TradePortDB.Finance.ServiceType
SET ServiceName = 'Updated Freight Service'
WHERE ServiceTypeID = 1;
-- 1	Orchestrate Frictionless Partnerships	Unloading	260.38	Hour	1	1

UPDATE TradePortDB.Finance.Contract
SET ContractStatus = 'Active'
WHERE ContractNumber = 'CON000001';
-- 1	175	CON000001	2025-06-23	2025-06-25	5	Prepaid	Expired	2025-06-24 00:00:00.000


INSERT INTO Finance.Customer
(CustomerID, CustomerCode, CustomerName, CustomerType, TIN, VATNumber, Phone, Email, Address, CountryID)
VALUES
(900001, 'TEST_CUST_0', 'TestCo 0', 'Company', 'TIN0', 'VAT0', '+989123456789', 'test0@example.com', 'Test Address 0', 1),
(900002, 'TEST_CUST_1', 'TestCo 1', 'Company', 'TIN1', 'VAT1', '+989123456789', 'test1@example.com', 'Test Address 1', 1);


INSERT INTO Finance.Contract
(ContractID, CustomerID, ContractNumber, StartDate, EndDate, BillingCycleID, PaymentTerms, ContractStatus, CreatedDate)
VALUES
(910001, 900001, 'TEST_CON_0', '2025-06-26', '2025-06-26', 1, 'Net 30', 'Active', '2025-06-26'),
(910002, 900001, 'TEST_CON_1', '2025-06-26', '2025-06-26', 1, 'Net 30', 'Active', '2025-06-26'),
(910003, 900001, 'TEST_CON_2', '2025-06-26', '2025-06-26', 1, 'Net 30', 'Active', '2025-06-26');


INSERT INTO Finance.Invoice
(InvoiceID, ContractID, InvoiceNumber, InvoiceDate, DueDate, Status, TotalAmount, TaxAmount, CreatedBy, CreatedDate)
VALUES
(920001, 910002, 'TEST_INV_0', '2025-06-26', '2025-06-26', 'Paid', 973.02, 247.50, 'tester0', '2025-06-26'),
(920002, 910001, 'TEST_INV_1', '2025-06-26', '2025-06-26', 'Paid', 538.43, 93.48, 'tester1', '2025-06-26'),
(920003, 910002, 'TEST_INV_2', '2025-06-26', '2025-06-26', 'Paid', 949.38, 220.84, 'tester2', '2025-06-26'),
(920004, 910001, 'TEST_INV_3', '2025-06-26', '2025-06-26', 'Paid', 578.56, 203.14, 'tester3', '2025-06-26'),
(920005, 910002, 'TEST_INV_4', '2025-06-26', '2025-06-26', 'Paid', 683.59, 173.26, 'tester4', '2025-06-26');


INSERT INTO Finance.Payment
(PaymentID, InvoiceID, PaymentDate, Amount, PaymentMethod, ConfirmedBy, ReferenceNumber, Notes)
VALUES
(930001, 920002, '2025-06-26', 674.39, 'Transfer', 'confirmer0', 'TEST_REF_0', 'Test payment'),
(930002, 920005, '2025-06-26', 486.00, 'Transfer', 'confirmer1', 'TEST_REF_1', 'Test payment'),
(930003, 920005, '2025-06-26', 1354.74, 'Transfer', 'confirmer2', 'TEST_REF_2', 'Test payment'),
(930004, 920004, '2025-06-26', 993.09, 'Transfer', 'confirmer3', 'TEST_REF_3', 'Test payment'),
(930005, 920001, '2025-06-26', 1251.35, 'Transfer', 'confirmer4', 'TEST_REF_4', 'Test payment'),
(930006, 920002, '2025-06-26', 956.60, 'Cash',     'confirmer5', 'TEST_REF_5', 'Test payment'),
(930007, 920004, '2025-06-26', 670.58, 'Cash',     'confirmer6', 'TEST_REF_6', 'Test payment');



INSERT INTO Finance.InvoiceLine (
    InvoiceLineID, InvoiceID, ServiceTypeID, TaxID,
    Quantity, UnitPrice, DiscountPercent, TaxAmount, NetAmount
)
VALUES
(1000001, 1, 1, 1, 5, 120.00, 0.05, 30.00, 570.00),
(1000002, 2, 2, 2, 3, 250.00, 0.10, 45.00, 675.00),
(1000003, 3, 3, 3, 2, 180.00, 0.00, 25.00, 360.00),
(1000004, 4, 4, 4, 7, 90.00, 0.15, 35.00, 535.50),
(1000005, 5, 5, 5, 1, 499.00, 0.00, 70.00, 499.00),
(1000006, 6, 6, 6, 10, 80.00, 0.20, 60.00, 640.00),
(1000007, 7, 7, 7, 4, 150.00, 0.05, 20.00, 570.00),
(1000008, 8, 8, 8, 6, 99.99, 0.00, 29.99, 599.94),
(1000009, 9, 9, 9, 3, 300.00, 0.10, 50.00, 810.00),
(1000010, 10, 10, 11, 8, 110.00, 0.05, 40.00, 836.00);
