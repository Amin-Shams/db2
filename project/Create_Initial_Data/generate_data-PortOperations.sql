---- غیرفعال‌سازی موقت همه‌ی قیدهای FK 
--EXEC sp_msforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL';

---- ۱. پاک‌سازی جداول Fact و Factless و عملیاتی
--DELETE FROM Common.OperationEquipmentAssignment;
--DELETE FROM PortOperations.ContainerYardMovement;
--DELETE FROM PortOperations.CargoOperation;
--DELETE FROM PortOperations.BerthAllocation;
--DELETE FROM PortOperations.PortCall;
--DELETE FROM PortOperations.Voyage;

---- ۲. پاک‌سازی جداول جزئیاتی
--DELETE FROM PortOperations.YardSlot;
--DELETE FROM PortOperations.Yard;
--DELETE FROM PortOperations.Equipment;
--DELETE FROM HumanResources.Employee;
--DELETE FROM PortOperations.Container;
--DELETE FROM PortOperations.Ship;
--DELETE FROM PortOperations.Port;
--DELETE FROM PortOperations.ContainerType;
--DELETE FROM PortOperations.EquipmentType;
--DELETE FROM Common.Country;

---- بازگردانی قیدهای FK
--EXEC sp_msforeachtable 'ALTER TABLE ? WITH CHECK CHECK CONSTRAINT ALL';


--------------------------------------------------------------------------------
-- 2.1 Common.Country
--------------------------------------------------------------------------------
INSERT INTO Common.Country (CountryID, CountryName, CountryCode)
VALUES
  (1,  'Afghanistan','AF'),(2,'Australia','AU'),
  (3,'Brazil','BR'),(4,'Canada','CA'),(5,'China','CN'),
  (6,'Egypt','EG'),(7,'France','FR'),(8,'Germany','DE'),
  (9,'India','IN'),(10,'Iran','IR'),(11,'Italy','IT'),
  (12,'Japan','JP'),(13,'Netherlands','NL'),(14,'Qatar','QA'),
  (15,'Russia','RU'),(16,'Singapore','SG'),
  (17,'South Africa','ZA'),(18,'South Korea','KR'),
  (19,'United Arab Emirates','AE'),(20,'United Kingdom','GB'),
  (21,'United States','US');

--------------------------------------------------------------------------------
-- 2.2 PortOperations.Port  (20 بندر واقعی)
--------------------------------------------------------------------------------
INSERT INTO PortOperations.Port (PortID, Name, Location)
VALUES
  (1, 'Port of Shanghai', 'Shanghai, China'),
  (2, 'Port of Singapore','Singapore'),
  (3, 'Port of Ningbo–Zhoushan','Ningbo, China'),
  (4, 'Port of Shenzhen','Shenzhen, China'),
  (5, 'Port of Guangzhou','Guangzhou, China'),
  (6, 'Port of Busan','Busan, South Korea'),
  (7, 'Port of Qingdao','Qingdao, China'),
  (8, 'Port of Tianjin','Tianjin, China'),
  (9, 'Port of Rotterdam','Rotterdam, Netherlands'),
  (10,'Port of Jebel Ali','Dubai, UAE'),
  (11,'Port of Los Angeles','Los Angeles, USA'),
  (12,'Port of Antwerp–Bruges','Antwerp, Belgium'),
  (13,'Port of Hamburg','Hamburg, Germany'),
  (14,'Port of Port Rashid','Dubai, UAE'),
  (15,'Port of Tanjung Pelepas','Johor, Malaysia'),
  (16,'Port of Kaohsiung','Kaohsiung, Taiwan'),
  (17,'Port of Valencia','Valencia, Spain'),
  (18,'Port of New York & New Jersey','NY/NJ, USA'),
  (19,'Port of Singapore','Singapore'),
  (20,'Port of Hamburg','Hamburg, Germany');

--------------------------------------------------------------------------------
-- 2.3 PortOperations.EquipmentType
--------------------------------------------------------------------------------
INSERT INTO PortOperations.EquipmentType (EquipmentTypeID, Description)
VALUES
  (1,'Crane'),(2,'Forklift'),
  (3,'Reach Stacker'),(4,'Straddle Carrier'),
  (5,'Yard Truck');

--------------------------------------------------------------------------------
-- 2.4 PortOperations.ContainerType
--------------------------------------------------------------------------------
INSERT INTO PortOperations.ContainerType (ContainerTypeID, Description, MaxWeightKG)
VALUES
  (1,'20ft Dry',20000),(2,'40ft Dry',30000),
  (3,'20ft Reefer',20000),(4,'40ft Reefer',30000),
  (5,'40ft HighCube',28000);

--------------------------------------------------------------------------------
-- 2.5 HumanResources.Employee  (100 کارمند)
--------------------------------------------------------------------------------
WITH N AS (
  SELECT TOP(100) ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS i
  FROM sys.all_objects
)
INSERT INTO HumanResources.Employee
 (EmployeeID, FullName, Position, NationalID, HireDate, BirthDate, Gender, MaritalStatus, Address, Phone, Email, EmploymentStatus)
SELECT
  i,
  'Emp_'+CAST(i AS varchar),
  CASE WHEN i%3=0 THEN 'Operator' ELSE 'Clerk' END,
  RIGHT('0000000000'+CAST(i AS varchar),10),
  DATEADD(DAY, -3*i, GETDATE()),
  DATEADD(YEAR, -(20 + i%30), GETDATE()),
  CASE WHEN i%2=0 THEN 'Male' ELSE 'Female' END,
  CASE WHEN i%2=0 THEN 'Single' ELSE 'Married' END,
  'Addr_'+CAST(i AS varchar),
  '09'+RIGHT('000000000'+CAST(i AS varchar),9),
  'emp'+CAST(i AS varchar)+'@mail.local',
  'Active'
FROM N;

--------------------------------------------------------------------------------
-- 2.6 PortOperations.Ship  (۲۰ کشتی واقعی)
--------------------------------------------------------------------------------
WITH ShipList AS (
    SELECT 'Ever Given'                          AS Name, '9811000' AS IMO UNION ALL
    SELECT 'Maersk Mc-Kinney Moller',            '9693460' UNION ALL
    SELECT 'MSC Zoe',                            '9683457' UNION ALL
    SELECT 'CMA CGM Benjamin Franklin',          '9424273' UNION ALL
    SELECT 'HMM Algeciras',                      '9839504' UNION ALL
    SELECT 'OOCL Hong Kong',                     '9441001' UNION ALL
    SELECT 'CSCL Globe',                         '9610102' UNION ALL
    SELECT 'MOL Triumph',                        '9392812' UNION ALL
    SELECT 'Madrid Maersk',                      '9651539' UNION ALL
    SELECT 'COSCO Shipping Taurus',              '9708230' UNION ALL
    SELECT 'APL France',                         '9203265' UNION ALL
    SELECT 'Hapag-Lloyd Hamburg Express',        '9115374' UNION ALL
    SELECT 'NYK Kagu Maru',                      '9441443' UNION ALL
    SELECT 'YM Efficiency',                      '9755063' UNION ALL
    SELECT 'ZIM Shanghai',                       '9350899' UNION ALL
    SELECT 'CMA CGM Antoine de Saint Exupéry',   '9731040' UNION ALL
    SELECT 'ONE Apus',                           '9820464' UNION ALL
    SELECT 'Berge Everest',                      '9651847' UNION ALL
    SELECT 'Madrid Maersk II',                   '9700137' UNION ALL
    SELECT 'Evergreen Ever Ace',                 '9876543'
),
NumberedShips AS (
    SELECT 
      ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS ShipID,
      'IMO' + IMO                                  AS IMO_Number,
      Name
    FROM ShipList
)
INSERT INTO PortOperations.Ship (ShipID, IMO_Number, Name, CountryID)
SELECT
  ShipID,
  IMO_Number,
  Name,
  -- تخصیص تصادفی CountryID از 1 تا تعداد کشورها
  ((ABS(CHECKSUM(NEWID())) % (SELECT COUNT(*) FROM Common.Country)) + 1)
FROM NumberedShips;
--------------------------------------------------------------------------------
-- 2.7 PortOperations.Yard  (۳ یارد به ازای هر بندر)
--------------------------------------------------------------------------------
INSERT INTO PortOperations.Yard (YardID, PortID, Name, UsageType)
SELECT 
    (PortID - 1) * 3 + 1,
    PortID,
    'Yard_Import_' + CAST(PortID AS varchar(4)),
    'IMPORT'
FROM PortOperations.Port
UNION ALL
SELECT 
    (PortID - 1) * 3 + 2,
    PortID,
    'Yard_Export_' + CAST(PortID AS varchar(4)),
    'EXPORT'
FROM PortOperations.Port
UNION ALL
SELECT 
    (PortID - 1) * 3 + 3,
    PortID,
    'Yard_Empty_' + CAST(PortID AS varchar(4)),
    'EMPTY'
FROM PortOperations.Port;

--------------------------------------------------------------------------------
-- 2.8 PortOperations.YardSlot  (فرض: هر یارد 50 اسلات → مجموع 20*3*50=3000)
--------------------------------------------------------------------------------
-- ۱) ابتدا اگر از قبل داده‌ای دارد خالی‌اش کن
-- یا اگر FK دارد:
-- DELETE FROM PortOperations.YardSlot;

-- ۲) CTE برای تولید اعداد 1 تا 50
WITH Tally AS (
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM (VALUES
      (0),(0),(0),(0),(0),(0),(0),(0),(0),(0)
    ) AS a(x)
    CROSS JOIN (VALUES
      (0),(0),(0),(0),(0)
    ) AS b(x)  -- 10 * 5 = 50 ردیف
),

-- ۳) ترکیب با جدول Yard برای ساخت YardSlot
YardSlots AS (
    SELECT
      p.YardID,
      t.n AS SlotIndex
    FROM PortOperations.Yard p
    CROSS JOIN Tally t
)

-- ۴) درج نهایی: هر یارد ۵۰ اسلات، با بلوک A–E، ردیف 1–10، طبقه 1–5
INSERT INTO PortOperations.YardSlot
  (YardSlotID, YardID, Block, RowNumber, TierLevel)
SELECT
  -- شناسه یکتا: (YardID-1)*50 + SlotIndex
  (YardID - 1) * 50 + SlotIndex,
  YardID,
  -- بلوک: A تا E بر اساس (SlotIndex-1)%5
  CHAR(65 + ((SlotIndex - 1) % 5)),  
  -- ردیف: 1 تا 10 بر اساس ((SlotIndex-1)/5)%10 +1
  ((SlotIndex - 1) / 5) % 10 + 1,
  -- طبقه: 1 تا 5 بر اساس ((SlotIndex-1)/50*5)%5+1
  ((SlotIndex - 1) / 10) % 5 + 1
FROM YardSlots;


--------------------------------------------------------------------------------
-- 2.9 PortOperations.Voyage  (500 سفر با مبدا و مقصد تصادفی)
--------------------------------------------------------------------------------
WITH Numbers AS (
  SELECT TOP(500) ROW_NUMBER() OVER(ORDER BY(SELECT NULL)) AS n
  FROM sys.all_objects a CROSS JOIN sys.all_objects b
), RandomPorts AS (
  SELECT n AS VoyageID,
         ((ABS(CHECKSUM(NEWID()))%(SELECT COUNT(*) FROM PortOperations.Ship))+1) AS ShipID,
         ((ABS(CHECKSUM(NEWID())) %20)+1) AS DeparturePortID,
         ((ABS(CHECKSUM(NEWID())) %20)+1) AS RawArrival
  FROM Numbers
), Cleaned AS (
  SELECT VoyageID, ShipID, DeparturePortID,
         CASE WHEN RawArrival=DeparturePortID THEN ((RawArrival%20)+1) ELSE RawArrival END AS ArrivalPortID
  FROM RandomPorts
)
INSERT INTO PortOperations.Voyage(VoyageID, ShipID, VoyageNumber, DeparturePortID, ArrivalPortID)
SELECT VoyageID, ShipID,
       'VYG'+RIGHT('0000'+CAST(VoyageID AS varchar),4),
       DeparturePortID, ArrivalPortID
FROM Cleaned;

--------------------------------------------------------------------------------
-- 2.10 PortOperations.PortCall  (1000 فراخوان با زمان تصادفی)
--------------------------------------------------------------------------------
WITH RandomVoyageCalls AS (
  SELECT TOP(1000)
    v.VoyageID,
    CASE WHEN NEWID()<0x80000000000000000000000000000000 THEN v.DeparturePortID ELSE v.ArrivalPortID END AS PortID,
    CAST(DATEADD(DAY, ABS(CHECKSUM(NEWID()))%DATEDIFF(DAY,'2023-01-01',GETDATE()), '2023-01-01') AS datetime) AS BaseDate
  FROM PortOperations.Voyage v ORDER BY NEWID()
)
INSERT INTO PortOperations.PortCall
  (PortCallID, VoyageID, PortID, ArrivalDateTime, DepartureDateTime, Status)
SELECT
  ROW_NUMBER() OVER(ORDER BY(SELECT NULL)),
  VoyageID, PortID,
  DATEADD(MINUTE, ABS(CHECKSUM(NEWID()))%1440, BaseDate),
  DATEADD(MINUTE, 1+ABS(CHECKSUM(NEWID()))%2880, DATEADD(MINUTE, ABS(CHECKSUM(NEWID()))%1440, BaseDate)),
  CASE WHEN DATEPART(HOUR, DATEADD(MINUTE, ABS(CHECKSUM(NEWID()))%1440, BaseDate))<12 THEN 'Docked' ELSE 'Departed' END
FROM RandomVoyageCalls;

--------------------------------------------------------------------------------
-- 2.11 PortOperations.Berth  (هر بندر 5 اسکله)
--------------------------------------------------------------------------------
WITH N AS (
  SELECT p.PortID, ROW_NUMBER() OVER(PARTITION BY p.PortID ORDER BY(SELECT NULL)) AS i
  FROM PortOperations.Port p
  CROSS JOIN (VALUES(1),(2),(3),(4),(5)) t(i)
)
INSERT INTO PortOperations.Berth(BerthID, PortID, Name, LengthMeters)
SELECT (PortID-1)*5+i, PortID, 'Berth_'+CAST(PortID AS varchar)+'_'+CAST(i AS varchar),100+20*i
FROM N;

--------------------------------------------------------------------------------
-- 2.12 PortOperations.BerthAllocation
--------------------------------------------------------------------------------
WITH Alloc AS (
  SELECT pc.PortCallID, b.BerthID,
         ROW_NUMBER() OVER(ORDER BY(SELECT NULL)) AS AllocationID
  FROM PortOperations.PortCall pc
  JOIN PortOperations.Berth b ON pc.PortID=b.PortID
)
INSERT INTO PortOperations.BerthAllocation
  (AllocationID, PortCallID, BerthID, AllocationStart, AllocationEnd, AssignedBy)
SELECT
  AllocationID,
  Alloc.PortCallID,
  BerthID,
  DATEADD(MINUTE, ABS(CHECKSUM(NEWID()))%60, ArrivalDateTime),
  DATEADD(MINUTE, 60+ABS(CHECKSUM(NEWID()))%120, ArrivalDateTime),
  'Scheduler_'+CAST((AllocationID%10)+1 AS varchar)
FROM Alloc
JOIN PortOperations.PortCall pc ON Alloc.PortCallID=pc.PortCallID;

--------------------------------------------------------------------------------
-- 2.13 PortOperations.Container  (10000 کانتینر با شرکت‌های واقعی)
--------------------------------------------------------------------------------
WITH N AS (
  SELECT TOP(10000) ROW_NUMBER() OVER(ORDER BY(SELECT NULL)) AS i
  FROM sys.all_objects a CROSS JOIN sys.all_objects b
)
INSERT INTO PortOperations.Container(ContainerID, ContainerNumber, ContainerTypeID, OwnerCompany)
SELECT
  i,
  'CONT'+RIGHT('00000000'+CAST(i AS varchar),8),
  ((i-1)%5)+1,
  CASE (i-1)%10
    WHEN 0 THEN 'A.P. Moller – Maersk' WHEN 1 THEN 'MSC'
    WHEN 2 THEN 'CMA CGM' WHEN 3 THEN 'Hapag-Lloyd'
    WHEN 4 THEN 'Evergreen Marine' WHEN 5 THEN 'COSCO Shipping'
    WHEN 6 THEN 'ONE' WHEN 7 THEN 'Yang Ming'
    WHEN 8 THEN 'HMM' WHEN 9 THEN 'ZIM'
  END
FROM N;

--------------------------------------------------------------------------------
-- 2.14 PortOperations.Equipment (مدل‌های واقعی)
--------------------------------------------------------------------------------
WITH N AS (
  SELECT TOP(50) ROW_NUMBER() OVER(ORDER BY(SELECT NULL)) AS i
  FROM sys.all_objects
)
INSERT INTO PortOperations.Equipment(EquipmentID, EquipmentTypeID, Model)
SELECT
  i,
  ((i-1)%5)+1,
  CASE ((i-1)%5)+1
    WHEN 1 THEN -- Crane
      CASE i%5 WHEN 0 THEN 'Liebherr LHM 600' WHEN 1 THEN 'Konecranes Gottwald HMK 6105'
                 WHEN 2 THEN 'Kalmar STS 800' WHEN 3 THEN 'SMT Titan' ELSE 'ZPMC STS' END
    WHEN 2 THEN -- Forklift
      CASE i%4 WHEN 0 THEN 'Toyota 8FGCU25' WHEN 1 THEN 'Caterpillar DP70'
                 WHEN 2 THEN 'Hyster H16.00XM-12' ELSE 'Komatsu FD70' END
    WHEN 3 THEN -- Reach Stacker
      CASE i%3 WHEN 0 THEN 'Kalmar DRG450-60S5' WHEN 1 THEN 'Fantuzzi FR'
                 ELSE 'Hyster RS46-31CT' END
    WHEN 4 THEN -- Straddle Carrier
      CASE i%3 WHEN 0 THEN 'Konecranes SC' WHEN 1 THEN 'Kalmar DCF' ELSE 'Terex SC' END
    WHEN 5 THEN -- Yard Truck
      CASE i%3 WHEN 0 THEN 'Kalmar Ottawa T2E' WHEN 1 THEN 'Hyster Yard Spotter' ELSE 'SMV 4531TB5' END
  END
FROM N;

--------------------------------------------------------------------------------
-- 2.15 PortOperations.CargoOperation (1,000,000 رکورد رندوم‌تر)
--------------------------------------------------------------------------------
WITH Numbers AS (
  SELECT TOP(1000000) ROW_NUMBER() OVER(ORDER BY(SELECT NULL)) AS n
  FROM sys.all_objects a CROSS JOIN sys.all_objects b
)
INSERT INTO PortOperations.CargoOperation
  (CargoOpID, PortCallID, ContainerID, OperationType, OperationDateTime, Quantity, WeightKG)
SELECT
  n,
  ((ABS(CHECKSUM(NEWID()))%500)+1),
  ((ABS(CHECKSUM(NEWID()))%1000)+1),
  CASE WHEN (ABS(CHECKSUM(NEWID()))%2)=0 THEN 'LOAD' ELSE 'UNLOAD' END,
  DATEADD(SECOND, ABS(CHECKSUM(NEWID())) % (DATEDIFF(SECOND,'2023-01-01',GETDATE())), '2023-01-01'),
  ((ABS(CHECKSUM(NEWID()))%5)+1),
  CAST((((ABS(CHECKSUM(NEWID()))%3950)+50)/100.0) AS DECIMAL(6,2))
FROM Numbers;



USE TradePortDB;
GO

-- 1) حذف همه ردیف‌ها
DELETE FROM Common.OperationEquipmentAssignment;
GO

--------------------------------------------------------------------------------
-- 2.16 Common.OperationEquipmentAssignment (200,000 تخصیص)
--------------------------------------------------------------------------------
WITH Numbers2 AS (
    SELECT TOP(1000000) ROW_NUMBER() OVER(ORDER BY(SELECT NULL)) AS n
    FROM sys.all_objects a CROSS JOIN sys.all_objects b
)
INSERT INTO Common.OperationEquipmentAssignment
  (AssignmentID, CargoOpID, EquipmentID, EmployeeID, StartTime, EndTime)
SELECT
  n,
  ((ABS(CHECKSUM(NEWID()))%(SELECT COUNT(*) FROM PortOperations.CargoOperation))+1),
  ((ABS(CHECKSUM(NEWID()))%(SELECT COUNT(*) FROM PortOperations.Equipment))+1),
  ((ABS(CHECKSUM(NEWID()))%(SELECT COUNT(*) FROM HumanResources.Employee))+1),
  c.OperationDateTime,
  DATEADD(MINUTE, 10 + ABS(CHECKSUM(NEWID()))%121, c.OperationDateTime)
FROM Numbers2 num
JOIN PortOperations.CargoOperation c
  ON c.CargoOpID = ((num.n-1)%(SELECT COUNT(*) FROM PortOperations.CargoOperation))+1;



BULK INSERT HumanResources.Employee
FROM 'E:\IUT\Database2\TradePortDB_project\employee.csv'
WITH (
    FIELDTERMINATOR = ':',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    CODEPAGE = '65001' -- برای UTF-8
);


--------------------------------------------------------------------------------
-- 2.17 PortOperations.ContainerYardMovement (200,000 تخصیص)
--------------------------------------------------------------------------------

-- 1) پاک کردن داده‌های قبلی (در صورت نیاز)
TRUNCATE TABLE PortOperations.ContainerYardMovement;
GO

-- 2) تولید 200,000 ردیف تصادفی
WITH
-- شماره‌گذاری 1..200000
Nums AS (
    SELECT TOP(200000)
      ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS MovementID
    FROM sys.all_objects a
    CROSS JOIN sys.all_objects b
),
-- لیست کانتینرها و yardslotها با شماره ردیفی برای join تصادفی
Containers AS (
    SELECT ContainerID, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
    FROM PortOperations.Container
),
YardSlots AS (
    SELECT YardSlotID, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
    FROM PortOperations.YardSlot
)
INSERT INTO PortOperations.ContainerYardMovement
  (MovementID, ContainerID, YardSlotID, MovementType, MovementDateTime)
SELECT
    n.MovementID,
    c.ContainerID,
    y.YardSlotID,
    -- حرکت با احتمال برابر
    CASE (ABS(CHECKSUM(NEWID())) % 3)
      WHEN 0 THEN 'IN'
      WHEN 1 THEN 'OUT'
      ELSE 'RELOCATE'
    END AS MovementType,
    -- تاریخ/زمان تصادفی بین 2023-01-01 و همین الان
    DATEADD(
      SECOND,
      ABS(CHECKSUM(NEWID())) % DATEDIFF(SECOND, '2023-01-01', GETDATE()),
      '2023-01-01'
    ) AS MovementDateTime
FROM Nums n
-- Join چرخی روی Containers
JOIN Containers c
  ON (n.MovementID - 1) % (SELECT COUNT(*) FROM PortOperations.Container) + 1 = c.rn
-- Join چرخی روی YardSlots
JOIN YardSlots y
  ON (n.MovementID - 1) % (SELECT COUNT(*) FROM PortOperations.YardSlot) + 1 = y.rn
ORDER BY n.MovementID;
GO
