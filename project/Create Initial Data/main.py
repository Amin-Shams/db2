import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from faker import Faker
import pyodbc
from tqdm import tqdm

fake = Faker()

# تاریخ‌های مورد نظر: امروز (۲۵ ژوئن ۲۰۲۵) و دو روز قبل
base_date = datetime(2025, 6, 26)
# date_choices = [base_date - timedelta(days=i) for i in range(3)]
date_choices = [base_date]

# پارامترهای کاهش‌یافته
# پارامترهای کاهش‌یافته (جدید)
N_COUNTRIES       = 30               
N_CUSTOMERS       = 500
N_BILLING_CYCLES  = 5
N_SERVICE_TYPES   = 100
N_TAXES           = 50
N_TARIFFS         = 200
N_CONTRACTS       = 1000
N_INVOICES        = 10000
N_PAYMENTS        = 150000
N_RECOGNITIONS    = 200000
N_INVOICE_LINES   = 1000000


# 1. Common.Country
countries = pd.DataFrame({
    "CountryID":   np.arange(1, N_COUNTRIES+1),
    "CountryName": [fake.country()[:100] for _ in range(N_COUNTRIES)],
    "CountryCode": [fake.country_code()[:10] for _ in range(N_COUNTRIES)]
})

# 2. Finance.Customer
customers = pd.DataFrame({
    "CustomerID":    np.arange(1, N_CUSTOMERS+1),
    "CustomerCode":  [f"CUST{str(i).zfill(4)}"[:19] for i in range(1, N_CUSTOMERS+1)],
    "CustomerName":  [fake.company()[:99] for _ in range(N_CUSTOMERS)],
    "CustomerType":  np.random.choice(['Individual','Company','Foreign'], N_CUSTOMERS),
    "TIN":           [fake.bothify(text='??####??')[:19] for _ in range(N_CUSTOMERS)],
    "VATNumber":     [fake.bothify(text='VAT###??')[:19] for _ in range(N_CUSTOMERS)],
    "Phone":         [fake.phone_number()[:19] for _ in range(N_CUSTOMERS)],
    "Email":         [fake.company_email()[:99] for _ in range(N_CUSTOMERS)],
    "Address":       [fake.address().replace("\n",", ")[:199] for _ in range(N_CUSTOMERS)],
    "CountryID":     np.random.choice(countries["CountryID"], N_CUSTOMERS),
})

# 3. Finance.BillingCycle
billing_cycles = pd.DataFrame({
    "BillingCycleID":    list(range(1, N_BILLING_CYCLES+1)),
    "CycleName":         ['Monthly','Quarterly','Annually','Bi-Weekly','Weekly'],
    "CycleLengthInDays": [30,90,365,14,7]
})

# 4. Finance.ServiceType
service_types = pd.DataFrame({
    "ServiceTypeID":   np.arange(1, N_SERVICE_TYPES+1),
    "ServiceName":     [fake.bs().title()[:99] for _ in range(N_SERVICE_TYPES)],
    "ServiceCategory": np.random.choice(['Unloading','Storage','Transport'], N_SERVICE_TYPES),
    "BaseRate":        np.round(np.random.uniform(50,499, N_SERVICE_TYPES),2),
    "UnitOfMeasure":   np.random.choice(['TEU','Hour','Ton'], N_SERVICE_TYPES),
    "Taxable":         np.random.choice([0,1], N_SERVICE_TYPES),
    "IsActive":        np.ones(N_SERVICE_TYPES, dtype=int)
})

# 5. Finance.Tax
taxes = pd.DataFrame({
    "TaxID":        np.arange(1, N_TAXES+1),
    "TaxName":      [f"Tax{str(i).zfill(3)}"[:49] for i in range(1, N_TAXES+1)],
    "TaxRate":      np.round(np.random.uniform(0.01,0.25, N_TAXES),2),
    "TaxType":      np.random.choice(['National','Service'], N_TAXES),
    "EffectiveFrom":[random.choice(date_choices) for _ in range(N_TAXES)],
    "EffectiveTo":  [random.choice(date_choices) for _ in range(N_TAXES)]
})

# 6. Finance.Tariff
tariffs = pd.DataFrame({
    "TariffID":      np.arange(1, N_TARIFFS+1),
    "ServiceTypeID": np.random.choice(service_types["ServiceTypeID"], N_TARIFFS),
    "ValidFrom":     [random.choice(date_choices) for _ in range(N_TARIFFS)],
    "ValidTo":       [random.choice(date_choices) for _ in range(N_TARIFFS)],
    "UnitRate":      np.round(np.random.uniform(100,999, N_TARIFFS),2)
})

# 7. Finance.Contract
contracts = pd.DataFrame({
    "ContractID":     np.arange(1, N_CONTRACTS+1),
    "CustomerID":     np.random.choice(customers["CustomerID"], N_CONTRACTS),
    "ContractNumber":[f"CON{str(i).zfill(6)}"[:49] for i in range(1, N_CONTRACTS+1)],
    "StartDate":      [random.choice(date_choices) for _ in range(N_CONTRACTS)],
    "EndDate":        [random.choice(date_choices) for _ in range(N_CONTRACTS)],
    "BillingCycleID": np.random.choice(billing_cycles["BillingCycleID"], N_CONTRACTS),
    "PaymentTerms":   np.random.choice(['Net 30','Net 60','Prepaid'], N_CONTRACTS),
    "ContractStatus": np.random.choice(['Active','Expired'], N_CONTRACTS),
    "CreatedDate":    [random.choice(date_choices) for _ in range(N_CONTRACTS)]
})

# 8. Finance.Invoice
invoices = pd.DataFrame({
    "InvoiceID":     np.arange(1, N_INVOICES+1),
    "ContractID":    np.random.choice(contracts["ContractID"], N_INVOICES),
    "InvoiceNumber":[f"INV{str(i).zfill(7)}"[:49] for i in range(1, N_INVOICES+1)],
    "InvoiceDate":   [random.choice(date_choices) for _ in range(N_INVOICES)],
    "DueDate":       [random.choice(date_choices) for _ in range(N_INVOICES)],
    "Status":        np.random.choice(['Paid','Overdue','Cancelled'], N_INVOICES),
    "TotalAmount":   np.round(np.random.uniform(500,19999, N_INVOICES),2),
    "TaxAmount":     np.round(np.random.uniform(50,1999, N_INVOICES),2),
    "CreatedBy":     [fake.user_name()[:99] for _ in range(N_INVOICES)],
    "CreatedDate":   [random.choice(date_choices) for _ in range(N_INVOICES)]
})

# 9. Finance.Payment
payments = pd.DataFrame({
    "PaymentID":      np.arange(1, N_PAYMENTS+1),
    "InvoiceID":      np.random.choice(invoices["InvoiceID"], N_PAYMENTS),
    "PaymentDate":    [random.choice(date_choices) for _ in range(N_PAYMENTS)],
    "Amount":         np.round(np.random.uniform(50,9999, N_PAYMENTS),2),
    "PaymentMethod":  np.random.choice(['Cash','Card','Transfer'], N_PAYMENTS),
    "ConfirmedBy":    [fake.user_name()[:100] for _ in range(N_PAYMENTS)],
    "ReferenceNumber":[f"REF{random.randint(10000,99998)}"[:49] for _ in range(N_PAYMENTS)],
    "Notes":          [fake.sentence()[:499] for _ in range(N_PAYMENTS)]
})

# 10. Finance.RevenueRecognition
recognitions = pd.DataFrame({
    "RecognitionID":   np.arange(1, N_RECOGNITIONS+1),
    "InvoiceID":       np.random.choice(invoices["InvoiceID"], N_RECOGNITIONS),
    "DateRecognized": [random.choice(date_choices) for _ in range(N_RECOGNITIONS)],
    "Amount":         np.round(np.random.uniform(20,7999, N_RECOGNITIONS),2),
    "Notes":          [fake.bs()[:499] for _ in range(N_RECOGNITIONS)]
})

# 11. Finance.InvoiceLine
invoice_lines = pd.DataFrame({
    "InvoiceLineID":   np.arange(1, N_INVOICE_LINES+1),
    "InvoiceID":       np.random.choice(invoices["InvoiceID"], N_INVOICE_LINES),
    "ServiceTypeID":   np.random.choice(service_types["ServiceTypeID"], N_INVOICE_LINES),
    "TaxID":           np.random.choice(taxes["TaxID"], N_INVOICE_LINES),
    "Quantity":        np.random.randint(1,49, N_INVOICE_LINES),
    "UnitPrice":       np.round(np.random.uniform(20,499, N_INVOICE_LINES),2),
    "DiscountPercent": np.round(np.random.uniform(0,0.3, N_INVOICE_LINES),2),
    "TaxAmount":       np.round(np.random.uniform(5,299, N_INVOICE_LINES),2),
    "NetAmount":       np.round(np.random.uniform(20,499, N_INVOICE_LINES),2)
})

def generate_insert_statements(df, table_name):
    lines = []
    columns = ', '.join(df.columns)
    for _, row in df.iterrows():
        values = []
        for val in row:
            if pd.isnull(val):
                values.append("NULL")
            elif isinstance(val, str):
                values.append("N'" + val.replace("'", "''") + "'")
            elif isinstance(val, (datetime, pd.Timestamp)):
                values.append("'" + val.strftime("%Y-%m-%d") + "'")
            else:
                values.append(str(val))
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({', '.join(values)});"
        lines.append(sql)
    return lines

sql_lines = []
batch_size = 1000
count = 0

tables = {
    "Common.Country":             countries,
    "Finance.BillingCycle":       billing_cycles,
    "Finance.ServiceType":        service_types,
    "Finance.Tax":                taxes,
    "Finance.Tariff":             tariffs,
    "Finance.Customer":           customers,
    "Finance.Contract":           contracts,
    "Finance.Invoice":            invoices,
    "Finance.Payment":            payments,
    "Finance.RevenueRecognition": recognitions,
    "Finance.InvoiceLine":        invoice_lines
}

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"  # اگر Driver 17 نصب نیست، از Driver 18 استفاده کن
    "SERVER=localhost;"                        # یا .\\SQLEXPRESS یا آدرس سرورت
    "DATABASE=TradePortDB;"                         # یا نام دیتابیس مورد نظر شما
    "Trusted_Connection=yes;"
)



try:
    # 1️⃣ Open connection and cursor
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("✅ Connected To Sql Server", datetime.now())

    for table_name, df in tables.items():
        print(f"🚀 Loading: {table_name} ({len(df)} rows)")

        # 2️⃣ Replace NaN with None
        df = df.where(pd.notnull(df), None)

        # 3️⃣ Build insert query
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['?'] * len(df.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        data = [tuple(row) for row in df.to_numpy()]

        try:
            with tqdm(total=len(data), desc=f"Inserting into {table_name}") as pbar:
                batch_size = 1000
                for i in range(0, len(data), batch_size):
                    chunk = data[i:i + batch_size]
                    cursor.executemany(insert_query, chunk)
                    conn.commit()
                    pbar.update(len(chunk))
            print(f"✅ Success: {table_name}")
        except Exception as e:
            conn.rollback()
            print(f"❌ Error in {table_name}: {e}")

    # 4️⃣ Close connection at the end
    cursor.close()
    conn.close()
    print("✅ Connection closed.")

except Exception as e:
    print("❌ Could not connect or load data:", e)

# 🔚 Close the connection
cursor.close()
conn.close()
print("✅ Connection closed.")








# for table_name, df in tables.items():
#     print(f"Generating INSERTs for: {table_name} ({len(df)} rows)")
#     statements = generate_insert_statements(df, table_name)
#     for stmt in tqdm(statements, desc=f"Inserting into {table_name}", unit="stmt"):
#         sql_lines.append(stmt)
#         count += 1
#         if count % batch_size == 0:
#             sql_lines.append("GO")

# if sql_lines and sql_lines[-1] != "GO":
#     sql_lines.append("GO")

# version = 'V4'
# with open(f"3 - insert_finance_data {version}.sql", "w", encoding="utf-8") as f:
#     f.write('\n'.join(sql_lines))

# print("✅ File 'insert_finance_data.sql' created successfully with GO separators.")

"""
USE TradePortDB;
GO

DECLARE @SchemaName NVARCHAR(128) = 'Finance';

SELECT 
    t.name AS TableName,
    SUM(p.rows) AS RowCount
FROM 
    sys.tables AS t
INNER JOIN 
    sys.schemas AS s ON t.schema_id = s.schema_id
INNER JOIN 
    sys.partitions AS p ON t.object_id = p.object_id
WHERE 
    p.index_id IN (0, 1) 
    AND s.name = @SchemaName
GROUP BY 
    t.name
ORDER BY 
    RowCount DESC;


"""