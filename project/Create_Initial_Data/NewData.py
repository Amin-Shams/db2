import pandas as pd
import numpy as np
from datetime import datetime
from faker import Faker
import pyodbc
from tqdm import tqdm

fake = Faker()
today = datetime(2025, 6, 26)

N_CUSTOMERS = 2
N_CONTRACTS = 3
N_INVOICES = 5
N_PAYMENTS = 7
N_INVOICE_LINES = 10

# ID offsets to avoid collision
CUSTOMER_ID_OFFSET = 900001
CONTRACT_ID_OFFSET = 910001
INVOICE_ID_OFFSET = 920001
PAYMENT_ID_OFFSET = 930001
LINE_ID_OFFSET = 940001

# # Fake test Customer
# customers = pd.DataFrame({
#     "CustomerID": np.arange(CUSTOMER_ID_OFFSET, CUSTOMER_ID_OFFSET + N_CUSTOMERS),
#     "CustomerCode": [f"TEST_CUST_{i}" for i in range(N_CUSTOMERS)],
#     "CustomerName": [f"TestCo {i}" for i in range(N_CUSTOMERS)],
#     "CustomerType": "Company",
#     "TIN": [f"TIN{i}" for i in range(N_CUSTOMERS)],
#     "VATNumber": [f"VAT{i}" for i in range(N_CUSTOMERS)],
#     "Phone": ["+989123456789"] * N_CUSTOMERS,
#     "Email": [f"test{i}@example.com" for i in range(N_CUSTOMERS)],
#     "Address": [f"Test Address {i}" for i in range(N_CUSTOMERS)],
#     "CountryID": 1
# })

# # Contract
# contracts = pd.DataFrame({
#     "ContractID": np.arange(CONTRACT_ID_OFFSET, CONTRACT_ID_OFFSET + N_CONTRACTS),
#     "CustomerID": np.random.choice(customers["CustomerID"], N_CONTRACTS),
#     "ContractNumber": [f"TEST_CON_{i}" for i in range(N_CONTRACTS)],
#     "StartDate": [today] * N_CONTRACTS,
#     "EndDate": [today] * N_CONTRACTS,
#     "BillingCycleID": 1,
#     "PaymentTerms": "Net 30",
#     "ContractStatus": "Active",
#     "CreatedDate": [today] * N_CONTRACTS
# })

# # Invoice
# invoices = pd.DataFrame({
#     "InvoiceID": np.arange(INVOICE_ID_OFFSET, INVOICE_ID_OFFSET + N_INVOICES),
#     "ContractID": np.random.choice(contracts["ContractID"], N_INVOICES),
#     "InvoiceNumber": [f"TEST_INV_{i}" for i in range(N_INVOICES)],
#     "InvoiceDate": [today] * N_INVOICES,
#     "DueDate": [today] * N_INVOICES,
#     "Status": "Paid",
#     "TotalAmount": np.round(np.random.uniform(500, 1500, N_INVOICES), 2),
#     "TaxAmount": np.round(np.random.uniform(50, 300, N_INVOICES), 2),
#     "CreatedBy": [f"tester{i}" for i in range(N_INVOICES)],
#     "CreatedDate": [today] * N_INVOICES
# })

# # Payment

# payments = pd.DataFrame({
#     "PaymentID": np.arange(PAYMENT_ID_OFFSET, PAYMENT_ID_OFFSET + N_PAYMENTS),
#     "InvoiceID": np.random.choice(invoices["InvoiceID"], N_PAYMENTS),
#     "PaymentDate": [today] * N_PAYMENTS,
#     "Amount": np.round(np.random.uniform(100, 1400, N_PAYMENTS), 2),
#     "PaymentMethod": np.random.choice(["Cash", "Card", "Transfer"], N_PAYMENTS),
#     "ConfirmedBy": [f"confirmer{i}" for i in range(N_PAYMENTS)],
#     "ReferenceNumber": [f"TEST_REF_{i}" for i in range(N_PAYMENTS)],
#     "Notes": ["Test payment"] * N_PAYMENTS
# })

# Invoice Line (assumes ServiceTypeID=1 and TaxID=1 already exist)
start_id = 1_000_001

invoice_lines = pd.DataFrame({
    "InvoiceLineID": np.arange(start_id, start_id + N_INVOICE_LINES),
    "InvoiceID": np.random.choice(invoices["InvoiceID"], N_INVOICE_LINES),
    "ServiceTypeID": 1,
    "TaxID": 1,
    "Quantity": np.random.randint(1, 5, N_INVOICE_LINES),
    "UnitPrice": np.round(np.random.uniform(100, 300, N_INVOICE_LINES), 2),
    "DiscountPercent": 0.1,
    "TaxAmount": np.round(np.random.uniform(10, 80, N_INVOICE_LINES), 2),
    "NetAmount": np.round(np.random.uniform(100, 1000, N_INVOICE_LINES), 2)
})


tables = {
    # "Finance.Customer": customers,
    # "Finance.Contract": contracts,
    # "Finance.Invoice": invoices,
    # "Finance.Payment": payments,
    "Finance.InvoiceLine": invoice_lines
}

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=TradePortDB;"
    "Trusted_Connection=yes;"
)

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    for table, df in tables.items():
        print(f"Inserting: {table} ({len(df)})")
        df = df.where(pd.notnull(df), None)
        cols = ", ".join(df.columns)
        placeholders = ", ".join(["?"] * len(df.columns))
        query = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        data = [tuple(row) for row in df.to_numpy()]
        with tqdm(total=len(data), desc=table) as bar:
            for i in range(0, len(data), 500):
                chunk = data[i:i+500]
                cursor.executemany(query, chunk)
                conn.commit()
                bar.update(len(chunk))

    cursor.close()
    conn.close()
    print("✅ Data inserted successfully")

except Exception as e:
    print("❌ Failed to insert:", e)
