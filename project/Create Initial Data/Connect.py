import pyodbc

# رشته اتصال به SQL Server
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"  # اگر Driver 17 نصب نیست، از Driver 18 استفاده کن
    "SERVER=localhost;"                        # یا .\\SQLEXPRESS یا آدرس سرورت
    "DATABASE=TradePortDB;"                         # یا نام دیتابیس مورد نظر شما
    "Trusted_Connection=yes;"
)

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT GETDATE();")  # یک کوئری ساده برای تست
    row = cursor.fetchone()
    print("✅ اتصال موفق بود. تاریخ/زمان سرور:", row[0])

    cursor.close()
    conn.close()
except Exception as e:
    print("❌ خطا در اتصال:", e)
