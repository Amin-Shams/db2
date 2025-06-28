import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import os

# تنظیم Faker برای داده‌های انگلیسی
fake = Faker('en_US')

# تنظیمات تعداد داده‌ها
NUM_EMPLOYEES = 1000
NUM_ATTENDANCE = 1000000
NUM_SALARY_PAYMENTS = 400000

# مسیر ذخیره فایل‌های CSV
OUTPUT_DIR = './tradeportdb_data'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# لیست مشاغل منطقی بندری
POSITIONS = ['Crane Operator', 'Forklift Driver', 'Dock Supervisor', 'Logistics Clerk', 'Port Manager', 
             'Maintenance Technician', 'Security Officer', 'Cargo Inspector']

# ----------------------------------
# تولید داده برای جدول Employee
# ----------------------------------
def generate_phone_number():
    """تولید شماره تلفن با فرمت +989XXXXXXXXX"""
    return f"+989{random.randint(100000000, 999999999)}"

def generate_national_id(used_ids):
    """تولید کد ملی 10 رقمی منحصربه‌فرد"""
    while True:
        nid = f"{random.randint(1000000000, 9999999999)}"
        if nid not in used_ids:
            used_ids.add(nid)
            return nid

employees = []
used_national_ids = set()
employment_status_weights = ['Active'] * 80 + ['OnLeave'] * 15 + ['Terminated'] * 5
gender_weights = ['Male'] * 70 + ['Female'] * 30
marital_status_weights = ['Married'] * 60 + ['Single'] * 40

for i in range(1, NUM_EMPLOYEES + 1):
    hire_date = fake.date_between(start_date=datetime(2015, 1, 1), end_date='today')
    birth_date = fake.date_between(start_date=datetime(1965, 1, 1), end_date=datetime(2007, 12, 31))
    employees.append({
        'EmployeeID': i,
        'FullName': fake.name(),
        'Position': random.choice(POSITIONS),
        'NationalID': generate_national_id(used_national_ids),
        'HireDate': hire_date,
        'BirthDate': birth_date,
        'Gender': random.choice(gender_weights),
        'MaritalStatus': random.choice(marital_status_weights),
        'Address': fake.address().replace('\n', ', '),
        'Phone': generate_phone_number(),
        'Email': fake.email(),
        'EmploymentStatus': random.choice(employment_status_weights)
    })

df_employees = pd.DataFrame(employees)
df_employees.to_csv(os.path.join(OUTPUT_DIR, 'employee.csv'),sep=",", index=False, encoding='utf-8-sig')
print(f"Generated {len(df_employees)} records for Employee table.")

# ----------------------------------
# تولید داده برای جدول Attendance
# ----------------------------------
attendance = []
active_onleave_ids = df_employees[df_employees['EmploymentStatus'].isin(['Active', 'OnLeave'])]['EmployeeID'].tolist()
attendance_status_weights = ['Present'] * 85 + ['Late'] * 10 + ['Absent'] * 5

def is_workday(date):
    """بررسی اینکه تاریخ روز کاری است (دوشنبه تا شنبه)"""
    return date.weekday() < 6  # 0=دوشنبه، 5=شنبه، 6=یکشنبه

for i in range(1, NUM_ATTENDANCE + 1):
    # تولید تاریخ تصادفی، با احتمال بیشتر برای روزهای کاری
    while True:
        attendance_date = fake.date_between(start_date=datetime(2020, 1, 1), end_date='today')
        if is_workday(attendance_date) or random.random() < 0.1:  # 10% شانس برای یکشنبه
            break
    
    status = random.choice(attendance_status_weights)
    
    # زمان ورود و خروج فقط برای Present یا Late
    check_in_time = None
    check_out_time = None
    hours_worked = None
    if status in ['Present', 'Late']:
        check_in_hour = random.randint(7, 8) if status == 'Present' else random.randint(8, 9)
        check_in_time = datetime.strptime(f"{check_in_hour}:{random.randint(0, 59):02d}:00", '%H:%M:%S').strftime('%H:%M:%S')
        hours = random.uniform(6, 10)
        check_out_time = (datetime.strptime(check_in_time, '%H:%M:%S') + timedelta(hours=hours)).strftime('%H:%M:%S')
        hours_worked = round(hours, 2)
    
    attendance.append({
        'AttendanceID': i,
        'EmployeeID': random.choice(active_onleave_ids),
        'AttendanceDate': attendance_date,
        'Status': status,
        'CheckInTime': check_in_time if check_in_time else '00:00:00',
        'CheckOutTime': check_out_time if check_out_time else '00:00:00',
        'HoursWorked': hours_worked if hours_worked else 0.0,
        'Notes': fake.sentence()
    })

df_attendance = pd.DataFrame(attendance)
df_attendance.to_csv(os.path.join(OUTPUT_DIR, 'attendance.csv'), index=False, encoding='utf-8-sig')
print(f"Generated {len(df_attendance)} records for Attendance table.")

# ----------------------------------
# تولید داده برای جدول SalaryPayment
# ----------------------------------
salary_payments = []
payment_method_weights = ['BankTransfer'] * 90 + ['Cash'] * 10

for i in range(1, NUM_SALARY_PAYMENTS + 1):
    # تولید تاریخ پرداخت (معمولاً پایان ماه)
    payment_date = fake.date_between(start_date=datetime(2020, 1, 1), end_date='today')
    payment_date = payment_date.replace(day=28)  # فرض پرداخت در روز 28 هر ماه
    
    amount = round(random.uniform(2000, 6000), 2)
    bonus = round(random.uniform(200, 1000), 2)
    deductions = round(random.uniform(100, 500), 2)
    net_amount = amount + bonus - deductions
    
    salary_payments.append({
        'SalaryPaymentID': i,
        'EmployeeID': random.choice(active_onleave_ids),
        'PaymentDate': payment_date,
        'Amount': amount,
        'Bonus': bonus,
        'Deductions': deductions,
        'NetAmount': round(net_amount, 2),
        'PaymentMethod': random.choice(payment_method_weights),
        'ReferenceNumber': f"REF{random.randint(100000, 999999)}"
    })

df_salary_payments = pd.DataFrame(salary_payments)
df_salary_payments.to_csv(os.path.join(OUTPUT_DIR, 'salary_payment.csv'), index=False, encoding='utf-8-sig')
print(f"Generated {len(df_salary_payments)} records for SalaryPayment table.")