import os
import time
import uuid
import requests
from decimal import Decimal
from datetime import datetime, timedelta
import json
from selenium.webdriver.support.ui import Select

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from google.cloud import bigquery
from google.oauth2 import service_account

# ================== CONFIG ==================
PROJECT_ID = "rhysman-data-warehouse-488306"
DATASET_ID = "rhysman"
TABLE_ID = "saokebank"

LOGIN_URL = "https://efast.vietinbank.vn/login"
HISTORY_URL = "https://efast.vietinbank.vn/api/v1/account/history"

username = os.getenv("VTB_USER")
password = os.getenv("VTB_PASS")

if not username or not password:
    print("Missing VTB_USER or VTB_PASS environment variables")
    exit()


# ================== DATE ==================
today = datetime.now()
yesterday = today - timedelta(days=1)

from_date_str = yesterday.strftime("%d/%m/%Y")
to_date_str = today.strftime("%d/%m/%Y")

print("Pull data from:", from_date_str, "to", to_date_str)


# ================== SELENIUM LOGIN ==================
options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)

options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

# Bật performance logging
options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=options
)
driver.execute_cdp_cmd("Network.enable", {})

driver.get(LOGIN_URL)

wait = WebDriverWait(driver, 30)

# Tìm ô username theo placeholder
username_input = wait.until(
    EC.presence_of_element_located(
        (By.XPATH, "//input[@placeholder='Tên đăng nhập']")
    )
)

# Tìm ô password theo placeholder
password_input = driver.find_element(
    By.XPATH, "//input[@placeholder='Mật khẩu']"
)

username_input.clear()
password_input.clear()

username_input.send_keys(username)
password_input.send_keys(password)

# Nhấn nút đăng nhập
login_button = driver.find_element(
    By.XPATH, "//button[contains(., 'Đăng nhập')]"
)
login_button.click()


print("Đang đăng nhập...")

# Chờ sidebar xuất hiện (dấu hiệu login thành công)
wait.until(
    EC.presence_of_element_located(
        (By.XPATH, "//span[contains(., 'Tài khoản')]")
    )
)

print("Login thành công")
time.sleep(3)
# ===== CLICK TÀI KHOẢN ĐẦU TIÊN =====
first_account = wait.until(
    EC.element_to_be_clickable(
        (By.XPATH, "//a[contains(text(),'113003010555')]")
    )
)
first_account.click()

print("Đã click tài khoản 113003010555")
time.sleep(5)


# ===== Đổi hiển thị sang 150 bằng Select =====
select_element = wait.until(
    EC.presence_of_element_located(
        (By.XPATH, "//select[@class='form-control']")
    )
)

# Clear log cũ
driver.get_log("performance")

Select(select_element).select_by_value("150")

print("Đã chọn 150 dòng")

# Chờ Angular reload
time.sleep(5)



# Lấy sessionId từ browser
session_id = driver.execute_script("""
return window.sessionStorage.getItem('sessionId') 
    || window.localStorage.getItem('sessionId');
""")

print("SessionId lấy được:", session_id)


# ================== PREPARE PAYLOAD ==================
payload = {
    "accountNo": "113003010555",
    "accountType": "DDA",
    "cardNo": "",
    "channel": "eFAST",
    "cifno": "EVMbADxrmwg9rTZj9aBARM8KYh0+/uNUvXIz1GTv0LqnoYEPerGpuqkwOsafwukiHg44DTcZP/c+HfMPpIyw1Erxk+dV6FI1ykAU6spw7CNbrxdl9oRojpDhXQR8TAmPjT/de5TlHVvtd8G7B1gQd4fACoX1MnRyWvrTxgaBnX8=",
    "dorcC": "Credit",
    "dorcD": "Debit",
    "endTime": "23:59:59",
    "fromAmount": 0,
    "language": "vi",
    "lastRecord": "",
    "newCore": "",
    "pageIndex": 0,
    "pageSize": "150",
    "queryType": "NORMAL",
    "requestId": str(uuid.uuid4()),
    "screenResolution": "842x911",
    "searchKey": "",
    "startTime": "00:00:00",
    "toAmount": 0,
    "username": "",   # có thể để trống vì đã có session
    "version": "1.0",
    "fromDate": from_date_str,
    "toDate": to_date_str
}

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://efast.vietinbank.vn",
    "Referer": "https://efast.vietinbank.vn/account/detail",
    "User-Agent": "Mozilla/5.0"
}



logs = driver.get_log("performance")

transactions_data = None

for entry in logs:
    log = json.loads(entry["message"])["message"]

    if log["method"] == "Network.responseReceived":
        url = log["params"]["response"]["url"]

        if "account/history" in url:
            request_id = log["params"]["requestId"]

            response = driver.execute_cdp_cmd(
                "Network.getResponseBody",
                {"requestId": request_id}
            )

            transactions_data = json.loads(response["body"])
            break

if not transactions_data:
    print("Không bắt được history API")
    driver.quit()
    exit()

if not transactions_data:
    print("Không bắt được history API")
    driver.quit()
    exit()

print("History data received")

if transactions_data.get("status", {}).get("code") != "1":
    print("API error:", transactions_data)
    driver.quit()
    exit()

transactions = transactions_data.get("transactions")

if not transactions:
    print("No transactions.")
    driver.quit()
    exit()

driver.quit()

# ================== FORMAT DATA ==================
rows_to_insert = []

for t in transactions:
    dt = datetime.strptime(t["tranDate"], "%d-%m-%Y %H:%M:%S")

    amount_raw = Decimal(t["amount"].replace("+", ""))

    if t["dorc"] == "D":
        amount = -abs(amount_raw)
    else:
        amount = abs(amount_raw)

    rows_to_insert.append({
        "STK": "VTB-113003010555",
        "tranDate": dt.strftime("%Y-%m-%d %H:%M:%S"),
        "corresponsiveAccount": t.get("corresponsiveAccount"),
        "corresponsiveName": t.get("corresponsiveName"),
        "remark": t.get("remark"),
        "amount": str(amount),
        "balance": str(Decimal(t["balance"]))
    })


# ================== INSERT TO BIGQUERY ==================
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

gcp_key = json.loads(os.getenv("GCP_SERVICE_ACCOUNT"))

credentials = service_account.Credentials.from_service_account_info(gcp_key)

bq_client = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)

# Delete old data
from_date_sql = datetime.strptime(from_date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
to_date_sql = datetime.strptime(to_date_str, "%d/%m/%Y").strftime("%Y-%m-%d")

delete_query = f"""
DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
WHERE DATE(tranDate) BETWEEN '{from_date_sql}' AND '{to_date_sql}'
 AND STK = 'VTB-113003010555'
"""

bq_client.query(delete_query).result()
print("Old data deleted.")

job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

load_job = bq_client.load_table_from_json(
    rows_to_insert,
    table_ref,
    job_config=job_config
)

load_job.result()


print("Data inserted successfully.")
