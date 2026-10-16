import os
import time
import glob
import datetime
import subprocess
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.keys import Keys

TARGET_FROM_DMY = "01/08/2026"
TARGET_MONTH_TEXT = "August 2026"
download_dir = (os.path



                .join(os.getcwd(), "downloads"))
os.makedirs(download_dir, exist_ok=True)
options = Options()
options.add_argument("--start-maximized")
prefs = {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True,
}
options.add_experimental_option("prefs", prefs)
driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 50)
def find_from_input():
    xpaths = [
        "//label[contains(normalize-space(.),'From')]/following::input[1]",
        "//input[contains(@placeholder,'From')]",
        "//input[contains(@name,'from')]",
        "//input[contains(@id,'from')]",
    ]
    for xp in xpaths:
        els = driver.find_elements(By.XPATH, xp)
        if els:
            return els[0]
    raise NoSuchElementException("From input not found")
def set_from_date_safely():
    from_input = find_from_input()
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", from_input)
    from_input.click()
    time.sleep(0.5)
    try:
        dp = driver.find_elements(By.CSS_SELECTOR, "div.react-datepicker")
        if dp:
            for _ in range(24):
                head = driver.find_element(By.CSS_SELECTOR, ".react-datepicker__current-month")
                if TARGET_MONTH_TEXT.lower() in head.text.strip().lower():
                    break
                driver.find_element(By.CSS_SELECTOR, ".react-datepicker__navigation--next").click()
                time.sleep(0.15)
            day1 = driver.find_element(
                By.CSS_SELECTOR,
                ".react-datepicker__day--001:not(.react-datepicker__day--outside-month)"
            )
            day1.click()
            time.sleep(0.3)
    except Exception:
        pass
    current_val = from_input.get_attribute("value") or ""
    if TARGET_FROM_DMY in current_val:
        print(f"From date set: {current_val}")
        return True
    print(f"Could not confirm From date. Current value: '{current_val}'")
    return False
def click_search_and_wait():
    btn = wait.until(EC.element_to_be_clickable((By.XPATH,
        "//button[contains(text(),'Search') or contains(@id,'search') or contains(@class,'search')]"
    )))
    driver.execute_script("arguments[0].click();", btn)
    print("Search button clicked!")
    try:
        wait.until(EC.presence_of_element_located((By.XPATH,
            "//table | //button[contains(text(),'Excel') or contains(@class,'excel')]"
        )))
        print("Results loaded!")
    except TimeoutException:
        print("Results did not appear within 50s")
def click_excel_if_present():
    try:
        excel_btn = wait.until(EC.element_to_be_clickable((By.XPATH,
            "//button[contains(text(),'Excel') or contains(@class,'excel')]"
        )))
        driver.execute_script("arguments[0].click();", excel_btn)
        print("Excel button clicked!")
    except TimeoutException:
        print("Excel button not found")
try:
    driver.get("https://ituze.rw/")
    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    if iframes:
        driver.switch_to.frame(iframes[0])
    wait.until(EC.visibility_of_element_located((By.XPATH,
        "//input[contains(@placeholder,'Username') or @id='username' or @name='username']"
    ))).send_keys("reconciliation")
    wait.until(EC.visibility_of_element_located((By.XPATH,
        "//input[contains(@placeholder,'Password') or @id='password' or @name='password']"
    ))).send_keys("reconciliation")
    login_btn = wait.until(EC.element_to_be_clickable((By.XPATH,
        "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'sign in')]"
    )))
    driver.execute_script("arguments[0].click();", login_btn)
    wait.until(EC.url_contains("/loggedin"))
    print("Logged in")
    driver.get("https://ituze.rw/loggedin/payments")
    time.sleep(1)
    driver.switch_to.default_content()
    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    if iframes:
        driver.switch_to.frame(iframes[0])
    if set_from_date_safely():
        click_search_and_wait()
    else:
        fi = find_from_input()
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", fi)
        fi.click()
        time.sleep(0.3)
        fi.send_keys(Keys.CONTROL, "a")
        fi.send_keys(Keys.DELETE)
        time.sleep(0.2)
        fi.send_keys(TARGET_FROM_DMY)
        time.sleep(0.5)
        current_val = fi.get_attribute("value")
        print(f"From input now contains: '{current_val}'")
        click_search_and_wait()
    click_excel_if_present()
    print("Waiting for CSV download to complete...")
    downloaded_file = None
    timeout = 60
    while timeout > 0:
        tmp_files = glob.glob(os.path.join(download_dir, "*.tmp")) + glob.glob(os.path.join(download_dir, "*.crdownload"))
        csv_files = glob.glob(os.path.join(download_dir, "*.csv"))
        if tmp_files:
            time.sleep(1)
            timeout -= 1
        elif csv_files:
            downloaded_file = max(csv_files, key=os.path.getctime)
            break
        else:
            time.sleep(1)
            timeout -= 1
    if downloaded_file:
        print(f"CSV download complete: {downloaded_file}")
        df = pd.read_csv(downloaded_file)
        df['Datetime'] = pd.to_datetime(df['Datetime'], dayfirst=True, errors="coerce")
        if 'Amount' not in df.columns:
            for col in df.columns:
                if 'amount' in col.lower():
                    df.rename(columns={col: 'Amount'}, inplace=True)
        df_filtered = df[['Datetime', 'Amount']].copy()
        df_filtered.loc[:, 'Date'] = df_filtered['Datetime'].dt.date
        summary = df_filtered.groupby('Date').agg(
            Total_Amount=('Amount', 'sum'),
            Transaction_Count=('Amount', 'count')
        ).reset_index()
        summary = summary.sort_values('Date', ascending=False)
        print("\nDay-wise summary of Amounts and Transaction Counts:")
        print(summary)
        today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        summary_file = os.path.join(download_dir, f"SFC_summary_{today_str}.txt")
        with open(summary_file, "w") as f:
            f.write(summary.to_string(index=False))
        print(f"Summary saved to: {summary_file}")
        #pscp_path = r"C:\\App\\bk_previos\\pscp.exe"
        pscp_path = r"C:\Users\eric.dusabimana\Downloads\\pscp.exe"
        remote_user = "edusabimana"
        remote_port = 20022
        remote_host = "41.186.24.69"
        remote_password = "acs@123"
        files_to_upload = [
            (downloaded_file, f"/data/reconciliation/daily/SFC/payments_{today_str}.csv"),
            (summary_file, f"/data/reconciliation/daily/SFC/SFC_summary_{today_str}.txt")
        ]
        for local_file, remote_file in files_to_upload:
            if not os.path.exists(pscp_path):
                print(f"pscp.exe not found at {pscp_path}")
                continue
            try:
                cmd = [
                    pscp_path,
                    "-P", str(remote_port),
                    "-pw", remote_password,
                    local_file,
                    f"{remote_user}@{remote_host}:{remote_file}"
                ]
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    print(f"File successfully copied to remote server: {remote_file}")
                else:
                    print(f"SCP failed for {local_file}:")
                    print(result.stderr)
            except Exception as e:
                print(f"SCP command failed for {local_file}:", e)
    else:
        print("CSV download failed or timed out")
except TimeoutException as e:
    print("Timeout waiting for element:", e)
    print(driver.page_source[:1000])
finally:
    driver.quit()








