import requests
import zipfile
from io import BytesIO, StringIO
import pandas as pd

def main():
    # Config
    TARGET_DISPLAY_NAME = "CM-UDiFF Common Bhavcopy Final (zip)"
    NSE_API_URL = "https://www.nseindia.com/api/daily-reports"
    NSE_HOME_URL = "https://www.nseindia.com"

    HEADERS = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'referer': 'https://www.nseindia.com/all-reports',
        'user-agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/114.0.0.0 Safari/537.36'
        ),
        'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    # Session with cookies
    session = requests.Session()

    try:
        print("[INFO] Visiting NSE homepage to establish session...")
        session.get(NSE_HOME_URL, headers=HEADERS, timeout=10)

        print("[INFO] Fetching report list JSON...")
        resp = session.get(NSE_API_URL, params={"key": "CM"}, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        # Locate our target ZIP entry
        entry = next(
            (item for item in data.get("PreviousDay", []) if item.get("displayName") == TARGET_DISPLAY_NAME),
            None
        )

        if not entry:
            print("[ERROR] Target zip file not found in PreviousDay.")
            return

        zip_url = entry.get("filePath", "") + entry.get("fileActlName", "")
        print(f"[INFO] Found ZIP URL: {zip_url}")

        print("[INFO] Downloading ZIP...")
        zip_resp = session.get(zip_url, headers=HEADERS, timeout=20)
        zip_resp.raise_for_status()

        # Process ZIP in memory
        print("[INFO] Extracting CSV from ZIP...")
        with zipfile.ZipFile(BytesIO(zip_resp.content)) as zf:
            csv_files = [name for name in zf.namelist() if name.lower().endswith('.csv')]
            if not csv_files:
                print("[ERROR] No CSV file found inside ZIP.")
                return

            csv_name_in_zip = csv_files[0]
            with zf.open(csv_name_in_zip) as csvfile:
                csv_bytes = csvfile.read()
                csv_text = csv_bytes.decode('utf-8', errors='replace')

        # Load CSV into pandas
        print("[INFO] Reading CSV with pandas...")
        df = pd.read_csv(StringIO(csv_text))

        # Filter and rename
        print("[INFO] Filtering and renaming columns...")
        df = df[df['SctySrs'] == 'EQ']
        df = df[['ISIN', 'TckrSymb', 'FinInstrmNm']]
        df = df.rename(columns={
            'TckrSymb': 'Symbol',
            'FinInstrmNm': 'Name'
        })

        # Save final CSV
        output_csv = "stock.csv"
        df.to_csv(output_csv, index=False)
        print(f"[SUCCESS] Final CSV saved as {output_csv}")

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Network error: {e}")
    except zipfile.BadZipFile as e:
        print(f"[ERROR] Invalid ZIP file: {e}")
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")

if __name__ == "__main__":
    main()
