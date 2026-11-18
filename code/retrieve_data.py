from pathlib import Path
from urllib.parse import quote
import zipfile
import sys
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed


def download_flights(year, month, output_dir="flight_data"):
    if year < 2018:
        raise ValueError("Marketing carrier data begins in 2018.")

    if not 1 <= month <= 12:
        raise ValueError("Month must be between 1 and 12.")

    filename = (
        "On_Time_Marketing_Carrier_On_Time_Performance_"
        f"Beginning_January_2018_{year}_{month}.zip"
    )

    url = "https://transtats.bts.gov/PREZIP/" + quote(filename)

    folder = Path(output_dir)
    folder.mkdir(parents=True, exist_ok=True)

    destination = folder / filename
    temporary = folder / (filename + ".part")

    if destination.exists():
        print("Already downloaded:", filename)
        return destination

    print("Downloading:", filename)

    try:
        with requests.get(url, stream=True, timeout=(30, 120)) as response:
            if response.status_code == 404:
                raise FileNotFoundError(
                    f"No file found for {year}-{month:02d}. "
                    "Check the filename in the BTS directory."
                )

            response.raise_for_status()

            with temporary.open("wb") as output:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        output.write(chunk)

        # Catch cases where the server returns an HTML page instead of a ZIP.
        if not zipfile.is_zipfile(temporary):
            raise ValueError("The downloaded response is not a ZIP file.")

        temporary.replace(destination)

    except Exception:
        temporary.unlink(missing_ok=True)
        raise

    print("Saved:", destination)
    return destination

def download_months(start_year, start_month, end_year, end_month,
                    output_dir="flight_data", max_workers=3):
    if not (1 <= start_month <= 12 and 1 <= end_month <= 12):
        raise ValueError("Months must be between 1 and 12.")

    if (start_year, start_month) > (end_year, end_month):
        raise ValueError("Start date must be before the end date.")
    
    periods = []
    year, month = start_year, start_month

    while (year, month) <= (end_year, end_month):
        periods.append((year, month))

        if month == 12:
            year += 1
            month = 1
        else:
            month += 1

    failed = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        tasks = {
            executor.submit(
                download_flights, year, month, output_dir
            ): (year, month)
            for year, month in periods
        }

        for task in as_completed(tasks):
            year, month = tasks[task]

            try:
                task.result()
                print(f"Completed: {year}-{month:02d}")
            except Exception as error:
                failed.append((year, month))
                print(f"Failed: {year}-{month:02d}: {error}")

    if failed:
        print("Months to retry:", failed)

    return failed

def main(start_year, start_month, end_year, end_month, output_dir):


    download_months(
        start_year,
        start_month,
        end_year,
        end_month,
        output_dir
    )


if __name__ == '__main__':
    if len(sys.argv) not in (5, 6):
        print(
            "Usage: python download_flights.py "
            "start_year start_month end_year end_month [output_dir]"
        )
        sys.exit(1)

    start_year = int(sys.argv[1])
    start_month = int(sys.argv[2])
    end_year = int(sys.argv[3])
    end_month = int(sys.argv[4])

    output_dir = sys.argv[5] if len(sys.argv) == 6 else "flight_data"
    main(start_year, start_month, end_year, end_month, output_dir)

