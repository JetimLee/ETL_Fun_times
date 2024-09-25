import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlite3
import logging

# Task 1: Write a function log_progress() to log the progress of the code at different stages
logging.basicConfig(
    filename="code_log.txt", level=logging.INFO, format="%(asctime)s - %(message)s"
)


def log_progress(message):
    logging.info(message)


# Task 2: Extract the tabular information from the given URL and save it to a dataframe
def extract():
    url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    # Locate the table under 'By market capitalization'
    table = soup.find("table", {"class": "wikitable sortable mw-collapsible"})
    rows = table.find_all("tr")

    data = []
    for row in rows[1:]:
        cols = row.find_all("td")
        if len(cols) > 1:
            rank = cols[0].text.strip()
            bank_name = cols[1].find("a").text.strip()
            market_cap = cols[2].text.strip()
            data.append([rank, bank_name, market_cap])

    # Convert to DataFrame
    df = pd.DataFrame(data, columns=["Rank", "Bank Name", "MC_USD_Billion"])
    df["MC_USD_Billion"] = df["MC_USD_Billion"].astype(float)  # Convert to float

    log_progress("Data extraction complete.")
    return df


# Task 3: Programmatically fetch the exchange rate CSV and use it to transform the dataframe
def download_exchange_rate_csv(url, filename):
    response = requests.get(url)
    with open(filename, "wb") as file:
        file.write(response.content)
    log_progress("Exchange rate CSV downloaded.")


def transform(df):
    # Download the exchange rate CSV
    csv_url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
    csv_filename = "exchange_rate.csv"
    download_exchange_rate_csv(csv_url, csv_filename)

    # Load the exchange rate CSV
    exchange_rate = pd.read_csv(csv_filename)

    # Extract relevant exchange rates
    gbp_rate = exchange_rate.loc[exchange_rate["Currency"] == "GBP", "Rate"].values[0]
    eur_rate = exchange_rate.loc[exchange_rate["Currency"] == "EUR", "Rate"].values[0]
    inr_rate = exchange_rate.loc[exchange_rate["Currency"] == "INR", "Rate"].values[0]

    # Apply transformations
    df["MC_GBP_Billion"] = (df["MC_USD_Billion"] * gbp_rate).round(2)
    df["MC_EUR_Billion"] = (df["MC_USD_Billion"] * eur_rate).round(2)
    df["MC_INR_Billion"] = (df["MC_USD_Billion"] * inr_rate).round(2)

    log_progress("Data transformation complete.")
    return df


# Task 4: Load the transformed dataframe to an output CSV file
def load_to_csv(df):
    df.to_csv("./Largest_banks_data.csv", index=False)
    log_progress("Data saved to CSV.")


# Task 5: Load the transformed dataframe to an SQL database server as a table
def load_to_db(df):
    conn = sqlite3.connect("Banks.db")
    df.to_sql("Largest_banks", conn, if_exists="replace", index=False)
    conn.close()
    log_progress("Data loaded to database.")


# Task 6: Run queries on the database table
def run_queries():
    conn = sqlite3.connect("Banks.db")
    cur = conn.cursor()

    # Query 1: Get the average market cap in GBP
    cur.execute("SELECT AVG(MC_GBP_Billion) FROM Largest_banks")
    avg_gbp = cur.fetchone()[0]
    print(f"Average Market Cap in GBP: {avg_gbp}")

    # Query 2: Get the 5th largest bank in EUR
    cur.execute(
        "SELECT `Bank Name`, MC_EUR_Billion FROM Largest_banks ORDER BY MC_EUR_Billion DESC LIMIT 1 OFFSET 4"
    )
    fifth_bank = cur.fetchone()
    print(
        f"5th largest bank by market cap in EUR: {fifth_bank[0]} with {fifth_bank[1]} Billion EUR"
    )

    log_progress("Queries executed.")
    conn.close()


# Task 7: Verify that the log entries have been completed
def verify_log():
    with open("code_log.txt", "r") as log_file:
        logs = log_file.readlines()
        print("Log file content:")
        for log in logs:
            print(log.strip())


# Main function to execute all tasks
def main():
    log_progress("Process started.")

    # Task 2: Extract data
    df = extract()

    # Task 3: Transform data
    df_transformed = transform(df)

    # Task 4: Save to CSV
    load_to_csv(df_transformed)

    # Task 5: Load to database
    load_to_db(df_transformed)

    # Task 6: Run queries
    run_queries()

    # Task 7: Verify logs
    verify_log()

    log_progress("Process complete.")


# Execute the main function
if __name__ == "__main__":
    main()
