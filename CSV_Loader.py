import glob
import os
import re
import sqlite3

import pandas as pd


class folders():
    crypto_data = "/Users/michaelchia/Documents/Python/Trading Bot/Data/Cryptocurrency"
    forex_data = "/Users/michaelchia/Documents/Python/Trading Bot/Data/Forex"
    asx_data = "/Users/michaelchia/Documents/Python/Trading Bot/Data/ASX"
    us_equities_data = "/Users/michaelchia/Documents/Python/Trading Bot/Data/US Equities"

def insert_data(folder, asset_type):
    file_patterns = [
        os.path.join(folder, "*D1.csv"),
        os.path.join(folder, "*H4.csv"),
        os.path.join(folder, "*H1.csv"),
        os.path.join(folder, "*M30.csv"),
        os.path.join(folder, "*M15.csv")
    ]

    # Initialize an empty DataFrame to store the combined data
    combined_data = pd.DataFrame()

    for pattern in file_patterns:
        csv_files = glob.glob(pattern)

        # Iterate over each CSV file
        for csv_file in csv_files:
            ticker, interval = os.path.splitext(os.path.basename(csv_file))[0].split('_')
            if asset_type.lower() == 'crypto' or asset_type.lower() == 'forex':
                df = pd.read_csv(csv_file, header=None, names=["Date", "Open", "High", "Low", "Close", "Volume"], index_col=False)
            else:
                df = pd.read_csv(csv_file, header=1, names=["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"], index_col=False)
                df = df.drop('Adj Close', axis=1)
            
            if asset_type.lower() == 'crypto':
                 df['Ticker'] = re.sub(r'(USDT)', '/USDT', ticker)
            elif asset_type.lower() == 'forex':
                df['Ticker'] = re.sub(r'([A-Za-z]{3})([A-Za-z]{3})', r'\1/\2', ticker)
            else:
                df['Ticker'] = ticker
            df['Interval'] = interval.lower()
            df['Interval'].replace({"d1": "1d", "h4": "4h", "h1": "1h", "m30": "30m", "m15": "15m"}, inplace=True)

            df['Datetime'] = pd.to_datetime(df['Date'], errors='coerce')
            df['Date'] = df['Datetime'].dt.date
            df['Time'] = df['Datetime'].dt.time.astype(str)
            df['Datetime'] = df['Datetime'].dt.strftime("%d/%m/%Y %H:%M:%S")
            df = df[["Datetime", "Interval", "Date", "Time", "Ticker", "Open", "High", "Low", "Close", "Volume"]]
            df['Date'] = df['Date'].apply(lambda x: pd.to_datetime(x).strftime('%d/%m/%Y'))

            # Concatenate the data and reset the index
            combined_data = pd.concat([combined_data, df], ignore_index=True).reset_index(drop=True)

    # Outside of the loop, print the final message
    conn = sqlite3.connect("stock_data.db")  # Connect to the existing db file.
    cursor = conn.cursor()

    # Define the tables and intervals
    tables = ["prices_1d", "prices_4h", "prices_1h", "prices_30m", "prices_15m"]
    intervals = ['1d', '4h', '1h', '30m', '15m']

    # Iterate over each table and interval
    for table, interval in zip(tables, intervals):
        # Select rows where 'Interval' column matches the current interval
        selected_data = combined_data[combined_data['Interval'] == interval]

        # Generate the INSERT INTO statement
        insert_statement = f"INSERT INTO {table} ({', '.join(selected_data.columns)}) VALUES ({', '.join([':' + col for col in selected_data.columns])})"

        # Create a list of dictionaries for the data corresponding to the current interval
        data_to_insert = [row.to_dict() for _, row in selected_data.iterrows()]

        # Execute the INSERT INTO statement with parameterized queries for the current interval
        cursor.executemany(insert_statement, data_to_insert)

        # Get the number of inserted rows for the current interval
        inserted_rows = cursor.rowcount
        print(f"Inserted {inserted_rows} rows into {table} table for interval {interval}.")

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

    print("Complete")

# Call the function for each asset type
insert_data(folders.crypto_data, 'crypto')
insert_data(folders.forex_data, 'forex')
insert_data(folders.asx_data, 'asx')
insert_data(folders.us_equities_data, 'us_equities')
