import pandas as pd
import sqlite3
import yfinance as yf

def fetch_and_clean_data(symbols, columns=None):
    """
    Fetches and cleans data for the given symbols from Yahoo Finance.
    
    Args:
    symbols (list): List of symbol strings to fetch from Yahoo Finance (e.g., ['^VIX', '^VXN', '^VXD']).
    columns (list): List of columns to keep (e.g., ['Close']). If None, all columns are kept.

    Returns:
    pd.DataFrame: A cleaned DataFrame with consistent dates and specified columns.
    """

    # Initialize an empty dictionary to store DataFrames for each symbol
    symbol_data = {}

    # Fetch data for each symbol
    for symbol in symbols:
        # Download data from Yahoo Finance
        df = yf.download(symbol)
        
        # Keep only the necessary columns if specified
        if columns is not None:
            df = df[columns]
        
        # Reset index to have 'Date' as a column
        df.reset_index(inplace=True)
        
        # Clean 'Date' column to remove any time component
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        
        # Rename columns to include the symbol prefix, e.g., 'Close' -> '^VIX_Close'
        df.rename(columns=lambda x: f"{symbol} {x}" if x != 'Date' else x, inplace=True)
        
        # Store DataFrame in the dictionary
        symbol_data[symbol] = df

    # Step 1: Find the common start date across all DataFrames
    common_start_date = max([df['Date'].min() for df in symbol_data.values()])
    common_end_date = min([df['Date'].max() for df in symbol_data.values()])

    # Step 2: Align all DataFrames to the common date range and clean data
    for symbol, df in symbol_data.items():
        # Filter data to the common date range
        symbol_data[symbol] = df[(df['Date'] >= common_start_date) & (df['Date'] <= common_end_date)].reset_index(drop=True)

    # Step 3: Merge all DataFrames on the 'Date' column
    merged_df = pd.DataFrame({'Date': pd.date_range(start=common_start_date, end=common_end_date)})

    # Convert merged_df 'Date' column to date only (remove time component)
    merged_df['Date'] = merged_df['Date'].dt.date

    for symbol, df in symbol_data.items():
        # Merge data on 'Date' column
        merged_df = pd.merge(merged_df, df, on='Date', how='left')

    # Step 4: Drop rows with any missing data to ensure clean dataset
    merged_df.dropna(inplace=True)

    return merged_df

def save_to_sqlite(df, db_path='final_result.db', table_name='market_data'):
    """
    Saves the given DataFrame to an SQLite database.
    
    Args:
    df (pd.DataFrame): DataFrame to save to the database.
    db_path (str): Path to the SQLite database file.
    table_name (str): Name of the table to store the data.
    """
    # Establish SQLite connection
    conn = sqlite3.connect(db_path)

    # Save DataFrame to SQLite database
    df.to_sql(table_name, conn, if_exists='replace', index=False)

    # Close the connection
    conn.close()
    print(f"Data has been successfully saved to {db_path} in table '{table_name}'.")

def main(symbols=['^VIX', '^VXN', '^VXD', 'Dia', 'SPY', 'QQQ'], columns=['Close']):
    """
    Main function to fetch, clean, and save Yahoo Finance data.
    
    Args:
    symbols (list): List of symbols to fetch.
    columns (list): List of columns to fetch. Defaults to ['Close'].
    """
    # Fetch and clean data
    cleaned_data = fetch_and_clean_data(symbols, columns)

    # Save cleaned data to SQLite database
    save_to_sqlite(cleaned_data)

if __name__ == '__main__':
    main()  # You can pass custom symbols and columns if desired
