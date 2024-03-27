import pandas as pd
def compare_totals(df1, df2):
    """
    Compares the totals of 'credit' in df1 with 'debit' in df2 and vice versa.
    Prints the results of the comparison.
    """
    total_credit_df1 = df1['credit'].sum()
    total_debit_df2 = df2['debit'].sum()
    
    total_debit_df1 = df1['debit'].sum()
    total_credit_df2 = df2['credit'].sum()
    
    print(f"Total Credit in DF1: {total_credit_df1}")
    print(f"Total Debit in DF2: {total_debit_df2}")
    
    if total_credit_df1 == total_debit_df2:
        print("Total Credit in DF1 matches Total Debit in DF2.")
    else:
        print("Mismatch: Total Credit in DF1 does NOT match Total Debit in DF2.")
        
    print(f"Total Debit in DF1: {total_debit_df1}")
    print(f"Total Credit in DF2: {total_credit_df2}")
    
    if total_debit_df1 == total_credit_df2:
        print("Total Debit in DF1 matches Total Credit in DF2.")
    else:
        print("Mismatch: Total Debit in DF1 does NOT match Total Credit in DF2.")


# Function to clean 'credit' and 'debit' columns
def clean_financial_data(df):
    # Convert 'credit' and 'debit' columns to numeric, handling commas and coercing errors to NaN
    df['credit'] = pd.to_numeric(df['credit'].replace(',', '', regex=True), errors='coerce')
    df['debit'] = pd.to_numeric(df['debit'].replace(',', '', regex=True), errors='coerce')
    
    # Fill NaN values with 0
    df.fillna({'credit': 0, 'debit': 0}, inplace=True)
    
    return df

# Paths to the CSV files
hikma_file_path = '../data/source/hikma.csv'
tawrdat_file_path = '../data/source/tawrdat.csv'

# Load hikma.csv with specified headers for columns of interest
hikma_df = pd.read_csv(hikma_file_path, header=None, usecols=[27, 28, 29, 30, 31, 32], names=['dis', 'date', 'jv number', 'jv', 'credit', 'debit'])

# Load tawrdat.csv with specified headers for columns of interest
tawrdat_df = pd.read_csv(tawrdat_file_path, header=None, usecols=[27, 28, 29, 30, 32, 33], names=['dis', 'date', 'jv number', 'jv', 'credit', 'debit'])

# Clean the DataFrames
hikma_df = clean_financial_data(hikma_df)
tawrdat_df = clean_financial_data(tawrdat_df)

# Define paths for the cleaned CSV files
cleaned_hikma_path = '../data/cleaned/hikma_cleaned.csv'
cleaned_tawrdat_path = '../data/cleaned/tawrdat_cleaned.csv'

# Ensure the target directories exist or adjust the paths as necessary

# Save the cleaned DataFrames to new CSV files
hikma_df.to_csv(cleaned_hikma_path, index=False)
tawrdat_df.to_csv(cleaned_tawrdat_path, index=False)

print("Cleaned data saved successfully.")

# Assuming hikma_df and tawrdat_df have already been cleaned with clean_financial_data function
compare_totals(hikma_df, tawrdat_df)
