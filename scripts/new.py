import pandas as pd
from datetime import timedelta

def load_and_clean_data(file_path, columns, names):
    try:
        # Load the CSV with specified columns and names
        df = pd.read_csv(file_path, header=None, usecols=columns, names=names)
        
        # Clean 'credit' and 'debit' columns: remove commas, convert to numeric, fill NaN with 0
        df['credit'] = pd.to_numeric(df['credit'].replace(',', '', regex=True), errors='coerce').fillna(0)
        df['debit'] = pd.to_numeric(df['debit'].replace(',', '', regex=True), errors='coerce').fillna(0)
        
        # Convert 'date' to datetime format
        df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
        
        return df
    except Exception as e:
        print(f"An error occurred while loading and cleaning data from {file_path}: {e}")
        return pd.DataFrame()

def split_credit_debit(df, file_base_path):
    # Filter to get only rows with non-zero credits
    credit_df = df[df['credit'] != 0]
    # Filter to get only rows with non-zero debits
    debit_df = df[df['debit'] != 0]

    # Save to CSV
    credit_df.to_csv(f'{file_base_path}_credit.csv', index=False)
    debit_df.to_csv(f'{file_base_path}_debit.csv', index=False)

    print(f"Saved credit transactions to {file_base_path}_credit.csv")
    print(f"Saved debit transactions to {file_base_path}_debit.csv")

# Example usage:
columns = [27, 28, 29, 30, 32, 33]  # Adjust the indices based on your CSV structure
names = ['dis', 'date', 'jv number', 'jv', 'credit', 'debit']

hikma_path = '../data/source/hikma.csv'  # Adjust the file path
tawrdat_path = '../data/source/tawrdat.csv'  # Adjust the file path

# Load and clean the data
hikma_df = load_and_clean_data(hikma_path, columns, names)
tawrdat_df = load_and_clean_data(tawrdat_path, columns, names)

# Split and save data into credit and debit files
split_credit_debit(hikma_df, '../data/cleaned/hikma')
split_credit_debit(tawrdat_df, '../data/cleaned/tawrdat')


def calculate_date_totals(df):
    # This function will calculate the sum of credit or debit for each date
    return df.groupby('date').agg({'credit': 'sum', 'debit': 'sum'}).reset_index()


def filter_matched_totals(hikma_file, tawrdat_file, type):
    # Load the datasets
    hikma_df = pd.read_csv(hikma_file)
    tawrdat_df = pd.read_csv(tawrdat_file)

    # Calculate totals for each date
    hikma_totals = calculate_date_totals(hikma_df)
    tawrdat_totals = calculate_date_totals(tawrdat_df)

    # Merge totals on date
    merged = pd.merge(hikma_totals, tawrdat_totals, on='date', suffixes=('_hikma', '_tawrdat'))

    # Filter out dates where the sums match
    if type == 'credit':
        unmatched = merged[merged['credit_hikma'] != merged['debit_tawrdat']]
    else:
        unmatched = merged[merged['debit_hikma'] != merged['credit_tawrdat']]

    # Filter original dataframes to only include unmatched dates
    unmatched_dates = unmatched['date']
    unmatched_hikma = hikma_df[hikma_df['date'].isin(unmatched_dates)]
    unmatched_tawrdat = tawrdat_df[tawrdat_df['date'].isin(unmatched_dates)]

    # Save filtered data
    unmatched_hikma.to_csv(hikma_file.replace('.csv', '_unmatched.csv'), index=False)
    unmatched_tawrdat.to_csv(tawrdat_file.replace('.csv', '_unmatched.csv'), index=False)

    print(f"Filtered unmatched data saved to: {hikma_file.replace('.csv', '_unmatched.csv')}")
    print(f"Filtered unmatched data saved to: {tawrdat_file.replace('.csv', '_unmatched.csv')}")


# File paths for the split files
hikma_credit_file = '../data/cleaned/hikma_credit.csv'
tawrdat_debit_file = '../data/cleaned/tawrdat_debit.csv'
hikma_debit_file = '../data/cleaned/hikma_debit.csv'
tawrdat_credit_file = '../data/cleaned/tawrdat_credit.csv'

# Compare hikma_credit with tawrdat_debit
filter_matched_totals(hikma_credit_file, tawrdat_debit_file, 'credit')

# Compare hikma_debit with tawrdat_credit
filter_matched_totals(hikma_debit_file, tawrdat_credit_file, 'debit')

#----------------------------------------


def load_unmatched_files():
    # Paths to the unmatched files
    hikma_credit_unmatched_path = '../data/cleaned/hikma_credit_unmatched.csv'
    tawrdat_debit_unmatched_path = '../data/cleaned/tawrdat_debit_unmatched.csv'
    hikma_debit_unmatched_path = '../data/cleaned/hikma_debit_unmatched.csv'
    tawrdat_credit_unmatched_path = '../data/cleaned/tawrdat_credit_unmatched.csv'

    # Load the data from these files
    hikma_credit_unmatched = pd.read_csv(hikma_credit_unmatched_path)
    tawrdat_debit_unmatched = pd.read_csv(tawrdat_debit_unmatched_path)
    hikma_debit_unmatched = pd.read_csv(hikma_debit_unmatched_path)
    tawrdat_credit_unmatched = pd.read_csv(tawrdat_credit_unmatched_path)

    # Print basic information about these datasets
    # print("Hikma Credit Unmatched:")
    # print(hikma_credit_unmatched.head())
   

    # print("Tawrdat Debit Unmatched:")
    # print(tawrdat_debit_unmatched.head())
    

    # print("Hikma Debit Unmatched:")
    # print(hikma_debit_unmatched.head())

    # print("Tawrdat Credit Unmatched:")
    # print(tawrdat_credit_unmatched.head())
    

    return hikma_credit_unmatched, tawrdat_debit_unmatched, hikma_debit_unmatched, tawrdat_credit_unmatched

# Load and check the data
hikma_credit_unmatched, tawrdat_debit_unmatched, hikma_debit_unmatched, tawrdat_credit_unmatched = load_unmatched_files()

# ____________________________



def load_and_compare_unmatched_files():
    # Loading the data using the previously defined function
    hikma_credit_unmatched, tawrdat_debit_unmatched, hikma_debit_unmatched, tawrdat_credit_unmatched = load_unmatched_files()

    # Function to find and remove matching transactions based on date and amount
    def remove_matched_transactions(hikma_df, tawrdat_df, hikma_field, tawrdat_field):
        # Merge dataframes on 'date' and compare 'credit' with 'debit' based on the specified fields
        matched_df = pd.merge(hikma_df, tawrdat_df, on='date')
        matched_df = matched_df[matched_df[hikma_field] == matched_df[tawrdat_field]]

        # Finding indices of matched transactions
        hikma_matched_indices = matched_df.index_left.unique()
        tawrdat_matched_indices = matched_df.index_right.unique()

        # Removing matched transactions
        hikma_df = hikma_df.drop(hikma_matched_indices)
        tawrdat_df = tawrdat_df.drop(tawrdat_matched_indices)

        return hikma_df, tawrdat_df

    # Matching transactions for credits and debits
    unmatched_hikma_credit, unmatched_tawrdat_debit = remove_matched_transactions(
        hikma_credit_unmatched, tawrdat_debit_unmatched, 'credit', 'debit'
    )
    unmatched_hikma_debit, unmatched_tawrdat_credit = remove_matched_transactions(
        hikma_debit_unmatched, tawrdat_credit_unmatched, 'debit', 'credit'
    )

    # Saving the unmatched data back to new files
    unmatched_hikma_credit.to_csv('../data/cleaned/hikma_credit_final_unmatched.csv', index=False)
    unmatched_tawrdat_debit.to_csv('../data/cleaned/tawrdat_debit_final_unmatched.csv', index=False)
    unmatched_hikma_debit.to_csv('../data/cleaned/hikma_debit_final_unmatched.csv', index=False)
    unmatched_tawrdat_credit.to_csv('../data/cleaned/tawrdat_credit_final_unmatched.csv', index=False)

    print("Updated unmatched data files have been saved.")

# Execute the function
load_and_compare_unmatched_files()



