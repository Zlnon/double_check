import pandas as pd
from datetime import timedelta

def load_and_clean_data(file_path, columns, names):
    try:
        df = pd.read_csv(file_path, header=None, usecols=columns, names=names)
        df['credit'] = pd.to_numeric(df['credit'].replace(',', '', regex=True), errors='coerce').fillna(0)
        df['debit'] = pd.to_numeric(df['debit'].replace(',', '', regex=True), errors='coerce').fillna(0)
        df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
        return df
    except Exception as e:
        print(f"An error occurred while loading and cleaning data from {file_path}: {e}")
        return pd.DataFrame()

def split_credit_debit(df, file_base_path):
    credit_df = df[df['credit'] != 0]
    debit_df = df[df['debit'] != 0]
    credit_df.to_csv(f'{file_base_path}_credit.csv', index=False)
    debit_df.to_csv(f'{file_base_path}_debit.csv', index=False)
    print(f"Saved credit transactions to {file_base_path}_credit.csv")
    print(f"Saved debit transactions to {file_base_path}_debit.csv")

def remove_exact_matched_transactions(hikma_df, tawrdat_df, hikma_field, tawrdat_field):
    # Reset indices to ensure they are unique and sequential
    hikma_df = hikma_df.reset_index(drop=True)
    tawrdat_df = tawrdat_df.reset_index(drop=True)

    # Initialize an empty list to store the matched transaction indices
    matched_hikma_indices = []
    matched_tawrdat_indices = []

    # Use a set to track which tawrdat indices have been matched, to ensure uniqueness
    matched_tawrdat_set = set()

    # Iterate over each row in the hikma dataframe
    for h_idx, h_row in hikma_df.iterrows():
        # Find the exact matching transactions in tawrdat dataframe
        matches = tawrdat_df[
            (tawrdat_df[tawrdat_field] == h_row[hikma_field]) &
            (tawrdat_df['date'] == h_row['date'])
        ]

        # If matches are found and have not been matched before, take the first one
        for t_idx in matches.index:
            if t_idx not in matched_tawrdat_set:
                matched_hikma_indices.append(h_idx)
                matched_tawrdat_indices.append(t_idx)
                matched_tawrdat_set.add(t_idx)
                break  # Ensure only one match per hikma row

    # Drop the matched transactions from both dataframes using the collected indices
    unmatched_hikma = hikma_df.drop(index=matched_hikma_indices)
    unmatched_tawrdat = tawrdat_df.drop(index=matched_tawrdat_indices)

    return unmatched_hikma, unmatched_tawrdat

columns = [27, 28, 29, 30, 32, 33]
names = ['dis', 'date', 'jv number', 'jv', 'credit', 'debit']

hikma_path = '../data/source/hikma.csv'
tawrdat_path = '../data/source/tawrdat.csv'

hikma_df = load_and_clean_data(hikma_path, columns, names)
tawrdat_df = load_and_clean_data(tawrdat_path, columns, names)

split_credit_debit(hikma_df, '../data/cleaned/split/hikma')
split_credit_debit(tawrdat_df, '../data/cleaned/split/tawrdat')

hikma_credit_unmatched = pd.read_csv('../data/cleaned/split/hikma_credit.csv')
tawrdat_debit_unmatched = pd.read_csv('../data/cleaned/split/tawrdat_debit.csv')
hikma_debit_unmatched = pd.read_csv('../data/cleaned/split/hikma_debit.csv')
tawrdat_credit_unmatched = pd.read_csv('../data/cleaned/split/tawrdat_credit.csv')

unmatched_hikma_credit, unmatched_tawrdat_debit = remove_exact_matched_transactions(
    hikma_credit_unmatched, tawrdat_debit_unmatched, 'credit', 'debit'
)
unmatched_hikma_debit, unmatched_tawrdat_credit = remove_exact_matched_transactions(
    hikma_debit_unmatched, tawrdat_credit_unmatched, 'debit', 'credit'
)

unmatched_hikma_credit.to_csv('../data/cleaned/hikma_credit_final_unmatched.csv', index=False)
unmatched_tawrdat_debit.to_csv('../data/cleaned/tawrdat_debit_final_unmatched.csv', index=False)
unmatched_hikma_debit.to_csv('../data/cleaned/hikma_debit_final_unmatched.csv', index=False)
unmatched_tawrdat_credit.to_csv('../data/cleaned/tawrdat_credit_final_unmatched.csv', index=False)

print("Updated unmatched data files have been saved.")

import pandas as pd

def load_data(file_path):
    df = pd.read_csv(file_path)
    df['date'] = pd.to_datetime(df['date'])
    return df

def summarize_transactions(file_path):
    df = load_data(file_path)
    summary = df.groupby('date').agg({'credit': 'sum', 'debit': 'sum'}).reset_index()
    return summary

# Paths to the final unmatched files
hikma_credit_final = '../data/cleaned/hikma_credit_final_unmatched.csv'
tawrdat_debit_final = '../data/cleaned/tawrdat_debit_final_unmatched.csv'
hikma_debit_final = '../data/cleaned/hikma_debit_final_unmatched.csv'
tawrdat_credit_final = '../data/cleaned/tawrdat_credit_final_unmatched.csv'

# Summarize each file
hikma_credit_summary = summarize_transactions(hikma_credit_final)
tawrdat_debit_summary = summarize_transactions(tawrdat_debit_final)
hikma_debit_summary = summarize_transactions(hikma_debit_final)
tawrdat_credit_summary = summarize_transactions(tawrdat_credit_final)

def load_data(file_path):
    df = pd.read_csv(file_path)
    df['date'] = pd.to_datetime(df['date'])
    return df

def match_and_remove(transactions_1, transactions_2, days_tolerance):
    # Extend the date range for matching
    transactions_1['date_start'] = transactions_1['date'] - timedelta(days=days_tolerance)
    transactions_1['date_end'] = transactions_1['date'] + timedelta(days=days_tolerance)

    # To store indices of matched entries
    matched_indices_1 = set()
    matched_indices_2 = set()

    print("Starting the matching process...")
    
    # Iterate over each transaction in transactions_1 to find matching transactions in transactions_2
    for idx1, row1 in transactions_1.iterrows():
        potential_matches = transactions_2[
            (transactions_2['date'] >= row1['date_start']) &
            (transactions_2['date'] <= row1['date_end']) &
            (transactions_2['debit'] == row1['credit'])  # Matching debit in transactions_2 with credit in transactions_1
        ]

        # print(f"Checking transaction from {transactions_1.name} at index {idx1}:")
        # print(f"Date range: {row1['date_start'].date()} to {row1['date_end'].date()}, Amount: {row1['credit']}")
        # print(f"Found {len(potential_matches)} potential matches in {transactions_2.name}")

        if not potential_matches.empty:
            matched_indices_1.add(idx1)
            matched_indices_2.update(potential_matches.index.tolist())
            # print(f"Matched with indices in {transactions_2.name}: {list(potential_matches.index)}")

    # Remove matched transactions
    unmatched_1 = transactions_1.drop(index=list(matched_indices_1))
    unmatched_2 = transactions_2.drop(index=list(matched_indices_2))

    print("Matching process completed. Updating unmatched files...")
    return unmatched_1, unmatched_2

# Load data
tawrdat_debit_unmatched = load_data('../data/cleaned/tawrdat_debit_final_unmatched.csv')
hikma_credit_unmatched = load_data('../data/cleaned/hikma_credit_final_unmatched.csv')
tawrdat_debit_unmatched.name = "Tawrdat Debit"
hikma_credit_unmatched.name = "Hikma Credit"

# Match and filter the data
unmatched_hikma_credit,unmatched_tawrdat_debit = match_and_remove(
     hikma_credit_unmatched,tawrdat_debit_unmatched, days_tolerance=4
)

# Save the unmatched data back to CSV files
unmatched_tawrdat_debit.to_csv('../data/cleaned/final/tawrdat_debit_filtered.csv', index=False)
unmatched_hikma_credit.to_csv('../data/cleaned/final/hikma_credit_filtered.csv', index=False)

print("Filtered unmatched files saved.1")

# Load data
hikma_debit_unmatched = load_data('../data/cleaned/hikma_debit_final_unmatched.csv')
tawrdat_credit_unmatched = load_data('../data/cleaned/tawrdat_credit_final_unmatched.csv')
hikma_debit_unmatched.name = "Hikma Debit"
tawrdat_credit_unmatched.name = "Tawrdat Credit"

# Match and filter the data
unmatched_tawrdat_credit ,unmatched_hikma_debit= match_and_remove(
     tawrdat_credit_unmatched, hikma_debit_unmatched, days_tolerance=4
)

# Save the unmatched data back to CSV files
unmatched_tawrdat_credit.to_csv('../data/cleaned/final/tawrdat_credit_filtered.csv', index=False)
unmatched_hikma_debit.to_csv('../data/cleaned/final/hikma_debit_filtered.csv', index=False)

print("Filtered unmatched files saved.2")



def load_and_summarize(file_path):
    # Load data from the CSV file
    df = pd.read_csv(file_path)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    # Summarize credit and debit by date
    summary = df.groupby('date').agg({'credit': 'sum', 'debit': 'sum'}).reset_index()
    return df, summary

def find_and_remove_balanced_dates(file1, file2):
    df1, summary1 = load_and_summarize(file1)
    df2, summary2 = load_and_summarize(file2)
    
    # Merge summaries to find balanced dates
    merged = pd.merge(summary1, summary2, on='date', suffixes=('_1', '_2'))
    balanced_dates = merged[((merged['credit_1'] - merged['debit_2']).abs() < 1e-2) & 
                            ((merged['credit_2'] - merged['debit_1']).abs() < 1e-2)]['date']
    
    # Remove transactions from original dataframes corresponding to balanced dates
    df1_filtered = df1[~df1['date'].isin(balanced_dates)]
    df2_filtered = df2[~df2['date'].isin(balanced_dates)]
    
    return df1_filtered, df2_filtered

def process_files(files):
    for i in range(0, len(files), 2):
        file1, file2 = files[i], files[i+1]
        df1_filtered, df2_filtered = find_and_remove_balanced_dates(file1, file2)
        
        # Save back the filtered data
        df1_filtered.to_csv(file1, index=False)
        df2_filtered.to_csv(file2, index=False)

# File paths
file_paths = [
    '../data/cleaned/final/hikma_credit_filtered.csv',
    '../data/cleaned/final/tawrdat_debit_filtered.csv',
    '../data/cleaned/final/hikma_debit_filtered.csv',
    '../data/cleaned/final/tawrdat_credit_filtered.csv'
]

# Process the files
process_files(file_paths)


