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

def remove_matched_transactions(hikma_df, tawrdat_df, hikma_field, tawrdat_field):
    hikma_df = hikma_df.reset_index()
    tawrdat_df = tawrdat_df.reset_index()
    matched_df = pd.merge(hikma_df, tawrdat_df, on='date', suffixes=('_hikma', '_tawrdat'))
    matched_df = matched_df[matched_df[hikma_field + '_hikma'] == matched_df[tawrdat_field + '_tawrdat']]
    unmatched_hikma = hikma_df.drop(matched_df['index_hikma'])
    unmatched_tawrdat = tawrdat_df.drop(matched_df['index_tawrdat'])
    return unmatched_hikma.drop(columns='index'), unmatched_tawrdat.drop(columns='index')

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

unmatched_hikma_credit, unmatched_tawrdat_debit = remove_matched_transactions(
    hikma_credit_unmatched, tawrdat_debit_unmatched, 'credit', 'debit'
)
unmatched_hikma_debit, unmatched_tawrdat_credit = remove_matched_transactions(
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

        print(f"Checking transaction from {transactions_1.name} at index {idx1}:")
        print(f"Date range: {row1['date_start'].date()} to {row1['date_end'].date()}, Amount: {row1['credit']}")
        print(f"Found {len(potential_matches)} potential matches in {transactions_2.name}")

        if not potential_matches.empty:
            matched_indices_1.add(idx1)
            matched_indices_2.update(potential_matches.index.tolist())
            print(f"Matched with indices in {transactions_2.name}: {list(potential_matches.index)}")

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
     hikma_credit_unmatched,tawrdat_debit_unmatched, days_tolerance=2
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
     tawrdat_credit_unmatched, hikma_debit_unmatched, days_tolerance=2
)

# Save the unmatched data back to CSV files
unmatched_tawrdat_credit.to_csv('../data/cleaned/final/tawrdat_credit_filtered.csv', index=False)
unmatched_hikma_debit.to_csv('../data/cleaned/final/hikma_debit_filtered.csv', index=False)

print("Filtered unmatched files saved.2")