import pandas as pd
from datetime import datetime

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

split_credit_debit(hikma_df, '../data/cleaned/hikma')
split_credit_debit(tawrdat_df, '../data/cleaned/tawrdat')

hikma_credit_unmatched = pd.read_csv('../data/cleaned/hikma_credit.csv')
tawrdat_debit_unmatched = pd.read_csv('../data/cleaned/tawrdat_debit.csv')
hikma_debit_unmatched = pd.read_csv('../data/cleaned/hikma_debit.csv')
tawrdat_credit_unmatched = pd.read_csv('../data/cleaned/tawrdat_credit.csv')

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

# Print the summaries to the terminal
print("Hikma Credit Summary:")
print(hikma_credit_summary)
print("\nTawrdat Debit Summary:")
print(tawrdat_debit_summary)
print("\nHikma Debit Summary:")
print(hikma_debit_summary)
print("\nTawrdat Credit Summary:")
print(tawrdat_credit_summary)
