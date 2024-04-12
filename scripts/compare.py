import pandas as pd
import matplotlib.pyplot as plt

# Define the function to clean 'credit' and 'debit' columns
def clean_financial_data(df):
    df['credit'] = pd.to_numeric(df['credit'].replace(',', '', regex=True), errors='coerce')
    df['debit'] = pd.to_numeric(df['debit'].replace(',', '', regex=True), errors='coerce')
    df.fillna({'credit': 0.1, 'debit': 0.1}, inplace=True)
    return df

# Load and clean the DataFrames
def load_and_clean_data(file_path, columns, names):
    df = pd.read_csv(file_path, header=None, usecols=columns, names=names)
    df = clean_financial_data(df)
    return df


# Paths to the CSV files
hikma_file_path = '../data/source/hikma.csv'
tawrdat_file_path = '../data/source/tawrdat.csv'

# Column indices and names for hikma and tawrdat (adjust as needed)
columns = [27, 28, 29, 30, 32, 33]
names = ['dis', 'date', 'jv number', 'jv', 'credit', 'debit']

# Load and clean datasets
hikma_df = load_and_clean_data(hikma_file_path, columns, names)
tawrdat_df = load_and_clean_data(tawrdat_file_path, columns, names)

hikma_df['date'] = pd.to_datetime(hikma_df['date'], format='%d/%m/%Y')
tawrdat_df['date'] = pd.to_datetime(tawrdat_df['date'], format='%d/%m/%Y')

# Group by 'date' and sum 'credit' for hikma_df and 'debit' for tawrdat_df
credit_totals_hikma = hikma_df.groupby('date')['credit'].sum().reset_index()
debit_totals_tawrdat = tawrdat_df.groupby('date')['debit'].sum().reset_index()

# Merge on 'date' and compare totals
matched_dates = pd.merge(credit_totals_hikma, debit_totals_tawrdat, on='date', suffixes=('_credit', '_debit'))
matched_dates = matched_dates[matched_dates['credit'] == matched_dates['debit']]['date']

# Filter out the matching dates from both original DataFrames
hikma_filtered = hikma_df[~hikma_df['date'].isin(matched_dates)]
tawrdat_filtered = tawrdat_df[~tawrdat_df['date'].isin(matched_dates)]

# # Save the filtered DataFrames for further analysis
hikma_filtered.to_csv('../data/cleaned/hikma_filtered.csv', index=True)
tawrdat_filtered.to_csv('../data/cleaned/tawrdat_filtered.csv', index=True)

print("Filtered data saved successfully.")


# Summarize discrepancies, ignoring zero differences
discrepancies_summary = pd.merge(credit_totals_hikma, debit_totals_tawrdat, on='date', how='outer', suffixes=('_credit', '_debit')).fillna(0)
discrepancies_summary['difference'] = discrepancies_summary['credit'] - discrepancies_summary['debit']


non_zero_discrepancies = discrepancies_summary[discrepancies_summary['difference'] != 0].reset_index(drop=True)

print(discrepancies_summary)
print("Non-Zero Discrepancies Summary:")
print(non_zero_discrepancies)




