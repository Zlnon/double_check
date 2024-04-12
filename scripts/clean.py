import pandas as pd

def load_and_clean_data(file_path, columns, names):
    try:
        # Load the data with specific columns and no header initially
        df = pd.read_csv(file_path, header=None, usecols=columns, names=names)

        # Convert and clean 'credit' and 'debit' columns
        df['credit'] = pd.to_numeric(df['credit'].replace(',', '', regex=True), errors='coerce').fillna(0.1)
        df['debit'] = pd.to_numeric(df['debit'].replace(',', '', regex=True), errors='coerce').fillna(0.1)

        # Convert 'date' to datetime format immediately after loading
        df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
        
        return df
    except Exception as e:
        print(f"Error loading or cleaning data from {file_path}: {e}")
        return pd.DataFrame()

def summarize_and_save(hikma_df, tawrdat_df):
    # Sum and reset index for grouping by 'date'
    credit_totals = hikma_df.groupby('date')['credit'].sum().reset_index()
    debit_totals = tawrdat_df.groupby('date')['debit'].sum().reset_index()

    # Merge and identify non-matching dates based on totals
    matched_dates = pd.merge(credit_totals, debit_totals, on='date', suffixes=('_credit', '_debit'))
    non_matching = matched_dates[matched_dates['credit'] != matched_dates['debit']]['date']

    # Filter and save the non-matching data
    hikma_filtered = hikma_df[hikma_df['date'].isin(non_matching)]
    tawrdat_filtered = tawrdat_df[tawrdat_df['date'].isin(non_matching)]

    hikma_filtered.to_csv('../data/cleaned/hikma_filtered.csv', index=False)
    tawrdat_filtered.to_csv('../data/cleaned/tawrdat_filtered.csv', index=False)
    print("Filtered data saved successfully.")

    # Calculate discrepancies
    discrepancies = pd.merge(credit_totals, debit_totals, on='date', how='outer', suffixes=('_credit', '_debit')).fillna(0)
    discrepancies['difference'] = discrepancies['credit'] - discrepancies['debit']
    non_zero_discrepancies = discrepancies[discrepancies['difference'] != 0].reset_index(drop=True)

    print("Non-Zero Discrepancies Summary:")
    print(non_zero_discrepancies)

# Define paths, columns, and column names
paths = ['../data/source/hikma.csv', '../data/source/tawrdat.csv']
columns = [27, 28, 29, 30, 32, 33]
names = ['dis', 'date', 'jv number', 'jv', 'credit', 'debit']

# Load and clean datasets
hikma_df = load_and_clean_data(paths[0], columns, names)
tawrdat_df = load_and_clean_data(paths[1], columns, names)

# Process and save the results
summarize_and_save(hikma_df, tawrdat_df)
