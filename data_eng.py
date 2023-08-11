import dask.dataframe as dd
import requests
import zipfile
import io
import glob

ano = 2023
new_data_url = f'https://portal.inmet.gov.br/uploads/dadoshistoricos/{ano}.zip'

response = requests.get(new_data_url)
with zipfile.ZipFile(io.BytesIO(response.content), 'r') as zip_ref:
    zip_ref.extractall('temp_folder')

path = "temp_folder/"
all_files = glob.glob(path + "*.csv")
dfs = []

for filename in all_files:
    try:
        with open(filename, 'r', encoding='latin1') as csv_file:
            first_line = csv_file.readline()
            second_line = csv_file.readline()
            
            # Adjust the column indices here
            value_from_first_line = first_line.split(';')[1]  # Adjust column index
            print(value_from_first_line)
            value_from_second_line = second_line.split(';')[2]  # Adjust column index
            print(value_from_second_line)

            dtype = object  # All columns as strings
            df = dd.read_csv(filename, header=None, sep=';', skiprows=8, encoding='latin1', assume_missing=True, blocksize=None, dtype=dtype)
            
            # Create new columns with values from the first and second lines
            df['ValueFromFirstLine'] = value_from_first_line
            df['ValueFromSecondLine'] = value_from_second_line
            
            dfs.append(df)
    except pd.errors.ParserError:
        print(f"Warning: Parsing error in '{filename}'. Skipping this file.")

# Concatenate DataFrames and set index
combined_df = dd.concat(dfs, axis=0, ignore_index=True)

# Compute and reset index for proper handling of Dask DataFrame
combined_df = combined_df.compute().reset_index(drop=True)

# Print the first few rows of the combined DataFrame
print(combined_df.head())
