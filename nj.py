import os
import requests
import zipfile
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

host = os.getenv("DB_HOST")
database = os.getenv("DB_DATABASE")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
port = os.getenv("DB_PORT")

db = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

years = range(2017, 2022)
counties = ['Burlington', 'Camden', 'Gloucester', 'Mercer']
table_names = ['Accidents', 'Drivers', 'Pedestrians', 'Occupants', 'Vehicles']
base_url = 'https://www.state.nj.us/transportation/refdata/accident/{year}/{county}{year}{table}.zip'

os.makedirs('downloads', exist_ok=True)
os.makedirs('extracted', exist_ok=True)

for year in years:
    for county in counties:
        for table in table_names:
            url = base_url.format(year=year, county=county, table=table)
            zip_path = f'downloads/{county}_{year}_{table}.zip'

            response = requests.get(url)
            if response.status_code == 200:
                with open(zip_path, 'wb') as file:
                    file.write(response.content)

                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    extract_path = f'extracted/{county}_{year}_{table}'
                    os.makedirs(extract_path, exist_ok=True)
                    zip_ref.extractall(extract_path)
                print(f'Downloaded and extracted {zip_path}')
            else:
                print(f'Failed to download {url}')

# merge county/year csvs
combined_data = {table: pd.DataFrame() for table in table_names}
missing_files = []
discrepancies = {table: [] for table in table_names}  # store file paths causing discrepancies

# report issues is source data
with open('nj_import_issue_report.txt', 'w') as report_file:
    for year in years:
        for county in counties:
            for table in table_names:
                file_path = f'extracted/{county}_{year}_{table}/{county}{year}{table}.txt'
                field_names_path = f'nj_fields/{table}.csv'  # assuming the CSVs are in a 'nj_fields' directory

                if os.path.exists(file_path):
                    if os.path.exists(field_names_path):
                        with open(field_names_path, 'r') as f:
                            field_names = f.read().splitlines()  # read field names and split by new lines

                        try:
                            def bad_line_report(bad_line):
                                report_file.write(f"Bad line in file {file_path}: {bad_line}\n")
                                return None

                            df = pd.read_csv(file_path, header=None, on_bad_lines=bad_line_report, engine='python', delimiter=',', skip_blank_lines=False)  # read text file without header
                            df.columns = map(str.lower, field_names)  # assign field names and convert to lower case

                            # check for rows with inconsistent number of fields
                            row_lengths = df.apply(lambda x: x.dropna().size, axis=1)
                            problematic_rows = df[row_lengths != len(field_names)].index

                            if not problematic_rows.empty:
                                report_file.write(f"Problematic rows in file {file_path}:\n")
                                for row in problematic_rows:
                                    report_file.write(f"Row {row + 1}: {df.iloc[row].tolist()}\n")
                                df.drop(problematic_rows, inplace=True)

                        except pd.errors.ParserError as e:
                            report_file.write(f"ParserError for file {file_path}: {e}\n")
                    else:
                        report_file.write(f"Field names file for table '{table}' not found.\n")
                else:
                    report_file.write(f"Data file for {county} in {year} for table {table} not found.\n")
                
                # check for missing columns
                if table not in combined_data:
                    combined_data[table] = pd.DataFrame()

                if not df.empty:
                    missing_cols = set(combined_data[table].columns) - set(df.columns)
                    if missing_cols:
                        discrepancies[table].append((file_path, missing_cols))

                    combined_data[table] = pd.concat([combined_data[table], df], ignore_index=True)
            else:
                missing_files.append(file_path)

# results of table review
no_discrepancies = True
with open('nj_table_report.txt', 'w') as report_file:
    if missing_files:
        no_discrepancies = False
        report_file.write("Missing files:\n")
        for file in missing_files:
            report_file.write(file + "\n")

    if any(discrepancies.values()):
        no_discrepancies = False
        report_file.write("\nDiscrepancies in columns:\n")
        for table, issues in discrepancies.items():
            if issues:
                report_file.write(f"Table: {table}\n")
                for file_path, cols in issues:
                    report_file.write(f"File: {file_path}\n")
                    report_file.write(f"Missing columns: {', '.join(cols)}\n")

# if there are no discrepancies, proceed
if no_discrepancies:
    # load CSV to DB
    for table, df in combined_data.items():
        if table == 'Accidents':
            table_name = 'crash_newjersey'
        else:
            table_name = f'crash_nj_{table.lower()}'
        df.to_sql(table_name, db, if_exists='replace', index=False)
