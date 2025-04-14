import os
import shutil
import pandas as pd
from datetime import datetime, timedelta


def sort_files_by_date(folder):
    today = datetime.now()
    three_months_ago = today - timedelta(weeks=12)
    one_month_ago = today - timedelta(weeks=4)
    five_days_ago = today - timedelta(days=5)

    # First, process all BhavCopy files
    files_list = [file for file in os.listdir(
        folder) if file.startswith("BhavCopy") and file.endswith(".csv")]
    for filename in files_list:
        file_path = os.path.join(folder, filename)
        try:
            # Read and preprocess the raw BhavCopy file
            df = pd.read_csv(file_path)

            # Map the original columns to new standardized columns
            df = df[["TckrSymb", "TradDt", "OpnPric", "HghPric", "LwPric",
                     "ClsPric", "TtlTradgVol", "FinInstrmNm", "SctySrs"]]
            df.columns = ["Symbol", "Date", "Open", "High",
                          "Low", "Close", "Volume", "Name", "Series"]

            # Extract date from filename for new name
            date_part = filename.split("_")[6]
            file_date = datetime.strptime(date_part, "%Y%m%d")
            new_filename = file_date.strftime("%Y-%m-%d") + "-NSE-NEW.csv"
            new_path = os.path.join(folder, new_filename)

            # Save the preprocessed data with new column names
            df.to_csv(new_path, index=False)
            print(f"Preprocessed and saved {filename} to {new_filename}")

            # Remove original file
            os.remove(file_path)
            print(f"Removed original file: {filename}")

            # Copy to appropriate subfolders
            copy_to_subfolders(new_path, file_date,
                               five_days_ago, one_month_ago, three_months_ago)

        except (IndexError, ValueError) as e:
            print(f"Error processing file {filename}: {e}")
            continue

    # Then handle any remaining NSE-NEW files that might have been missed
    converted_files = [file for file in os.listdir(
        folder) if file.endswith("-NSE-NEW.csv")]
    for filename in converted_files:
        file_path = os.path.join(folder, filename)
        try:
            date_part = filename.split("-")[0:3]
            file_date = datetime.strptime("-".join(date_part), "%Y-%m-%d")
            copy_to_subfolders(file_path, file_date,
                               five_days_ago, one_month_ago, three_months_ago)
        except ValueError as e:
            print(f"Error parsing date from converted filename: {
                  filename}: {e}")
            continue


def copy_to_subfolders(file_path, file_date, five_days_ago, one_month_ago, three_months_ago):
    """
    Copy the file to appropriate subfolders based on its date.
    """
    if file_date >= three_months_ago:
        if not os.path.exists("DATA/3 MONTHS"):
            os.makedirs("DATA/3 MONTHS")
        shutil.copy(file_path, "DATA/3 MONTHS")
        print(f"Copied {file_path} to DATA/3 MONTHS")

    if file_date >= one_month_ago:
        if not os.path.exists("DATA/1 MONTH"):
            os.makedirs("DATA/1 MONTH")
        shutil.copy(file_path, "DATA/1 MONTH")
        print(f"Copied {file_path} to DATA/1 MONTH")

    if file_date >= five_days_ago:
        if not os.path.exists("DATA/5 DAYS"):
            os.makedirs("DATA/5 DAYS")
        shutil.copy(file_path, "DATA/5 DAYS")
        print(f"Copied {file_path} to DATA/5 DAYS")


def process_files(folder, output_file):
    files_list = [file for file in os.listdir(
        folder) if file.endswith("-NSE-NEW.csv")]
    if not files_list:
        print(f"No converted CSV files found in {folder}")
        return

    all_data = []
    for filename in sorted(files_list):
        file_path = os.path.join(folder, filename)
        try:
            df = pd.read_csv(file_path)
            # Convert column names to sentence case
            df.columns = [col.title() for col in df.columns]

            required_columns = {"Symbol", "Date", "Open",
                                "High", "Low", "Close", "Volume", "Name", "Series"}
            if not required_columns.issubset(df.columns):
                print(f"Skipping {filename}: required columns missing.")
                continue

            df = df[df["Series"].isin(["EQ", "BE"])]
            all_data.append(
                df[["Symbol", "Series", "Close", "Volume", "Date"]])

        except Exception as e:
            print(f"Error processing file {filename}: {e}")
            continue

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        try:
            existing_df = pd.read_excel(output_file)
            combined_df = pd.concat(
                [existing_df, combined_df], ignore_index=True)
        except FileNotFoundError:
            print(f"{output_file} does not exist. Creating a new file.")

        combined_df.to_excel(output_file, index=False)
        print(f"Processed data saved to {output_file}")


def create_results_folders():
    """
    Create RESULTS folder and its subfolders if they don't exist.
    """
    results_folder = "RESULTS"
    subfolders = ["DESCENDING", "ASCENDING"]

    if not os.path.exists(results_folder):
        os.makedirs(results_folder)
        print(f"Created {results_folder} folder")

    for subfolder in subfolders:
        subfolder_path = os.path.join(results_folder, subfolder)
        if not os.path.exists(subfolder_path):
            os.makedirs(subfolder_path)
            print(f"Created {subfolder_path} folder")


def calculate_and_filter_watchlist(folder, index, data_folder):
    """
    Calculate ROC and directly generate filtered watchlists in both ascending and descending order.
    """
    files_list = [file for file in os.listdir(
        folder) if file.endswith("-NSE-NEW.csv")]

    if len(files_list) < 2:
        print(f"Not enough files in {folder} for comparison")
        return

    # Get price data from one month folder for filtering
    one_month_folder = os.path.join(data_folder, "1 MONTH")
    filter_files = [f for f in os.listdir(
        one_month_folder) if f.endswith("-NSE-NEW.csv")]

    if len(filter_files) < 5:
        print("Not enough files for closing filter analysis")
        return

    # Read files for ROC calculation
    report = pd.read_csv(os.path.join(folder, files_list[-1]))
    df = pd.read_csv(os.path.join(folder, files_list[0]))

    # Read files for price filtering
    current_file = pd.read_csv(os.path.join(
        one_month_folder, filter_files[-2]))
    f1 = pd.read_csv(os.path.join(one_month_folder, filter_files[-3]))
    f2 = pd.read_csv(os.path.join(one_month_folder, filter_files[-4]))
    f3 = pd.read_csv(os.path.join(one_month_folder, filter_files[-5]))

    # Convert column names to sentence case
    for dataframe in [report, df, current_file, f1, f2, f3]:
        dataframe.columns = [col.title() for col in dataframe.columns]

    # Calculate ROC
    query = {}
    for symbol in report["Symbol"]:
        if symbol in df["Symbol"].tolist():
            report_index = report.index.get_loc(
                report[report['Symbol'] == symbol].index[0])
            df_index = df.index.get_loc(df[df['Symbol'] == symbol].index[0])
            old_close = df.at[df_index, 'Close']
            current_close = report.at[report_index, 'Close']
            roc = ((current_close-old_close)/old_close)*100
            query[symbol] = roc

    report = report[report["Series"].isin(["EQ", "BE"])]
    report.insert(len(report.columns), 'Roc',
                  value=report['Symbol'].map(query))

    # Create price dictionaries for filtering
    cmp = dict(zip(current_file['Symbol'], current_file['Close']))
    d1 = dict(zip(f1['Symbol'], f1['Close']))
    d2 = dict(zip(f2['Symbol'], f2['Close']))
    d3 = dict(zip(f3['Symbol'], f3['Close']))

    # Define conditions
    condition1 = '((Close < 1.01*Cmp) & (Close>.99*Cmp)) | ((Cmp < 1.01*D1) & (Cmp>.99*D1)) | ((D1 < 1.01*D2) & (D1>.99*D2)) | ((D2 < 1.01*D3) & (D2>.99*D3))'
    condition2 = '((Close < 1.01*Cmp) & (Close>.99*Cmp)) & ((Cmp < 1.01*D1) & (Cmp>.99*D1)) & ((D1 < 1.01*D2) & (D1>.99*D2)) & ((D2 < 1.01*D3) & (D2>.99*D3))'

    # Output file mapping
    output_files = {
        0: ('3 MONTHS (OR).xlsx', '3 MONTHS (AND).xlsx'),
        1: ('1 MONTH (OR).xlsx', '1 MONTH (AND).xlsx'),
        2: ('5 DAYS (OR).xlsx', '5 DAYS (AND).xlsx')
    }

    del_columns = ['Tottrdval', 'Timestamp', 'Totaltrades',
                   'Isin', 'Unnamed: 13', 'Last']

    # Generate files for both ascending and descending order
    for sort_order in ['ASCENDING', 'DESCENDING']:
        ascending = sort_order == 'ASCENDING'
        sorted_report = report.sort_values(by='Roc', ascending=ascending)
        top_report = sorted_report.head(49).copy()

        # Add required columns for filtering
        top_report['Cmp'] = top_report['Symbol'].map(cmp)
        top_report['D1'] = top_report['Symbol'].map(d1)
        top_report['D2'] = top_report['Symbol'].map(d2)
        top_report['D3'] = top_report['Symbol'].map(d3)

        # Create copies for different conditions
        df_or = top_report.copy()
        df_and = top_report.copy()

        # Remove specified columns
        for col in del_columns:
            if col.title() in df_or.columns:
                df_or.pop(col.title())
            if col.title() in df_and.columns:
                df_and.pop(col.title())

        # Remove Roc column after sorting
        df_or.pop('Roc')
        df_and.pop('Roc')

        # Get output filenames
        or_output, and_output = output_files[index]

        # Create output paths
        or_output_path = os.path.join('RESULTS', sort_order, or_output)
        and_output_path = os.path.join('RESULTS', sort_order, and_output)

        # Apply filters and save
        df_or.query(condition1).to_excel(or_output_path, index=False)
        print(f"Generated OR condition results: {or_output_path}")

        df_and.query(condition2).to_excel(and_output_path, index=False)
        print(f"Generated AND condition results: {and_output_path}")


def main():
    data_folder = "DATA"
    if not os.path.exists(data_folder):
        print("DATA folder not found.")
        return

    # Create RESULTS folders structure
    create_results_folders()

    # Sort and organize files
    sort_files_by_date(data_folder)

    # Process files in each subfolder
    subfolders = {"5 DAYS": 2, "1 MONTH": 1, "3 MONTHS": 0}
    for subfolder, index in subfolders.items():
        folder_path = os.path.join(data_folder, subfolder)
        if os.path.exists(folder_path):
            print(f"Processing files in {folder_path}...")
            process_files(folder_path, f"{subfolder}.xlsx")
            calculate_and_filter_watchlist(folder_path, index, data_folder)
        else:
            print(f"{folder_path} does not exist, skipping.")


if __name__ == "__main__":
    main()
