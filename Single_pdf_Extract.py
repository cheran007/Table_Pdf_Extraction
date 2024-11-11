import os
import pdfplumber
import pandas as pd

# Specify the exact PDF file you want to process
pdf_file = "/home/usr/Downloads/oct29.pdf"  # Replace with the specific PDF file
excel_dir = "/home/usr/Downloads/output_excel/"

# Create the folder to save Excel files if it doesn't exist
os.makedirs(excel_dir, exist_ok=True)

# Set the Excel file path
excel_file = os.path.basename(pdf_file).replace('.pdf', '.xlsx')
excel_path = os.path.join(excel_dir, excel_file)

print(f"Processing {pdf_file}...")

# Extract table data from the PDF
with pdfplumber.open(pdf_file) as pdf:
    all_tables = []
    for page in pdf.pages:
        tables = page.extract_tables()
        if tables:
            for table in tables:
                # Convert table to a pandas DataFrame and add to list
                df = pd.DataFrame(table)
                all_tables.append(df)

    # Concatenate all DataFrames (if multiple tables) into one
    if all_tables:
        final_df = pd.concat(all_tables, ignore_index=True)
        # Save the DataFrame to an Excel file with the same name as the PDF
        final_df.to_excel(excel_path, index=False)
        print(f"Saved {excel_file}")
    else:
        print(f"No tables found in {pdf_file}, skipping...")
