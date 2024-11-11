import os
import pdfplumber
import pandas as pd
from multiprocessing import Pool
import torch

def process_page(page_number, pdf_file, is_first_page, device):
    
    print(f"Processing page {page_number} of {pdf_file} on {device}...")

    with pdfplumber.open(pdf_file) as pdf:
        page = pdf.pages[page_number]
        tables = page.extract_tables()
        if tables:
            page_data = [pd.DataFrame(table) for table in tables]
            if is_first_page:
                return pd.concat(page_data, ignore_index=True)
            else:
               
                page_data = [df.iloc[1:] for df in page_data]
                return pd.concat(page_data, ignore_index=True)
    return None

def process_pdf(pdf_file, excel_dir, num_processes):
   
    excel_file = os.path.basename(pdf_file).replace('.pdf', '.xlsx')
    excel_path = os.path.join(excel_dir, excel_file)
    print(f"Processing {pdf_file}...")

    with pdfplumber.open(pdf_file) as pdf:
        num_pages = len(pdf.pages)

    page_numbers = list(range(num_pages))
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    print(device)
    with Pool(processes=num_processes) as pool:
        
        results = pool.starmap(process_page, [(page_number, pdf_file, page_number == 0, device) for page_number in page_numbers])

    valid_results = [df for df in results if df is not None]
    if valid_results:
        final_df = pd.concat(valid_results, ignore_index=True)
        final_df.to_excel(excel_path, index=False)
        print(f"Saved {excel_file}")
    else:
        print(f"No tables found in {pdf_file}, skipping...")

def process_all_pdfs(pdf_dir, excel_dir, num_processes):
   
    os.makedirs(excel_dir, exist_ok=True)

    for filename in os.listdir(pdf_dir):
        if filename.lower().endswith('.pdf'):
            pdf_file_path = os.path.join(pdf_dir, filename)
            process_pdf(pdf_file_path, excel_dir, num_processes)

if __name__ == "__main__":
    
    pdf_dir = "/home/usr/Downloads/Agriculture Electricity Pump Subsidy/" 
    excel_dir = "/home/usr/Downloads/Agriculture Electricity Pump Subsidy Excel Files/"
    num_processes = 22  
    process_all_pdfs(pdf_dir, excel_dir, num_processes)
