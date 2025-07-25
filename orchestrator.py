#!/usr/bin/env python3
"""
JPX Data Pipeline Orchestrator
-------------------------------
This script automates the complete data processing workflow by executing the
three main scripts in the correct sequence. It reads 'config.ini' to
determine the scraping mode (full or targeted).
"""

import os
import sys
import subprocess
import time
import shutil
from pathlib import Path

# --- Helper Functions ---

def print_header(title: str):
    """Prints a formatted header to the console."""
    print("\n" + "=" * 80)
    print(f"==> {title.upper()}")
    print("=" * 80)

def print_success(message: str):
    """Prints a success message."""
    print(f"[OK] {message}")

def print_error(message: str):
    """Prints an error message and exits."""
    print(f"[ERROR] {message}", file=sys.stderr)
    sys.exit(1)

def create_default_config():
    """Creates a default config.ini if it doesn't exist."""
    config_path = Path('config.ini')
    if not config_path.exists():
        print("[INFO] `config.ini` not found. Creating a default file.")
        default_config = """# JPX Scraper Configuration
# ---------------------------
# mode: full     - Scrapes all historical data from 2015-2025.
#       targeted - Scrapes only for the specific year, month, and week below.
#
# For targeted mode, please specify the year, month, and week.
# month: Use 3-letter abbreviation (e.g., Jan, Feb, Mar, etc.)
# week:  The week number as seen on the website (e.g., 1, 2, 3, 4, 5).

[ScraperSettings]
mode = full
year = 2023
month = Dec
week = 2
"""
        with open(config_path, 'w') as f:
            f.write(default_config)
        print_success(f"Default '{config_path}' created. Please review it before running again if needed.")

def check_required_scripts():
    """Verify that all necessary scripts are present in the directory."""
    required = ['Scrape.py', 'new2.py', 'multiple.py']
    missing = [script for script in required if not Path(script).exists()]
    if missing:
        print_error(f"The following required script(s) are missing: {', '.join(missing)}")
    print_success("All required scripts are present.")


def run_workflow():
    """Executes the entire data processing pipeline."""
    
    start_time = time.time()
    
    # --- STAGE 1: SCRAPE PDFS ---
    print_header("Stage 1: Running Scraper (Scrape.py)")
    
    try:
        # Enforce UTF-8 encoding for the subprocess environment
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        process = subprocess.run(
            [sys.executable, 'Scrape.py'], 
            check=True, 
            capture_output=True, 
            text=True, 
            encoding='utf-8', 
            errors='replace',
            env=env
        )
        print(process.stdout)
        if process.stderr:
             print("--- Scraper Warnings/Errors ---\n", process.stderr)

    except FileNotFoundError:
        print_error("Python executable not found. Make sure Python is in your system's PATH.")
    except subprocess.CalledProcessError as e:
        print_error(f"Scrape.py failed to execute.\n\n--- STDOUT ---\n{e.stdout}\n\n--- STDERR ---\n{e.stderr}")

    scraper_output_dir = Path('jpx_temp_storage')
    if not scraper_output_dir.exists() or not any(scraper_output_dir.iterdir()):
        print_error(f"Scraper finished, but the output directory '{scraper_output_dir}' is empty or was not created.")
    
    print_success("Stage 1 Complete: PDF files downloaded successfully.")


    # --- STAGE 2: CONVERT PDFS TO EXCEL ---
    print_header("Stage 2: Converting PDFs to Excel (using new2.py logic)")
    
    try:
        from new2 import TitleEnhancedConverter
    except ImportError as e:
        print_error(f"Could not import 'TitleEnhancedConverter' from new2.py. {e}")

    pdf_source_folders = [d for d in scraper_output_dir.iterdir() if d.is_dir() and any(d.glob('*.pdf'))]
    if not pdf_source_folders:
        print_error(f"No folders containing PDFs found inside '{scraper_output_dir}'. The scrape may not have found the target report.")

    print(f"Found {len(pdf_source_folders)} folders with PDFs to process.")
    
    excel_output_folders = []
    for pdf_folder in sorted(pdf_source_folders):
        print(f"\n--- Processing PDF folder: {pdf_folder.name} ---")
        excel_output_path = pdf_folder.parent / f"{pdf_folder.name}_excel_output"
        excel_output_folders.append(excel_output_path)
        
        converter = TitleEnhancedConverter(
            pdf_folder=str(pdf_folder),
            excel_output_folder=str(excel_output_path),
            max_workers=6
        )
        converter.process_all_files()

    print_success("Stage 2 Complete: Relevant PDF pages converted to Excel files.")


    # --- STAGE 3: PARSE AND CONSOLIDATE EXCEL FILES ---
    print_header("Stage 3: Consolidating Excel Data (using multiple.py logic)")

    try:
        from multiple import JGBFParser
    except ImportError as e:
        print_error(f"Could not import 'JGBFParser' from multiple.py. {e}")
        
    if not excel_output_folders:
        print_error("No Excel output folders were generated in Stage 2. Cannot proceed.")

    print(f"Found {len(excel_output_folders)} Excel folders to parse.")
    
    parser = JGBFParser(output_folder="parsed_output")
    parser.process_folders(excel_output_folders)
    
    print_success("Stage 3 Complete: Final consolidated report has been generated.")


    # --- FINAL SUMMARY ---
    total_time = time.time() - start_time
    print_header("Workflow Complete")
    print(f"[SUCCESS] Entire data pipeline finished successfully in {total_time:.2f} seconds.")
    print(f"  - Raw PDFs are in: '{scraper_output_dir}'")
    print(f"  - Intermediate Excel files are in folders like: '{excel_output_folders[0].name}', etc.")
    print(f"  - Your final, consolidated report is in: 'parsed_output'")
    
    cleanup_choice = input("\nDo you want to delete the intermediate files (PDFs and individual Excel folders)? (y/N): ").lower().strip()
    if cleanup_choice == 'y':
        print("\n[INFO] Cleaning up intermediate files...")
        try:
            shutil.rmtree(scraper_output_dir)
            print(f"  - Removed: {scraper_output_dir}")
        except Exception as e:
            print(f"  [WARN] Could not remove '{scraper_output_dir}': {e}")
        for folder in excel_output_folders:
            if folder.exists():
                try:
                    shutil.rmtree(folder)
                    print(f"  - Removed: {folder}")
                except Exception as e:
                    print(f"  [WARN] Could not remove '{folder}': {e}")
        print_success("Cleanup complete.")


if __name__ == "__main__":
    check_required_scripts()
    create_default_config() # Create config if it doesn't exist
    run_workflow()