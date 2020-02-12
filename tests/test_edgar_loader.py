import ingestion.edgar_loader as edgar
import shutil
import os


def test_crawl_year_and_quarter():
    filing_days_to_process = edgar.crawl_year_and_quarter(2019, 4)
    assert len(filing_days_to_process) == 61


def test_download_single_day_filings():
    os.mkdir('edgar')
    try:
        extract_dir, files_downloaded = edgar.download_single_day_filings(2019, 4, '20191231', data_dir='edgar')
        assert len(files_downloaded.items()) > 0
    finally:
        shutil.rmtree('edgar')
