# -*- coding: utf-8 -*-
"""

"""
import argparse
import sys
import logging
import boto3
from botocore.exceptions import ClientError
import requests
from bs4 import BeautifulSoup
import itertools
import os.path
import os
import tarfile
import shutil

# from sec_edgar_to_s3 import __version__

__author__ = "Phat Loc"
__copyright__ = "Phat Loc"
__license__ = "mit"
__version__ = "0.0.1"

_logger = logging.getLogger(__name__)

SEC_EDGAR_URL = 'https://www.sec.gov/Archives/edgar/'


def map_filing_to_downloaded_path(filings_dir: str):
    """
        Builds a dictionary of filing to downloaded path
    Args:
        filings_dir:

    Returns:

    """
    filings = map(lambda file_name: (os.path.splitext(file_name)[0], os.path.join(filings_dir, file_name)),
                  os.listdir(filings_dir))
    filings = filter(lambda entry: os.path.isfile(entry[1]), filings)
    filings = dict(filings)
    return filings


def download_single_day_filings(year: int, quarter: int, filing_date: str, data_dir: str = '/home/ploc/edgar'):
    """

    :param year:
    :param quarter:
    :param filing_date:
    :param data_dir:
    :return:
    """
    daily_filings_file_name = '{filing_date}.nc.tar.gz'.format(filing_date=filing_date)
    daily_filings_url = SEC_EDGAR_URL + 'Feed/{year}/QTR{quarter}/{daily_filings_file_name}'. \
        format(year=year, quarter=quarter, daily_filings_file_name=daily_filings_file_name)

    daily_filings_tar_gz_file_path = os.path.join(data_dir, daily_filings_file_name)
    _logger.info('Downloading {0} to {1}'.format(daily_filings_url, daily_filings_tar_gz_file_path))
    try:
        with requests.get(daily_filings_url, stream=True) as r_daily_filings:
            r_daily_filings.raise_for_status()
            with open(daily_filings_tar_gz_file_path, 'wb') as daily_filings_tar_gz:
                for chunk in r_daily_filings.iter_content(chunk_size=8192):
                    if chunk:
                        daily_filings_tar_gz.write(chunk)

        extract_dir = os.path.join(data_dir, filing_date)
        _logger.info('Extracting {0} to {1}'.format(daily_filings_tar_gz_file_path, extract_dir))
        with tarfile.open(daily_filings_tar_gz_file_path, 'r:gz') as filings_tar:
            filings_tar.extractall(extract_dir)
    finally:
        os.remove(daily_filings_tar_gz_file_path)

    files_downloaded = map_filing_to_downloaded_path(extract_dir)
    return extract_dir, files_downloaded


def download_and_store_single_day(year: int, quarter: int, filing_date: str,
                                  bucket: str = 'dataengine-xyz-edgar-raw-data', data_dir: str = '/home/ploc/edgar'):
    """
    Downloads and stores a single day of SEC filings to S3
    Args:
        year:
        quarter:
        filing_date:
        bucket:
        data_dir:

    Returns:

    """

    # Step 1: Download and parse the master.{filing_date}.idx to determine what filings were made
    master_daily_idx_url = SEC_EDGAR_URL + 'daily-index/{year}/QTR{quarter}/master.{filing_date}.idx'. \
        format(year=year, quarter=quarter, filing_date=filing_date)
    # master_daily_idx_url = 'https://www.sec.gov/Archives/edgar/daily-index/2020/QTR1/master.20200107.idx'
    r_master_idx = requests.get(master_daily_idx_url)
    master_idx_file_name = master_daily_idx_url.split('/')[-1]

    master_idx_filings = r_master_idx.content.decode('UTF-8')

    # remove SEC header
    # The default key format i.e. 'CIK|Company Name|Form Type|Date Filed|File Name'
    master_idx_filings = master_idx_filings.splitlines()
    master_idx_filings = list(itertools.dropwhile(
        lambda line: not line.startswith('CIK|Company Name|Form Type|Date Filed|File Name'),
        master_idx_filings))[2:]

    master_idx_filings = [r for r in master_idx_filings if lambda filing: len(filing) > 0]

    # Extract the filename
    file_name_to_obj_key = map(lambda k: k.split('/')[-1], master_idx_filings)
    # Drop the .txt
    file_name_to_obj_key = map(lambda file_name: file_name[:len(file_name) - 4], file_name_to_obj_key)
    file_name_to_obj_key = list(file_name_to_obj_key)

    # Move the company name to end because it mess up how S3 partitions the data
    def reorder_row(row: str):
        parts = row.split('|')
        return '|'.join([parts[0], parts[2], parts[3], parts[1], parts[4]])

    master_idx_filings = [reorder_row(row) for row in master_idx_filings]
    filing_to_key_dict = dict(zip(file_name_to_obj_key, master_idx_filings))

    # Step 2: Download and unzip the actual filings
    extract_dir, files_downloaded = download_single_day_filings(year, quarter, filing_date, data_dir)

    try:
        # Step 3: Upload to S3
        _logger.info('Uploading to S3 {bucket}'.format(bucket=bucket))
        for filing_id, file_name in files_downloaded.items():
            if filing_id in filing_to_key_dict:
                object_name = filing_to_key_dict[filing_id]
                s3_client = boto3.client('s3')
                try:
                    response = s3_client.upload_file(file_name, bucket, object_name)
                except ClientError as e:
                    _logger.error(e)
    finally:
        # Step 4: Remove the Extracted files
        _logger.info('Deleting {extract_dir}'.format(extract_dir=extract_dir))
        shutil.rmtree(extract_dir)


def crawl_year_and_quarter(year: int, quarter: int):
    """
    Provide a year and quarter and this function returns which days are available to download
    :param year:
    :param quarter:
    :return: A list of tuples (year, quarter, filing_date)
    """
    daily_idx_url = SEC_EDGAR_URL + 'daily-index/{year}/QTR{quarter}/'.format(year=year, quarter=quarter)
    _logger.info('Downloading master idx file: {0}'.format(daily_idx_url))
    r = requests.get(daily_idx_url)
    soup = BeautifulSoup(r.content, 'html.parser')
    dates_with_filings = list(
        filter(lambda txt: txt.startswith('master.'), map(lambda ele: ele.text, soup.find_all('a'))))

    filing_days_to_process = map(lambda filing_date: (year, quarter, filing_date.split('.')[1]), dates_with_filings)
    return list(filing_days_to_process)


def store_a_quarter_of_filings(year: int, quarter: int, bucket: str = 'dataengine-xyz-edgar-raw-data',
                               data_dir: str = '/home/ploc/edgar'):
    """

    Args:
        year:
        quarter:
        bucket:

    Returns:

    """
    days_in_quarter = crawl_year_and_quarter(year, quarter)

    for year, quarter, filing_date in days_in_quarter:
        _logger.info(
            'Loading {year} {quarter} {filing_date}'.format(year=year, quarter=quarter, filing_date=filing_date))
        try:
            download_and_store_single_day(year, quarter, filing_date, bucket, data_dir)
        except Exception as e:
            _logger.error(e)
            raise e


def parse_args(args):
    """Parse command line parameters

    Args:
      args ([str]): command line parameters as list of strings

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(
        description="Downloads daily SEC edgar filings for a given year and quarter and saves it S3")
    parser.add_argument(
        dest="year",
        help="Filing Year",
        type=int,
        metavar="INT")
    parser.add_argument(
        dest="quarter",
        help="Filing Quarter",
        type=int,
        metavar="INT")
    parser.add_argument(
        dest="data_dir",
        help="Data dir",
        type=str)
    parser.add_argument(
        dest="bucket",
        help="S3 Bucket",
        type=str)
    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO)
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG)
    return parser.parse_args(args)


def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(level=loglevel, stream=sys.stdout,
                        format=logformat, datefmt="%Y-%m-%d %H:%M:%S")


def main(args):
    """Main entry point allowing external calls

    Args:
      args ([str]): command line parameter list
    """
    pass
    args = parse_args(args)
    setup_logging(args.loglevel)
    _logger.debug("Starting downloading from edgar and saving to S3")
    store_a_quarter_of_filings(args.year, args.quarter, args.bucket, args.data_dir)
    _logger.info("Script ends here")


def run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
