# Importing data manipulation methods:
import ftplib
import os
import pandas as pd
import argparse

# Breaking out the main function into small sub functions:
def build_path_main_dir(date: str, hour: str):
    """
    Args:
        date (str): Thedate value used to build the dir path (YYYYMMDD)
    
        hour (str): The hour file value used to build the dir path (00, 12, 18, etc)
        
    Returns:
        str: The hourly directory path
        
    """
    return f"pub/data/nccf/com/gfs/v16.2/gfs.{date}/{hour}/wave/gridded/"

def build_grib_file_names(hour: str, file_index: str):
    """Builds a list of all files to be downloaded based on the hour.
    
    Args:
    
        hour (str): The hour value used to determine the file name (00, 12, 18). 
        
        file_index (str): A file index used to build a specific file number. Usually between
            000 - 007. 
        
    Returns:
        dict: A dictionary containin the idx and the grib2 file names to be downloaded
        
    """
    main_file_name = f"gfswave.t{hour}z.atlocn.0p16.f{file_index}.grib2"
    return {
        "grib2": main_file_name,
        "idx": f"{main_file_name}.idx"}

def download_grib_data(ftp_con, date:str, hour:str, file_index:str):
    """The method that ingests the ftp connection to the NOAA server and downloads the specific grib2 
    and .idx files according to the specified date, hour and index. 

    This is the main function that is called in the main program's loop.

    Args:
        
        date (str): Thedate value used to build the dir path (YYYYMMDD)

        hour (str): The hour file value used to build the dir path (00, 12, 18, etc)

        file_index (str): A file index used to build a specific file number. Usually between 000-007.

    """
    # Re-setting the FTP diretory:
    ftp_con.cwd("~")
    
    # Building the directory:
    file_dir = build_path_main_dir(date=date, hour=hour)
    ftp_con.cwd(file_dir)
    
    # Creating a folder for date specific files:
    if not os.path.exists(date):
        os.makedirs(date)
    
    # Building the filename to be downloaded:
    files = build_grib_file_names(hour=hour, file_index=file_index)
    
    # Downloading the grib2 and idx files from the FTP server:
    for file_type in files:
        file_name = files[file_type]
    
        with open(f"{date}/{file_name}", "wb") as file:
            # use FTP's RETR command to download the file
            ftp_con.retrbinary(f"RETR {file_name}", file.write) # This is the command that actually downloads it.


## Main Method ##:
def get_grib_data(start_date=None, end_date=None, date=None):
    # Hard coding the hour and index values:
    hour_ranges = ["00", "06", "12", "18"]
    f_num = ["000", "001", "002", "003", "004", "005", "006", "007"]

    # Creating a connection to the FTP server:
    ftp = ftplib.FTP("ftp.ncep.noaa.gov")
    ftp.login()
    ftp.encoding = "utf-8"

    if date != None:
        # If a single date range is provided create a len 1 list:
        date_range = [date]
    else:
        # If a start_date and end_date is provided create a multiple date range:
        date_range = [date.strftime("%Y%m%d") for date in pd.date_range(start=start_date, end=end_date)]

    # Iterating over the date ranges:
    for date in date_range:
        for hour in hour_ranges:
            for num in f_num:
                download_grib_data(ftp_con=ftp, date=date, hour=hour, file_index=num)


# Parsing Input from the command line:
# All optional values - this is sketch:
parser = argparse.ArgumentParser(description="Downloads NOAA Grib2 data based on hard coded values")
parser.add_argument("--start_date", metavar="start_date", type=str, help="The Start Date of Grib2 files to be downloaded")
parser.add_argument("--end_date", metavar="end_date", type=str, help="The End Date of Grib2 files to be downloaded")
parser.add_argument("--date", metavar="date", type=str, help="The single date value for Grib2 files to be downloaded")

args = parser.parse_args()

if args.date:
    get_grib_data(date=args.date)

else:
    if args.start_date and args.end_date:
        get_grib_data(start_date=args.start_date, end_date=args.end_date)






