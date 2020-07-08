import os
import tarfile
import sys
import csv
import subprocess
import pandas as pd
import numpy as np
import natsort as ns
import requests
from multiprocessing import Pool, freeze_support

URL = "http://portoalegre.andrew.cmu.edu:88/BLUED/location_001_dataset_0YX.tar"

GLOABL_START_TIME_1_12 = pd.to_datetime("2011/10/20 11:58:32.6234999999996440291")
GLOABL_START_TIME_14_16 = pd.to_datetime("2011/10/26 01:20:48.5454583333330178151")

"""
Downloads untars, unzips the BLUED dataset.
Stores data in /data/raw.

"""


def download(left, right):
        for i in range(1, 7):
                
            newurl = URL.replace('X', str(i)[1]).replace('Y', str(i)[0])
            filename = newurl.split('/')[-1]
            filename = os.path.join("data", "raw", filename)

            # Download one folder of the dataset
            print ("Starting to download: " + filename)
            r = requests.get(newurl)
            with open(filename, "wb") as code:
                code.write(r.content)
            print ("Finished Downloading: " + filename)

            # Untar folder and delets .tar after the process.
            untar(filename, os.path.join("data", "raw"))

            folderpath = os.path.join("data", "raw", newurl.split('/')[-1].split('.')[0])

            # Unzips files and deletes them afer the process.
            filepath = []
            for bz2 in os.listdir(folderpath):
                filepath.append(os.path.join(folderpath, bz2))

            # Set CPU number here.
            pool = Pool(processes=15)
            result = pool.map_async(unbz2, filepath)
            result.get()


            # Start of the event extraction.
            print ("Starting preprocessing for: " + folderpath)
            concat_files_to_csv(B=right, A=left)

            command = "rm -r -f " + folderpath
            subprocess.check_call(command.split())


"""
Concats all .txt data file to a single Pandas-Dataframe.

"""


def concat_files_to_csv(A, B):
    column = ['X_Value', 'Current A', 'Current B', 'VoltageA']
    for location in os.listdir(os.path.join("data", "raw")):
        if 'location_001_dataset_' in location:
                file_folder = os.path.join("data", "raw", location)
                df_list = []
                concat_df = pd.DataFrame()
                for data in ns.natsorted(os.listdir(file_folder), key=lambda y: y.lower()):
                        if 'location_001_ivdata_' in data:
                                print ("Reading datafile: " + data)
                                df = pd.read_csv(os.path.join(file_folder, data), header=None, skiprows=23, index_col=False)
                                df_list.append(df)

                concat_df = pd.concat(df_list, axis=0)
                concat_df.columns = column
                concat_df.index = np.arange(len(concat_df.index))
                give_events(concat_df, file_folder, location, A, B)


"""
Provides the event extraction for the BLUD data set.

"""


def give_events(data, file_folder, location, A, B):
    print ("Reading eventslist .csv for: " + location)
    events = pd.read_csv(os.path.join(file_folder, "location_001_eventslist.txt"))

    print ("Starting to get timestamp windows.")
    for i, timestamp in enumerate(events['Timestamp']):

        if pd.to_datetime(timestamp) < GLOABL_START_TIME_14_16:
                time_delta_sec = (pd.to_datetime(timestamp) - GLOABL_START_TIME_1_12).total_seconds()
                mid = np.searchsorted(data['X_Value'], time_delta_sec)
                

        else:
                time_delta_sec = (pd.to_datetime(timestamp) - GLOABL_START_TIME_14_16).total_seconds()
                mid = np.searchsorted(data['X_Value'], time_delta_sec)
                

        upper = mid + B
        lower = mid - A

        try:
                print ("Found index " + str(mid) + " for delta: " + str(time_delta_sec)
                       + " actual delta: " + str(data['X_Value'][mid]))
        except KeyError:
                print("Found index " + str(mid) + " with " + str(time_delta_sec) + " is the last element.")

        window = data[lower:upper]

        # Shifts event widnow to start with a new cycle
        if str(events['Phase'].loc[i]) == 'A':
                idx = np.where(np.diff(np.signbit(window['Current A'][:250])))[0][0]
        else:
                idx = np.where(np.diff(np.signbit(window['Current B'][:250])))[0][0]

        window = data[lower + idx: upper + idx]

        new_path = os.path.join("data", "preprocessed")
        if not os.path.exists(new_path):
            os.makedirs(new_path)

        # Strange names because windows compatibility
        print ("Saving data window for: " + timestamp)
        tmp = timestamp.replace(' ', '_').replace('/', '-').replace(':', '+') + ',' + str(events['Label'].loc[i]) + ',' + str(events['Phase'].loc[i] + '.csv')

        window.to_csv(os.path.join(new_path, tmp))


"""
Untars and delets a .tar.
Reference: https://sukhbinder.wordpress.com/2014/03/06/untar-a-tar-file-with-python/

"""


def untar(file_path, dest_path):
    if (file_path.endswith(".tar")):
        tar = tarfile.open(file_path)
        tar.extractall(path=dest_path)
        tar.close()
        print ("Extracted: " + file_path)
        os.remove(file_path)
        print ("Removed: " + file_path)
    else:
        print ("Not a .tar file: " + file_path)


"""
Unzips and delets a bz2.


"""


def unbz2(file_path):
    if (file_path.endswith(".bz2")):
        command = "bzip2 -d " + file_path
        subprocess.check_call(command.split())
        print ("Extracted: " + file_path)
    else:
        print("Not a .bz2 file: " + file_path)


# Set the amount of samples left of the event and right of the event.
if __name__ == '__main__':
    download(left=6400, right=12200)


