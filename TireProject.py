#   GOAL: Automated data pulls and cleansing, as modular/reusable as possible, time permitting.
#   METHOD: Came up with skeletal idea, and developed while testing in Jupyter Notebooks, since
#       ultimately the purpose of the goal is streamlining things for the end user, and removing
#       as many steps in the process as possible, as broadly as possible.
#   POTENTIAL UPGRADES: Check/update/cleanse on an interval, changelog and error log, a proper
#       naming scheme so that historical data could be kept as a reference, using datetime module
#       on the various dates in the dataframe instead of simple string cleanup.
#   NOTE: __file__ won't work properly in jupyter notebooks, and these functions aren't meant to
#       be run in the notebooks. Another possible upgrade might be modifying the paths so that
#       the functions could be run in the notebook, if the end user were so inclined.

import requests
import zipfile
import hashlib
import os
import pandas as pd
from clint.textui import progress


#   Function that compares currently available TSV to the one in production.
#   1) Download
#   2) Get MD5 from contents of ZIP
#   3) Compare MD5s; if no change, return False. If changed, return True.
#   Could be set on an interval in production.
#   Concept here is to be as modular and reusable as possible, not just this particular TSV.
#   Room for improvement:
#       1) Unit testing
#       2) Naming convention would make logging changes and some historical file keeping possible.
#       3) A way to deal with different archive file types and multi-volume archives.
#       4) A way to deal with archives that contain multiple relevant files.
def compare_zip_md5(url, destination=os.path.dirname(os.path.abspath(__file__))):
    #   Download archive.
    print("Downloading %s" % url)
    try:
        request = requests.get(url, stream=True)
        request.raise_for_status()
    except requests.exceptions.HTTPError:
        print("Error 404: file not found.")
    with open(destination + "/latest.zip", 'wb') as file:
        file.write(request.content)

        #   Download progress bar to preserve sanity if dealing with large files!
        length = int(request.headers.get('content-length'))
        for chunk in progress.bar(request.iter_content(chunk_size=1024), expected_size=(length / 1024) + 1):
            if chunk:
                file.write(chunk)
                file.flush()

    #   Compare latest.zip to currently in-use zip md5, stored in a txt for efficiency.
    new_archive = zipfile.ZipFile(destination + "/latest.zip")
    comparison_table = []
    block_size = 1024**2
    for file in new_archive.namelist():
        entry = new_archive.open(file)
        md5 = hashlib.md5()
        while True:
            block = entry.read(block_size)
            if not block:
                break
            md5.update(block)
        comparison_table.append(md5.hexdigest())
    #   Check if an MD5 .txt already exists, and if not, assumes this is the first time data has been pulled
    #   and creates a new one with the latest.zip MD5.
    try:
        with open(destination + '/current_md5.txt', 'r') as file:
            current_archive_md5 = file.read()
        comparison_table.append(current_archive_md5)
        if comparison_table[0] == comparison_table[1]:
            print("No changes detected.")
            os.remove(destination + "/latest.zip")
            return False
        else:
            print("Change detected. Updating files...")
            new_archive.extractall(destination)
            new_archive.close()
            os.remove(destination + "/latest.zip")
            new_md5_txt = open(destination + "/current_md5.txt", "w+")
            new_md5_txt.write(comparison_table[0])
            new_md5_txt.close()
            print("Update complete.")
            return True
    except IndexError:
        print("Unable to compare MD5 values.")
    except FileNotFoundError:
        print("current_md5.txt not detected. Updating files...")
        new_archive.extractall(destination)
        new_archive.close()
        os.remove(destination + "/latest.zip")
        new_md5_txt = open(destination + "/current_md5.txt", "w+")
        new_md5_txt.write(comparison_table[0])
        new_md5_txt.close()
        print("Update complete.")
        return True


#   Cleans the dates in FLAT_RCL
#   Unfortunately not super reusable
#   Fortunately quicker to code these funcs to order, rather than a comprehensively reusable one
def clean_date(date):
    dirty_date = str(date)
    if dirty_date != "nan":
        cleaned_date = dirty_date[0:4] + "-" + dirty_date[4:6] + "-" + dirty_date[6:8]
    else:
        cleaned_date = "N/A"
    return cleaned_date


#   This function is a prototype of what a reusable data cleaning function might look like.
#   Known issue: reimporting exported JSON has a few kinks...works, but unideal resorting of cols.
#   Note: if running in jupyter notebooks, __file__ won't work, and you may have trouble exporting
#   if permissions aren't properly setup. In any case, this function isn't really meant to be run
#   in the notebook to begin with. I suspect the problem is with the orient parameter.
def clean_new_flat_rcl(filename="FLAT_RCL.txt"):
    #   Set headings with external TXT. Makes func more reusable and easier to edit if changes occur.
    try:
        with open(os.path.dirname(os.path.abspath(__file__)) + '/names.txt', 'r') as file:
            names = file.read().splitlines()
            print(names)

        df = pd.read_csv(filename, delimiter="\t", header=None, names=names, encoding="ISO-8859-1")

        #   Clean up entries in all columns with the word Date
        for i in names:
            if "Date" in i:
                df[i] = df[i].apply(clean_date)

        #   Same premise as with headings, external MANUFACTURERS list makes future edits easier.
        try:
            with open(os.path.dirname(os.path.abspath(__file__)) + '/manufacturers.txt', 'r') as file:
                manufacturers = file.read().splitlines()
            df = df.loc[df["Make"].isin(manufacturers)]

        except FileNotFoundError:
            print("Unable to filter manufacturers: no manufacturers.txt found.")
            pass

        except KeyError:
            pass

    except FileNotFoundError:
        df = pd.read_csv(filename, delimiter="\t", header=None, encoding="ISO-8859-1")
        names = None
        print("Error: no names.txt file found.")
        pass

    #   Export to JSON. Two versions: complete and without duplicates.
    #   Although this takes up more space on the server and might not be feasible for datasets beyond
    #   a certain size, up to that point it could save the user some time and extra steps if they
    #   already know that they want to work with the duplicate records removed.
    df.to_json(os.path.dirname(os.path.abspath(__file__)) + "/FLAT_RCL_complete.json", orient="records")
    if names is not None:
        df = df.drop_duplicates(subset="Campaign Number")
        df.to_json(os.path.dirname(os.path.abspath(__file__)) + "/FLAT_RCL_no_duplicates.json", orient="records")
    else:
        pass
