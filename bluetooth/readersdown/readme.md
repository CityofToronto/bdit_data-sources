# How to use brokenreaders.py
***
The script `brokenreaders.py` finds all of the most recent non-functioning bluetooth readers, and creates a `.csv` file that contains the names of these readers, the time they were last active, in addition to the routes that were affected by these readers. Follow these steps to use this script:

<br>

1. Download and open the script.

<br>

2. Between the two pound sign breaks, there is a variable `cfg_path`. To this variable, assign the file path of your `.cfg` file used for connecting to the Postgres database.

<br>

3. Run the `brokenreaders.py` file in your python environment.

<br>

4. Proceed to your `documents` folder on your computer. You will see a `broken_readers.csv` file. It will contain the table mentioned above. 
