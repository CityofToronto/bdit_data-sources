# Comparative Analysis of BT Segment and OD Data 

This is a thorough and detailed comparitive analysis of BT Segment Data and Origin-Destination Data. The goal of the analysis was to determing which dataset is more comprehensive, and investigate discrepancies between the datsets (if they exist). Here are some fascinating high level results from that analysis. 


* 35% of OD Data can be potentially represented via Bluetooth Segments: i.e. for a path total of x, there are x-1 segments.
<br>

* For a simple 'A to B' path, approximately 84% of data are accurately represented. However, for path lengths of 3 (i.e. simply one interesection more than an A to B path), this number drops to around 40%. As paths become more complicated, the percentage of BT Segments representing OD Data decreses.
<br>

* As path total increases, the percentage of paths that contain segments that did not correspond with the points in the path increases as well
<br>

* 65% of OD Data CANNOT be represented via Bluetooth Segments: i.e. for a path total of x, there are not x-1 segments
<br>

* A majority of these are just missing 1-2 segments. However, there are paths that are missing 5 or more BT segments
<br>

* 19.3% of these paths have segments that do not fall in the path list. Therefore, 12.5% of all paths have non-matching segments in addition to segments that do not correspond to the path points
<br>

# How to use the pivot functions

In the `pivot.ipynb` file, there are two functions that may be of interest to someone working with OD datasets. These functions are `pivot_se` and `pivot_path`. 
<br>

* If one has a raw start/end dataset similar to that of the first dataframe (`df`) in `pivot.ipynb`:
    * Read in the start/end data into a pandas dataframe.
    * Call the `pivot_se` function on this data frame. The dataframe returned will be a perfectly pivoted start/end dataset
<br>

* If one has a path dataset similar to the second dataframe (`df2`) in `pivot.ipynb`: 
    * Read in the path data into a pandas dataframe.
    * Call the `pivot_path` function on this data frame. The dataframe returned will be perfectly pivoted, with a path list ordered by distance for each `userid`. 

