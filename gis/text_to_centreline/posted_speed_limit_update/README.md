# Updating the Posted Speed Limit Layer 


## Table of Contents
1. [Description of Project Problem](#description-of-project-problem)
2. [Methodology](#Methodology)
3. [Information on Subfolders](#Information-on-Subfolders)

## Description of Project Problem

We were given a file of approximately 8000 descriptions of locations on Toronto streets, 
and each location represented a change to the posted speed limit on a certain stretch or road. 
Speed limit changes are enacted through bylaws in the City of Toronto, so each record was from a bylaw. Each record also contains and ID (the ID of the location/text description), bylaw number, 
a date for when the bylaw was passed, the original speed limit of the location, and the updated speed. 

Our job is to use this file to update the Toronto centreline file so it contains speed limit data. 


## Methodology 

See the [text to centreline](https://github.com/CityofToronto/bdit_data-sources/tree/master/gis/text_to_centreline_geometry_scripts) documentation for information on our algorithm that matches bylaw text descriptions to centreline street segment geometries. 

## Information on Subfolders

This folder contains 3 subfolders: `automated`, `manual`, and `original_documentation_summer_2018`. 

The `original_documentation_summer_2018` folder contains the documentation for our original process for creating an updated speed limit layer. It contains a `README` with a lot of code. This `README` was created before the `text_to_centreline` function existed, and it is basically goes through the process for how to match bylaw text to geometries, with not code that is not the most efficient. This speed limit layer that was created from the code in this repo was used in the [Vision Zero Challenge](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/educational-campaigns/vision-zero-challenge/) during the Summer of 2018, and the location of the output layer can be found [here](https://github.com/CityofToronto/vz_challenge/tree/master/transportation/posted_speed_limits). 

The `manual` folder contains an attempt to get a perfect posted speed limit layer of the City, which was worked on mainly in `April 2019`. A lot of maunal work was done in this case because there are many bylaws that are written very incorrectly. In the end, there were still around 150/8000 bylaw text fields that were unmatched and the matched were not `QC`'d

The `automated` folder is the most recently updated folder. It has work from the most recent update of the speed limit file (August 2019). The confidence in the matching of this file is fairly high. However, no manual changes were made to the matched/unmatched geometries and we know for a fact there are issues with some of the matches. These general issues were explored in the [QC subfolder](./automated/QC). There are also over 200 bylaws that were not matched to centreline segments. 

At this point, if you have anymore questions about the contents about any of these folders, there is a `README` in each folder, so look there.
