# Volume/Bluetooth Expansion Factor Analysis 

The goal of this analysis was to discover if bluetooth observations can be a proxy for Miovision volume data, and if so, how good of a proxy are they. 

<br>
The analysis proceeds on the hypothesis that Volume is simply a scalar funciton of Bluetooth observations:

<br>

![equation](http://latex.codecogs.com/gif.latex?Volume%20%3D%20a%20*%20Observations)

The analysis can be broken down into a few sections:
* An appropriate intersection was selected to be analyzed
* Scalar Regression Analysis was performed on the intersection
* The expansion factor was graphed vs bluetooth observations and volume
* Aggregation of data was implemented to assumably smoothen results
* An exponential fit was tried
<br>

A supplementary analysis, found in `vol_percentage_analysis.ipynb`, was conducted with the purpose of analyzing bluetooth observations as a percentage of volume.

<br>
It was concluded that as the data exsits now, bluetooth observations may not be a good proxy for volume data. 
