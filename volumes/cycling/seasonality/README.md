# Bicycle Volume Seasonality Model

## Building the Seasonality Model

### Model
A linear regression model is currently employed to estimate daily correction factors for cyclists. This model was estimated using available weather and sunlight data (see below) and cycling data in 2019 from the permanent count station located at Bloor and Huron. Two seperate models were estimated: one for weekdays, and one for weekends and holidays.

**Independent Variable:** The ratio of cyclists on a specific day to the average number of daily cyclists in 2019 for that day type.

**Dependent Variables:** 
- Amount of Precipitation (mm): square root is used to diminish the impact of increasing amounts of rainfall.
- Maximum Temperature (deg C): this is capped at 20 degrees, at which point any temperature in excess of this is set to 20 degrees as well. 
- Amount of Sunlight (hours).

#### Model Results

Linear regression models were estimated in R using native packages and data stored in the team's PostgreSQL RDS. This file is located in this branch. The results of this exercise are provided below:

![image](https://user-images.githubusercontent.com/20650047/142346738-251f1aff-e416-4159-a0e8-2016bce81844.png)

The parameters above are currently hard coded into the view used to generate seasonality adjustment factors cycling volumes, `activeto.modelled_seasonality_curve`

### Input Datasets

Daily correction factors are estimated for each day of the year based on two distinct datasets:

- **Weather Data**: . This data is currently stored in `activeto.weather` and needs to be updated periodically from Environment Canada's historical weather dataset for Toronto City Centre, located [here](https://climate.weather.gc.ca/climate_data/daily_data_e.html?StationID=48549). A CSV file for maintaining this data (and uploading to the RDS) is located [in the `_lookups/` folder](_lookups/).
- **Sunlight Data**: This data is currently stored in `activeto.sunlight` and needs to be updated periodically from [Weather Stats](https://weather-stats.com/canada/toronto/sun).  A CSV file for maintaining this data (and uploading to the RDS) is located [in the `_lookups/` folder](_lookups/).


## Applying the Seasonality Model

The seasonality model is applied by selecting a baseline period that will remain un-adjusted for seasonality, and then applying the ratio of the various daily seasonality factors to obtain the adjusted volumes. Applying the model introduces some difficult questions about which period to choose as the unadjusted time, and how to communicate publicly and explain in non-technical language the nature of the adjustments and why they are used. The factors should be applied when you are attempting to make before/after comparisons of cycling volumes comparing across periods (for example a before period in May compared to an after period in July)

In some cases for transparency it may be worth sharing both adjusted and unadjusted numbers. For example when you start to adjust fall or winter numbers, the adjustments can be significant and introduce a perception of biasing or inflating the numbers. The goal of the adjustments should be front and centre: it is to say statements like "cycling has grown on this corridor by x% after the installation of the new bicycle infrastructure, after controlling for the effects of weather and seasonality"

Note that even with the model, there are still days when the counts are conducted in awful weather and you should really just toss those count days. You need to use some professional judgement in all cases.
