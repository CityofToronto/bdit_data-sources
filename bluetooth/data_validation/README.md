## Would the location of sensors affect the directional bias of bt data?
If we look at the top 10 imbalanced routes from this [full analysis](https://github.com/CityofToronto/bdit_king_pilot_dashboard/blob/gh-pages/bluetooth/bt_data_validation.ipynb),
as shown below, we can see that most of the sensors are located at the SW corner of the intersection. 
The numbering is to associate them with the results found on [comparison of obs and spd](https://github.com/CityofToronto/bdit_data-sources/blob/blip_data_validation/bluetooth/data_validation/comparison_1month_obs_spd.ipynb) where the summarised version is also shown below.

![Top 10 imbalanced routes](https://github.com/CityofToronto/bdit_data-sources/blob/blip_data_validation/bluetooth/data_validation/imbalanced_routes_with_numbering.PNG?raw=true)

**Comparing 1 month of data:**

|no.|street|interxn_1|interxn_2|bt_ratio|he_ratio|bt_bias|he_bias|bt_spd|he_spd|bt_bias|he_bias|
|---|---|--------|----------|------|-------|------|-------|-------|--------|-----|---------|
|1|Dundas|Dufferin|Bathurst|0.538|0.518|EB|WB|-0.067|-0.056|WB|WB|
|2|Dundas|Bathurst|Spadina|0.567|0.521|EB|WB|-2.734|0.923|WB|EB|
|3|Dundas|Spadina|University|0.624|0.522|EB|WB|3.509|2.946|EB|EB|
|4|Dundas|Jarvis|Parliament|0.672|0.658|EB|EB|1.674|1.946|EB|EB|
|5|King|Spadina|University|0.577|0.502|WB|WB|0.577|0.380|EB|EB|
|6|King|University|Yonge|0.546|0.606|EB|EB|0.183|1.736|EB|EB|
|7|Front|Bathurst|Spadina|0.587|0.549|EB|WB|-6.419|0.423|WB|EB|
|8|Front|Spadina|University|0.703|0.556|EB|EB|1.283|-0.532|EB|WB|
|9|University|Queen|Dundas|0.759|0.529|SB|SB|5.1125|4.276|NB|NB|
|10|University|Dundas|College|-|0.582|-|SB|-|6.404|-|NB|

**Comparing 2 days of data:**

|no.|street|interxn_1|interxn_2|bt_ratio|he_ratio|bt_bias|he_bias|bt_spd|he_spd|bt_bias|he_bias|
|---|---|--------|----------|------|-------|------|-------|-------|--------|-----|---------|
|1|Dundas|Dufferin|Bathurst|0.539|0.505|EB|WB|-1.015|-1.087|WB|WB|
|2|Dundas|Bathurst|Spadina|0.561|0.522|EB|WB|-3.099|-0.531|WB|WB|
|3|Dundas|Spadina|University|0.632|0.517|EB|EB|0.261|1.588|EB|EB|
|4|Dundas|Jarvis|Parliament|0.662|0.681|EB|EB|2.562|1.595|EB|EB|
|5|King|Spadina|University|0.656|0.513|WB|EB|4.566|0.891|EB|EB|
|6|King|University|Yonge|0.541|0.680|EB|EB|3.854|0.881|EB|EB|
|7|Front|Bathurst|Spadina|0.552|0.560|EB|WB|-6.179|1.598|WB|EB|
|8|Front|Spadina|University|0.669|0.589|EB|EB|1.430|0.444|EB|EB|
|9|University|Queen|Dundas|0.736|0.552|SB|SB|4.892|3.391|NB|NB|
|10|University|Dundas|College|-|0.603|-|SB|-|6.697|-|NB|

> For speed, positive sign indicates EB / NB direction whereas negative sign indicates WB / SB direction.

Generally speaking, since there's only one sensor for each intersection tracking traffic flow in all four directions, the sensor located at a corner may be more inclined to get more results from traffic flow close to it.
A figure to show that is depicted below.
![Sensor locatoin bias](https://github.com/CityofToronto/bdit_data-sources/blob/blip_data_validation/bluetooth/data_validation/blip_sensor_bias.PNG?raw=true)

However, comparing both the number of observation and median speed for both BT and HERE data as shown [here](https://github.com/CityofToronto/bdit_data-sources/blob/blip_data_validation/bluetooth/data_validation/comparison_1month_obs_spd.ipynb), 
it seems like the directional bias is due to actual traffic flow in real life.
