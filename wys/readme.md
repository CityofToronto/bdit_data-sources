# Watch Your Speed <!-- omit in toc -->

- [How to Access the Data](#how-to-access-the-data)
- [Open Data](#open-data)


!['A sign mounted on a pole with the words "Your Speed" and underneath a digital sign displaying "31"'](https://www.toronto.ca/wp-content/uploads/2018/09/9878-landscape-mwysp2-e1538064432616-1024x338.jpg)

The city has installed [Watch Your Speed Signs](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/safety-initiatives/initiatives/watch-your-speed-program/) that display the speed a vehicle is travelling at and flashes if the vehicle is travelling over the speed limit. Installation of the sign was done as part of 3 programs: the normal watch your speed sign program, mobile watch your speed which has signs mounted on trailers that move to a different location every few weeks, and school watch your speed which has signs installed at high priority schools. As part of the [Vision Zero Road Safety Plan](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/), these signs aim to reduce speeding.

- [`api`](api/) contains code and documentation about how the dataset is extracted, transformed and loaded
- [`validation`](validation/) contains notebooks analyzing the differences between WYS observation counts and other volume counts to determine how well these signs can be used as permanent volume counters

## How to Access the Data

For most inquiries, the data prepared for Open Data should suffice, accessed either at the links [below](#open-data), or in the `open_data` schema of the `bigdata` database.  
For more complex inquiries or investigations, the raw data is stored in the `wys` schema of `bigdata`. The aggregation process is described in detail in [api/README.md](api/README.md).  

## Open Data

Semi-aggregated and monthly summary data are available for the two programs (Stationary School Safety Zone signs and Mobile Signs) and are updated monthly. Because the mobile signs are moved frequently, they do not have accurate locations beyond a text description, and are therefore presented as a separate dataset. See [WYS documentation](api/README.md) for more information on how the datasets are processed.

  - [School Safety Zone Watch Your Speed Program – Locations](https://open.toronto.ca/dataset/school-safety-zone-watch-your-speed-program-locations/): The locations and operating parameters for each location where a permanent Watch Your Speed Program Sign was installed.
  - [School Safety Zone Watch Your Speed Program – Detailed Speed Counts](https://open.toronto.ca/dataset/school-safety-zone-watch-your-speed-program-detailed-speed-counts/): An hourly aggregation of observed speeds for each location where a Watch Your Speed Program Sign was installed in 5 km/hr speed range increments.
  - [Safety Zone Watch Your Speed Program – Monthly Summary](https://open.toronto.ca/dataset/safety-zone-watch-your-speed-program-monthly-summary/): A summary of observed speeds for each location where a Safety Zone Watch Your Speed Program Sign was installed.
  - [Mobile Watch Your Speed Program – Detailed Speed Counts](https://open.toronto.ca/dataset/mobile-watch-your-speed-program-detailed-speed-counts/): An hourly aggregation of observed speeds for each sign installation in 5 km/hr speed range increments for each location where a Mobile Watch Your Speed Program Sign was installed.
  - [Mobile Watch Your Speed Program – Speed Summary](https://open.toronto.ca/dataset/mobile-watch-your-speed-program-speed-summary/): A summary of observed speeds for each location where a Mobile Watch Your Speed Program Sign was installed.
