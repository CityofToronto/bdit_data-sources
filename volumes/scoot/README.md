#Scoot Documentation
------
## Table of Contents
1. [Background](#1-background)
 * [Traffic Signal Control in Toronto](#traffic-signal-control-in-toronto-1)
 * [How SCOOT Works](#how-scoot-works-2)
 * [SCOOT Locations](#scoot-locations-3)
2. [Objective](#2-objective)
3. [Key Terminology](#3-key-terminology)

##1. Background
###Traffic Signal Control in Toronto [<sup>1</sup>](http://www1.toronto.ca/wps/portal/contentonly?vgnextoid=a6c4fec1181ec410VgnVCM10000071d60f89RCRD&vgnextchannel=9452722c231ec410VgnVCM10000071d60f89RCRD)
Transportation Services uses two traffic signal systems to control its 2,330 traffic control signals.

**TransSuite Traffic Control System (TransSuite TCS)**: TransSuite TCS is a hybrid traffic control system that relies on second-by-second communication to monitor signal operations, but relies on field equipment to maintain coordination (i.e. the field equipment can maintain signal coordination for about 24 hours if there is a loss of communication). TransSuite TCS does not directly control signal movements, but commands each intersection controller to follow a timing plan that resides within its local database. TransSuite then verifies that the controller adheres to the commanded timing plan. Intersection controllers are monitored and controlled through a user interface. TransSuite TCS supports a variety of phase-based controllers.

**Split Cycle Offset Optimization Technique/Urban Traffic Control (SCOOT/UTC)**: SCOOT is an adaptive traffic control system that determines its traffic timing plans based on real-time information received from vehicle detectors located on the approaches to signalized intersections. Like MTSS, SCOOT relies on telephone communication to maintain signal coordination. UTC is a traffic control system that operates in tandem with SCOOT; it also relies on telephone communications. UTC provides pre-determined signal timing plans and is used as a stopgap measure if SCOOT is not available. SCOOT signals are sometimes called "smart" signals.

###How SCOOT Works [<sup>2</sup>](http://www.scoot-utc.com/DetailedHowSCOOTWorks.php) 
The operation of the SCOOT model is summarised in the diagram below. SCOOT obtains information on traffic flows from detectors. As an adaptive system, SCOOT depends on good traffic data so that it can respond to changes in flow. Detectors are normally required on every link. Their location is important and they are usually positioned at the upstream end of the approach link. Inductive loops are normally used, but other methods are also available.
!['how_scoot_works'](http://www.scoot-utc.com/images/HowWorks.gif)
When vehicles pass the detector, SCOOT receives the information and converts the data into its internal units and uses them to construct "Cyclic flow profiles" for each link. The sample profile shown in the diagram is colour coded green and red according to the state of the traffic signals when the vehicles will arrive at the stopline at normal cruise speed. Vehicles are modelled down the link at cruise speed and join the back of the queue (if present). During the green, vehicles discharge from the stopline at the validated saturation flow rate.

The data from the model is then used by SCOOT in three optimisers which are continuously adapting three key traffic control parameters - the amount of green for each approach (Split), the time between adjacent signals (Offset) and the time allowed for all approaches to a signalled intersection (Cycle time). These three optimisers are used to continuously adapt these parameters for all intersections in the SCOOT controlled area, minimising wasted green time at intersections and reducing stops and delays by synchronising adjacent sets of signals. This means that signal timings evolve as the traffic situation changes without any of the harmful disruption caused by changing fixed time plans on more traditional urban traffic control systems.

###SCOOT Locations [<sup>3</sup>](http://www1.toronto.ca/wps/portal/contentonly?vgnextoid=965b868b5535b210VgnVCM1000003dd60f89RCRD) 
SCOOT is currently installed on the following routes:

- Lake Shore Blvd from East Don Roadway to Windermere Ave
- Black Creek Dr from Lawrence Ave to Weston Rd
- The Queensway from The West Mall to Colborne Lodge
- Bayview Ave from Steeles Ave to Moore Ave
- Eglinton Ave E from Leslie St to Cedar Rd
- Don Mills Rd from Steeles Ave to Overlea Blvd
- Dundas St from Neilson Rd to Aukland Rd
- Steeles Ave from Yonge St to Kennedy Rd
- Yonge St from Steeles Ave to Mill St
- Avenue Rd from Highway 401 WB Offramp to Chaplin Cr
- Bloor St from Avenue Rd to Castlefrank Rd
- Downtown Core (Bay St, Church St, Charles St, Davenport Rd, Grosvenor St, Isabella St, Jarvis St, Sherbourne St and Wellesley St)
- Eglinton Ave W from Royal York Rd to Highway 27
- Lawrence Ave W from Bolingroke Rd to Shermount Ave

!['SCOOT Intersection Locations'](img/scoot_locations.png)

##2. Objective
This document provides an overview of SCOOT data, including how to extract information from the ASTRID system, and overview of file structure, and (eventually) how to set up ongoing messages/reporting from SCOOT.

##2. Overview of ASTRID
This information is mostly transcribed from the ASTRID User Guide.

ASTRID is the database designed to colelct infomration and store the data for later retreieval and analysis, as described in the ASTRID User Guide:

> The SCOOT Urban Traffic Control system optimises signal timings in a network to minimise stops and delay. Data used by the SCOOT model in the optimisation process, such as delays, flows and congestion, are available to traffic engineers through the ASTRID database system, which automatically collects, stores and processes traffic information for display or analysis. If the data is converted into an appropriate format, ASTRID can also process data from sources other than SCOOT.
> 
ASTRID was originally developed at the University of Southampton for the Transport and Road Research Laboratory as an off-line version running on an IBM-compatible PC ASTRID has been further developed into an on-line version as part of the DRIVE 2 projects HERMES and SCOPE/ ROMANSE, and received funding from DIM division, Department of Transport. This version has been operating successfully in Southampton and London since 1993.
> 
ASTRID is a database designed to collect information from a SCOOT traffic control system, or other source of time-varying traffic data, and to store it in a database for later retrieval and analysis. The name ASTRID means Automatic SCOOT Traffic Information Database.
> 
The on-line version of ASTRID runs on a computer operating under the Open VMS operating system. In the past ASTRID has required a separate machine for 
its operation. Feasibility studies showed that it was possible to run ASTRID in the same machine as a UTC system, and ASTRID has now been issued to allow this to be used in practice.

###Messages
Each cell supplies a stream of messages that contain the data required to be stored in the database. The messages for ASTRID are in an amended format from the standard SCOOT messages, but contain the same information. The following information is required for each message to be processed by ASTRID: 
1. Date 
2. Start time 
3. End time 
4. Message type 
5. Site 
6. Data value 
7. Fault indicator 
A message can contain more than one data value. 

###Database Organization
####Site
The ASTRID database is organised by site which means the location for which the data is collected. Sites are one of several types:
1. An **area** represents a whole cell. Because data can be collected for more than one cell, there may be more than one area. 
2. A **region** is a SCOOT region, or an equivalent for a non-SCOOT data source. 
3. A **node** is a SCOOT node, or an equivalent for a non-SCOOT data source. 
4. A **stage** is a SCOOT stage, or an equivalent for a non-SCOOT data source. 
5. A **link** is a SCOOT link, or an equivalent for a non-SCOOT data source. 
6. A **detector** is a SCOOT detector, or an equivalent for a non-SCOOT data source. 
7. A route is a set of links which has been defined to ASTRID by the user. A route may consist of links representing a particular route through the network, or it may be a set of disconnected links which the user wishes to consider together. 
8. A **group** is sub-area or equivalent group of equipment. 
9. A **car park** is a single car park. It may have a number of in and out counts.
10. A **cnt** is a count taken directly from a count source, not derived by the use of conversion factors.

####Resolution 
The time resolution of data (except the trend periods) can be configured as part of the initial installation of ASTRID. The values given here are typical values. Data are stored at one of three types of resolution 
1. Data  entering  ASTRID  are  at high resolution. The exact resolution depends on the message being processed, but M02 messages are normally cyclic or at 5-minute  resolution,  and  M08  and  M29  messages  are  cyclic.  Data  at  their resolution are stored for a few days, at most, as the storage requirements are very large. This resolution of data can be appropriate for analysing incidents or other effects on the network. The raw files are stored at this resolution. 
2. Data   are  stored   at   medium   resolution   for   the   longer   term.   The  time resolution  of  15  minutes  is  a  compromise between detail of information and storage requirements. The profiles are stored at this resolution.
3. Data at low resolution are divided into a few time periods per day, covering peak and off-peak periods as a whole. Because the storage requirements are so  much  less,  data  can  be  stored  for  years  at  this  resolution.  The  trend files are stored at this resolution. 

####File types 
The  files  are  organised  as  four  separate  file  types  which  contain  data  at  different 
resolutions and for different periods: 
1. Raw files: contain raw data for the last few days only at high resolution. 
2. Profiles files: contain average data per weekday at a medium resolution. 
3. Trend files: contain data per day at low resolution. 
4. Bac files: contain data per day at medium resolution. 

###Basic Data Types


##3. Key Terminology
**ASTRID**: Automatic SCOOT Traffic Information Database
**SCOOT**: Split Cycle Offset Optimisation Technique. 
**UTC**: Urban Traffic Control
**MTSS**: Main Traffic Control System