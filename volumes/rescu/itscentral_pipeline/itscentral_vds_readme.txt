**Simon Foo: "While the data belong to the City and it is free to access, the database schema is proprietary to Transnomis and I trust that the database schema will be kept confidential. All proprietary rights and copyrights are reserved with respect to the database schema."
Can we publish this to bdit_data-sources?**

public.vdsdatalatest
--'VdsDataLatest (only the latest data for each VDS)'
--'I guess this coudld be used for streaming? It only has the latest 1 record for each VDS'
    column_name data_type      sample   Comments
0    divisionid  smallint           2   Always 2 or 8001 in this table.
1  timestamputc   integer  1661663420
2         vdsid   integer          50
3      lanedata     bytea          []

public.vdsdata
--'VdsData (latest + historical)'
    column_name data_type                                             sample    comments
0    divisionid  smallint                                               8001    
1  timestamputc   integer                                         1667031307    min: 2021-08-14 | max: 2023-06-06
2         vdsid   integer                                            2009588
3      lanedata     bytea  [b'\x01', b'\xff', b'\xff', b'\x00', b'\x00', ...    'lanedata column is binary for efficiency'

For column lanedata, we were given the following C# code to interpret from Simon Foo:
--Note: the volume by vehicle lengths appear to be empty.
'''
//Get binary lane data
var laneData = (byte[]) record["VdsData_laneData"];

//Parse lane data
using (var ms = new MemoryStream(laneData))
using (var br = new BinaryReader(ms))
{
while (ms.Position < ms.Length)
{
//Get lane
var lane = (int)br.ReadByte();

//Get speed
var speed = br.ReadUInt16(); //Stored in km/h * 100. 65535 for null value. Convert 0 to null to maintain backward compatibility
var speedKmh = speed == UInt16.MaxValue || speed == 0 ? (double?) null : speed / 100.0;

//Get volume
var volume = br.ReadUInt16(); //Stored in vehicles per hour. 65535 for null value.
var volumeVehiclesPerHour = volume == UInt16.MaxValue ? (int?)null : volume;

//Get occupancy
var occupancy = br.ReadUInt16(); //Stored in percent * 100. 65535 for null value.
var occupancyPercent = occupancy == UInt16.MaxValue ? (double?)null : occupancy / 100.0;

//Get volume by vehicle lengths
var passengerVolume = br.ReadUInt16(); //Stored in vehicles per hour. 65535 for null value.
var volumePassengerVehiclesPerHour = passengerVolume == UInt16.MaxValue ? (int?)null : passengerVolume;
var singleUnitTrucksVolume = br.ReadUInt16(); //Stored in vehicles per hour. 65535 for null value.
var volumeSingleUnitTrucksPerHour = singleUnitTrucksVolume == UInt16.MaxValue ? (int?)null : singleUnitTrucksVolume;
var comboTrucksVolume = br.ReadUInt16(); //Stored in vehicles per hour. 65535 for null value.
var volumeComboTrucksPerHour = comboTrucksVolume == UInt16.MaxValue ? (int?)null : comboTrucksVolume;
var multiTrailerTrucksVolume = br.ReadUInt16(); //Stored in vehicles per hour. 65535 for null value.
var volumeMultiTrailerTrucksPerHour = multiTrailerTrucksVolume == UInt16.MaxValue ? (int?)null : multiTrailerTrucksVolume;
}
}
'''

public.vdsconfig
--'VdsConfig has the sourceid which is the name of the VDS.'
          column_name                    data_type                  sample  Comment
0          divisionid                     smallint                    8001
1               vdsid                      integer                 5462004
2            sourceid            character varying           PX1408-Det019  This column matches our detector_id. Convert both to uppercase. 
3   starttimestamputc  timestamp without time zone     2022-09-26 13:04:41
4     endtimestamputc  timestamp without time zone                    None
5               lanes                     smallint                       1
6          hasgpsunit                      boolean                   False
7       managementurl            character varying                        
8         description            character varying                        
9       fssdivisionid                      integer                    None
10              fssid                      integer                    None
11       rtmsfromzone                      integer                       1
12         rtmstozone                      integer                       1
13       detectortype                     smallint                       1
14          createdby            character varying  TorontoSpatDataGateway
15   createdbystaffid                         uuid                    None
16           signalid                      integer                 2005518
17   signaldivisionid                     smallint                    8001
18           movement                     smallint                    None

public.EntityLocationLatest
--'EntityLocationLatest has the latest locations of the VDS (entityid = vdsid).'
                                                  sample 
divisionid                                          8000 
entitytype                                            14 
entityid                                         2000043 
locationtimestamputc          2020-06-18 17:21:59.477414 
latitude                                        43.65388 
longitude                                     -79.353754 
altitudemetersasl                                   None 
headingdegrees                                      None 
speedkmh                                            None 
numsatellites                                       None 
dilutionofprecision                                 None 
mainroadid                                            24 
crossroadid                                         3471 
secondcrossroadid                                   None 
mainroadname                                 Bayview Ave 
crossroadname                                 Front St E 
secondcrossroadname                                      
streetnumber                                             
offsetdistancemeters                           26.648764 
offsetdirectiondegrees                        349.930336 
locationsource                                         1 
locationdescriptionoverwrite                        None 

public.VdsVehicleData
--'VdsVehicleData has the vehicle-by-vehicle data from the VDS.'
                                sample
divisionid                        8001
timestamputc       2021-08-15 02:54:39
vdsid                          2009067
lane                                 1
sensoroccupancyds                 None
speedkmhdiv100                    None
lengthmeterdiv100                 None
personoccupancy                   None
vehicletype                       None

More sample data for a single detector/lane: 
 divisionid |        timestamputc        | vdsid | lane | sensoroccupancyds | speedkmhdiv100 | lengthmeterdiv100 | personoccupancy | vehicletype 
------------+----------------------------+-------+------+-------------------+----------------+-------------------+-----------------+-------------
          2 | 2023-06-06 15:14:29.712022 |     3 |    1 |                 2 |           8880 |               377 |                 |            
          2 | 2023-06-06 15:14:28.731642 |     3 |    1 |                 2 |            681 |                   |                 |            
          2 | 2023-06-06 15:14:03.867163 |     3 |    1 |                 1 |           2108 |                79 |                 |            
          2 | 2023-06-06 15:14:01.974969 |     3 |    1 |                 1 |           1581 |                63 |                 |            
          2 | 2023-06-06 15:13:59.641201 |     3 |    1 |                 1 |           1663 |                62 |                 |            