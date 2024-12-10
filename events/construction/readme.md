# Introduction

[Road Disruption Activity Reporting System (RoDARS)](https://www.toronto.ca/services-payments/streets-parking-transportation/road-restrictions-closures/road-disruption-activity-reporting-system-rodars/)

> RoDARS is a system that informs the public of planned roadway closures throughout the City. The submission procedure follows the acquisition of an approved Street Occupation Permit (construction) or Street Closure Permit (event).
> 
> When occupying any portion of the City’s public right of way that is not an expressway, the applicant must submit a RoDARS Notification Form (opens in new window)  to TMC Dispatch at least two business days before the start of occupation. > The RoDARS Notification Form must be approved by the appropriate Work Zone Traffic Coordinator (WZTC) before being submitted to TMC Dispatch.
> 
> When occupying any portion of a City expressway (F.G.G., DVP or Allen Rd between Eglinton Ave W and Transit Rd), the applicant must submit a RoDARS Notification Form to TMC Dispatch at least seven business days before the start of > occupation. The RoDARS Notification Form must be approved by the appropriate City project manager/engineer before submittal to TMC Dispatch. Once attained from TMC Dispatch, TMC’s RESCU Unit will then notify the applicant of the approval > verdict.
> 
> A separate RoDARS Notification Form is required for each occupied roadway. If the daily schedule varies, separate RoDARS Notification Forms are required for each day. Once the RoDARS form has been submitted and approved, the information then > appears on the Traffic Restrictions Map. Please refer to the City Expressway Closure Guidelines (opens in new window) for allowable roadway occupancy times.
> 
> The applicant must notify the City if either of the following situations arise:
> 
>     the work schedule and/or work zone plan has been revised or postponed. The applicant must submit a revised and approved RoDARS Notification Form at least one business day before changes occur
>     the work has been cancelled or completed early. The applicant must contact TMC Dispatch


## RODARS DAG

<!-- rodars_pull_doc_md -->

- `pull_rodars`: pulls RODARS issue data from ITSC and inserts into RDS.

<!-- rodars_pull_doc_md -->

The RoDARS form is public here: https://rodars.transnomis.com/Permit/PermitApplicationCreate/a9180443-b97f-548e-ae1c-fc70cae18a7a?previewMode=Applicants

RODARs form showing extremely detailed lane management plan dropdowns.
![Rodars Form](rodars_form.png)

**Questions:**

What is included in RODARS vs. not?
RODARS new vs old?
Sources besides RODARS on ITS Central? (divisionid)

[Road Restrictions Map](https://www.toronto.ca/services-payments/streets-parking-transportation/road-restrictions-closures/restrictions-map/#location=2%20Muggs%20Island%20Pk&lat=43.62414889248682&lng=-79.38697494415&zoom=14)

- Hazard: what's this?
- Construction: just RODARs?
- Road Closed: what's this?


Column Questions: 

`timeoption`: 0-4
    - 
`lanesaffected`: "{""LocationDescription"":""Huron St from Harbord St to Classic Ave"",""EncodedCoordinates"":""{_oiGpyrcNrDoA"",""LaneApproaches"":[{""Direction"":3,""RoadName"":""Huron St"",""FeatureId"":1143425,""RoadId"":3716,""LanesAffectedPattern"":""LOWO"",""LaneBlockLevel"":2,""RoadClosureType"":20},{""Direction"":2,""RoadName"":""Huron St"",""FeatureId"":1143425,""RoadId"":3716,""LanesAffectedPattern"":""LOWO"",""LaneBlockLevel"":2,""RoadClosureType"":20}],""LocationBlockLevel"":3,""RoadClosureType"":20}"
    - `laneblocklevel`
    - `LanesAffectedPattern`
    - `LocationBlockLevel`
    - `RoadClosureType`

`priority`: 1-5
