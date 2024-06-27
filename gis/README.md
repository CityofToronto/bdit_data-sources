# GIS - Geographic Information System <!-- omit in toc -->


## Table of Contents <!-- omit in toc -->

- [1. Overview](#1-overview)
- [2. text\_to\_centreline](#2-text_to_centreline)
- [3. Centreline Dataset](#3-centreline-dataset)
- [4. gccview](#4-gccview)
- [5. speed\_limit](#5-speed_limit)
- [6. school\_safety\_zones](#6-school_safety_zones)
- [7. assets](#7-assets)

## 1. Overview

This is an overview of the GIS folder.

## 2. [text_to_centreline](./text_to_centreline/)

This folder contains information about the Text Description to Centreline Geometry Automation. 
It contains sql used to transform text description of street (in particular bylaws) into centreline geometries. 
See the [README](text_to_centreline) for details on how to use it.

## 3. [Centreline Dataset](./centreline/)

This folder contains brief introduction of what the centreline dataset is, how our unit uses it and how we store it.

## 4. [gccview](./gccview/)

This folder contains code used by the `pull_gcc_layers` Airflow DAG to update GIS layers from the GCC map servers and instructions on how to add new layers to the DAG. 

## 5. [speed_limit](./speed_limit/)

This folder contains information about a project to convert text bylaws into a usable speed limit layer ([`gis.bylaws_speed_limit_layer`](./speed_limit/automated/sql/mat-view-bylaws_speed_limit_layer.sql)).

## 6. [school_safety_zones](./school_safety_zones/)

This folder contains code used by the `vz_google_sheets` Airflow DAG which pulls info on School Safety Zones from google sheets maintained by the Vision Zero Team to support the [Vision Zero Mapping Tool](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/safety-measures-and-mapping/).

## 7. [assets](./assets/)

Contains information about the `traffic_signals_dag` Airflow DAG which is used to pull traffic signals and red light camera data from the Open Data portal. 