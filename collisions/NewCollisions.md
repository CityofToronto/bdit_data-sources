# Collisions

The *new* collision dataset contains information about traffic collisions, and the people involved, that occurred in the City of Toronto's Right-of-Way between January 1, 2023 to present.

> [!Important]
> This dataset is in [beta](https://www.ontario.ca/page/service-design-playbook#section-3).
>
> "The goal of [the beta phase] is to build a real service that works well for a larger group of people... collect real data and user feedback. Feedback is used to refine the service, adding and adjusting features until the service is complete."

### Scope of the new dataset

- Date Range: January 1, 2023 to present
- Data Source: Toronto Police Services (TPS) reported collisions ONLY
- No verified or human edited data
- As of 2025-08-14, we are _not_ filtering out private property collisions (but will introduce this soon)

### Documentation

The new collision pipeline is thoroughly documented in [bdit_collisions](https://github.com/CityofToronto/bdit_collisions/tree/qa).

Start with [bdit_collisions/pipelines](https://github.com/CityofToronto/bdit_collisions/tree/qa/pipelines), and follow the breadcrumbs from there.

### Where can I find the dataset?

As of 2025-08-13, you can find the new dataset in `bigdata`, under the `collisions_pipeline` schema.

### Table Structure

There are three main materialized views: `event_location`, `involved`, and `vehicle`. These three concepts have been intentionally separated to address some of the shortcomings of the legacy dataset (`ACC`) (e.g. inconsistent event-level data across involved rows, a super wide table).

Core views:

- `event_location`: Collision event and location data. One row per collision event. [LINK](https://github.com/CityofToronto/bdit_collisions/tree/qa/views#event_location-view)
- `involved`: Involved people. One row per person. [LINK](https://github.com/CityofToronto/bdit_collisions/tree/qa/views#involved-view)
- `vehicle`: Involved vehicles. One row per vehicle. [LINK](https://github.com/CityofToronto/bdit_collisions/tree/qa/views#vehicle-view)

Additional purpose-built views:

- `sla`: SLA between collision date and load date. [LINK](https://github.com/CityofToronto/bdit_collisions/tree/qa/views#sla-view)
- `damage_city_property`: Collisions that resulted in damage to City of Toronto property. [LINK](https://github.com/CityofToronto/bdit_collisions/tree/qa/views#city_dmg-view)
- `acc`: A backwards-compatible translation of the new dataset to the legacy structure. [LINK](https://github.com/CityofToronto/bdit_collisions/tree/qa/views#acc-view)
- `vz`: Collision classifications for Vision Zero. Classified by involved road users, collision impact type/scenario, and location. [LINK](https://github.com/CityofToronto/bdit_collisions/tree/qa/views#vz_view-view)

### Who to contact with questions?

Please direct your questions, bugs, and requests to the Collision Pipeline Team (Natalie, Adam, Maddy, Leo). Or, [log an issue in bdit_collisions](https://github.com/CityofToronto/bdit_collisions/issues).

