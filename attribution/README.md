# Attribution Model

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](<>)

In a customer journey to conversion, customers may interact with multiple ads from the same advertiser.
Attribution modeling is the process of assigning credit to the different marketing channels or touchpoints that have contributed to a user's conversion.

In this code, an approach to Attribution modeling is proposed using Google Analytics 4 (GA4) data to track digital interactions and conversions.

The solution considers online interactions with advertisement campaigns as tracked by GA4.
Online purchases are considered as conversions and traffic sources as tracked by GA4 are the touch-points.
Data extraction from GA4 and preparation is carried out in SQL taking advantage of Google Cloud Platform (GCP) BigQuery whereas Attribution Model is implemented in Python using ChannelAttribution library to implement the model.

## Getting started

In order to work with this repository you need to have poetry installed on your local environment.

NOTE that if you are using Windows then the installation of ChannelAttribution requires Microsoft Visual C++ 14.0 or greater (see [here](https://visualstudio.microsoft.com/it/downloads/)).

## Attribution

The proposed Attribution Model aims to estimate the impact that each ad interaction had in the digital conversions of customers.

Here you can find some info about the **naming**.

- Conversion: we considered as conversions online purchases made through website;
- Channel: here we refer to channel as to the different types of digital ad interactions.

**Perimeter**\
We only consider digital events occurred after January 1st 2023. This time interval has been chosen since before such date no data related to a web visit's source was collected.

**Events data preparation** (script `ga4_events_preparation.sql`)\
The aim of this script is to identify all valid channel interactions. This is done in the following steps:

1. Events retrieval: first all valid online events are retrieved from GA4 data.
   - An event is considered as valid if either its collected traffic source or collected traffic medium are not null. This is because collected traffic source and medium are then used to identify the event's channel;
   - Multiple events corresponding to the same collected traffic source, collected traffic medium pair in the same session and date are dropped. Only the first event by timestamp is kept.
1. Events mapping: each event is mapped into its corresponding channel using GA4 default channel grouping (see [here](https://support.google.com/analytics/answer/9756891?hl=en&ref_topic=11151952&sjid=5040598599104191390-EU#)).
1. Session data integration: each web session is enriched with high-level info such as the number of items and the amount purchased.

The output of this script serves as a base table of preprocessed GA4 events from which attribution chains will be constructed and on which further analysis can be derived.

**Chains base preparation** (script `ga4_attribution_chains_preparation.sql`)\
Here, the table built at previous step is filtered in order to have a support table only consisting of conversions in the attribution model perimeter and their related events. This is done in the following steps:

1. Conversions identification: all purchases in the conversion period are extracted.
1. Events identification: all digital interactions of a given customer which occurred in a lookback range from the same customer's conversion identified in the step above are considered.

The output of this script is a support table only consisting of the relevant conversions and events for the attribution model's purposes and perimeter.

## Contributors

- Pietro Greselin (p.greselin@gmail.com)
