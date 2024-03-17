# Marketing Mix Model

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](<>)

To promote a brand or a product, a company usually carries out differnt marketing campaigns across multiple channels (like on social networks, on tv, ...).
Marketing Mix Modeling (MMM) is the statistical approach to quantify the incremental sales impact and Return On Investment (ROI) of marketing activities.

In this code, an approach to MMM is proposed using Robyn, a library developed by Meta in R.

The solution considers weekly online and offline marketing investments over 3 years from the beginning of 2021 to the end of 2023.
Target variable is the amount of sales generated in each week.

## Getting started

In order to work with this repository you need to have poetry installed on your local environment.

## Marketing Mix

The proposed MMM aims to estimate the incremental sales and ROI generated by each marketing channel on the revenues.

Data is collected at weekly level from January 2021 to December 2023, the target of this analysis is the weekly amount of revenues and considered marketing channels range from online to offline, namely:
- Online: we collected investments and impressions over online search, display advertising and social media. The data from these different subchannels as been considered as a whole.
- Conventional TV: investments and GRPs from traditional TV advertisements.
- Unconventional TV: investments and GRPs from unconventional ads on TV such as sponsorships and promos during events.
- Third Party: investments and number of generated leads from third party provided contact lists.
- Direct Mail (DM): investments and number of reached people from direct mail communications.
- Telemarketing: investments and number of reached customers and leads from telemarketing promos.

Furthermore we collected metrics from competitors' marketing performances on TV channel. In particular, investments and GRPs on conventional TV.

## Contributors

- Pietro Greselin (p.greselin@gmail.com)