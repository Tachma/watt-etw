# Battery Optimization in the Greek Electricity Market — Challenge Brief

## Problem Statement

Greece's electricity market is undergoing a rapid transition as renewable energy sources (RES) continue to expand. This growing penetration of solar and wind generation creates increasing variability in the system, leading to more frequent periods of surplus generation, renewable curtailments, and stronger intraday and day-ahead price volatility. In 2025, curtailments in Greece rose sharply, while the need for flexibility and storage became more urgent in order to absorb excess renewable output and shift energy to periods of higher demand.

In this context, battery energy storage systems are beginning to enter the Greek market as a key source of flexibility. Greece's first standalone batteries entered the Day-Ahead Market in test mode in April 2026, marking the start of battery integration into the system, while additional storage capacity is expected to follow. Their role is to charge during periods of lower-priced and surplus energy — often associated with high renewable output — and discharge during periods when energy is more valuable to the system.

## Deliverable

Participants are expected to propose a complete battery optimization solution for the Greek electricity market. Based on a set of general guidelines provided by the organizers, they should identify, collect, and use the data sources they consider necessary in order to determine when a battery should charge, discharge, or remain idle, with the objective of maximizing economic value while respecting all technical and operational constraints of an asset.

Electricity price information — like Day-Ahead Market (DAM) — may be used as one of the key inputs to the proposed solution. However, the core of the challenge is not the market itself, but the design of a robust battery optimization framework that can operate in a realistic environment with limited asset-specific information.

The final deliverable should therefore present a well-structured and realistic optimization approach that combines the necessary forecasting inputs, battery scheduling logic, and constraint-aware decision-making. Since the challenge is intentionally framed under data scarcity, participants are expected to demonstrate how their proposed solution can still produce feasible and economically meaningful battery schedules even without access to a rich battery telemetry history.

## Energy Market — DAM

### Understanding the Day-Ahead Market (DAM)

The Day-Ahead Market (DAM) is the main electricity market where energy for the next delivery day is traded. In Greece, the DAM is operated by HEnEx, the Hellenic Energy Exchange, which is the designated market operator for the Greek Day-Ahead and Intraday markets. The Greek spot market was launched in November 2020 under the Target Model design, and the Greek DAM was later integrated into the Single Day-Ahead Coupling (SDAC) in December 2020.

When the Greek DAM was first launched, it operated at an hourly resolution, meaning that one market price was determined for each hour of the following day. This later changed as part of the wider European transition toward shorter market intervals. From **1 October 2025**, the Greek bidding zone moved to a **15-minute Market Time Unit (MTU)** in the coupled day-ahead market. As a result, DAM prices are now determined for each 15-minute delivery period, rather than only once per hour.

For the purposes of this challenge, the DAM is relevant because it provides the energy price signal used in battery scheduling. Participants should therefore think of the DAM as the source of a time series of electricity prices — hourly in the earlier years and 15-minute from 1 October 2025 onward — that can be used to determine whether a battery should charge, discharge, or remain idle.

### How is the DAM price determined?

In simple terms, the DAM price is formed by matching electricity demand and electricity supply for each delivery period. Market participants submit bids to buy electricity and offers to sell electricity for the following day. These bids and offers can be represented as demand and supply curves. HEnEx publishes both DAM market results and aggregated buy/sell curves, which reflect this market-clearing process.

The demand curve shows how much electricity buyers are willing to purchase at different price levels. The supply curve shows how much electricity sellers are willing to provide at different price levels. The market clearing process identifies the point where these two curves intersect. This intersection determines two things for each delivery period: the market clearing quantity, and the market clearing price.

This is based on the standard marginal pricing principle used in day-ahead electricity markets. In practice, the final price is set by the last accepted offer needed to satisfy demand for that time period. Although the real European day-ahead market is cleared through a common algorithm that also considers cross-border transmission constraints and market coupling, the basic idea can be understood as the intersection of supply and demand.

### A simple example

The example below is simplified and is intended only to explain the concept. Suppose that for one 15-minute delivery period, the market receives the following sell offers:

- 50 MWh at €40/MWh
- 70 MWh at €60/MWh
- 60 MWh at €90/MWh

These offers create an upward supply curve: the cheapest electricity is accepted first, then more expensive electricity is added if more demand must be met. Now suppose that total demand in that same 15-minute period is 120 MWh. To cover this demand, the market needs:

- the first 50 MWh at €40/MWh, and
- the next 70 MWh at €60/MWh.

In this case, the market clears at a quantity of 120 MWh, and the market clearing price becomes €60/MWh, because the second offer is the last one needed to satisfy demand. Under a uniform price auction design, all accepted sellers receive €60/MWh, and all accepted buyers pay €60/MWh for that delivery period.

The same logic is repeated for every other delivery period of the day. Since the Greek DAM now clears at a 15-minute resolution, the market produces a sequence of 15-minute prices across the day. This means that prices can change significantly within the same hour, which is particularly relevant in systems with a high share of renewable generation.

### Why this matters for battery optimization

For a battery, the DAM price is the key economic signal that indicates whether it may be attractive to:

- charge when prices are relatively low,
- discharge when prices are relatively high, or
- remain idle when price spreads are not attractive enough.

In other words, participants do not need to become experts in market design. They only need to understand that the DAM produces the next-day energy prices, and that these prices reflect the balance between electricity supply and demand for each delivery period. Their main task is then to use those prices as an input for the battery optimization problem.

## Data Scarcity

Data scarcity means that the available data is not sufficient to fully describe the problem or support a purely data-driven solution. In this challenge, data scarcity mainly concerns the battery asset itself: while participants may have access to market, system, and weather information, they do not have a long and mature historical record of battery operation. This is a realistic assumption for Greece, since large-scale standalone batteries only began entering the market in test mode in April 2026, while renewable curtailments and the need for storage have already been increasing.

As a result, participants cannot rely only on historical battery behavior and instead must work with a limited set of battery specifications, operational constraints, and external market signals. In practice, this means the challenge is about building a robust optimization framework that can still produce feasible and economically meaningful battery schedules even when battery-specific information is limited.

## Data Sources

### HEnEx
The Hellenic Energy Exchange (HEnEx) is the operator of the Greek electricity spot markets, including the Day-Ahead Market (DAM) and Intraday markets, and publishes official market results, prices, and aggregated buy/sell curves.
- Day-Ahead Market — EnExGroup
- **Key data sources:** Market Results / Results

### IPTO (ADMIE)
IPTO (ADMIE) is the Greek transmission system operator and provides key system data such as load forecasts, RES forecasts, ATC values, dispatch-related information, and market statistics relevant to electricity market analysis.
- Data | IPTO
- **Key data sources:** Data Type / ISP Requirements

### Open-Meteo
Open-Meteo is an open weather data provider offering historical and forecast weather data through an API, including variables such as temperature, wind speed, cloud cover, and solar radiation.
- Free Open-Source Weather API | Open-Meteo.com

### TTF ICE
ICE Endex / ICE provides market data for Dutch TTF Natural Gas Futures, which are widely used as a benchmark for European gas prices and can help explain fuel-related movements in electricity prices.
- Dutch TTF Natural Gas Futures Pricing

### EEX
EEX (European Energy Exchange) provides market data for EU ETS allowances (EUA), including spot and futures products, which are relevant for understanding the carbon cost component of thermal power generation.
- Market Data Hub | EEX

### ENTSO-E
ENTSO-E operates the Transparency Platform, a pan-European source of electricity market data covering generation, load, transmission, balancing, outages, and congestion management across European countries.
- Transparency Platform
