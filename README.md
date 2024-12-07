
# AE Bootcamp Capstone Proposal

## Table of Contents
- [Introduction](#introduction)
- [Problem Statement](#problem-statement)
- [Conceptual Data Model](#conceptual-data-model)
- [System Design](#system-design)

## 👋 Introduction
My name is Bryan Galindo. I'm a Software Engineer at Bank of America. For the capstone project I've paired up a small business called [Vinty](https://getvinty.com), which specializes in reselling second-hand luxury handbags. After a couple of conversations with the business owner, here's their business problem I'm trying to solve.

## 🔍 Problem Statement
Vinty wants to **identify current trends in the second-hand luxury handbag resale market** to help guide their procurement department on which bags to purchase to **minimize their average product time to sell** and ultimately **increase their sales volume**. To identify the trends, Vinty needs at least daily snapshots of all the handbags that are for sale on the market, along with prior sold handbag data (if available).

## 🧠 Conceptual Data Model
I've created the conceptual data model below, which includes the data sources, the ingestion process + any challenges associated with them, and the KPIs we need to extract from the data models. Due to time constraints, the intial build will only focus on two data sources, Rebag and VSP.

![Data Model](https://i.imgur.com/PrvhRoC.png)

## 📟 Technologies
I will be working with the following technologies, although due to time constraints, I may have to build an initial version of the pipeline using only the core technologies below. 
- **Core Technologies**
    - Python
    - Apache Airflow (managed by Astronomer)
    - Amazon S3
    - dbt
    - Trino (managed by Starburst)
    - Tableau
- **Nice-to-have Technologies**
    - Apache Iceberg
    - Apache Spark
    - PostgreSQL

## 🖥️ System Design
This the ideal system design for the capstone, although I may have to further simplify this design due to time constraints. The primary goal should be to get an initial build out on time, rather than worry about using the best tech + optimizations (even though I want to 😢).

![System Design](https://i.imgur.com/i4PU4dy.png)





![Logo](https://content.techcreator.io/dataexpert-wordmark.svg)
