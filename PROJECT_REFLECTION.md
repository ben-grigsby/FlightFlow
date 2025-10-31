# Project Reflection

## 1. Introduction

This document serves as a deeper reflection on the architectural design, motivations, and technical trade-offs behind **HermesFlow** - an end-to-end data engineering and machine learning pipeline built to predict flight arrival using real-world aviation data.

While the main `README.md` focuses on functionality and reproducibility, this reflection aims to unpack the *why* behind each major decision - from tool selection to data modeling, architecture philosophy, and lessons learned throughout development.

---

## 2. Motivation and Vision

There were several reasons behind why I chose to build *HermesFlow*, but the main motivators were rooted in two things: wanting to work on something **closer to industry-level systems**, and wanting to **push past the fear** of using complex tools I once thought were “too advanced” for me.

### Moving Beyond Static Datasets
For a long time, most of my projects followed the same pattern: download a CSV, analyze it, build a model, and move on. While valuable at first, that approach eventually felt disconnected from how real data systems work. I wanted to create something that felt *alive* — a full, end-to-end pipeline where data didn’t just sit in a file but continuously flowed, transformed, and informed predictions.

This desire also came from my internship experience, where I was first introduced to the world of **data engineering**. I wanted to expand that understanding by creating something that combined the entire lifecycle: ingestion, transformation, storage, and machine learning — all within one cohesive system.

### Facing “Advanced” Technology Head-On
Another major reason was personal. I realized I had built up a quiet fear of tools like **Kafka**, **Airflow**, and **Docker** — believing I wouldn’t understand them because they seemed too complex or “enterprise-level.” That frustration became motivation. I wanted to throw myself into an environment where I *had* to learn these tools through necessity, not theory. If it meant breaking things and rebuilding them ten times, so be it. The goal was to stop hesitating and start building.

### From PostgreSQL to Iceberg: Evolving Through Iteration
The project initially started small — a simple data pipeline using **PostgreSQL** as the storage layer. But it didn’t take long for me to hit scaling and convenience issues. Each schema change required container restarts and multiple manual adjustments across files. I found myself spending more time maintaining the system than expanding it.

While researching solutions, I kept seeing **dbt** mentioned in job descriptions. Curious, I looked into it and quickly realized it could eliminate much of the repetitive table definition and transformation work. It was a great step forward: transformations became easier, and the pipeline more modular.

Still, dbt wasn’t perfect for my goals. Modifying upstream data often meant re-running entire pipelines to maintain consistency. Then, while listening to a data engineering podcast, I came across **Apache Iceberg** — described as a next-generation data lakehouse format designed for versioning, schema evolution, and time travel. It immediately clicked. I realized Iceberg would let me manage data at scale without losing flexibility or lineage tracking. After some experimentation, I replaced PostgreSQL entirely, and the data storage system became far more robust and production-like.

I now view **Apache Iceberg** as somewhat analogous to the *microbots* from *Big Hero 6* — with each Parquet file acting as an individual unit. On their own, they’re inert and unstructured, but when coordinated, they can dynamically build, reshape, or dismantle entire systems in seconds. That’s the power of Iceberg: it doesn’t just store data efficiently — it enables you to **build and rebuild structure on demand**, without the overhead of rigid schemas or full system restarts.


### Expanding the Scope: From Static Data to Live APIs
As my technical foundation grew, so did the scale of data I wanted to work with. Initially, I used small telemetry datasets with fields like latitude, longitude, airline, and aircraft type. But when I discovered the **AviationStack API**, I realized I could access richer flight-level data — schedules, gates, delays, and even real-time flight status.

The challenge was that the free API tier allowed only 100 calls per month, far too low for what I envisioned. Rather than giving up, I decided to reach out to the company directly and explained that I was working on a non-commercial personal project. To my surprise, they agreed to grant me access to their upgraded plan — increasing my quota from 100 calls per month to **500,000**. That single moment changed everything. Suddenly, I could treat the data realistically, simulate near-live ingestion, and design a system that mirrored what an actual airport analytics pipeline might look like.

## 3. Major Lessons Learned

