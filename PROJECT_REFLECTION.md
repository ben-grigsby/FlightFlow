# Project Reflection

## 1. Introduction

This document serves as a deeper reflection on the architectural design, motivations, and technical trade-offs behind **HermesFlow** - an end-to-end data engineering and machine learning pipeline built to predict flight arrival using real-world aviation data.

While the main `README.md` focuses on functionality and reproducibility, this reflection aims to unpack the *why* behind each major decision - from tool selection to data modeling, architecture philosophy, and lessons learned throughout development.

---
## 2. Motivation and Vision

There were several reasons behind why I chose to build *HermesFlow*, but the main motivators were rooted in two things: wanting to work on something **closer to industry-level systems**, and wanting to **push past the fear** of using complex tools I once thought were “too advanced” for me.

### Moving Beyond Static Datasets
For a long time, most of my projects followed the same pattern: download a CSV, analyze it, build a model, and move on. While valuable at first, that approach eventually felt disconnected from how real data systems work.  
I wanted to create something that felt *alive* — a full, end-to-end pipeline where data didn’t just sit in a file but continuously flowed, transformed, and informed predictions.

This desire also came from my internship experience, where I was first introduced to the world of **data engineering**.  
I wanted to expand that understanding by creating something that combined the entire lifecycle — ingestion, transformation, storage, and machine learning — all within one cohesive system.

### Facing “Advanced” Technology Head-On
Another major reason was personal. I realized I had built up a quiet fear of tools like **Kafka**, **Airflow**, and **Docker** — believing I wouldn’t understand them because they seemed too complex or “enterprise-level.” That frustration became motivation.  
I wanted to throw myself into an environment where I *had* to learn these tools through necessity, not theory. If it meant breaking things and rebuilding them ten times, so be it.  
The goal was to stop hesitating and start building.

### Early Challenges and Evolution
The first version of *HermesFlow* used **PostgreSQL** as the central storage layer. It was simple and effective for small-scale testing, but it quickly became a bottleneck.  
Each schema change required container rebuilds and constant manual adjustments. That friction led me to explore **dbt**, which helped modularize transformations and reduce redundancy — my first real step toward a scalable architecture.

Even then, I wanted more flexibility. While listening to a data engineering podcast, I came across **Apache Iceberg**, a lakehouse technology that offered schema evolution, versioning, and time travel. It seemed like the perfect way to handle data growth without sacrificing structure.  
Replacing PostgreSQL with Iceberg became a pivotal point — one that reshaped how I thought about data systems altogether.

I now view **Apache Iceberg** as somewhat analogous to the *microbots* from *Big Hero 6* — with each Parquet file acting as an individual unit. On their own, they’re inert and unstructured, but when coordinated, they can dynamically build, reshape, or dismantle entire systems in seconds. That’s the power of Iceberg: it doesn’t just store data efficiently — it enables you to **build and rebuild structure on demand**, without the overhead of rigid schemas or full system restarts.

### Expanding the Scope: From Static Data to Live APIs
As my technical foundation grew, so did the scale of data I wanted to work with. Initially, I used small telemetry datasets with fields like latitude, longitude, airline, and aircraft type. But when I discovered the **AviationStack API**, I realized I could access richer flight-level data — schedules, gates, delays, and even real-time flight status.

The challenge was that the free API tier allowed only 100 calls per month — far too low for what I envisioned.  
Rather than giving up, I decided to reach out to the company directly and explained that I was working on a non-commercial personal project. To my surprise, they agreed to grant me access to their upgraded plan — increasing my quota from 100 calls per month to **500,000**.  
That single moment changed everything. Suddenly, I could treat the data realistically, simulate near-live ingestion, and design a system that mirrored what an actual airport analytics pipeline might look like.

---

## 3. Evolutionary Timeline

### Stage 1: Foundation — Building the Core System
*HermesFlow* began as an experiment in creating something closer to an **industry-grade data pipeline** rather than a typical academic project.  
The focus was on understanding how real-world data moves — not just analyzing static CSVs.

The first stage revolved around five key technologies:
- **Kafka** for real-time ingestion and decoupled data flow  
- **PostgreSQL** for structured storage  
- **Docker** for environment consistency  
- **Spark** for distributed transformations  
- A lightweight version of **Airflow** for orchestration  

At this point, the system ingested data from the AviationStack and OpenSky APIs, streamed a small portion through Kafka, stored it in PostgreSQL, and used Spark jobs for transformation. Airflow orchestrated each step, ensuring the pipeline ran end-to-end once I configured all components correctly.  

This stage was about *making things work* — connecting containers, debugging broken mounts, and ensuring services could communicate. It was my first experience managing multiple distributed systems and realizing how much engineering effort goes into simply *keeping things alive*.

---

### Stage 2: Expansion — From Manual SQL to Declarative Modeling
As the project stabilized, new challenges emerged. Maintaining PostgreSQL schemas became repetitive and error-prone.  
Every data change required table rebuilds, restarts, or SQL rewrites — slowing progress and cluttering the workflow.

This led me to adopt **dbt (Data Build Tool)**, which allowed for declarative modeling and modular transformations.  
It simplified schema management and let me version control SQL models more cleanly.  
However, dbt still relied on static database logic, which meant I often had to re-run entire pipelines to reflect upstream changes.

Still, this phase marked the project’s first real turning point — from ad hoc scripts to structured, model-driven data engineering.

---

### Stage 3: Transition — Discovering the Lakehouse
The next transformation came when I encountered **Apache Iceberg** — a modern data lakehouse format that combined the scalability of data lakes with the reliability of data warehouses.  
It solved nearly every pain point I had been fighting: schema rigidity, rebuild requirements, and lack of version control.

Migrating from PostgreSQL to Iceberg fundamentally changed how HermesFlow operated:
- No more manual schema definitions  
- Incremental updates and rollbacks became trivial  
- Storage became scalable, traceable, and production-grade  

This was the stage where *HermesFlow* truly became a **data platform** rather than just a data pipeline.  
The architecture matured, and each tool started serving a clear role within a cohesive ecosystem.

---

### Stage 4: Integration and Intelligence
With the architecture stabilized, I began expanding *HermesFlow’s* scope — making it both **intelligent** and **autonomous**.

This stage introduced:
- **Weekly model retraining** pipelines using PySpark + XGBoost  
- **Daily delay prediction** DAGs for upcoming flights (T+8)  
- A formal **Medallion Architecture (Bronze → Silver → Gold)** to standardize transformations  
- A lightweight **dashboard** for visualizing flight statistics and model performance  

At this point, *HermesFlow* wasn’t just a tech experiment — it was a **living data ecosystem** capable of continuous learning and insight generation.

---

### Stage 5: Reflection and Refinement
The final stage was less about adding tools and more about *understanding the system as a whole*.  
I cleaned up the repository, standardized structures, and documented each decision — not just the technical “how,” but the conceptual “why.”  

Looking back, every major decision — Kafka for decoupling, dbt for modularity, Iceberg for scalability — was a deliberate step toward a single goal:  
to build a pipeline that **behaves like a production system** and **forces me to think like an engineer** rather than a data analyst.

## 3. Major Lessons Learned

One of the most annoying/repeitive problems that I ran into was Airflow being unable to locate a file due to it having a different root configuration than my local computer. So many times I had to keep adding paths to the volumes section under the different Airflow pieces in my .yml yet errors still arose. I eventually settled on writing everthing locally first to ensure that the program was logically sound, and then I would convert the paths from local paths to Airflow paths.

Another problem I dealt with was setting up Spark. I genuinely did not understand what the errors were and HEAVILY relied on ChatGPT to help figure those things out.

The other problems were just getting used to the how the different softwares work and how to best manipulate them. 