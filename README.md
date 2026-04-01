# 🏗️ Data Refinery — The Medallion Lakehouse

**Role:** Full-Stack Data Platform Architect  
**Architecture:** Medallion (Bronze/Silver/Gold) Lakehouse  
**Stack:** Apache Iceberg, Project Nessie, Apache Spark, MinIO, Next.js 14

---

## 💎 Executive Summary
The **Data Refinery** is a high-availability, ACID-compliant data platform designed for large-scale analytical processing. It leverages **Apache Iceberg** as the open table format to provide SQL-like reliability on top of S3-compatible storage (**MinIO**). By using **Project Nessie** as the catalog, it introduces Git-like versioning (branching, merging, and tagging) to the data lake, ensuring zero-loss processing and 100% traceability.

### 🚀 Key Value Propositions
- **Operational Excellence**: Multi-table ACID transactions and Iceberg Time Travel for instant snapshot recovery.
- **Scalability**: Horizontal Spark scaling on containerized infrastructure (Docker) to handle terabyte-scale JSON streams.
- **Data Governance**: Strict Schema Enforcement (Data Contracts) and PII Masking (SHA-256 / Anonymization) built into the pipeline.
- **Observability**: A "Glass Refinery" dashboard built with Next.js 14, providing real-time lineage mapping and snapshot monitoring.

---

## 🛠️ Infrastructure & Backend
The backend is a containerized ecosystem orchestration:

| Service | Technology | Role |
| :--- | :--- | :--- |
| **Orchestration** | Docker Compose | Multi-container microservices management |
| **Execution** | Apache Spark 3.5 | Distributed data processing engine |
| **Catalog** | Project Nessie | Versioned Iceberg REST Catalog (JDBC-backed) |
| **Storage** | MinIO (S3) | High-performance, S3-compatible object store |
| **Table Format**| Apache Iceberg | Rich SQL semantics, schema evolution, and hidden partitioning |

### 🌊 The Medallion Pipeline (`refinery_pipeline.py`)
1. **Bronze (Raw)**: Captures simulated e-commerce clickstreams (JSON) without modification.
2. **Silver (Cleaned)**: 
   - Enforces strict schema contracts.
   - Filters nulls on revenue-critical fields.
   - **PII Masking**: Hashes emails and anonymizes IP addresses for GDPR/CCPA compliance.
3. **Gold (Aggregated)**: Computes high-value KPIs (Revenue per Hour, Unique Buyers) for consumption.
4. **Time Travel**: Demonstrates data integrity by rolling back to healthy snapshots after simulated corruption.

---

## 🖥️ Frontend: The "Glass Refinery"
A high-end dashboard designed for data reliability visibility.

- **Stack**: Next.js 14, Tailwind CSS, Framer Motion, Lucide React, Recharts.
- **Glassmorphism**: Deep Dark mode with emerald neon accents for a premium monitoring experience.
- **Bento Grid**: A 12-column responsive layout featuring:
  - **Animated Data Lineage**: Real-time flow visualization from Bronze to Gold.
  - **Time Travel Slider**: Interactive snapshot management for Iceberg table history.
  - **Performance Charts**: High-fidelity Recharts visualizing hourly metrics.

---

## 🚦 Getting Started

### 1. Launch Infrastructure
```bash
docker compose up -d
```
*Wait for MinIO and Nessie health checks to pass.*

### 2. Run the Refinery Pipeline
```bash
docker compose exec spark-master spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,\
             org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.76.3,\
             org.apache.hadoop:hadoop-aws:3.3.4,\
             com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /pipeline/refinery_pipeline.py
```

### 3. Launch the Monitor Dashboard
```bash
cd dashboard
npm install
npm run dev
```
Explore the dashboard at `http://localhost:3000`.

---

## 📐 Design Philosophy
- **Immutability First**: All transformations create new versioned states without destructive overwrites.
- **Security by Design**: PII is masked at the earliest possible stage (Silver) to minimize exposure.
- **Traceability**: Every row in the Gold layer can be traced back through Silver snapshots to its Bronze origin.
