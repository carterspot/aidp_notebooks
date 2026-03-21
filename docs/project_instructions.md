# Oracle AI Data Platform (AIDP) – Project Reference
> Curated resource list for Oracle AIDP. Last updated: March 2026.  
> Organized by **Use Case** and **Resource Type** for fast lookup.

---

## 🧠 Project System Prompt (paste into Claude Project Instructions)

```
You are an Oracle AI Data Platform (AIDP) expert with deep, hands-on skills across:

- Data Engineering: Apache Spark/PySpark, SQL, Scala, pipeline design, Medallion Architecture (Bronze/Silver/Gold), Delta Lake, Iceberg table formats
- ML & AI: model development and deployment, vector search, RAG pipelines, in-database ML, GenAI-augmented data workflows
- Platform & Infrastructure: OCI architecture, compute cluster configuration, object storage, IAM policy setup, instance management
- Governance: role-based access control (admin, auditor, data engineer), data lineage, audit logs, catalog and schema management
- Integrations: Oracle Analytics Cloud (OAC), Oracle Fusion Cloud ERP/SCM, Oracle AI Database 26ai, Agent Hub, Fusion Data Intelligence (FDI)
- Notebook Development: Python, SQL, Scala, Java notebooks with Git integration, workflow orchestration, task dependencies, scheduling

When answering questions:
- Default to expert-level explanations — skip basics unless asked
- Reference specific AIDP components, APIs, or config patterns where relevant
- Suggest best practices around governance, performance, and architecture
- Use the attached resource list to cite or recommend specific documentation, videos, or guides
```

---

## 🗂 By Use Case

### 🚀 Getting Started / Instance Setup
- [Oracle AIDP Workbench – Official Documentation Hub](https://docs.oracle.com/en/cloud/paas/ai-data-platform/) — Main portal for setup, access, and administration
- [How to Create an Instance in Oracle AIDP Workbench](https://www.youtube.com/watch?v=KGsSn690ZDc) — Compartments, IAM policies, environment naming, role-based access
- [Getting Started on Your Oracle AIDP Journey (Oracle Blog)](https://blogs.oracle.com/ai-data-platform/getting-started-on-your-oracle-ai-data-platform-journey) — Full data lifecycle overview: ingestion to AI-driven insights
- [Getting Started with AIDP Sample Notebooks (A-Team Oracle)](https://www.ateam-oracle.com/oracle-ai-data-platform-getting-started-with-the-sample-notebooks) — Configure environment, run GitHub sample notebooks, cluster and workspace setup

### 🏗 Medallion Architecture & Pipelines
- [How to Build a Basic Medallion Architecture in Oracle AIDP](https://www.youtube.com/watch?v=pZ6n5jTAkmI) — Bronze/Silver/Gold layers, GenAI data augmentation, Gold dataset curation
- [Using Oracle AI Data Platform Workbench (PDF – Jan 2026)](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/using-oracle-ai-data-platform-workbench.pdf) — Medallion Architecture, Spark workflows, ML deployment, governance, pricing
- [Oracle's AI Data Platform: Building a Foundation for Enterprise AI (Terillium)](https://terillium.com/oracles-ai-data-platform-building-a-foundation-for-enterprise-ai/) — Unified data lake+warehouse+analytics, Medallion Architecture, AI lifecycle tools

### 📓 Notebooks & Workspaces
- [How to Set Up Workspaces, Clusters, and Notebooks](https://www.youtube.com/watch?v=3qq_uLz9ucU) — Spark clusters (Python/SQL), library installs via requirements.txt, cluster-to-notebook linking
- [Getting Started with AIDP Sample Notebooks (A-Team Oracle)](https://www.ateam-oracle.com/oracle-ai-data-platform-getting-started-with-the-sample-notebooks) — Object storage examples, workspace creation, sample notebook walkthroughs
- [Oracle AIDP GitHub Samples Repository](https://github.com/oracle-samples/oracle-aidp-samples) — Official sample notebooks: Spark at scale, agent development, orchestration, catalog management
- [Using Oracle AI Data Platform Workbench (HTML Guide)](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/index.html) — Full procedural guide including notebook tasks

### ⚙️ Workflows & Automation
- [How to Automate Workflows in Oracle AIDP Workbench](https://www.youtube.com/watch?v=9gAgRnCjjqo) — Multi-step workflows, task dependencies, branching, job parameters, scheduling
- [Using Oracle AI Data Platform Workbench (PDF – Jan 2026)](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/using-oracle-ai-data-platform-workbench.pdf) — Workflow automation reference including low-code/no-code pipeline patterns

### 🗄 Catalogs, Schemas & Data Management
- [How to Organize Data with Catalogs and Schemas](https://www.youtube.com/watch?v=dSuQdIOaHX8) — Standard and external catalogs, schema config, table import, volume management
- [Using Oracle AI Data Platform Workbench (HTML Guide)](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/index.html) — Catalog and schema procedures
- [Oracle AIDP GitHub Samples Repository](https://github.com/oracle-samples/oracle-aidp-samples) — Catalog management sample notebooks

### 🔐 Security, Roles & Governance
- [How to Create and Assign Roles in Oracle AIDP Workbench](https://www.youtube.com/watch?v=Bfs4AYVuleE) — Admin/auditor/data engineer roles, OCID-based user assignment, permission inheritance
- [Using Oracle AI Data Platform Workbench (HTML Guide)](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/index.html) — Security and audit log reference
- [Oracle AIDP Licensing Information](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidli/index.html) — Licensing terms and third-party acknowledgments

### 🤖 AI, ML & Advanced Capabilities
- [Strategic Analysis: Oracle AIDP (Centroid Whitepaper)](https://www.centroid.com/library/strategic-analysis-oracle-ai-data-platform-aidp/) — Vector search, in-database ML, OCI Supercluster, RAG pipelines
- [My Top 5 Key Takeaways from Oracle AI World 2025 (Apps Associates)](https://appsassociates.com/blog/my-top-5-key-takeaways-from-oracle-ai-world-2025/) — Agent Hub, enterprise catalog, Oracle AI Database 26ai integration, pricing
- [Oracle AIDP GitHub Samples Repository](https://github.com/oracle-samples/oracle-aidp-samples) — Agent development and orchestration sample notebooks

### 📅 Roadmap & Community
- [Oracle AIDP Community Hub](https://community.oracle.com/products/oracleaidp/) — Live events, webinars, sharing center (notebooks, templates, scripts)
- [Oracle AIDP Product Roadmap (December 2025)](https://community.oracle.com/products/oracleanalytics/discussion/27733/oracle-ai-data-platform-product-roadmap-for-december-2025) — Official roadmap PDF and webinar links from Oracle Product Management

---

## 📁 By Resource Type

### 📄 Official Documentation
| Resource | URL |
|----------|-----|
| AIDP Workbench Documentation Hub | https://docs.oracle.com/en/cloud/paas/ai-data-platform/ |
| Using AIDP Workbench (HTML Guide) | https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/index.html |
| Using AIDP Workbench (PDF – Jan 2026) | https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/using-oracle-ai-data-platform-workbench.pdf |
| AIDP Licensing Information | https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidli/index.html |

### ▶ Videos
| Resource | URL |
|----------|-----|
| How to Build a Basic Medallion Architecture | https://www.youtube.com/watch?v=pZ6n5jTAkmI |
| How to Create an Instance | https://www.youtube.com/watch?v=KGsSn690ZDc |
| How to Create and Assign Roles | https://www.youtube.com/watch?v=Bfs4AYVuleE |
| How to Organize Data with Catalogs and Schemas | https://www.youtube.com/watch?v=dSuQdIOaHX8 |
| How to Set Up Workspaces, Clusters, and Notebooks | https://www.youtube.com/watch?v=3qq_uLz9ucU |
| How to Automate Workflows | https://www.youtube.com/watch?v=9gAgRnCjjqo |

### ✍ Blogs & Articles
| Resource | URL |
|----------|-----|
| Getting Started on Your AIDP Journey (Oracle Blog) | https://blogs.oracle.com/ai-data-platform/getting-started-on-your-oracle-ai-data-platform-journey |
| Getting Started with Sample Notebooks (A-Team Oracle) | https://www.ateam-oracle.com/oracle-ai-data-platform-getting-started-with-the-sample-notebooks |
| Building a Foundation for Enterprise AI (Terillium) | https://terillium.com/oracles-ai-data-platform-building-a-foundation-for-enterprise-ai/ |
| Top 5 Takeaways from Oracle AI World 2025 (Apps Associates) | https://appsassociates.com/blog/my-top-5-key-takeaways-from-oracle-ai-world-2025/ |
| Strategic Analysis: Oracle AIDP (Centroid Whitepaper) | https://www.centroid.com/library/strategic-analysis-oracle-ai-data-platform-aidp/ |

### ⬡ Community & GitHub
| Resource | URL |
|----------|-----|
| Oracle AIDP Community Hub | https://community.oracle.com/products/oracleaidp/ |
| Oracle AIDP GitHub Samples Repository | https://github.com/oracle-samples/oracle-aidp-samples |
| Oracle AIDP Product Roadmap (December 2025) | https://community.oracle.com/products/oracleanalytics/discussion/27733/oracle-ai-data-platform-product-roadmap-for-december-2025 |

---

## ⚡ Quick Reference

| Topic | Detail |
|-------|--------|
| Compute default | AMD 2 OCPU / 32GB Memory |
| Pricing unit | AIDP Units (2 OCPU + 32GB = 3 units/node) |
| Min cluster cost | 230 AIDP Units/hour |
| Notebook languages | Python, SQL, Scala, Java |
| Medallion layers | Bronze (raw) → Silver (transformed) → Gold (curated) |
| Table formats | Delta Uniform, Apache Iceberg |
| Catalog naming | `catalog_name.schema.table` (3-part) |
| Access to OCI nav | cloud.oracle.com → Analytics and AI → AI Data Platform |

---

*18 resources · 7 use case groups · 4 resource type categories*
