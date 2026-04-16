Architecture Decision Record: Stage 3 Streaming Extension

File: adr/stage3_adr.md
Author: Sandor Vas
Date: 2026-04-13
Status: Final

⸻

Context

Stage 3 introduced a streaming extension requirement to the existing batch-oriented data pipeline. The mobile product team required near-real-time visibility into transaction activity, specifically through two outputs: current_balances and recent_transactions. The streaming interface consisted of a directory-based ingestion pattern, where micro-batch JSONL files were continuously delivered into /data/stream/. The system was expected to poll this directory and process new files with an SLA of approximately 5 minutes from file arrival to Gold layer update.

Prior to Stage 3, the pipeline had already evolved through Stage 1 and Stage 2 into a structured batch processing system. It included ingestion (ingest.py), transformation (transform.py), and Gold layer construction (gold.py) with Delta Lake outputs. The pipeline used deterministic surrogate keys, partitioned Delta tables, and modular functions for building dimensions and facts. The total codebase was approximately 1,500–2,000 lines, with clear separation between ingestion, transformation, and output layers.

⸻

Architecture Overview (Block Diagram)

flowchart LR
    A[Stream JSONL Files] --> B[Polling Layer]
    B --> C[Ingestion Module]

    C --> D[Transformation Layer]
    D --> E[Gold Layer (Delta)]

    E --> F[current_balances]
    E --> G[recent_transactions]

    E --> H[Velocity Detection]
    H --> I[Risk Scoring]
    I --> J[Explainability]

    E --> K[DuckDB Analytics]


⸻

Streaming Processing Flow (Sequence Diagram)

sequenceDiagram
    participant FS as /data/stream/
    participant Poller
    participant Ingest
    participant Transform
    participant Gold
    participant Delta

    FS->>Poller: New JSONL file arrives
    Poller->>Ingest: Read file
    Ingest->>Transform: Clean & normalize
    Transform->>Gold: Build fact + dimensions
    Gold->>Delta: MERGE / UPSERT
    Delta-->>Gold: Commit success


⸻

Decision 1: How did your existing Stage 1 architecture facilitate or hinder the streaming extension?

The Stage 1 and Stage 2 architecture significantly facilitated the transition to streaming in several ways. The modular separation between ingestion (ingest.py), transformation (transform.py), and Gold layer (gold.py) allowed the streaming logic to reuse transformation and enrichment functions without major rewrites. The use of Delta Lake was particularly beneficial, as the MERGE-based upsert patterns used in Stage 2 could be directly applied to streaming micro-batches when maintaining stateful outputs such as current_balances.

Additionally, the use of partitioned Delta tables (by province and transaction_date) ensured that incremental updates remained efficient. The deterministic surrogate key generation using xxhash64 also ensured consistency across batch and streaming contexts.

However, certain design choices introduced friction. The main pipeline entry point (run_all.py) was designed for batch execution and did not originally support mode-based execution (batch vs streaming). This required adding conditional logic to handle streaming flows, which reduced clarity. Furthermore, some schema definitions were embedded directly within transformation functions rather than centralized, making it harder to extend outputs like current_balances without tracing dependencies across multiple files.

Overall, approximately 80–85% of the Stage 1/2 codebase was reused unchanged. Most changes involved extending ingestion logic to support directory polling and adding new stateful transformations rather than rewriting existing components.

⸻

Decision 2: What design decisions in Stage 1 would you change in hindsight?

In hindsight, one of the most impactful changes would have been to design the pipeline entry point (run_all.py) with explicit mode handling from the start. Introducing a --mode batch|stream argument early would have avoided the need to retrofit conditional logic during Stage 3, making the pipeline easier to reason about and maintain.

Another key improvement would have been to centralize schema definitions into a dedicated module (e.g., config/schemas.py). In the current implementation, schema logic is partially embedded in transformation functions such as gold.py, which made extending the pipeline to support new outputs like current_balances more complex than necessary.

Additionally, I would have designed the ingestion layer to be interface-driven rather than file-path driven. Currently, ingestion assumes static input paths (e.g., JSONL and CSV files). A more flexible design would have abstracted ingestion into a pluggable interface supporting both batch files and streaming directories, reducing the effort required to integrate the /data/stream/ source.

Finally, I would have avoided tightly coupling timestamp generation (e.g., current_timestamp()) inside transformation logic and instead passed it explicitly as a pipeline parameter. This would improve determinism and make testing and replay scenarios easier in a streaming context.

⸻

Decision 3: How would you approach this differently if you had known Stage 3 was coming from the start?

If the full three-stage specification had been known from the beginning, I would have designed the pipeline as a dual-mode system from Day 1, supporting both batch and streaming ingestion through a unified interface. The ingestion layer would expose a common contract, allowing inputs to be either static files or continuously arriving micro-batches, with a consistent downstream API.

For state management, I would have explicitly designed for incremental updates using Delta Lake MERGE operations from the outset, particularly for tables like current_balances that require maintaining running state. This would include designing idempotent transformations and clear primary key definitions early in the pipeline.

The Gold layer would have been structured as a reusable module capable of handling both batch and streaming outputs, rather than being implicitly batch-oriented. I would also have separated pipeline orchestration from transformation logic more clearly, possibly introducing a control-plane style execution layer to manage batch runs, streaming polling, and scheduling independently.

Finally, I would have introduced configuration-driven pipeline behavior earlier (e.g., YAML-based pipeline definitions), allowing new data sources, outputs, and processing modes to be added without modifying core logic. This would have made the Stage 3 extension largely a configuration exercise rather than a structural change.

⸻

Appendix

This ADR reflects a layered architecture:
	•	Ingestion → Transformation → Gold Layer → Intelligence → Explainability → ML

The addition of streaming reused most of the batch logic while extending ingestion and state handling, demonstrating good architectural separation and reuse.
