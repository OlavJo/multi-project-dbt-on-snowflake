# Silver Layer Design

## The Shared Foundation for Multi-Project dbt on Snowflake

### Design Document for Data Engineering and Silver Stewards

| | |
|---|---|
| **Version** | 1.5 |
| **Date** | 2026-10-09 |
| **Status** | **Proposed** |
| **Proposed By** | Olav Jordens (Lead Data Engineer, PSM CAI) dbt Certified Developer |
| **Companion document** | *Multi-Project dbt on Snowflake: Architecture Overview for Analytics Teams* v1.5 (referred to below as the **Overview**) |

---

## 1. Purpose and Scope

### 1.1 The Silver Layer as analytics infrastructure

The Silver Layer (Tier 0 Source Entity modules and the Tier 0.5 Core module) is the foundation every analytics project builds on. In medallion terms, raw landed data is Bronze (owned by the platform team), the published outputs of the Silver Layer is Silver, and the published outputs of Tier 1 and Tier 2 projects are Gold.

The Silver Layer's transformations are deliberately simple: shaping, never business logic. Yet it is where the hardest properties of the system meet:

- **Untrusted data enters.** Freshness, ordering, landing completeness and source defects are all handled once, here.
- **Many teams share one artefact.** Tier 1 and Tier 2 projects each have one owner; the Silver Layer may have contributions from all of them.
- **Everything inherits its guarantees.** If Silver provides them, the Tiers above get them almost for free. If it does not, no care above can recover them.
- **Its failures have the largest blast radius.**

The Silver Layer is therefore designed and operated as **infrastructure**: with a steward team (of analytics support data engineers), service levels, a release process and an operational control plane. It is designed for **safe change for consumers**: contracts, model versions from v1, independent publication units and a phased scope (Section 4.5). These features let it evolve without disrupting consumers, and accommodate new sources and new analytics teams.

### 1.2 Guarantees and how they are met

The Overview (Section 3.2) promises consumers the following. Each is delivered by a specific mechanism here:

|  | Data Engineering Guarantee | Mechanism | Section |
|---|---|---|---|
| G1 | **Gated**: nothing published failed its tests | Clone, freeze, build and test in isolation; explicit success check; atomic `SWAP` on success only | 9 |
| G2 | **Consistent**: one point in time per source | Frozen inputs: all raw tables of a source cloned at one timestamp; input clones of upstream units for units that read them | 6, 9.6 |
| G3 | **Dated**: every unit carries a watermark | Watermarks are Snowflake-side timestamps generated from landing-complete records per batch per publication unit| 10, 11 |
| G4 | **Contracted, documented and classified** | Enforced contracts, `persist_docs`, generated source package, classification tags that propagate onto derived columns | 5.3, 12.1 |
| G5 | **Versioned** | Every published model versioned from v1; new versions with deprecation dates; impact queries | 12.2, 12.3 |
| G6 | **Shaping only** | Structural join rule, scope-creep test, steward review gate | 4.3, 5.5 |
| G7 | **Stable keys** | Durable-key rules in the Core module | 8.3 |
| G8 | **Historicised**: as-at queries possible | History models built from the CDC log, or snapshots where no CDC exists, with system time from source ordering or the unit watermark | 5.7 |

**Service levels** (per-source freshness targets, publication windows, time to restore, and the steward review turnaround for contributions) are to be agreed with the platform team (responsible for ingestion into Snowflake) and consumers, and recorded in run-control configuration (Section 11.3). This document defines the mechanisms that measure them, not the targets.

### 1.3 Scope

In scope: the ingestion contract, Silver project structure and stewardship, Source Entity and Core modules, frozen inputs, test placement, publication, freshness, run control, and the steward's duties towards consumers.

Publication and run control are designed here because Silver's constraints drive them, but Tier 1 and Tier 2 projects use the same mechanism (Overview Section 9).

Open items for the detailed design are listed in Section 15.

---

## 2. Snowflake Features This Design Relies On

The architecture uses embedded dbt deliberately, and replaces missing dbt-Labs capabilities with Snowflake features (Overview Section 9). The Silver Layer depends on these:

| Snowflake feature | Used for | Section |
|---|---|---|
| dbt project objects, `EXECUTE DBT PROJECT`, live version | Running the Silver project inside Snowflake; persisting dbt state between runs | Appendix A |
| `SYSTEM$LOCATE_DBT_ARTIFACTS`, and the `IMPORTS` clause of `EXECUTE DBT PROJECT` | Importing the canonical production state into CI | A.2 |
| `ALTER DBT PROJECT ... ADD VERSION` | In-place deployment that keeps grants and canonical state | A.2 |
| Zero-copy schema clone | Building each unit in a copy of its last good state | 9.4 |
| Zero-copy table clone with time travel (`CLONE ... AT`) | Frozen inputs: raw tables cloned at the unit watermark | 6 |
| `ALTER SCHEMA ... SWAP WITH` | Atomic publication of a unit | 9.4 |
| Multi-statement transactions | The unit lease (Section 11.4); DML publication option (Section 9.8) | 11.4, 9.8 |
| Snowflake Tasks | One gated, scheduled task per unit | 11.7 |
| Caller's rights stored procedures | The generic publish procedure | 9.4 |
| Database roles and per-object grants | Consumer access to published objects only | 9.7 |
| `persist_docs` (object and column comments) | Documentation inside Snowflake | 5.3 |
| `ACCESS_HISTORY`, `OBJECT_DEPENDENCIES`, Snowflake lineage | Impact analysis across project boundaries | 12.2 |
| Telemetry event table | dbt execution logs and traces | 11.1 |
| Object tags with propagation, tag-based masking (including value-aware policies), row access policies | PII protection on raw, frozen inputs and derived published columns | 5.3, 6.4 |
| Resource monitors, object and query tags | Cost control and attribution | 15 |

See [Appendix A: Snowflake Constraints and Prerequisites](#appendix-a-snowflake-constraints-and-prerequisites) for more details about technical behaviour of dbt Projects on Snowflake.

---


## 3. The Ingestion Contract

The platform team is resourced to land enterprise source data in Snowflake, not to work in dbt. This is an enterprise structural reality rather than a design choice.

### 3.1 The contract

The boundary is treated as a **contract rather than a handoff**. Every raw object has a declared freshness expectation, monitored by analytics, and failures are attributable to ingestion before any dependent build is attempted. The contract is to be agreed with the platform team, since the freeze, freshness and watermark mechanisms all rest on it.

The platform team provides:

> - A `_loaded_at` (or equivalent) column on every landed object.
> - For change-data-capture feeds, a **source ordering column** (sequence number, log sequence number, or source commit timestamp plus operation order). `_loaded_at` alone cannot order changes landed in the same batch, and CDC deduplication and history are impossible without a reliable ordering.
> - A **landing-complete record** written to a platform-owned control table per source system and batch, including batches with zero new rows, and **written only after every table of the batch has committed**. This record is the authoritative "data as of" signal for each source. (The meaning is clearly "data seen in Snowflake as of".) It drives freshness (Section 10.3), the freeze timestamp (Section 6) and the raw watermark (Section 11.5). `max(_loaded_at)` is not a substitute, because a quiet source with no new rows would appear stale.
> - Batch loads of one source must never overlap.
> - **Raw objects that support `CLONE ... AT`**: standard Snowflake tables. External tables cannot be cloned. Snowflake managed Iceberg tables support clones, but will require testing and confirmation before use as raw.
> - **Time travel retention** on raw tables long enough to clone at the watermark (minutes in normal operation; to be agreed).

Analytics is granted read access to the platform teams' landing-complete table. The platform team needs no access to analytics objects.

### 3.2 Batch cadence

Ingestion is batch-oriented: sources, including change-data-capture feeds, land in batches on a daily or hourly cadence. A CDC batch carries many change records for a key, ordered by the source ordering column; the landing-complete record closes the batch.

Continuous ingestion is out of scope for now. If it arrives, the contract extends with a periodic **heartbeat** landing-complete record carrying the connector's committed low-watermark, so the freeze, freshness and watermark mechanisms continue unchanged.

### 3.3 Attribution

Every Silver failure is classed as `source` (ingestion owns it) or `transform` (stewards own it), from where the failing test sits (Section 7). Run control records the class (Section 11.2).

---

## 4. Structure and Stewardship

### 4.1 One project, one repository

The Silver Layer is a single dbt project in a single repository ("monorepo"):

- One folder (module) per source system, plus the Core module.
- Shared macros and generic tests.
- One live-version dbt project object in Snowflake, executed per unit (Section 9.3).

A single project gives compile-time `ref()` between Core and the Source Entity modules, one CI pipeline, one set of conventions, and full in-project lineage. It reinforces the Silver Layer as the catalogue of analysis-ready sources.

### 4.2 Database and schema layout

| Object | Example | Visible to consumers |
|---|---|---|
| Silver database | `silver` | Analytics roles only |
| Published schema per source module | `silver.erp`, `silver.crm` | Yes (published objects only, Section 9.6) |
| Build schema per source module | `silver.erp__build` | No |
| Published and build schemas for Core | `silver.core`, `silver.core__build` | `silver.core`: published only |
| Run control | `analytics_ops.control` | Read access for stewards and project teams |

Frozen inputs live **inside** each published schema, alongside the objects that read them, but are never granted to consumers (Section 6.3).

### 4.3 Stewardship

The no-business-logic rule for Tier 0 is the load-bearing rule of this architecture. It erodes one reasonable-looking pull request at a time unless it is actively defended.

- A named **steward team (of data engineers)** owns the repository, CI, releases, run control and on-call (business hours only) for the Silver Layer.
- `CODEOWNERS` assigns each source module, and the Core module, to stewards. Contributions from any analytics team are welcome. Merges require steward approval, within the agreed review service level.
- Stewards review against the structural join rule (Section 5.5), the scope-creep test (Section 5.5.2), test placement (Section 7) and classification (Section 7.3).
- Generated base models (Section 5.2) and the rule of two for entity models (Section 5.8) keep the volume of contributions, and therefore the review load, low.

### 4.4 Splitting later

If the repository becomes too large, splitting by source system is straightforward and yields essentially independent repositories, because Source Entity modules have no cross-source joins. Different load cadences do **not** require a split: each module is already an independent publication unit with its own schedule. Splitting is not expected, and this document treats the Silver Layer as one project.

### 4.5 Phased scope

| Release | Published content | Rationale |
|---|---|---|
| 1 | Generated base models, CDC current state, history (CDC history or snapshots), Core conformed dimensions | Proves publication, freshness and run control on the simplest models; unblocks Tier 1 teams quickly |
| 2 | Entity models admitted by the rule of two; Core crosswalks | Entity models bring the joined-model trap (Section 5.6) and fan-out testing; introduced once the machinery is proven |

---

## 5. Source Entity Modules (Tier 0)

### 5.1 Definition

A Source Entity module reconstructs one source system's **logical data models** as typed, deduplicated base models and, where admitted, denormalised, contract-enforced, tested **entity models** (logical entities in the domain-driven-design sense, without business logic). It is the single source of truth for operational data entering the analytics warehouse, and **it contains no business logic**.

Each module is a source-named folder set in the Silver project. 

> **Included: Modules use sources, base models, history models, entity models, snapshots, seeds, macros and tests.** 
>
> **Excluded: Intermediate and mart models: they live in Tiers 1 and 2.**

### 5.2 What a module contains, and how it is materialised

| Type | dbt Folder | Purpose | Materialisation |
|------|-------|---------|-----------------|
| Frozen inputs (`raw__*`) | n/a (created by the freeze step) | Zero-copy clones of the source's raw tables, all at the unit watermark. Declared as a dbt source; never granted to consumers | Transient clone (Section 6) |
| Base models | `staging/<source>/` | 1:1 with source tables: cast, rename, reshape columns, filter soft deletes. **Generated for every table** when a source is onboarded, then refined by stewards | View over the frozen input |
| CDC current state | `staging/<source>/` | Deduplicate change-data-capture rows to one current row per key, using the source ordering column | Incremental table |
| CDC history | `history/<source>/` | One row per key per version, with `valid_from` and `valid_to` from the source's own change ordering (G8) | Incremental table (permanent) |
| Entity models (`ent_*`) | `entities/<source>/` | Denormalise normalised source tables into logical entities; union of tables split in the source | Incremental table (or table) |
| Snapshots | `snapshots/<source>/` | SCD2 history for sources lacking change capture (G8) | Snapshot (permanent table) |
| Helpers | `staging/<source>/` | Internal logic shared by the above | Ephemeral |

Base models are published for every table of an onboarded source, because they are cheap views, mechanical to generate, and remove most reasons for a Tier 1 team to wait on a contribution. Folder names follow dbt's structure guidance: `staging/` holds only single-table preparation, and joins live in `entities/`.

### 5.3 Required declarations

- **Two source declarations per raw source.** One for the live raw tables, used only by the freeze step and observability (Section 10.3). One for the frozen inputs, used by every model and source test, resolving to the build schema of the unit being published. **Project macros are not available when a source's `schema:` property is rendered**, so the redirection to the build schema lives in the `source()` override, not in the source YAML.
- **Contracts and versions:** `contract: enforced: true` on every published model, and every published model versioned from v1. Contracted incrementals use `on_schema_change: append_new_columns` or `fail`.
- **Documentation.** `persist_docs` enabled for relations and columns. Every published column is described and typed, and descriptions state which tests the column has passed, so consumers can make consistent assumptions.
- **Classification.** Masking and row access policies on raw carry over to frozen-input clones. Classification tags are created with **`PROPAGATE = ON_DEPENDENCY_AND_DATA_MOVEMENT`** and propagate to derived columns automatically, so no post-hook is needed and a masking policy attached to the tag applies to every derived column. The following are noteworthy behaviours related to tag-based classification and propagation in Snowflake:
  - Propagation covers `CREATE TABLE AS SELECT`, `MERGE`, `INSERT ... SELECT`, DDL-created targets, joins and **views** (the weaker `ON_DATA_MOVEMENT` stops at views, and base views are in every chain). Tags are present immediately.
  - Propagation follows lineage, not meaning: `lower`, `sha2`, `left`, `coalesce`, concatenation, `length` and aggregates all carry the tag; two values of one tag combine to a tag value of `CONFLICT`, policies still enforced.
  - **Each classification tag needs a masking policy for every data type it can reach.** Functions that return metadata about the data rather than the data itself—like `length`, `count`, or structural aggregates do not propagate the tag.
  - **Declassification is an explicit tag value, not an `UNSET`.** `UNSET TAG` is not durable (the next `MERGE` re-applies the propagated tag, and consumers see clear data in between). 
  - Creating a propagating tag requires `APPLY TAG ON ACCOUNT`, so tag creation is a governance capability, not a unit or steward one.
  - The Silver service role reads raw unmasked, so a `CREATE TABLE AS` under it stores clear values; **CI verification of the tag on derived columns remains the safety net**.

- **Table type:** dbt-snowflake creates tables as transient by default. Rebuildable Silver tables (CDC current state, entity models) stay transient, avoiding Fail-safe storage on every publication. History models and snapshots, which cannot be rebuilt beyond raw's retention, are permanent (`transient: false`) and carry **`full_refresh: false`**, so a stray `--full-refresh` cannot destroy history (S4).
- **CDC deduplication and history** use the source ordering column (Section 3.1), not `_loaded_at` alone.
- **Grants:** dbt `grants` config on published models only (Section 9.6).

### 5.4 What a module does NOT contain

- Any join or filter driven by a **business decision**.
- Any **cross-source-system** join. Cross-source joins for identity and conformance belong in the Core module; all others belong in Tier 1.
- Any derived metric or KPI resting on a business rule.
- Any model whose grain changes based on a business filter.

### 5.5 The structural join rule

Joins are permitted in an entity model **if and only if** they are:

- Driven by the source's own **foreign key relationship**.
- Grain-preserving for the entity root, or a strict many:1 enrichment.
- Present **regardless of what any analyst wants to do** with the data. This is usually (but not always) evidenced by the foreign key being useful for only one particular join, for example order items joined to an order.

**Allowed:** `order_items LEFT JOIN orders ON order_items.order_id = orders.order_id` (FK-driven enrichment).

**Not allowed:** `order_items JOIN orders WHERE orders.status = 'completed'` (business filter).

#### 5.5.1 Carve-out: source-semantic filters

Three filters are *structural* rather than business-driven and are explicitly permitted:

1. **Soft deletes.** Excluding rows the source system itself considers deleted (`is_deleted`, `deleted_at is not null`). A soft-deleted row is not data the source believes in.
2. **Deduplication of change-data-capture rows.** Selecting the current version per key, using the source ordering column.
3. **Tenant or instance scoping**, where the source is multi-tenant and only one tenant is in scope. For example, a source audit table includes records of various kinds, of which only one kind is relevant for analytics.

Non-structural filters belong in Tier 1.

#### 5.5.2 The scope-creep test

The boundary between structural and business logic erodes at child aggregations. `order_item_count` is structural; `net_revenue` is not. The distinguishing test:

> **If two competent analysts could compute it differently, it belongs in Tier 1.**

`order_item_count` has one reasonable definition. `net_revenue` requires decisions about discounts, returns, tax and cancellations. Hence it is business logic, however obvious it looks in one source system.

#### 5.5.3 Fan-out must be tested, not asserted

"Strict many:1" is a claim, and an unnoticed 1:N join silently inflates every metric downstream of it. Fan-out is a defect in Silver code, so these tests sit on outputs (Section 7). Every published entity model carries, at `error` severity:

- `unique` or `dbt_utils.unique_combination_of_columns` on its declared grain.
- `dbt_utils.relationships_where` on each FK used for enrichment.
- A row-count guard asserting the entity's row count equals its root table's row count (for grain-preserving entity models).

### 5.6 Incremental entity models: the joined-model trap

A denormalised entity model materialised incrementally on its root key **will silently miss updates to joined child tables** if the incremental filter only looks at the root. For example, on the `orders` entity with `unique_key: order_id` and `merge`, filtered on the order's own timestamp, a change to an order *item* is never noticed.

The required pattern is to collect changed keys from **every** contributing frozen input, then rebuild those roots in full. The pattern is specified in the detailed design.

Snowflake dynamic tables handle incremental refresh across joins natively and would remove this trap. Under Snowflake-managed refresh, however, they change data outside any gate, so this design does not use dynamic tables for any published objects.

### 5.7 History (G8)

Consumers in a financial institution need as-at reporting and restatement, so history is a published guarantee, not an afterthought.

- **CDC sources: history from the change log.** A CDC history model publishes one row per key per version, with `valid_from` and `valid_to` taken from the source ordering column (source commit time and operation order). This records system time as the source saw it, which no snapshot can recover.
- **Sources without CDC: snapshots.** A snapshot records change as observed between batches. Because builds read frozen inputs, the snapshot's validity timestamps must be the **unit watermark**, not the build's current time; the snapshot time is overridden accordingly, so a retried build stamps the same time. The stamp comes from the unit watermark, which the freeze records in `<unit>__build._freeze` and a `snowflake__snapshot_get_time` override reads. The snapshot is identical after a discarded and retried build, including when the failed build had already run the snapshot.
- **The CDC history model is incremental** (merge on key and source sequence) and **recomputes the validity of every key that received a change**. It is expected that inserts, updates, deletes (as tombstones), a redelivered change (stored once) and a late change (slots in by ordering value, re-closes its neighbour, does not become the current state) are all correct. Generic tests `history_contiguous`, `one_open_version` and `current_matches_history` run at `error` severity and catch a tampered history.
- **Capture at base grain.** History and snapshots capture base models at their natural grain. Any change to any joined-in column of a denormalised entity would spawn a new version, so history becomes high-churn and expensive. Denormalisation happens downstream of history, not upstream of it.

### 5.8 Entity model design decisions

Entity models are admitted by a **rule of two**: an entity model is built when two consumers need it, or when the stewards judge the source too normalised to consume through base models alone. Until then, the join belongs in the Tier 1 project that needs it.

The rule decides **whether** an entity model exists. The structural join rule (Section 5.5) decides **what it may contain**: only joins that would be made regardless of analysis. Consumer demand never justifies a join the structural rule forbids.

Per admitted entity, the stewards decide:

- Whether to emit the **parent with child aggregations** (`orders` with item count, total quantity).
- Whether to emit the **child with parent attributes** (`order_items` enriched with order header fields).
- Whether to emit **both**, which is common for highly normalised sources.

---

## 6. Frozen Inputs

### 6.1 Principle

**Nothing in the Silver Layer reads live raw data.** Each build of a Source Entity unit first clones every raw table of the source, **all at the same timestamp** (the unit watermark from the latest landing-complete record), into the unit's build schema. Every model and every source test reads these frozen inputs.

### 6.2 The freeze step

The freeze is a dbt `run-operation` macro, invoked by the publish procedure (Section 9.4). It reads the live-raw source declaration, so the list of tables lives in dbt alongside the models, and development and CI run the same logic. For each raw table it performs the equivalent of:

```sql
CREATE OR REPLACE TRANSIENT TABLE silver.erp__build.raw__orders
  CLONE raw.erp.orders AT (TIMESTAMP => :watermark);
```

The Core module has no frozen inputs: it reads published Source Entity outputs and seeds, never raw.

### 6.3 Consequences

- **Raw data is not duplicated into Silver.**
- **Gateable views without copying data.** A view over a frozen input cannot show data newer than the input, so 1:1 base models are views as recommended by dbt-Labs. 
- **A consistent snapshot across tables (G2).** All tables of a source are read at one point in time, so a batch landing mid-build can never split a join.
- **One point in time.** The frozen inputs, the unit watermark and the freshness check all refer to the same landing-complete record provided by the platform team.
- **Frozen inputs publish with the unit.** They live in the published schema and swap with the views that read them. They are never granted to consumers (Section 9.6).
- **Incremental models get stable inputs.** Their change detection reads the same frozen inputs as everything else.

### 6.4 Costs and risks

- **Query-time shaping.** Base views re-execute their casts and renames on every consumer query. This is cheap for light 1:1 shaping; heavier work stays in incremental tables.
- **Retained storage.** A clone shares micro-partitions with raw at creation. A replaced clone's unique micro-partitions (those raw has since rewritten) are retained for time travel. Transient clones avoid Fail-safe. Frozen-input clones in the Silver Layer hold **no storage of their own** within raw's time travel period; the cost sits on raw as time travel bytes of the rewritten partitions. A transient clone takes the retention period of the schema it is created in.
- **Clone time.** Cloning is metadata-only but not instantaneous for tables with many micro-partitions. Production-volume clone time is untested and should be measured early on real tables.
- **Governance policies.** Masking policies and tag associations carry over to table clones, so frozen inputs inherit raw's protection. Frozen inputs are transient, hold exactly the rows as of the watermark and have no grants beyond ownership; `CLONE ... AT (TIMESTAMP => ...)` reads a `TIMESTAMP_NTZ` as UTC regardless of session timezone.
- **Replication.** If Silver is replicated for disaster recovery, frozen-input clones replicate logically only when raw is in the same replication or failover group; otherwise they replicate as physical copies (Section 9.8).
- **View name binding.** Base views reference frozen inputs in their own schema.

---

## 7. Test Placement

**Each assertion is tested where it can first fail, and only there.**

- **Source defects** are problems in what landed. Silver code cannot cause or repair them. They are tested once, on the frozen inputs.
- **Transformation defects** can only be introduced by Silver code. They are tested on the outputs.

A property that passes unchanged through a 1:1 base view (a key not null, an accepted value) is not re-tested on the view.

| Assertion | Tested on | Owner of a failure |
|---|---|---|
| Freshness | Landing-complete records (gate, Section 10.3) | Ingestion |
| Not null, accepted values, castability of raw columns | Frozen input | Ingestion |
| Uniqueness at the source's own grain (full-load tables) | Frozen input | Ingestion |
| Referential integrity within the source | Frozen input | Ingestion |
| Source ordering column present and unique per key change | Frozen input | Ingestion |
| One current row per key after CDC deduplication | Output | Stewards |
| History validity (no gaps or overlaps per key, one open version) | Output | Stewards |
| Entity grain, row-count guards, enrichment FKs (Section 5.5.3) | Output | Stewards |
| Snapshot validity (one current record per key, no overlapping validity ranges) | Output | Stewards |
| Contracts (names, types) | Output | Stewards |
| Classification tags carried (Section 5.3) | CI | Stewards |
| Crosswalk match-rate floors (Core) | Output | Stewards |

### Notes:

- **Castability must be a source test.** Creating a view does not evaluate it, so a value that fails a cast in a base view would not fail the build; it would fail the first consumer query after publication. A source test (rows where `try_cast(col)` is null but `col` is not) catches it before publication.
- **Gating is native.** `dbt build` runs source tests before dependent models, and skips those models when an `error` test fails. A source defect stops the unit before any transformation runs.
- **Severity carries meaning.** `error` blocks publication; `warn` publishes and is logged. Only tests that evidence wrong data are `error`.
- **Incremental testing of large sources.** Source tests over a full frozen input rescan history that has already passed. For large sources, tests are restricted to the new batch with the test `where` config (for example, `_loaded_at` later than the previous watermark). Uniqueness needs care: a new row can collide with an old one, so it uses a custom test comparing new keys against the whole table.
- **Contributed tests.** Tests contributed by Tier 1 teams follow the same placement; most are source assertions on frozen inputs.

---

## 8. The Core Module (Tier 0.5)

### 8.1 Why it exists

Before any domain business logic, there are shared concerns requiring special handling:

1. **Conformed dimensions and spines:** cross-domain `dim_date` and fiscal calendar, currency and geography lookups held in seed datasets.
2. **Cross-source identity resolution:** a customer in the ERP and the same customer in the CRM. Source Entity modules forbid cross-source joins, and Tier 1 is domain-scoped, so neither can own this.

Identity resolution is honestly **business logic**: matching rules are judgements, and two competent analysts could write them differently. It fails the scope-creep test. The Core module exists because this logic must be shared and must have exactly one owner. It is the **one governed exception** where shared business logic lives below Tier 1, and it is governed accordingly.

The Core module is expected to remain small, with critical but niche datasets.

### 8.2 What it contains

| Layer | Purpose | Materialisation |
|-------|---------|-----------------|
| Conformed dimensions | `dim_core__date`, `dim_core__currency`, `dim_core__fiscal_period` | Table |
| Crosswalks | `xref_core__customer` mapping source-system keys to a durable `customer_key` | Table (history-backed) |
| Reference seeds | Curated lists, mappings, hierarchies under version control | Seed (Table) |

Tier 1 projects may read all published Core outputs. Tier 2 projects may read the **conformed dimensions** directly (Overview Section 3), so Tier 1 projects need not re-publish them.

### 8.3 Governance

The Core module is the **only** Silver module permitted to join across source systems, and that privilege is narrowly scoped:

- It may join across sources **for identity resolution or conformance only**, not to answer a business question.
- Every crosswalk publishes its **match rate and unmatched-key counts as tested metrics**, with an `error`-severity floor. An entity-resolution rule that quietly stops matching is worse than one that fails.
- **Durable keys are never reassigned (G7).** Once a `customer_key` is issued, it continues to identify the same real-world entity when matching rules change. Merges and splits are recorded explicitly (for example, a superseded-key mapping), never by silently re-pointing a key. Key stability is harder than match rate and is specified in the detailed design.
- Matching rules are documented in prose next to the model, and any change to them is treated as breaking (Overview Section 8.4).
- It is contracted and versioned exactly as Source Entity outputs are.
- It is its own publication unit (`silver.core`).

### 8.4 Admission criteria

Because Tier 1 projects may not read each other, shared reference data will be proposed for Core. A dataset is admitted to Core only if all of these hold:

- It is needed by more than one domain, or by Tier 2 directly.
- It is conformance or identity, not a domain's business definition. A domain's own hierarchy (for example, finance's product hierarchy) stays in that domain and is published there.
- It has exactly one owner, who accepts Core's governance (breaking-change rules, match-rate floors where relevant).

The stewards review Core's contents periodically against these criteria, so "expected to remain small" stays true.

### 8.5 Why not simply relax Tier 0's rule?

The no-cross-source-join rule is the clearest, most enforceable statement in this architecture, and a carve-out inside Tier 0 could be used to justify a genuine business join. Isolating the exception in a separate module with its own review gate keeps Tier 0's rule absolute.

---

## 9. Publication (Write-Audit-Publish)

### 9.1 Two requirements in tension

With a plain `dbt build`, tests run after models are replaced. By the time a test fails, published tables have already changed. **Tests observe the incident rather than prevent it.**

Gating publication on tests fixes this. But a single gate over the whole Silver Layer means one failing source blocks every analytics team. Both are required:

> **(a)** Nothing ever builds on data that failed its tests.  
> **(b)** A failure blocks only what depends on it.  

### 9.2 Principle: failures block by staleness, not by status

A failed build never replaces published data. The last good version keeps serving, and the failure shows up downstream as **staleness**: the unit's watermark stops advancing.

Downstream units are never gated on an upstream *failure*. They are gated on upstream *freshness* against their own declared tolerance (Section 10). This satisfies both requirements:

- Failed data is never published, so nothing can read it (a).
- Unrelated units are unaffected (b).
- Dependent units either proceed on last-good data within tolerance, or skip.

### 9.3 Publication units

A **publication unit** is the smallest set of objects published atomically. Each unit has exactly one schema holding everything the unit builds, and one build schema.

| Tier | Unit | Example schemas |
|---|---|---|
| Tier 0 | One per source system | `silver.erp`, `silver.erp__build` |
| Tier 0.5 | One unit | `silver.core`, `silver.core__build` |
| Tier 1 / 2 | One per project by default; a project may define more | `finance.mart`, `finance.mart__build` |

Within a unit's schema, published objects are granted to consumers object by object; frozen inputs (Silver) and internal models (Tier 1 and 2) are never granted (Section 9.6). One schema per unit means one atomic swap publishes everything together.

Source systems work as units because Source Entity modules forbid cross-source joins: each is an independent subgraph. Within a unit, publication is all-or-nothing. Across units, publication is independent, and each unit keeps its own schedule.

All Silver units share one live-version dbt project object and run concurrently, each with its own `--target-path` and `--log-path` (Appendix A.1). Each unit is selected by its own selector in `selectors.yml` (for example `unit_erp`), which includes the unit's models, snapshots, frozen-input sources and source tests. **A unit selector must be closed under the unit: select by folder path only, never with `children: true`**.

### 9.4 Publish procedure

One generic, caller's rights procedure, parameterised by unit and driven by run-control configuration (Section 11), performs the steps in the pseudocode below. It runs as the unit's own service role (Appendix A.1). `EXECUTE DBT PROJECT` **raises** on a dbt failure (Appendix A.1), so the procedure has one exception handler for every step, which logs `FAILED` with the stage, the failure class and the exception text, discards the build, releases the lease and returns `FAILED:<stage>`. The `SUCCESS` column of each result is also checked. The code below represents the happy path.

```sql
-- 0. Acquire the unit lease (Section 11.4). If it is held, log SKIPPED (locked) and stop.

-- 1. Gate (Section 10 and also 11.7): upstream watermarks within tolerance, and something new to build
--    (an upstream PUBLISHED or a DEPLOYED event since this unit last published).
--    If the gate fails, log SKIPPED (stale_upstream | nothing_new), release the lease and stop.
--
--    The unit watermark is the minimum of its upstream watermarks, read at this gate.
--    For Tier 0 units, this minimum is the minimum over a single raw upstream which is provided 
--    by the landing-complete record defined per source system and batch.
--    For Tier 0.5, this is the minimum of two or more published watermarks. As we see below, 
--    the watermark labels the build, but does not bound what it reads.

-- 2. Log STARTED for this build. Clone the last good (output) state: zero-copy, preserves incremental, 
--    history and snapshot state, and WITH MANAGED ACCESS, because a plain clone loses it.
--    This clone is used as the basis for the new build.
CREATE OR REPLACE SCHEMA silver.<unit>__build CLONE silver.<unit> WITH MANAGED ACCESS;

-- 3.  Prepare inputs, so the build reads one fixed version of each upstream source.
--     Note that the published last good state (cloned in step 2) for Tier 0 also includes the input 
--     clones (frozen inputs) used to build the last good state. These need to be replaced in this step.
--
--     There are two mutually exclusive possibilities, so exactly one of 3a or 3b applies to the unit:
--
-- 3a. Tier 0 (Source Entities): Raw upstream. 
--     Overwrite each frozen input table in the clone created in step 2 with a clone of raw AT the new 
--     watermark, and also record the watermark in <unit>__build._freeze (Section 6.2). 
--     For <unit> being erp by way of example:
EXECUTE DBT PROJECT silver.ops.silver_layer
  ARGS = 'run-operation freeze_sources --args "{unit: erp, watermark: ...}"
          --vars "{publish_unit: erp}" --target prod_erp
          --target-path target/erp --log-path logs/erp';
--
--     The macro freeze_sources essentially does this for each source table (use source table orders
--     from <unit> erp as example:
--     CREATE OR REPLACE TRANSIENT TABLE <db>.erp__build.raw__orders
--       CLONE raw.erp.orders AT (timestamp => '<watermark>');
--
-- 3b. (Units that read (only) other Silver units):
--     In this case the frozen inputs are NOT included in the published schema, but rather are contained 
--     within a schema 'next to' the build schema as scaffolding. This schema is read during the build, 
--     never published, and dropped after the swap. This also implies that the build can only include 
--     views if they are against tables also part of the build and not upstream tables. In practice this 
--     is not a significant restriction.
--     Zero-copy clones of each upstream published schema, taken now, again for the example 
--     of upstream source being erp:
CREATE OR REPLACE SCHEMA silver.core__in_erp CLONE silver.erp;

-- 4. Test sources, build, test outputs: one dbt build
EXECUTE DBT PROJECT silver.ops.silver_layer
  ARGS = 'build --selector unit_erp --vars "{publish_unit: erp}" --target prod_erp
          --target-path target/erp --log-path logs/erp';

-- 5. Atomic publish. Schema-level USAGE moves with the schema object, not the name,
--    so grant it on the build schema just before the swap and revoke it just after
GRANT USAGE ON SCHEMA silver.erp__build TO DATABASE ROLE silver.reader;
ALTER SCHEMA silver.erp__build SWAP WITH silver.erp;
REVOKE USAGE ON SCHEMA silver.erp__build FROM DATABASE ROLE silver.reader;
-- For units that read from Silver, drop the input schema clones.

-- 6. Canonical state for CI (Appendix A.2): compile with no publish_unit, no writeback
EXECUTE DBT PROJECT silver.ops.silver_layer ARGS = 'compile --target prod_erp' WRITEBACK = FALSE;
-- read its query id from RESULT_SCAN of the statement itself (not of a later one)

-- 7. Log PUBLISHED with watermark, state query id and the input clones; release the lease
```

The order is **freeze, test sources, build, test outputs, publish**. Tests always run against the exact frozen copy that will be published. Testing live raw and cloning afterwards would publish anything that landed between the two steps untested.

If any step before the swap fails, nothing published has changed: the previous frozen inputs, views and tables keep serving together, out of date but correct, and there is no rollback because there is nothing to roll back. The build schema is discarded and re-cloned at the next run, and the next batch recovers the unit. A failure in step 6 does not affect consumers; it is logged and alerted, and CI keeps using the previous canonical state.

**Failure attribution.** The raised exception text names the failing node, so the procedure sets `failure_class` from it: a failing source test, or a freeze that names a raw object or time travel on one, is `source` (ingestion's); other tests and models are `transform` (the stewards'); anything else is `unclassified`. `SYSTEM$GET_DBT_LOG(<query id>)` returns the tail of the debug log, but the exception text, not the log, is what attributes the failure.

**Roles.** The procedure runs as a per-unit service role that owns both of the unit's schemas, since `SWAP WITH` requires OWNERSHIP of both. The role reads raw and its upstream units, may execute the shared project object, and writes to run control only through an owner's-rights `log_event` procedure (unit roles have no `INSERT` on the log).

**Schema redirection.** A custom `generate_schema_name`, through a `unit_schema(unit)` macro, resolves each unit's schema by target. In production targets, the unit being published (`var('publish_unit')`) goes to `<unit>__build`; an upstream unit that it reads goes to its input clone `<publish_unit>__in_<upstream>` (variable `input_clones`); everything else resolves to the published schema. This is how the Core module reads last-good Source Entity outputs through `ref()` while keeping in-project lineage. In non-production targets every unit lives in `<target.schema>_<unit>` (Section 9.9). Sketch of the `unit_schema` macro:

```jinja
{% macro unit_schema(unit) -%}
  {%- if target.name.startswith('prod') -%}
    {%- if unit == var('publish_unit', none) -%}{{ unit }}__build
    {%- elif unit in var('input_clones', {}).get(var('publish_unit', none), []) -%}
      {{ var('publish_unit') }}__in_{{ unit }}
    {%- else -%}{{ unit }}{%- endif -%}
  {%- else -%}{{ target.schema }}_{{ unit }}{%- endif -%}
{%- endmacro %}
```

### 9.5 Views and schema-relative references

dbt renders fully qualified names, so a view in `silver.erp__build` referencing a sibling object would still point at `silver.erp__build` after the swap, reading the stale objects now living under that name.

Snowflake documents that rendering **same-schema references from views as unqualified identifiers** makes views schema swap safe, implying the binding happens at query time rather than at creation. References to other schemas stay fully qualified; they follow the swap by name. Tables, incrementals and tests stay fully qualified, because their SQL resolves against the session. Sketch of the `ref` override:

```jinja
{% macro ref() %}
  {%- set rel = builtins.ref(*varargs, **kwargs) -%}
  {%- if model.config.materialized == 'view'
        and rel.database == this.database
        and rel.schema == this.schema -%}
    {%- do return(rel.include(database=false, schema=false)) -%}
  {%- endif -%}
  {%- do return(rel) -%}
{% endmacro %}
```

The same treatment applies to `source()` references from base views to frozen inputs. Base views depend on these Snowflake features:
> - unqualified same-schema identifiers in views bind at query time in the view's own schema, 
> - such views survive survive `SWAP WITH`, and 
> - such views rebind under a schema `CLONE`. 

#### IMPORTANT NOTE: Semantic views are different.

 Semantic views in Snowflake are not Views. They are a container for metadata. An unqualified table name in `CREATE SEMANTIC VIEW` is resolved at creation against the session's current schema and stored fully qualified, so the unqualified identifier rule for views (resolved at query time) cannot apply. A semantic view in the build schema that references the **published** fully qualified table name publishes atomically with the swap and reads the new tables after the swap. **Because columns are validated at creation of a semantic view**, a semantic view cannot use a column that is new in the same build: new-column changes to semantic views lag one publication, or are applied after the swap. \[`CREATE SCHEMA` also switches the session's current schema, which matters to any script that creates objects after it.\]

### 9.6 Notable Features of the Publish Procedure

1. **Nothing published reads live raw data.** Silver models read frozen inputs. A view over live raw changes the moment data lands, so it cannot be gated. 
2. **Views in published schemas** reference other units' objects fully qualified, and same-unit objects only through bare identifiers. Snowflake semantic views are the exception: they reference published names (Section 9.5).
3. **Consistent reads across publication units.** Frozen inputs make each Tier 0 (Source Entity) unit read raw at a single point in time. Units that read other units (the Core module, Tier 1 and Tier 2 projects) generate zero-copy clones of each upstream published schema it reads into a private input schema (`<unit>__in_<upstream>`). The build reads those clones, and they are dropped after the swap (and on failure). The mapping from unit to the upstreams it clones is the `input_clones` variable, derived from `unit_dependencies`.

4. **Tests are placed as per Section 7, and severity carries meaning.**
5. **Retries are idempotent.** Every run starts from a clone of the last good state, so a failed run can simply be re-run under the lease. This holds for incrementals, history and snapshots alike.
6. **Consumers are granted object by object, never on build schemas.** Object grants come from dbt's `grants` config on published models only, never from future grants on tables. This keeps frozen inputs and internal models invisible to consumers. Grants on child objects travel with a schema clone and a swap. Schema-level `USAGE` **moves with the schema object, not the name**: a plain swap locks consumers out of the published name and exposes the previous version under the build name. The procedure therefore grants `USAGE` on the build schema just before the swap and revokes it from the new build schema just after (Section 9.4), so there is no gap and no consumer access to a build schema. A plain clone does not inherit managed access, and `ENABLE MANAGED ACCESS` needs the account-level `MANAGE GRANTS`, so the build schema is created with `CLONE ... WITH MANAGED ACCESS`, which keeps the published schema managed-access after every swap.
7. **Each unit has its own artifact paths** on the shared project object.
8. **Every `EXECUTE DBT PROJECT` failure is handled** (it raises, Appendix A.1) and every `SUCCESS` value is checked before the next step (Section 9.4).
9. **A schema is deployed with `ADD VERSION`**, never `CREATE OR REPLACE`, once consumers hold grants (Appendix A.2).

### 9.7 Failure behaviour

| Failure | Published result | Downstream |
|---|---|---|
| Raw ERP batch does not land | No new landing-complete record; raw ERP watermark stops advancing; ERP gate skips | Staleness visible in run control, owned by ingestion |
| ERP source test fails on frozen inputs | `silver.erp` stays at last good; no ERP models built | FAILED with `failure_class: source`, owned by ingestion. |
| ERP output test fails | `silver.erp` stays at last good | `crm` unaffected. `core` and ERP-dependent Tier 1 projects proceed on last-good ERP if within tolerance, otherwise skip; their watermarks inherit ERP's older watermark |
| Core crosswalk match rate below floor | Previous crosswalk keeps serving | All consumers stay on the last accepted identity mapping |
| Finance build fails | Finance marts stay at last good | Tier 2 gated by finance freshness |
| Canonical state compile fails | Publication already complete | CI uses the previous canonical state; alert to stewards |
| A run dies mid-way | Nothing published changed | Lease expires; `STARTED` without a terminal event is flagged as stale (Section 11.6) |
| Raw table missing, or a raw column renamed | Nothing published changes. The freeze fails (class `source`) or the build fails (class `transform`) | Recovered by the next batch (observed, S3). A task's own status says nothing about this: tasks showed `SUCCEEDED` for publications that failed (because the publication process has completed), so monitoring reads run control |


### 9.8 Object identity: published objects are replaced on each publication

Clone-and-swap gives every published table and view a **new object identity on every publication**. This is accepted as the baseline (Option A), with stated consequences:

- **Time travel** on a published object reaches back only to the creation of its current clone. History is served by history models (G8), not time travel.
- **Streams and dynamic tables** on published objects are not supported: a stream loses its offset and a dynamic table must reinitialise when its base object is replaced. Consumer rules are in Overview Section 8.4.
- **Per-object history** (data metric function results, Snowsight lineage, `ACCESS_HISTORY` object ids) restarts with each publication. Impact and monitoring queries match by name.
- **Disaster-recovery replication.** Clones replicate logically only when the original and the clone are in the same replication or failover group; otherwise they replicate as physical copies, and recreated objects can briefly disappear from the secondary during refresh. If the analytics databases are replicated, raw and analytics must share a replication or failover group, or analytics is excluded from replication and rebuilt from raw after failover. Whether the analytics databases are replicated is an open item (Section 15).

**Option B: publish tables by transactional DML.** Views and frozen inputs keep swap-based publication; tables are built in the build schema, tested, and then written to stable published tables inside one multi-statement transaction (DML is transactional, whereas DDL auto-commits). Object identity is preserved, so time travel, streams and per-object history survive, at the cost of physical writes on each publication and more complex handling of incrementals. Sufficient motivation would be required to justify the additional complexity.

### 9.9 Development and CI targets

The same project runs in three kinds of target (`profiles.yml`): the production targets (`prod_<unit>`, one per unit, each under the unit's service role), `ci`, and `dev`.

- In `dev` and `ci` every unit lives in `<target.schema>_<unit>` (for example `silver.dev_erp`, `silver.ci_erp`), so a developer's or pull request's objects never touch the published schemas.
- The freeze macro creates the dev or CI schema in non-production targets.
- Views keep bare identifiers, and PII is masked for the developer's role.
- CI builds with `state:modified.body+ --defer` against the canonical state (Appendix A.2); without `--defer`, a CI build of a table whose parent view is not in the CI schema fails.
- The `ref` and `source` overrides compile as designed in all three targets, and source tests redirect to `<unit>__build` while a unit is published and to the published schema otherwise.

### 9.10 Run roles

Required roles:

> - A platform role that lands raw.
> - A steward role that owns run control and shared objects.
> - Database roles (`silver.reader`) for consumers, granted `USAGE` on a published schema only during the swap window of Section 9.4
> - A governance role that owns tags and masking policies

Secondary roles should be disabled. The detailed design would need to choose between two role designs relating to the units:

 - **Option A:** One service role per unit (`SVC_SILVER_<UNIT>`) that owns the unit's schemas, is named in the unit's dbt target, may execute the shared project object (`USAGE` on the DBT PROJECT) and reads its raw schema. A task is owned by the unit's service role, so tasks and roles are per unit.
 - **Option B:** A single shared Silver service role that owns every unit schema would permit one task graph (Section 11.7), at the price of every unit being able to touch every other unit's schemas.

---

## 10. Freshness and Watermarks

### 10.1 The silent-wrongness problem

Suppose a Tier 2 project is triggered after all upstream Tier 1 tasks have completed. Finance completed this morning; digital last succeeded three days ago. Tier 2 builds successfully, publishes a cross-domain fact mixing fresh and stale data, and **no test anywhere fails.** The dashboard is wrong with no warning.

Completion is not freshness. Every cross-unit dependency needs a freshness assertion, not just a completion signal.

### 10.2 Propagated watermarks

Every publication unit carries a **watermark**: the time its data is current as of.

- A raw source's watermark is the time of its latest landing-complete record (Section 3).
- A Source Entity unit's watermark is the freeze timestamp of its frozen inputs, the watermark of it's upstream raw source.
- Any other unit's watermark is the **minimum of its upstream watermarks**, read at gate time.

Staleness therefore propagates automatically. In the example above, the Tier 2 unit's watermark is digital's three-day-old watermark, not finance's fresh one.

Each unit declares a **tolerance per upstream dependency** (Section 11.3). Before building, the unit's gate checks every upstream watermark against its tolerance, and skips if any is exceeded. Skipping leaves the last good version published and the watermark unchanged, so the staleness remains visible. Dashboards display the watermark as **"data as of"** (meaning "data received in Snowflake as of").

### 10.3 Source freshness at the raw boundary

**The gate is the single freshness check.** For a Source Entity unit, the publish procedure's gate reads the raw unit's watermark from `v_landing_complete` (Section 11.5) and compares it with the tolerance in `unit_dependencies`. A breach skips the unit's build, with an alert whose owner is unambiguously ingestion. One check, in one place, avoids an extra `EXECUTE DBT PROJECT` per run and two definitions to keep in step.

`dbt source freshness` remains available for **observability** (populating `sources.json` and dbt's freshness views), pointing at the same records so the two cannot disagree. It requires a live-version project object (Appendix A.2) and is run on its own schedule, outside the publish path. It uses `loaded_at_query` (dbt v1.10 and later) rather than `loaded_at_field`, because `max(loaded_at_field)` makes a quiet source with no new rows look stale. Illustrative config sketch:

```yaml
sources:
  - name: raw_erp
    config:
      freshness:
        warn_after: {count: 6, period: hour}
        error_after: {count: 12, period: hour}
      loaded_at_query: |
        select max(completed_at)
        from {{ env_var('DBT_OPS_DB') }}.control.v_landing_complete
        where source_system = 'ERP'
```

Run control reads the same landing-complete records to derive raw watermarks (Section 11.5), so the gate, the freeze timestamp and the watermark cannot disagree.

---

## 11. Run Control

### 11.1 Design principle: record facts, not state

Run control coordinates publication, freshness and full-refresh events across all units, in all Tiers. Its main simplicity lever is that it **records immutable facts rather than maintaining mutable state**: rows are appended, never updated, and all current state is derived by views. **The one deliberate exception is the unit lease** (Section 11.4), because mutual exclusion needs an atomic conditional write, complicating the design. The run control requirements are:

- one append-only event log table
- two small configuration tables describing units and their dependencies, maintained under version control
- one lease table
- a view of platform landing records
- two status views for unit status and freshness
- the single publish procedure (Section 9.4)
- one scheduled task per unit
- a consumer manifest registry

Run control is, in effect, a small orchestrator. It is owned by the Silver stewards.

Run control operates at **unit granularity only**. Per-model results stay in dbt's artifacts (written back to the live-version project object) and Snowflake's telemetry event table, and are not duplicated.

Run control lives in its own database or schema (for example `analytics_ops.control`), owned by the steward role.

### 11.2 The event log

```sql
create table analytics_ops.control.publish_log (
  event_ts    timestamp_ntz default sysdate(),  -- UTC
  unit        varchar,        -- 'silver.erp', 'tier1.finance'
  event_type  varchar,        -- STARTED | PUBLISHED | FAILED | SKIPPED | DEPLOYED | FULL_REFRESH
  watermark   timestamp_ntz,  -- data as-of (UTC), set on PUBLISHED
  run_id      varchar,
  detail      variant         -- failure_class on FAILED; skip reason on SKIPPED;
                              -- state_query_id on PUBLISHED; commit on DEPLOYED
);
```

All timestamps in run control are UTC `timestamp_ntz`, so watermarks compare unambiguously across teams and sessions.

The log table is append-only: no updates, no `MERGE` contention, and a complete audit trail by construction. This can be enforced by allowing INSERT privileges, but withholding DELETE, UPDATE, and TRUNCATE privileges to prevent modifications or data removal. Writers are restricted to:

- the publish procedure (STARTED, PUBLISHED, FAILED, SKIPPED)
- each project's CI deployment step (DEPLOYED)
- a small steward procedure for FULL_REFRESH events

### 11.3 Configuration

Static configuration is maintained in a small ops repository and deployed as seeds or tables. Changes go through pull request review.

```sql
-- One row per publication unit, including raw units owned by the platform team
units (unit, tier, owner, dbt_project_object, selector, schedule, max_runtime_minutes)

-- One row per dependency edge; this is the cross-project dependency graph
unit_dependencies (unit, upstream_unit, max_age_hours)
```

The `units` table also names each unit's selector and dbt target. The dependency graph is declared, never inferred at runtime. Service-level targets (Section 1.2) are recorded here as tolerances.

### 11.4 The unit lease

Two runs of one unit must never overlap: both would replace the same build schema, and one could swap the other's half-built schema into production. Scheduled tasks do not overlap themselves, but a manual re-run or retry can.

```sql
create table analytics_ops.control.unit_lease (
  unit        varchar,        -- one row per unit
  run_id      varchar,        -- null when free
  expires_at  timestamp_ntz   -- UTC
);
```

The procedure acquires the lease with a single conditional `UPDATE` (set `run_id` and `expires_at` where the lease is free or expired) inside a transaction, and proceeds only if one row was updated. It releases the lease on every exit path. Expiry uses the unit's `max_runtime_minutes`, so a run that dies cannot block the unit indefinitely.

### 11.5 Raw units and the landing boundary

The platform team keeps writing its own landing-complete table (Section 3); analytics has read access only. A view, `v_landing_complete`, presents each landing-complete record as a PUBLISHED event for the corresponding raw unit (for example `raw.erp`). Raw units therefore appear in status and gating exactly like any other unit, with the platform team as owner.

The same view is the source of the gate's raw freshness check and the target of the `loaded_at_query` used by `dbt source freshness` (Section 10.3), so there is a single source of truth for raw freshness.

### 11.6 Status views

Illustrative sketch:

```sql
create or replace view analytics_ops.control.unit_status as
with events as (
  select unit, event_type, event_ts, watermark
  from analytics_ops.control.publish_log
  union all
  select 'raw.' || lower(source_system), 'PUBLISHED', completed_at, completed_at
  from analytics_ops.control.v_landing_complete
)
select
  unit,
  max(iff(event_type = 'PUBLISHED', event_ts, null))                 as published_at,
  max_by(watermark, iff(event_type = 'PUBLISHED', event_ts, null))   as watermark,
  max(iff(event_type = 'DEPLOYED', event_ts, null))                  as deployed_at,
  max_by(event_type, event_ts)                                       as last_event,
  max(event_ts)                                                      as last_event_at
from events
group by unit;
```

A second view, `unit_freshness`, joins `unit_status` to `unit_dependencies` and flags every edge whose upstream watermark exceeds its tolerance. It also flags any unit whose last event is `STARTED` and older than its `max_runtime_minutes`, which indicates a run that died without a terminal event. This is the operational dashboard for stewards and project teams, and the natural target for Snowflake alerts.

### 11.7 Gating and scheduling

As an initial approach, each unit runs as a **scheduled task with a gate**, not as part of an event-driven task graph. The gate proceeds only when both conditions hold:

1. Every upstream watermark is within its tolerance. For Source Entity units, this is the raw unit's landing-complete watermark (Section 10.3).
2. There is something new to build: at least one upstream has published, or the unit's project has been deployed (`DEPLOYED`), since this unit last published. Without the deployment condition, a code release would not publish until upstream data changed.

If either condition fails, the procedure logs SKIPPED and exits. On success, the new watermark is the minimum of the upstream watermarks read at gate time (for Source Entity units, the freeze timestamp).

This avoids a dispatcher entirely and suits the daily and hourly batch cadence. If latency later matters, it can be enhanced in either of two ways, without changing the log design:

- A stream on `publish_log` feeding one dispatcher triggered task replaces the schedules.
- A single task graph in `analytics_ops.control` calls every unit's procedure. Task graphs require all tasks to share an owner, database and schema, but the tasks can live in the ops schema while executing project objects anywhere. Graph tasks must always succeed and record outcomes in the log, so that a failed unit blocks by staleness, not by graph status. A task graph needs one owner, so it implies a single Silver service role (Section 9.10); with a task per unit role, each unit runs as its own task. Either way, a task's status is not the publication's status, so monitoring reads run control.

### 11.8 Freshness visible in dbt

`unit_status` is included as a source in the generated source package, together with a generic `upstream_fresh(unit, max_age)` test at `warn` severity. Tier 1 and Tier 2 developers then see upstream freshness in their own dbt output, without querying run control directly. The gate, not this test, enforces tolerance.

### 11.9 Consumer manifest registry

On every production deployment, each Tier 1 and Tier 2 project's CI writes the sources it declares (upstream relation, model version, columns where declared) into `analytics_ops.control.consumer_sources`, replacing that project's previous rows. This gives the impact query (Section 12.2) a deterministic, immediate list of every dbt consumer.

---

## 12. Serving Consumers

### 12.1 The generated source package

Silver CI generates the consumers' source definitions from the Silver manifest (published relations, column names, types, descriptions, classification tags, model versions and deprecation dates) and releases a new package version automatically with each Silver release. The package also carries shared macros and generic tests, including `upstream_fresh` and a deprecation test that warns as a consumed version approaches its `deprecation_date` (dbt's own deprecation warnings never reach `source()` consumers). It contains no models. Generation removes source-sync drift as a failure mode. Frozen inputs and other unpublished objects are excluded.

The package is tagged with a moving major-version tag (`v1`) as well as an exact version, so consumers pick up additive releases by refreshing their `package-lock.yml` rather than editing a pin (Overview Section 5.2).

### 12.2 Impact analysis

Before any breaking change, a steward runs the standing **"who consumes this?" query**, which unions:

- **The consumer manifest registry** (Section 11.9): every dbt consumer, including tables and incrementals, immediately and deterministically.
- **`SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY`**: which queries, roles and users touched a given object, with column-level granularity. This catches BI tools and ad hoc consumers that no dbt manifest knows about. It requires Enterprise Edition, has up to 3 hours latency and 365 days retention, and omits failed queries and intermediate views.
- **`SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES`**: views and other objects that reference a given object, including across databases. It does not record tables built by `CREATE TABLE AS SELECT`, `INSERT` or `MERGE`.

Because published objects are replaced on each publication (Section 9.8), these queries match objects by name, not by object id.

### 12.3 Versioning duty

Every published Silver model is versioned from v1. Every breaking change is made through a new dbt model version with a `deprecation_date`, following the process in Overview Section 7.2. Both versions publish side by side until the date. Additive changes need no version.

Breaking changes are batched into a predictable cadence (once or twice a year, announced in advance), following dbt's guidance for widely used models.

### 12.4 Full-refresh coordination

A full refresh of a published Silver incremental is a coordinated event (Overview Section 7.5). The stewards announce it to consumers found by the impact query, run it through the normal publication procedure so it is still gated, and log a FULL_REFRESH event in run control. History models and snapshots are never fully refreshed, since their history cannot be rebuilt beyond raw's retention.

--- 

## 13. References

### 13.1 dbt-Labs documentation

- [dbt: Staging](https://docs.getdbt.com/best-practices/how-we-structure/2-staging)
- [dbt: Snapshots](https://docs.getdbt.com/docs/build/snapshots)
- [dbt: Data contracts](https://docs.getdbt.com/docs/mesh/govern/model-contracts)
- [dbt: Model versions](https://docs.getdbt.com/docs/mesh/govern/model-versions)
- [dbt: Incremental models](https://docs.getdbt.com/docs/build/incremental-models)
- [dbt: persist_docs](https://docs.getdbt.com/reference/resource-configs/persist_docs)
- [dbt: freshness (including `loaded_at_query`)](https://docs.getdbt.com/reference/resource-configs/freshness)
- [dbt: Partial parsing](https://docs.getdbt.com/reference/parsing)
- [dbt: Snowflake configurations (transient, copy_grants)](https://docs.getdbt.com/reference/resource-configs/snowflake-configs)
- [dbt: About static analysis (Fusion)](https://docs.getdbt.com/docs/build/about-static-analysis)

### 13.2 Snowflake documentation

- [dbt Projects on Snowflake](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake)
- [dbt Projects on Snowflake: supported commands](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-supported-commands)
- [dbt Projects on Snowflake: limitations](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-limitations)
- [dbt Projects on Snowflake: supported dbt versions](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-dbt-core-versions)
- [Understand dbt project objects](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-understanding-dbt-project-objects)
- [Use dbt artifacts for Slim CI and defer to production](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-slim-ci-defer-to-prod)
- [EXECUTE DBT PROJECT](https://docs.snowflake.com/en/sql-reference/sql/execute-dbt-project)
- [dbt project objects migrate to a single mutable live version (2026_06 bundle)](https://docs.snowflake.com/en/release-notes/bcr-bundles/2026_06/bcr-2362)
- [Cloning considerations](https://docs.snowflake.com/en/user-guide/object-clone) and [SWAP WITH](https://docs.snowflake.com/en/sql-reference/sql/alter-schema)
- [Replication considerations (replication and cloning)](https://docs.snowflake.com/en/user-guide/account-replication-considerations)
- [AT | BEFORE (Time travel)](https://docs.snowflake.com/en/sql-reference/constructs/at-before)
- [Object name resolution](https://docs.snowflake.com/en/sql-reference/name-resolution)
- [Task graphs and dependencies](https://docs.snowflake.com/en/user-guide/tasks-graphs) and [Triggered tasks](https://docs.snowflake.com/en/user-guide/tasks-triggered)
- [Streams](https://docs.snowflake.com/en/user-guide/streams-intro)
- [OBJECT_DEPENDENCIES](https://docs.snowflake.com/en/sql-reference/account-usage/object_dependencies) and [ACCESS_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/access_history)
- [Known limitations for dynamic tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-limitations)
- [Tag-based masking policies](https://docs.snowflake.com/en/user-guide/tag-based-masking-policies)
- [Resource monitors](https://docs.snowflake.com/en/user-guide/resource-monitors)

### 13.3 Tooling and community

- [dbt-utils](https://github.com/dbt-labs/dbt-utils)
- [Performing a blue/green deploy of your dbt project on Snowflake](https://discourse.getdbt.com/t/performing-a-blue-green-deploy-of-your-dbt-project-on-snowflake/1349)
- [Databricks: medallion lakehouse architecture](https://docs.databricks.com/aws/en/lakehouse/medallion)

---

## <a id="appendix-a-snowflake-constraints-and-prerequisites"> Appendix A. Snowflake Constraints and Prerequisites

### A.1 Verified Constraints of dbt Projects on Snowflake

These documented Snowflake behaviours shape the design:

- **Only listed dbt commands are supported.** `build`, `run`, `test`, `seed`, `snapshot`, `run-operation`, `compile`, `list`, `parse`, `show`, `deps`, `clean`, `retry` and `source freshness` are supported through `EXECUTE DBT PROJECT`. **`source freshness`, `retry` and `clean` require a live-version project object** (Section A.2). The newer `dbt freshness` command is not listed, so this design uses `dbt source freshness`.
- **Failure is raised, not returned .** `EXECUTE DBT PROJECT` raises a Snowflake error when dbt fails: a model's warehouse error, a test at `error` severity, a compile error, a missing selector, a failing `run-operation` and an unsupported command all raise. The error text carries dbt's output, names the failing node and includes a query id for `SYSTEM$GET_DBT_LOG`. Tests at `warn` severity, `compile` and `source freshness` return normally with `SUCCESS = true`. The result has three columns, `SUCCESS`, `EXCEPTION` and `STDOUT`, which are read by name; on success `EXCEPTION` is the string `'None'`, not `NULL`. The publish procedure therefore wraps every execution in an exception handler, classifies the failure from the raised text, and also checks `SUCCESS` (Section 9.4).

- **The runtime version defaults to 1.9.4.** Unless `DBT_VERSION` is set on the project object or the execution, dbt 1.9.4 is used. Every project object pins its version (Section A.3). A `DBT_VERSION` on one execution overrides the object's pin for that execution only.
- **Concurrent executions need distinct artifact paths.** Several executions of one project object may run at once. With writeback enabled, each should use distinct, non-overlapping `--target-path` and `--log-path` directories inside the project. Execution artifacts are to be kept regardless of writeback.
- **Caller's rights procedures.** Stored procedures that call `EXECUTE DBT PROJECT` must be caller's rights procedures. The execution runs as the role in the project's profile, further restricted to the caller's privileges. Because the procedure creates schemas as the caller's primary role while dbt runs as the profile role, **each unit must be published by its own service role** with its own dbt target. The dynamic `ARGS` of `EXECUTE DBT PROJECT` can be built with `EXECUTE IMMEDIATE` and its result read with `RESULT_SCAN(LAST_QUERY_ID())`.
- **User-managed warehouses.** Tasks that execute `EXECUTE DBT PROJECT` must specify a user-managed warehouse; serverless tasks cannot be used. The warehouse is named in the dbt profile.
- **Dependencies are installed in CI.** Running `dbt deps` in CI and deploying with `dbt_packages` included avoids the need for an external access integration.
- **Managed dbt runtime.** Snowflake manages the dbt runtime. Arbitrary Python plugins cannot be installed.
- **Task graphs are single-schema.** All tasks in a task graph must have the same owner and be stored in the same database and schema (up to 1,000 tasks). The tasks need not live with the objects they act on (Section 11.7).

- **Scripting conventions.** `SET var = <function call>` is unsupported (use a subquery); `SYSTEM$` function results must not be read through `RESULT_SCAN` (declared lengths are enforced; call them inline); allowed prefixes of a Git API integration match whole path segments and the repository URL needs its `.git`; `INFORMATION_SCHEMA` table functions need a current database; `ALTER ... RENAME TO` targets must be qualified. Service and CI users set `DEFAULT_SECONDARY_ROLES = ()` (the default is `ALL`, which makes privilege behaviour depend on every granted role), and the account or service users set `TIMEZONE = 'UTC'`.

**Design choice: one project object for all Silver units.** All Silver units execute on one live-version project object, each with its own artifact paths (Section 9.3). This keeps one deployment and one version of Silver code, but it means the object's "last run" artifacts belong to whichever unit ran last (`SYSTEM$DBT_GET_LAST_SUCCESSFUL_RUN_TARGET` is one pointer per object). Section A.2 deals with the consequence.

### A.2 The live version of dbt project objects, and canonical state

**This design assumes live-version dbt project objects.** Opting in is a platform prerequisite to agree with the Snowflake account administrator.

Originally, each deployment of a dbt project object created a new immutable, numbered version (`VERSION$1`, `VERSION$2`, ...). Because those files could not change, an execution could not save the artifacts dbt produces (`manifest.json`, `run_results.json`, `sources.json`), and every run started from scratch.

Under the 2026_06 behaviour change bundle, a project object instead has a single, mutable version named **`live`**. Executions can write their artifacts back to the object (controlled by the `WRITEBACK` setting). This enables:

- `dbt source freshness`
- `dbt retry` from the failed node
- partial parsing
- `--state` and `state:modified`, including Slim CI and defer-to-production

Consequences for this design:

- **Version history lives in Git only.** Numbered object versions and `DEFAULT_VERSION` are removed. Rolling back code means redeploying an earlier commit from CI.
- **Concurrent runs need separate artifact paths** (Section A.1).
- **Partial parsing needs constant inputs per path.** Changing `--vars`, `profiles.yml`, `dbt_project.yml`, packages, the dbt version or `generate_x_name`/builtin overrides forces a full re-parse. Each unit always runs with the same `--vars` in its own target path, so its partial-parse state stays valid.
- **Opting in.** An account opts in by enabling the 2026_06 bundle (check with `SYSTEM$BEHAVIOR_CHANGE_BUNDLE_STATUS('2026_06')`), or by asking the Snowflake account representative to enable the separate single-live-version feature. New or replaced project objects are then live-version objects; existing objects migrate with `SYSTEM$MIGRATE_DBT_PROJECT`. Once the bundle is fully released, remaining versioned objects are migrated automatically, **so this is Snowflake's direction of travel regardless**.

**Deployment is in place.** A release is `ALTER DBT PROJECT ... ADD VERSION FROM <Git path>`, which replaces the live version's files and **keeps the object, the unit roles' grants and the canonical state of earlier executions**. `CREATE OR REPLACE DBT PROJECT` drops every grant on the object and makes all earlier executions' artifacts unreachable, so it is for first creation only.The Git commit can be read from `SHOW DBT PROJECTS` (`last_deployed_from`).

**Canonical state.** A unit's production run compiles its own models into `<unit>__build` schemas (Section 9.4), so its manifest is the wrong state for CI: `--defer` would resolve unselected models to build schemas, which hold the previous version after a swap and a half-built one during the next run. In addition, `SYSTEM$DBT_GET_LAST_SUCCESSFUL_RUN_TARGET` takes a project object, not a target path, so with several units on one object it returns whichever unit finished last. Therefore:

1. After every successful publication, the publish procedure runs a **canonical `dbt compile`** with no `publish_unit` variable and `WRITEBACK = FALSE`, so every relation resolves to its published schema.
2. The query id of that execution is recorded in the `PUBLISHED` event (Section 11.2).
3. CI hands the canonical state to dbt with the `IMPORTS` clause: `EXECUTE DBT PROJECT ... IMPORTS = ('snow://dbt/<db>.<schema>.<project>/results/query_id_<q>/target/' AS 'state') ARGS = 'build --state ./imports/state --defer --select state:modified.body+'`. The path is the `target/` directory below the path `SYSTEM$LOCATE_DBT_ARTIFACTS` returns for the recorded query id. The production object must have a qualifying execution within the previous seven days for the artifact functions to apply.
Note that `state:modified.body+ --defer` builds exactly the changed view and its dependent table in the CI schema, reads unselected parents from the published schema, and leaves the published schema untouched; without `--defer` the build fails. 

Tier 1 and Tier 2 projects follow the same pattern.

### A.3 dbt runtime policy

- **dbt Core 1.12.3** (with dbt-snowflake 1.12.0) is the recommended runtime, pinned with `DBT_VERSION` on every project object (Silver, Tier 1, Tier 2, CI). The runtime emits a harmless `EnvironmentVariableNamespaceDeprecation` warning about `DBT_ENGINE_MANAGE_STATE` (set by Snowflake's managed runtime), so no policy should treat deprecation warnings as errors. The adapter version is recorded alongside the core version.
- **dbt Fusion** (2.0, generally available on Snowflake since May 2026) is the expected next runtime. It offers faster parsing and, in strict static-analysis mode, compile-time detection of column and type errors. Adoption waits for confirmation of its stability and of the migration path for this design's custom macros (`ref` and `source` overrides, `generate_schema_name`, the `snapshot_get_time` override, the freeze `run-operation`). These macros and the `IMPORTS` state flow are specifically verified on Core, so a move to Fusion is a migration project, not a configuration change.
- **Upgrades are deliberate.** A runtime change is a release of the project object, and requires thorough testing in CI against the canonical state.

---


## Appendix B. Alternatives Considered

As part of developing this design document, many small tests and spikes were run on a Snowflake trial account to validate the documented behaviour of Snowflake objects as discussed in this document. the Snowflake documentation (and also the dbt-Labs documentation) was found to be excellent and consistent with tested functionality. The build of this design will no doubt present quirks and challenges as per any system design and build process. This is the point of providing a HLD which motivates a subsequent LLD. The sophistication and well-documented successes of both dbt Core and the Snowflake platform inspire confidence that all challenges can be resolved.

A number of alternatives were considered and rejected for this design based mainly on conceptual considerations. These are documented here as there may be reason later to reconsider.

| Alternative | Assessment |
|---|---|
| **Single gate over the whole Silver Layer** | One failing source would block every analytics team. Replaced by per-source publication units |
| **Base models as tables** (copying raw) | Duplicates raw storage and build compute for 1:1 shaping. Replaced by views over frozen inputs |
| **Pinned views over live raw** (watermark filter, or time travel `AT` in the view) | A watermark filter leaks in-place updates. Time travel in a view is unconfirmed and fails outright once staleness exceeds raw's retention period |
| **Test live raw, then clone** | Anything landing between the tests and the clone would be published untested. Frozen inputs are cloned first and tested in place |
| **Database-level blue/green swap**, with `ref` rendering schema-relative names | A known community pattern that fixes view binding. Rejected in favour of schema-level units: per-source isolation would need a database per unit, and the relative-reference side effects are the same. |
| **Per-table `ALTER TABLE ... SWAP`** | Not atomic across a unit, so consumers could see inconsistent tables within one source |
| **Blue/green schemas behind pointer views** | Reintroduces the view-binding problem, and repointing many views is not atomic |
| **Publishing tables by transactional DML** (Option B, Section 9.8) | Preserves object identity, at the cost of physical writes and more complex incrementals. No pressing benefit |
| **Separate internal and published schemas per unit** | Requires two swaps with a partial-failure state between them. Replaced by one schema per unit with per-object grants |
| **Dynamic tables as published Silver objects** | Snowflake-managed refresh bypasses the gate. Internal use may be acceptable - further investigation required |
| **Reading upstream with time travel for consistent reads** | Not viable: published objects are recreated on each publication |
| **Publication windows for consistent reads** | Couples unit schedules and adds state to run control. Upstream input clones adopted instead; windows remain an untested alternative |
| **Plain `PROPAGATE = ON_DATA_MOVEMENT` tags, or a dbt post-hook, for classification** | The first stops at views and the second must be maintained per column. Replaced by `ON_DEPENDENCY_AND_DATA_MOVEMENT` tags with a reserved declassification value |
| **`CREATE OR REPLACE DBT PROJECT` for deployment** | Drops grants and orphans canonical state. Replaced by `ALTER DBT PROJECT ... ADD VERSION` |
| **Event-driven task graph from the start** | Viable, since tasks can share one ops schema while executing project objects anywhere, but graph failure semantics must be neutralised and a dispatcher adds state. Scheduled gated tasks first; a graph or stream-driven dispatcher later if latency requires it. This can be easily bolted on |
| **Separate `dbt source freshness` step in the publish path** | Duplicates the gate's check against the same records and costs an extra execution per run. Kept for observability only |


---


## Appendix C. Open Items for the Detailed Design

- **Input-clone privilege model** for units that read other units: inherited unit roles (as tested), a steward-owned clone step, or a dedicated read-only role.
- **One shared Silver service role versus a role and task per unit** (Section 9.10).
- **Untested deployment cases**: removal of files with `ADD VERSION`, and a change of dbt version through it (Appendix A.2).
- **Clone time at production volumes**, and whether the analytics databases' retention settings follow Section 6.4.
- **Publication windows** as an alternative to input clones, if clone cost proves material (Section 9.6).
- **Semantic views**: handling of new columns, which lag one publication (Section 9.5).
- **Masking policies per data type** for every classification tag, and the governance review of declassification (Section 5.3).
- Agreement of the ingestion contract with the platform team (Section 3).
- Whether the raw and analytics databases are replicated for disaster recovery, and if so, a shared replication or failover group (Section 9.8).
- The changed-key pattern for incremental entity models (Section 5.6).
- Durable-key management in crosswalks: issuance, merges, splits (Section 8.3).
- Service-level targets per source, and the steward review turnaround, agreed with the platform team and consumers.
- Generator for base models and classification tags at source onboarding (Section 5.2, 5.3).
- Development and CI environments beyond the targets of Section 9.9: per-developer and per-pull-request databases, and how Tier 1 and Tier 2 projects import canonical state.
- Role catalogue: contributor and consumer roles, database roles for Silver readers, grant behaviour in managed-access schemas (unit service roles and the swap grant sequence are in Sections 9.4 and 9.10).
- Warehouse sizing per unit, resource monitors, and cost attribution by object and query tags.
- Alerting on run-control views (Snowflake alerts or task notifications).
- The custom uniqueness test for batch-restricted source testing (Section 7).
- dbt Fusion migration criteria and path (Appendix A.3).

---
---
