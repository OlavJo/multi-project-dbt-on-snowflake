# Silver Layer Design

## The Shared Foundation for Multi-Project dbt on Snowflake

### Design Document for Data Engineering and Silver Stewards

| | |
|---|---|
| **Version** | 1.3 |
| **Date** | 2026-10-05 |
| **Status** | **Proposed** for review |
| **Proposed By** | Olav Jordens, Lead Data Engineer, PSM CAI |
| **Companion document** | *Multi-Project dbt on Snowflake: Architecture Overview for Analytics Teams* v1.3 (referred to below as the **Overview**) |

---

## 1. Purpose and Scope

### 1.1 The Silver Layer as analytics infrastructure

The Silver Layer (Tier 0 Source Entity modules and the Tier 0.5 Core module) is the foundation every analytics project builds on. In medallion terms, raw landed data is Bronze (owned by the platform team), the published outputs of the Silver Layer is Silver, and the published outputs of Tier 1 and Tier 2 projects are Gold.

The Silver Layer's transformations are deliberately simple: shaping, never business logic. Yet it is where the hardest properties of the system meet:

- **Untrusted data enters.** Freshness, ordering, landing completeness and source defects are all handled once, here.
- **Many teams share one artefact.** Tier 1 and Tier 2 projects each have one owner; the Silver Layer may have contributions from all of them.
- **Everything inherits its guarantees.** If Silver provides them, the Tiers above get them almost for free. If it does not, no care above can recover them.
- **Its failures have the largest blast radius.**

The Silver Layer is therefore designed and operated as **infrastructure**: with a steward team (of analytics support data engineers), service levels, a release process and an operational control plane. It is designed for **safe change for consumers**: contracts, model versions from v1, independent publication units and a phased scope (Section 5.5). These features let it evolve without disrupting consumers, and accommodate new sources and new analytics teams.

### 1.2 Guarantees and how they are met

The Overview (Section 3.2) promises consumers the following. Each is delivered by a specific mechanism here:

|  | Data Engineering Guarantee | Mechanism | Section |
|---|---|---|---|
| G1 | **Gated**: nothing published failed its tests | Clone, freeze, build and test in isolation; explicit success check; atomic `SWAP` on success only | 10 |
| G2 | **Consistent**: one point in time per source | Frozen inputs: all raw tables of a source cloned at one timestamp | 7 |
| G3 | **Dated**: every unit carries a watermark | Watermarks from landing-complete records, propagated as a minimum | 11, 12 |
| G4 | **Contracted, documented and classified** | Enforced contracts, `persist_docs`, generated source package, classification tags carried onto derived columns | 6.3, 13.1 |
| G5 | **Versioned** | Every published model versioned from v1; new versions with deprecation dates; impact queries | 13.2, 13.3 |
| G6 | **Shaping only** | Structural join rule, scope-creep test, steward review gate | 5.3, 6.5 |
| G7 | **Stable keys** | Durable-key rules in the Core module | 9.3 |
| G8 | **Historised**: as-at queries possible | History models built from the CDC log, or snapshots where no CDC exists, with system time from source ordering or the unit watermark | 6.7 |

**Service levels** (per-source freshness targets, publication windows, time to restore, and the steward review turnaround for contributions) are to be agreed with the platform team (responsible for ingestion into Snowflake) and consumers, and recorded in run-control configuration (Section 12.3). This document defines the mechanisms that measure them, not the targets.

### 1.3 Scope

In scope: the ingestion contract, Silver project structure and stewardship, Source Entity and Core modules, frozen inputs, test placement, publication, freshness, run control, and the steward's duties towards consumers.

Publication and run control are designed here because Silver's constraints drive them, but Tier 1 and Tier 2 projects use the same mechanism (Overview Section 9).

Open items for the detailed design are listed in Section 16.

---

## 2. Snowflake Features This Design Relies On

The architecture uses embedded dbt deliberately, and replaces missing dbt-Labs capabilities with Snowflake features (Overview Section 10). The Silver Layer depends on these:

| Snowflake feature | Used for | Section |
|---|---|---|
| dbt project objects, `EXECUTE DBT PROJECT`, live version | Running the Silver project inside Snowflake; persisting dbt state between runs | 3 |
| `SYSTEM$LOCATE_DBT_ARTIFACTS` and related artifact functions | Importing the canonical production state into CI | 3.2 |
| Zero-copy schema clone | Building each unit in a copy of its last good state | 10.4 |
| Zero-copy table clone with Time Travel (`CLONE ... AT`) | Frozen inputs: raw tables cloned at the unit watermark | 7 |
| `ALTER SCHEMA ... SWAP WITH` | Atomic publication of a unit | 10.4 |
| Multi-statement transactions | The unit lease (Section 12.4); DML publication option (Section 10.10) | 12.4 |
| Snowflake Tasks | One gated, scheduled task per unit | 12.7 |
| Caller's rights stored procedures | The generic publish procedure | 10.4 |
| Database roles and per-object grants | Consumer access to published objects only | 10.7 |
| `persist_docs` (object and column comments) | Documentation inside Snowflake | 6.3 |
| `ACCESS_HISTORY`, `OBJECT_DEPENDENCIES`, Snowflake lineage | Impact analysis across project boundaries | 13.2 |
| Telemetry event table | dbt execution logs and traces | 12.1 |
| Object tags, tag-based masking, row access policies | PII protection on raw, frozen inputs and derived published columns | 6.3, 7.4 |
| Resource monitors, object and query tags | Cost control and attribution | 16 |

---

## 3. Snowflake Constraints and Prerequisites

### 3.1 Constraints of dbt Projects on Snowflake

These documented Snowflake behaviours shape the design:

- **Only listed dbt commands are supported.** `build`, `run`, `test`, `seed`, `snapshot`, `run-operation`, `compile`, `list`, `parse`, `show`, `deps`, `clean`, `retry` and `source freshness` are supported through `EXECUTE DBT PROJECT`. **`source freshness`, `retry` and `clean` require a live-version project object** (Section 3.2). The newer `dbt freshness` command is not listed, so this design uses `dbt source freshness`.
- **Failure is returned, not raised.** `EXECUTE DBT PROJECT` returns the columns `0|1 Success`, `EXCEPTION`, `STDOUT` and `OUTPUT_ARCHIVE_URL`. When dbt fails, the statement completes and returns `FALSE` in the success column; it does not raise an error. The publish procedure must inspect the result of every execution before proceeding (Section 10.4).
- **The runtime version defaults to 1.9.4.** Unless `DBT_VERSION` is set on the project object or the execution, dbt 1.9.4 is used. Every project object pins its version (Section 3.3).
- **Concurrent executions need distinct artifact paths.** Several executions of one project object may run at once. With writeback enabled, each must use distinct, non-overlapping `--target-path` and `--log-path` directories inside the project; otherwise writeback is disabled for the execution (`WRITEBACK = FALSE`). Execution artifacts are kept per query regardless of writeback.
- **Caller's rights procedures.** Stored procedures that call `EXECUTE DBT PROJECT` must be caller's rights procedures. The execution runs as the role in the project's profile, further restricted to the caller's privileges.
- **User-managed warehouses.** Tasks that execute `EXECUTE DBT PROJECT` must specify a user-managed warehouse; serverless tasks cannot be used. The warehouse is named in the dbt profile.
- **Dependencies are installed in CI.** Running `dbt deps` in CI and deploying with `dbt_packages` included avoids the need for an external access integration.
- **Managed dbt runtime.** Snowflake manages the dbt runtime. Arbitrary Python plugins cannot be installed.
- **Task graphs are single-schema.** All tasks in a task graph must have the same owner and be stored in the same database and schema (up to 1,000 tasks). The tasks need not live with the objects they act on (Section 12.7).

**Design choice: one project object for all Silver units.** All Silver units execute on one live-version project object, each with its own artifact paths (Section 10.3). This keeps one deployment and one version of Silver code, but it means the object's "last run" artifacts belong to whichever unit ran last. Section 3.2 deals with the consequence.

### 3.2 The live version of dbt project objects, and canonical state

**This design assumes live-version dbt project objects.** Opting in is a platform prerequisite to agree with the Snowflake account administrator.

Originally, each deployment of a dbt project object created a new immutable, numbered version (`VERSION$1`, `VERSION$2`, ...). Because those files could not change, an execution could not save the artifacts dbt produces (`manifest.json`, `run_results.json`, `sources.json`), and every run started from scratch.

Under the 2026_06 behaviour change bundle, a project object instead has a single, mutable version named **`live`**. Executions can write their artifacts back to the object (controlled by the `WRITEBACK` setting). This enables:

- `dbt source freshness`
- `dbt retry` from the failed node
- partial parsing
- `--state` and `state:modified`, including Slim CI and defer-to-production

Consequences for this design:

- **Version history lives in Git only.** Numbered object versions and `DEFAULT_VERSION` are removed. Rolling back code means redeploying an earlier commit from CI.
- **Concurrent runs need separate artifact paths** (Section 3.1).
- **Partial parsing needs constant inputs per path.** Changing `--vars`, `profiles.yml`, `dbt_project.yml`, packages, the dbt version or `generate_x_name`/builtin overrides forces a full re-parse. Each unit always runs with the same `--vars` in its own target path, so its partial-parse state stays valid.
- **Opting in.** An account opts in by enabling the 2026_06 bundle (check with `SYSTEM$BEHAVIOR_CHANGE_BUNDLE_STATUS('2026_06')`), or by asking the Snowflake account representative to enable the separate single-live-version feature. New or replaced project objects are then live-version objects; existing objects migrate with `SYSTEM$MIGRATE_DBT_PROJECT`. Once the bundle is fully released, remaining versioned objects are migrated automatically, **so this is Snowflake's direction of travel regardless**.

**Canonical state.** A unit's production run compiles its own models into `<unit>__build` schemas (Section 10.4), so its manifest is the wrong state for CI: `--defer` would resolve unselected models to build schemas, which hold the previous version after a swap and a half-built one during the next run. In addition, `SYSTEM$DBT_GET_LAST_SUCCESSFUL_RUN_TARGET` takes a project object, not a target path, so with several units on one object it returns whichever unit finished last. Therefore:

1. After every successful publication, the publish procedure runs a **canonical `dbt compile`** with no `publish_unit` variable and `WRITEBACK = FALSE`, so every relation resolves to its published schema.
2. The query id of that execution is recorded in the `PUBLISHED` event (Section 12.2).
3. CI imports the canonical state with `SYSTEM$LOCATE_DBT_ARTIFACTS` using the latest recorded query id, and runs `state:modified` selection and `--defer` against it. The production object must have a qualifying execution within the previous seven days for the artifact functions to apply.

Tier 1 and Tier 2 projects follow the same pattern. Spikes S6 and S8 confirm the behaviour.

### 3.3 dbt runtime policy

- **dbt Core 1.11.11** is the initial runtime, pinned with `DBT_VERSION` on every project object (Silver, Tier 1, Tier 2, CI). dbt Labs' active support for 1.11 ends on 18 December 2026; Snowflake continues to support versions on its platform beyond dbt Labs' dates.
- **dbt Fusion** (2.0, generally available on Snowflake since May 2026) is the expected next runtime. It offers faster parsing and, in strict static-analysis mode, compile-time detection of column and type errors. Adoption waits for confirmation of its stability and of the migration path for this design's custom macros (`ref` and `source` overrides, `generate_schema_name`, the freeze `run-operation`), assessed in spike S13.
- **Upgrades are deliberate.** A runtime change is a release of the project object, tested in CI against the canonical state, never an account-default change.

---

## 4. The Ingestion Contract

The platform team is resourced to land enterprise source data in Snowflake, not to work in dbt. This is an enterprise structural reality rather than a design choice.

### 4.1 The contract

The boundary is treated as a **contract rather than a handoff**. Every raw object has a declared freshness expectation, monitored by analytics, and failures are attributable to ingestion before any dependent build is attempted. The contract is to be agreed with the platform team **before the spikes start**, since the freeze, freshness and watermark mechanisms all rest on it.

The platform team provides:

> - A `_loaded_at` (or equivalent) column on every landed object.
> - For change-data-capture feeds, a **source ordering column** (sequence number, log sequence number, or source commit timestamp plus operation order). `_loaded_at` alone cannot order changes landed in the same batch, and CDC deduplication and history are impossible without a reliable ordering.
> - A **landing-complete record** written to a platform-owned control table per source system and batch, including batches with zero new rows, and **written only after every table of the batch has committed**. This record is the authoritative "data as of" signal for each source. It drives freshness (Section 11.3), the freeze timestamp (Section 7) and the raw watermark (Section 12.5). `max(_loaded_at)` is not a substitute, because a quiet source with no new rows would appear stale.
> - **Raw objects that support `CLONE ... AT`**: standard Snowflake tables. External tables cannot be cloned; Iceberg tables require confirmation before use as raw.
> - **Time Travel retention** on raw tables long enough to clone at the watermark (minutes in normal operation; to be agreed).

Analytics is granted read access to the landing-complete table. The platform team needs no access to analytics objects.

### 4.2 Batch cadence

Ingestion is batch-oriented: sources, including change-data-capture feeds, land in batches on a daily or hourly cadence. A CDC batch carries many change records for a key, ordered by the source ordering column; the landing-complete record closes the batch.

Continuous ingestion is out of scope for now. If it arrives, the contract extends with a periodic **heartbeat** landing-complete record carrying the connector's committed low-watermark, so the freeze, freshness and watermark mechanisms continue unchanged.

### 4.3 Attribution

Every Silver failure is classed as `source` (ingestion owns it) or `transform` (stewards own it), from where the failing test sits (Section 8). Run control records the class (Section 12.2).

---

## 5. Structure and Stewardship

### 5.1 One project, one repository

The Silver Layer is a single dbt project in a single repository ("monorepo"):

- One folder (module) per source system, plus the Core module.
- Shared macros and generic tests.
- One live-version dbt project object in Snowflake, executed per unit (Section 10.3).

A single project gives compile-time `ref()` between Core and the Source Entity modules, one CI pipeline, one set of conventions, and full in-project lineage. It reinforces the Silver Layer as the catalogue of analysis-ready sources.

### 5.2 Database and schema layout

| Object | Example | Visible to consumers |
|---|---|---|
| Silver database | `silver` | Analytics roles only |
| Published schema per source module | `silver.erp`, `silver.crm` | Yes (published objects only, Section 10.7) |
| Build schema per source module | `silver.erp__build` | No |
| Published and build schemas for Core | `silver.core`, `silver.core__build` | Published only |
| Run control | `analytics_ops.control` | Read access for stewards and project teams |

Frozen inputs live **inside** each published schema, alongside the objects that read them, but are never granted to consumers (Section 7.3).

### 5.3 Stewardship

The no-business-logic rule for Tier 0 is the load-bearing rule of this architecture. It erodes one reasonable-looking pull request at a time unless it is actively defended.

- A named **steward team (of data engineers)** owns the repository, CI, releases, run control and on-call for the Silver Layer.
- `CODEOWNERS` assigns each source module, and the Core module, to stewards. Contributions from any analytics team are welcome. Merges require steward approval, within the agreed review service level (Section 1.2).
- Stewards review against the structural join rule (Section 6.5), the scope-creep test (Section 6.5.2), test placement (Section 8) and classification (Section 6.3).
- Generated base models (Section 6.2) and the rule of two for entity models (Section 6.8) keep the volume of contributions, and therefore the review load, low.

### 5.4 Splitting later

If the repository becomes too large, splitting by source system is straightforward and yields essentially independent repositories, because Source Entity modules have no cross-source joins. Different load cadences do **not** require a split: each module is already an independent publication unit with its own schedule. Splitting is not expected, and this document treats the Silver Layer as one project.

### 5.5 Phased scope

| Release | Published content | Rationale |
|---|---|---|
| 1 | Generated base models, CDC current state, history (CDC history or snapshots), Core conformed dimensions | Proves publication, freshness and run control on the simplest models; unblocks Tier 1 teams quickly |
| 2 | Entity models admitted by the rule of two; Core crosswalks | Entity models bring the joined-model trap (Section 6.6) and fan-out testing; introduced once the machinery is proven |

---

## 6. Source Entity Modules (Tier 0)

### 6.1 Definition

A Source Entity module reconstructs one source system's **logical data models** as typed, deduplicated base models and, where admitted, denormalised, contract-enforced, tested **entity models** (logical entities in the domain-driven-design sense, without business logic). It is the single source of truth for operational data entering the analytics warehouse, and **it contains no business logic**.

Each module is a source-named folder set in the Silver project. **Modules use sources, base models, history models, entity models, snapshots, seeds, macros and tests.** Intermediate and mart models are excluded: they live in Tiers 1 and 2.

### 6.2 What a module contains, and how it is materialised

| Type | dbt Folder | Purpose | Materialisation |
|------|-------|---------|-----------------|
| Frozen inputs (`raw__*`) | n/a (created by the freeze step) | Zero-copy clones of the source's raw tables, all at the unit watermark. Declared as a dbt source; never granted to consumers | Transient clone (Section 7) |
| Base models | `staging/<source>/` | 1:1 with source tables: cast, rename, reshape columns, filter soft deletes. **Generated for every table** when a source is onboarded, then refined by stewards | View over the frozen input |
| CDC current state | `staging/<source>/` | Deduplicate change-data-capture rows to one current row per key, using the source ordering column | Incremental table |
| CDC history | `history/<source>/` | One row per key per version, with `valid_from` and `valid_to` from the source's own change ordering (G8) | Incremental table (permanent) |
| Entity models (`ent_*`) | `entities/<source>/` | Denormalise normalised source tables into logical entities; union of tables split in the source | Incremental table (or table) |
| Snapshots | `snapshots/<source>/` | SCD2 history for sources lacking change capture (G8) | Snapshot (permanent table) |
| Helpers | `staging/<source>/` | Internal logic shared by the above | Ephemeral |

Base models are published for every table of an onboarded source, because they are cheap views, mechanical to generate, and remove most reasons for a Tier 1 team to wait on a contribution. Folder names follow dbt's structure guidance: `staging/` holds only single-table preparation, and joins live in `entities/`.

### 6.3 Required declarations

- **Two source declarations per raw source.** One for the live raw tables, used only by the freeze step and observability (Section 11.3). One for the frozen inputs, used by every model and source test, resolving to the build schema of the unit being published.
- **Contracts and versions:** `contract: enforced: true` on every published model, and every published model versioned from v1. Contracted incrementals use `on_schema_change: append_new_columns` or `fail`.
- **Documentation:** `persist_docs` enabled for relations and columns. Every published column is described and typed, and descriptions state which tests the column has passed, so consumers can make consistent assumptions.
- **Classification:** masking and row access policies on raw carry over to frozen-input clones, but **not** into tables that dbt materialises. Every published column derived from a classified raw column therefore carries the same classification tag, set from dbt (column `meta` applied as a Snowflake tag by a post-hook) so tag-based masking applies to it. CI fails a published column that derives from a tagged raw column without carrying the tag. The Silver service role reads raw unmasked, so this check is what keeps PII masked downstream.
- **Table type:** dbt-snowflake creates tables as transient by default. Rebuildable Silver tables (CDC current state, entity models) stay transient, avoiding Fail-safe storage on every publication. History models and snapshots, which cannot be rebuilt beyond raw's retention, are permanent (`transient: false`).
- **CDC deduplication and history** use the source ordering column (Section 4.1), not `_loaded_at` alone.
- **Grants:** dbt `grants` config on published models only (Section 10.7).

### 6.4 What a module does NOT contain

- Any join or filter driven by a **business decision**.
- Any **cross-source-system** join. Cross-source joins for identity and conformance belong in the Core module; all others belong in Tier 1.
- Any derived metric or KPI resting on a business rule.
- Any model whose grain changes based on a business filter.

### 6.5 The structural join rule

Joins are permitted in an entity model **if and only if** they are:

- Driven by the source's own **foreign key relationship**.
- Grain-preserving for the entity root, or a strict many:1 enrichment.
- Present **regardless of what any analyst wants to do** with the data. This is usually (but not always) evidenced by the foreign key being useful for only one particular join, for example order items joined to an order.

**Allowed:** `order_items LEFT JOIN orders ON order_items.order_id = orders.order_id` (FK-driven enrichment).

**Not allowed:** `order_items JOIN orders WHERE orders.status = 'completed'` (business filter).

#### 6.5.1 Carve-out: source-semantic filters

Three filters are *structural* rather than business-driven and are explicitly permitted:

1. **Soft deletes.** Excluding rows the source system itself considers deleted (`is_deleted`, `deleted_at is not null`). A soft-deleted row is not data the source believes in.
2. **Deduplication of change-data-capture rows.** Selecting the current version per key, using the source ordering column.
3. **Tenant or instance scoping**, where the source is multi-tenant and only one tenant is in scope. For example, a source audit table includes records of various kinds, of which only one kind is relevant for analytics.

Non-structural filters belong in Tier 1.

#### 6.5.2 The scope-creep test

The boundary between structural and business logic erodes at child aggregations. `order_item_count` is structural; `net_revenue` is not. The distinguishing test:

> **If two competent analysts could compute it differently, it belongs in Tier 1.**

`order_item_count` has one reasonable definition. `net_revenue` requires decisions about discounts, returns, tax and cancellations. Hence it is business logic, however obvious it looks in one source system.

#### 6.5.3 Fan-out must be tested, not asserted

"Strict many:1" is a claim, and an unnoticed 1:N join silently inflates every metric downstream of it. Fan-out is a defect in Silver code, so these tests sit on outputs (Section 8). Every published entity model carries, at `error` severity:

- `unique` or `dbt_utils.unique_combination_of_columns` on its declared grain.
- `dbt_utils.relationships_where` on each FK used for enrichment.
- A row-count guard asserting the entity's row count equals its root table's row count (for grain-preserving entity models).

### 6.6 Incremental entity models: the joined-model trap

A denormalised entity model materialised incrementally on its root key **will silently miss updates to joined child tables** if the incremental filter only looks at the root. For example, on the `orders` entity with `unique_key: order_id` and `merge`, filtered on the order's own timestamp, a change to an order *item* is never noticed.

The required pattern is to collect changed keys from **every** contributing frozen input, then rebuild those roots in full. The pattern is specified in the detailed design.

Snowflake dynamic tables handle incremental refresh across joins natively and would remove this trap. Under Snowflake-managed refresh, however, they change data outside any gate, so they are not used for published objects. Using them as an internal, orchestrator-refreshed engine is optional spike S7.

### 6.7 History (G8)

Consumers in a financial institution need as-at reporting and restatement, so history is a published guarantee, not an afterthought.

- **CDC sources: history from the change log.** A CDC history model publishes one row per key per version, with `valid_from` and `valid_to` taken from the source ordering column (source commit time and operation order). This records system time as the source saw it, which no snapshot can recover.
- **Sources without CDC: snapshots.** A snapshot records change as observed between batches. Because builds read frozen inputs, the snapshot's validity timestamps must be the **unit watermark**, not the build's current time; the snapshot time is overridden accordingly, so a retried build stamps the same time (spike S4).
- **Capture at base grain.** History and snapshots capture base models at their natural grain. Any change to any joined-in column of a denormalised entity would spawn a new version, so history becomes high-churn and expensive. Denormalisation happens downstream of history, not upstream of it.

### 6.8 Entity model design decisions

Entity models are admitted by a **rule of two**: an entity model is built when two consumers need it, or when the stewards judge the source too normalised to consume through base models alone. Until then, the join belongs in the Tier 1 project that needs it.

The rule decides **whether** an entity model exists. The structural join rule (Section 6.5) decides **what it may contain**: only joins that would be made regardless of analysis. Consumer demand never justifies a join the structural rule forbids.

Per admitted entity, the stewards decide:

- Whether to emit the **parent with child aggregations** (`orders` with item count, total quantity).
- Whether to emit the **child with parent attributes** (`order_items` enriched with order header fields).
- Whether to emit **both**, which is common for highly normalised sources.

---

## 7. Frozen Inputs

### 7.1 Principle

**Nothing in the Silver Layer reads live raw data.** Each build of a Source Entity unit first clones every raw table of the source, **all at the same timestamp** (the unit watermark from the latest landing-complete record), into the unit's build schema. Every model and every source test reads these frozen inputs.

### 7.2 The freeze step

The freeze is a dbt `run-operation` macro, invoked by the publish procedure (Section 10.4). It reads the live-raw source declaration, so the list of tables lives in dbt alongside the models, and development and CI run the same logic. For each raw table it performs the equivalent of:

```sql
CREATE OR REPLACE TRANSIENT TABLE silver.erp__build.raw__orders
  CLONE raw.erp.orders AT (TIMESTAMP => :watermark);
```

The Core module has no frozen inputs: it reads published Source Entity outputs and seeds, never raw.

### 7.3 Consequences

- **Gateable views without copying data.** A view over a frozen input cannot show data newer than the input, so 1:1 base models are views. Raw data is not duplicated into Silver.
- **A consistent snapshot across tables (G2).** All tables of a source are read at one point in time, so a batch landing mid-build can never split a join.
- **One point in time.** The frozen inputs, the unit watermark and the freshness check all refer to the same landing-complete record.
- **Frozen inputs publish with the unit.** They live in the published schema and swap with the views that read them. They are never granted to consumers (Section 10.7).
- **Incremental models get stable inputs.** Their change detection reads the same frozen inputs as everything else.

### 7.4 Costs and risks

- **Query-time shaping.** Base views re-execute their casts and renames on every consumer query. This is cheap for light 1:1 shaping; heavier work stays in incremental tables.
- **Retained storage.** A clone shares micro-partitions with raw at creation. A replaced clone's unique micro-partitions (those raw has since rewritten) are retained for time travel. Transient clones avoid Fail-safe; a short retention period on Silver schemas should contain the rest (spike S9).
- **Clone time.** Cloning is metadata-only but not instantaneous for tables with many micro-partitions (spike S5).
- **Governance policies.** Masking and row access policies, and tag associations, are documented to carry over to table clones, so frozen inputs inherit raw's protection. Materialised outputs do not inherit it; Section 6.3 covers them.
- **Replication.** If Silver is replicated for disaster recovery, frozen-input clones replicate logically only when raw is in the same replication or failover group; otherwise they replicate as physical copies (Section 10.10).
- **View name binding.** Base views reference frozen inputs in their own schema, which depends on spike S1 (Section 10.5). A contingency exists (Section 10.6).

---

## 8. Test Placement

**Each assertion is tested where it can first fail, and only there.**

- **Source defects** are problems in what landed. Silver code cannot cause or repair them. They are tested once, on the frozen inputs.
- **Transformation defects** can only be introduced by Silver code. They are tested on the outputs.

A property that passes unchanged through a 1:1 base view (a key not null, an accepted value) is not re-tested on the view.

| Assertion | Tested on | Owner of a failure |
|---|---|---|
| Freshness | Landing-complete records (gate, Section 11.3) | Ingestion |
| Not null, accepted values, castability of raw columns | Frozen input | Ingestion |
| Uniqueness at the source's own grain (full-load tables) | Frozen input | Ingestion |
| Referential integrity within the source | Frozen input | Ingestion |
| Source ordering column present and unique per key change | Frozen input | Ingestion |
| One current row per key after CDC deduplication | Output | Stewards |
| History validity (no gaps or overlaps per key, one open version) | Output | Stewards |
| Entity grain, row-count guards, enrichment FKs (6.5.3) | Output | Stewards |
| Snapshot validity (one current record per key, no overlapping validity ranges) | Output | Stewards |
| Contracts (names, types) | Output | Stewards |
| Classification tags carried (Section 6.3) | CI | Stewards |
| Crosswalk match-rate floors (Core) | Output | Stewards |

Notes:

- **Castability must be a source test.** Creating a view does not evaluate it, so a value that fails a cast in a base view would not fail the build; it would fail the first consumer query after publication. A source test (rows where `try_cast(col)` is null but `col` is not) catches it before publication.
- **Gating is native.** `dbt build` runs source tests before dependent models, and skips those models when an `error` test fails. A source defect stops the unit before any transformation runs.
- **Severity carries meaning.** `error` blocks publication; `warn` publishes and is logged. Only tests that evidence wrong data are `error`.
- **Incremental testing of large sources.** Source tests over a full frozen input rescan history that has already passed. For large sources, tests are restricted to the new batch with the test `where` config (for example, `_loaded_at` later than the previous watermark). Uniqueness needs care: a new row can collide with an old one, so it uses a custom test comparing new keys against the whole table.
- **Contributed tests.** Tests contributed by Tier 1 teams follow the same placement; most are source assertions on frozen inputs.

---

## 9. The Core Module (Tier 0.5)

### 9.1 Why it exists

Before any domain business logic, there are shared concerns requiring special handling:

1. **Conformed dimensions and spines:** cross-domain `dim_date` and fiscal calendar, currency and geography lookups held in seed datasets.
2. **Cross-source identity resolution:** a customer in the ERP and the same customer in the CRM. Source Entity modules forbid cross-source joins, and Tier 1 is domain-scoped, so neither can own this.

Identity resolution is honestly **business logic**: matching rules are judgements, and two competent analysts could write them differently. It fails the scope-creep test. The Core module exists because this logic must be shared and must have exactly one owner. It is the **one governed exception** where shared business logic lives below Tier 1, and it is governed accordingly.

The Core module is expected to remain small, with critical but niche datasets.

### 9.2 What it contains

| Layer | Purpose | Materialisation |
|-------|---------|-----------------|
| Conformed dimensions | `dim_core__date`, `dim_core__currency`, `dim_core__fiscal_period` | Table |
| Crosswalks | `xref_core__customer` mapping source-system keys to a durable `customer_key` | Table (history-backed) |
| Reference seeds | Curated lists, mappings, hierarchies under version control | Seed (Table) |

Tier 1 projects may read all published Core outputs. Tier 2 projects may read the **conformed dimensions** directly (Overview Section 3), so Tier 1 projects need not re-publish them.

### 9.3 Governance

The Core module is the **only** Silver module permitted to join across source systems, and that privilege is narrowly scoped:

- It may join across sources **for identity resolution or conformance only**, not to answer a business question.
- Every crosswalk publishes its **match rate and unmatched-key counts as tested metrics**, with an `error`-severity floor. An entity-resolution rule that quietly stops matching is worse than one that fails.
- **Durable keys are never reassigned (G7).** Once a `customer_key` is issued, it continues to identify the same real-world entity when matching rules change. Merges and splits are recorded explicitly (for example, a superseded-key mapping), never by silently re-pointing a key. Key stability is harder than match rate and is specified in the detailed design.
- Matching rules are documented in prose next to the model, and any change to them is treated as breaking (Overview Section 8.4).
- It is contracted and versioned exactly as Source Entity outputs are.
- It is its own publication unit (`silver.core`).

### 9.4 Admission criteria

Because Tier 1 projects may not read each other, shared reference data will be proposed for Core. A dataset is admitted to Core only if all of these hold:

- It is needed by more than one domain, or by Tier 2 directly.
- It is conformance or identity, not a domain's business definition. A domain's own hierarchy (for example, finance's product hierarchy) stays in that domain and is published there.
- It has exactly one owner, who accepts Core's governance (breaking-change rules, match-rate floors where relevant).

The stewards review Core's contents periodically against these criteria, so "expected to remain small" stays true.

### 9.5 Why not simply relax Tier 0's rule?

The no-cross-source-join rule is the clearest, most enforceable statement in this architecture, and a carve-out inside Tier 0 could be used to justify a genuine business join. Isolating the exception in a separate module with its own review gate keeps Tier 0's rule absolute.

---

## 10. Publication (Write-Audit-Publish)

### 10.1 Two requirements in tension

With a plain `dbt build`, tests run after models are replaced. By the time a test fails, published tables have already changed. **Tests observe the incident rather than prevent it.**

Gating publication on tests fixes this. But a single gate over the whole Silver Layer means one failing source blocks every analytics team. Both are required:

> **(a)** Nothing ever builds on data that failed its tests.  
> **(b)** A failure blocks only what depends on it.  

### 10.2 Principle: failures block by staleness, not by status

A failed build never replaces published data. The last good version keeps serving, and the failure shows up downstream as **staleness**: the unit's watermark stops advancing.

Downstream units are never gated on an upstream *failure*. They are gated on upstream *freshness* against their own declared tolerance (Section 11). This satisfies both requirements:

- Failed data is never published, so nothing can read it (a).
- Unrelated units are unaffected (b).
- Dependent units either proceed on last-good data within tolerance, or skip.

### 10.3 Publication units

A **publication unit** is the smallest set of objects published atomically. Each unit has exactly one schema holding everything the unit builds, and one build schema.

| Tier | Unit | Example schemas |
|---|---|---|
| Tier 0 | One per source system | `silver.erp`, `silver.erp__build` |
| Tier 0.5 | One unit | `silver.core`, `silver.core__build` |
| Tier 1 / 2 | One per project by default; a project may define more | `finance.mart`, `finance.mart__build` |

Within a unit's schema, published objects are granted to consumers object by object; frozen inputs (Silver) and internal models (Tier 1 and 2) are never granted (Section 10.7). One schema per unit means one atomic swap publishes everything together.

Source systems work as units because Source Entity modules forbid cross-source joins: each is an independent subgraph. Within a unit, publication is all-or-nothing. Across units, publication is independent, and each unit keeps its own schedule.

All Silver units share one live-version dbt project object and run concurrently, each with its own `--target-path` and `--log-path` (Section 3.1). Each unit is selected by its own selector in `selectors.yml` (for example `unit_erp`), which includes the unit's models, snapshots, frozen-input sources and source tests.

### 10.4 Publish procedure

One generic, caller's rights procedure, parameterised by unit and driven by run-control configuration (Section 12), performs the steps below. Every `EXECUTE DBT PROJECT` result is checked: any `FALSE` in `0|1 Success` logs `FAILED`, releases the lease and stops.

```sql
-- 0. Acquire the unit lease (Section 12.4). If it is held, log SKIPPED (locked) and stop.

-- 1. Gate (Section 12.7): upstream watermarks within tolerance, and something new to build
--    (an upstream PUBLISHED or a DEPLOYED event since this unit last published).
--    If the gate fails, log SKIPPED, release the lease and stop.

-- 2. Log STARTED. Clone last good state (zero-copy; preserves incremental, history and snapshot state)
CREATE OR REPLACE SCHEMA silver.erp__build CLONE silver.erp;

-- 3. Freeze (Source Entity units only): replace the frozen inputs with
--    clones of raw at the new watermark (Section 7.2)
EXECUTE DBT PROJECT silver.ops.silver_layer
  ARGS = 'run-operation freeze_sources --args "{unit: erp, watermark: ...}"
          --vars "{publish_unit: erp}" --target-path target/erp --log-path logs/erp';
-- check success

-- 4. Test sources, build, test outputs: one dbt build
EXECUTE DBT PROJECT silver.ops.silver_layer
  ARGS = 'build --selector unit_erp --vars "{publish_unit: erp}"
          --target-path target/erp --log-path logs/erp';
-- check success

-- 5. Atomic publish; schema-level grants handled as established by spike S2
ALTER SCHEMA silver.erp__build SWAP WITH silver.erp;

-- 6. Canonical state for CI (Section 3.2)
EXECUTE DBT PROJECT silver.ops.silver_layer
  ARGS = 'compile' WRITEBACK = FALSE;
-- record the query id

-- 7. Log PUBLISHED with watermark and state query id; release the lease
```

The order is **freeze, test sources, build, test outputs, publish**. Tests always run against the exact frozen copy that will be published. Testing live raw and cloning afterwards would publish anything that landed between the two steps untested.

If step 3 or 4 fails, nothing published has changed. The previous frozen inputs, views and tables keep serving together, out of date but correct. There is no rollback, because there is nothing to roll back. The build schema is discarded and re-cloned at the next run. A failure in step 6 does not affect consumers; it is logged and alerted, and CI keeps using the previous canonical state.

The procedure runs as a per-unit service role that owns both of the unit's schemas, since `SWAP WITH` requires OWNERSHIP of both.

**Schema redirection.** A custom `generate_schema_name` sends only the models of the unit being published (`var('publish_unit')`) into `__build` schemas. All other references resolve to published schemas. This is how the Core module reads last-good Source Entity outputs through `ref()` while keeping in-project lineage. Sketch (to be validated, and extended for dev and CI targets):

```jinja
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- set base = (custom_schema_name or target.schema) | trim -%}
  {%- if target.name == 'prod' and base == var('publish_unit', none) -%}
    {{ base }}__build
  {%- else -%}
    {{ base }}
  {%- endif -%}
{%- endmacro %}
```

### 10.5 Views and schema-relative references (spike S1)

dbt renders fully qualified names, so a view in `silver.erp__build` referencing a sibling object would still point at `silver.erp__build` after the swap, reading the stale objects now living under that name.

Snowflake documents that "all unqualified objects in a view or UDF definition will be resolved in the view's or UDF's schema only". Rendering **same-schema references from views** as bare identifiers therefore makes views swap-safe, provided the binding happens at query time rather than at creation, which spike S1 confirms. References to other schemas stay fully qualified; they follow the swap by name. Tables, incrementals and tests stay fully qualified, because their SQL resolves against the session. Sketch:

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

The same treatment applies to `source()` references from base views to frozen inputs. Base views depend on this, so **spike S1 is load-bearing**, though the documentation makes a pass likely. S1 also tests that a **schema clone** rebinds such views to the cloned siblings, which Section 10.9 relies on.

### 10.6 Contingency if spike S1 fails: in-place frozen inputs

Each Source Entity unit gets a separate, permanent source schema (for example `silver.erp_src`) holding the frozen inputs and base views. The views never move schema, so the binding question disappears. Once source tests pass against newly cloned inputs in a staging area, the inputs are replaced in place in `erp_src`. Entity models and CDC tables continue to use build-and-swap in `silver.erp`. The costs:

- **Unit atomicity is lost.** DDL auto-commits, so replacing N inputs happens at N separate moments; a consumer joining two base views mid-sequence can see mismatched watermarks.
- **Base views and entity models can drift.** If source tests pass but an output test fails, base views advance while entity models stay on the previous watermark.

The contingency is still preferable to materialising base models as tables.

### 10.7 Rules

1. **Nothing published reads live raw data.** Silver models read frozen inputs. A view over live raw changes the moment data lands, so it cannot be gated. This holds regardless of spike S1.
2. **Views in published schemas** reference other units' objects fully qualified, and same-unit objects only through bare identifiers (Section 10.5). Snowflake semantic views are included in this rule and in spike S1.
3. **Tests are placed by Section 8, and severity carries meaning.**
4. **Retries are idempotent.** Every run starts from a clone of the last good state, so a failed run can simply be re-run under the lease. This holds for incrementals, history and snapshots alike.
5. **Consumers are granted object by object, never on build schemas.** Object grants come from dbt's `grants` config on published models only, never from future grants on tables. This keeps frozen inputs and internal models invisible to consumers. Grants on child objects travel with a schema clone and a swap. For schema-level `USAGE`, Snowflake documents that `SWAP WITH` "also swaps all access control privileges granted on the schemas"; spike S2 establishes whether this leaves `USAGE` with the published name (no re-grant needed) or with the moved schema (then grant `USAGE` on the build schema just before the swap and revoke it from the new build schema just after, so there is no gap and no consumer access to a build schema).
6. **Each unit has its own artifact paths** on the shared project object.
7. **Every `EXECUTE DBT PROJECT` result is checked** before the next step (Section 10.4).

### 10.8 Failure behaviour

| Failure | Published result | Downstream |
|---|---|---|
| Raw ERP batch does not land | No new landing-complete record; raw ERP watermark stops advancing; ERP gate skips | Staleness visible in run control, owned by ingestion |
| ERP source test fails on frozen inputs | `silver.erp` stays at last good; no ERP models built | FAILED with `failure_class: source`, owned by ingestion. As below |
| ERP output test fails | `silver.erp` stays at last good | `crm` unaffected. `core` and ERP-dependent Tier 1 projects proceed on last-good ERP if within tolerance, otherwise skip; their watermarks inherit ERP's older watermark |
| Core crosswalk match rate below floor | Previous crosswalk keeps serving | All consumers stay on the last accepted identity mapping |
| Finance build fails | Finance marts stay at last good | Tier 2 gated by finance freshness |
| Canonical state compile fails | Publication already complete | CI uses the previous canonical state; alert to stewards |
| A run dies mid-way | Nothing published changed | Lease expires; `STARTED` without a terminal event is flagged as stale (Section 12.6) |

### 10.9 Open issue: consistent reads across publication units

Frozen inputs make each Source Entity unit read raw at a single point in time. Units that read *other units* (the Core module, Tier 1 and Tier 2 projects) read published schemas directly. A swap is atomic, but a downstream build runs many queries over several minutes. If an upstream unit publishes mid-build, early models may read the old version and later models the new one.

The watermark recorded at gate time understates freshness in this case (the safe direction), but models within one downstream build can disagree. Candidate mitigations, for spike S10, in order of preference:

- **Upstream schema clone at gate time.** The downstream unit's procedure zero-copy clones each upstream published **schema** it reads into a private input schema, and the downstream build reads those clones. A schema clone carries views, and swap-safe views (Section 10.5) rebind to the cloned frozen inputs beside them, so base views are covered. The clone runs under the steward-owned service role, since consumers have no access to frozen inputs. Depends on spike S1.
- **Publication windows.** Run control prevents an upstream unit from swapping while a dependent build that read it is in progress. This couples schedules and adds state to run control.

Reading upstream with time travel at the gate timestamp is **not** viable: published objects are recreated on each publication (Section 10.10), so an object published after the gate timestamp has no history at that time.

### 10.10 Object identity: published objects are replaced on each publication

Clone-and-swap gives every published table and view a **new object identity on every publication**. This is accepted as the baseline (Option A), with stated consequences:

- **Time travel** on a published object reaches back only to the creation of its current clone. History is served by history models (G8), not time travel.
- **Streams and dynamic tables** on published objects are not supported: a stream loses its offset and a dynamic table must reinitialise when its base object is replaced. Consumer rules are in Overview Section 9.4.
- **Per-object history** (data metric function results, Snowsight lineage, `ACCESS_HISTORY` object ids) restarts with each publication. Impact and monitoring queries match by name.
- **Disaster-recovery replication.** Clones replicate logically only when the original and the clone are in the same replication or failover group; otherwise they replicate as physical copies, and recreated objects can briefly disappear from the secondary during refresh. If the analytics databases are replicated, raw and analytics must share a replication or failover group, or analytics is excluded from replication and rebuilt from raw after failover. Whether the analytics databases are replicated is an open item (Section 16).

**Option B: publish tables by transactional DML.** Views and frozen inputs keep swap-based publication; tables are built in the build schema, tested, and then written to stable published tables inside one multi-statement transaction (DML is transactional, whereas DDL auto-commits). Object identity is preserved, so time travel, streams and per-object history survive, at the cost of physical writes on each publication and more complex handling of incrementals. Spike S11 measures Option A's effects and costs Option B for selected Tier 1 marts, where stable identity may matter to consumers.

---

## 11. Freshness and Watermarks

### 11.1 The silent-wrongness problem

Suppose a Tier 2 project is triggered after all upstream Tier 1 tasks have completed. Finance completed this morning; digital last succeeded three days ago. Tier 2 builds successfully, publishes a cross-domain fact mixing fresh and stale data, and **no test anywhere fails.** The dashboard is wrong with no warning.

Completion is not freshness. Every cross-unit dependency needs a freshness assertion, not just a completion signal.

### 11.2 Propagated watermarks

Every publication unit carries a **watermark**: the time its data is current as of.

- A raw source's watermark is the time of its latest landing-complete record (Section 4).
- A Source Entity unit's watermark is the freeze timestamp of its frozen inputs.
- Any other unit's watermark is the **minimum of its upstream watermarks**, read at gate time.

Staleness therefore propagates automatically. In the example above, the Tier 2 unit's watermark is digital's three-day-old watermark, not finance's fresh one.

Each unit declares a **tolerance per upstream dependency** (Section 12.3). Before building, the unit's gate checks every upstream watermark against its tolerance, and skips if any is exceeded. Skipping leaves the last good version published and the watermark unchanged, so the staleness remains visible. Dashboards display the watermark as **"data as of"**.

### 11.3 Source freshness at the raw boundary

**The gate is the single freshness check.** For a Source Entity unit, the publish procedure's gate reads the raw unit's watermark from `v_landing_complete` (Section 12.5) and compares it with the tolerance in `unit_dependencies`. A breach skips the unit's build, with an alert whose owner is unambiguously ingestion. One check, in one place, avoids an extra `EXECUTE DBT PROJECT` per run and two definitions to keep in step.

`dbt source freshness` remains available for **observability** (populating `sources.json` and dbt's freshness views), pointing at the same records so the two cannot disagree. It requires a live-version project object (Section 3.2) and is run on its own schedule, outside the publish path. It uses `loaded_at_query` (dbt v1.10 and later) rather than `loaded_at_field`, because `max(loaded_at_field)` makes a quiet source with no new rows look stale. Illustrative sketch:

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

Run control reads the same landing-complete records to derive raw watermarks (Section 12.5), so the gate, the freeze timestamp and the watermark cannot disagree.

---

## 12. Run Control

### 12.1 Design principle: record facts, not state

Run control coordinates publication, freshness and full-refresh events across all units, in all Tiers. Its main simplicity lever is that it **records facts rather than maintaining state**: rows are appended, never updated, and all current state is derived by views. The one deliberate exception is the unit lease (Section 12.4), because mutual exclusion cannot be derived from an append-only log. The complete design is:

- one append-only event log
- one lease table
- two small configuration tables, maintained under version control
- a consumer manifest registry
- a view of platform landing records
- two status views
- the single publish procedure (Section 10.4)
- one scheduled task per unit

Run control is, in effect, a small orchestrator. It is owned, staffed and on-call as part of the Silver stewards' remit.

Run control operates at **unit granularity only**. Per-model results stay in dbt's artifacts (written back to the live-version project object) and Snowflake's telemetry event table, and are not duplicated.

Run control lives in its own database or schema (for example `analytics_ops.control`), owned by the steward role.

### 12.2 The event log

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

The log is append-only: no updates, no `MERGE` contention, and a complete audit trail by construction. Writers are restricted to:

- the publish procedure (STARTED, PUBLISHED, FAILED, SKIPPED)
- each project's CI deployment step (DEPLOYED)
- a small steward procedure for FULL_REFRESH events

### 12.3 Configuration

Static configuration is maintained in a small ops repository and deployed as seeds or tables. Changes go through pull request review.

```sql
-- One row per publication unit, including raw units owned by the platform team
units (unit, tier, owner, dbt_project_object, selector, schedule, max_runtime_minutes)

-- One row per dependency edge; this is the cross-project dependency graph
unit_dependencies (unit, upstream_unit, max_age_hours)
```

The dependency graph is declared, never inferred at runtime. Service-level targets (Section 1.2) are recorded here as tolerances.

### 12.4 The unit lease

Two runs of one unit must never overlap: both would replace the same build schema, and one could swap the other's half-built schema into production. Scheduled tasks do not overlap themselves, but a manual re-run or retry can.

```sql
create table analytics_ops.control.unit_lease (
  unit        varchar,        -- one row per unit
  run_id      varchar,        -- null when free
  expires_at  timestamp_ntz   -- UTC
);
```

The procedure acquires the lease with a single conditional `UPDATE` (set `run_id` and `expires_at` where the lease is free or expired) inside a transaction, and proceeds only if one row was updated. It releases the lease on every exit path. Expiry uses the unit's `max_runtime_minutes`, so a run that dies cannot block the unit indefinitely. Spike S12 confirms the pattern under concurrent attempts.

### 12.5 Raw units and the landing boundary

The platform team keeps writing its own landing-complete table (Section 4); analytics has read access only. A view, `v_landing_complete`, presents each landing-complete record as a PUBLISHED event for the corresponding raw unit (for example `raw.erp`). Raw units therefore appear in status and gating exactly like any other unit, with the platform team as owner.

The same view is the source of the gate's raw freshness check and the target of the `loaded_at_query` used by `dbt source freshness` (Section 11.3), so there is a single source of truth for raw freshness.

### 12.6 Status views

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

### 12.7 Gating and scheduling

As an initial approach, each unit runs as a **scheduled task with a gate**, not as part of an event-driven task graph. The gate proceeds only when both conditions hold:

1. Every upstream watermark is within its tolerance. For Source Entity units, this is the raw unit's landing-complete watermark (Section 11.3).
2. There is something new to build: at least one upstream has published, or the unit's project has been deployed (`DEPLOYED`), since this unit last published. Without the deployment condition, a code release would not publish until upstream data changed.

If either condition fails, the procedure logs SKIPPED and exits. On success, the new watermark is the minimum of the upstream watermarks read at gate time (for Source Entity units, the freeze timestamp).

This avoids a dispatcher entirely and suits the daily and hourly batch cadence. If latency later matters, it can be enhanced in either of two ways, without changing the log design:

- A stream on `publish_log` feeding one dispatcher triggered task replaces the schedules.
- A single task graph in `analytics_ops.control` calls every unit's procedure. Task graphs require all tasks to share an owner, database and schema, but the tasks can live in the ops schema while executing project objects anywhere. Graph tasks must always succeed and record outcomes in the log, so that a failed unit blocks by staleness, not by graph status.

### 12.8 Freshness visible in dbt

`unit_status` is included as a source in the generated source package, together with a generic `upstream_fresh(unit, max_age)` test at `warn` severity. Tier 1 and Tier 2 developers then see upstream freshness in their own dbt output, without querying run control directly. The gate, not this test, enforces tolerance.

### 12.9 Consumer manifest registry

On every production deployment, each Tier 1 and Tier 2 project's CI writes the sources it declares (upstream relation, model version, columns where declared) into `analytics_ops.control.consumer_sources`, replacing that project's previous rows. This gives the impact query (Section 13.2) a deterministic, immediate list of every dbt consumer.

---

## 13. Serving Consumers

### 13.1 The generated source package

Silver CI generates the consumers' source definitions from the Silver manifest (published relations, column names, types, descriptions, classification tags, model versions and deprecation dates) and releases a new package version automatically with each Silver release. The package also carries shared macros and generic tests, including `upstream_fresh` and a deprecation test that warns as a consumed version approaches its `deprecation_date` (dbt's own deprecation warnings never reach `source()` consumers). It contains no models. Generation removes source-sync drift as a failure mode. Frozen inputs and other unpublished objects are excluded.

The package is tagged with a moving major-version tag (`v1`) as well as an exact version, so consumers pick up additive releases by refreshing their `package-lock.yml` rather than editing a pin (Overview Section 6.2).

### 13.2 Impact analysis

Before any breaking change, a steward runs the standing **"who consumes this?" query**, which unions:

- **The consumer manifest registry** (Section 12.9): every dbt consumer, including tables and incrementals, immediately and deterministically.
- **`SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY`**: which queries, roles and users touched a given object, with column-level granularity. This catches BI tools and ad hoc consumers that no dbt manifest knows about. It requires Enterprise Edition, has up to 3 hours latency and 365 days retention, and omits failed queries and intermediate views.
- **`SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES`**: views and other objects that reference a given object, including across databases. It does not record tables built by `CREATE TABLE AS SELECT`, `INSERT` or `MERGE`.

Because published objects are replaced on each publication (Section 10.10), these queries match objects by name, not by object id.

### 13.3 Versioning duty

Every published Silver model is versioned from v1. Every breaking change is made through a new dbt model version with a `deprecation_date`, following the process in Overview Section 8.2. Both versions publish side by side until the date. Additive changes need no version.

Breaking changes are batched into a predictable cadence (once or twice a year, announced in advance), following dbt's guidance for widely used models.

### 13.4 Full-refresh coordination

A full refresh of a published Silver incremental is a coordinated event (Overview Section 8.5). The stewards announce it to consumers found by the impact query, run it through the normal publication procedure so it is still gated, and log a FULL_REFRESH event in run control. History models and snapshots are never fully refreshed, since their history cannot be rebuilt beyond raw's retention.

---

## 14. Spikes Required Before the Detailed Design

The spikes are run as one **walking skeleton**: one small source, one Tier 1 project and one Tier 2 project, end to end through lease, gate, freeze, build, test, swap, canonical state, watermark and CI. Several spikes interact (notably S1, S2, S3, S6, S8 and S12), and isolated passes do not prove the combination.

|   | Spike | Question |
|---|---|---|
| S1 | View-relative resolution (**load-bearing**) | Create view `s1.v` as `select * from t`, swap `s1` with `s2`, check which `t` the view reads. Confirm `CREATE VIEW` accepts the bare identifier at creation; repeat for semantic views. Also clone `s1` and check the clone's view reads the clone's `t`. Documentation indicates a pass; failure invokes the contingency in Section 10.6 |
| S2 | Swap and grants | Establish where schema-level `USAGE` ends up after `SWAP WITH`; confirm per-object grants travel with the swap; confirm no gap and no consumer access to build schemas under the chosen grant sequence |
| S3 | Failure signalling | `EXECUTE DBT PROJECT` is documented to return `FALSE` rather than raise. Confirm the procedure reads the result reliably for build, `run-operation` and compile, including test failures and warehouse errors |
| S4 | Snapshots and history | Snapshot correctness under clone-build-discard-retry, with validity timestamps set from the unit watermark; CDC history model correctness from the source ordering column |
| S5 | Clone cost | Clone time and storage for the largest incremental models, and for cloning the largest raw tables `AT` a timestamp |
| S6 | Macros and state across targets | `generate_schema_name`, `ref` and `source` overrides across dev, CI and prod; `state:modified` and `--defer` against the canonical state |
| S7 | Dynamic tables (optional) | Whether an orchestrator-refreshed internal dynamic table, cloned to a regular table at publish, can remove the joined-model trap without reinitialising every cycle |
| S8 | Concurrent units | Two Silver units concurrently on one live-version project object with distinct artifact paths: writeback, partial parsing and `--state` behave independently; which manifest `SYSTEM$DBT_GET_LAST_SUCCESSFUL_RUN_TARGET` returns; effect of a deployment during running units |
| S9 | Frozen inputs | Frozen inputs stay invisible under per-object grants; retained storage from replaced transient clones on high-churn raw, and whether short retention contains it; classification tags carried onto derived columns and enforced in CI |
| S10 | Consistent reads across units | Upstream schema clone at gate time (Section 10.9): correctness, privileges and cost; publication windows as the alternative |
| S11 | Object identity | Effects of per-publication object replacement on time travel, data metric functions, lineage and replication; cost of DML publication (Option B) for selected Tier 1 marts |
| S12 | Unit lease | Lease acquisition under concurrent attempts, expiry, stale `STARTED` detection, and the `DEPLOYED` trigger |
| S13 | dbt Fusion (optional) | Compatibility of the custom macros and `run-operation` freeze with Fusion; whether strict static analysis validates columns of `source()` relations |

---

## 15. Alternatives Considered

| Alternative | Assessment |
|---|---|
| **Single gate over the whole Silver Layer** | One failing source would block every analytics team. Replaced by per-source publication units |
| **Base models as tables** (copying raw) | Duplicates raw storage and build compute for 1:1 shaping. Replaced by views over frozen inputs |
| **Pinned views over live raw** (watermark filter, or time travel `AT` in the view) | A watermark filter leaks in-place updates. Time travel in a view is unconfirmed and fails outright once staleness exceeds raw's retention period |
| **Test live raw, then clone** | Anything landing between the tests and the clone would be published untested. Frozen inputs are cloned first and tested in place |
| **Database-level blue/green swap**, with `ref` rendering schema-relative names | A known community pattern that fixes view binding. Rejected in favour of schema-level units: per-source isolation would need a database per unit, and the relative-reference side effects are the same. Revisit if spike S1 fails |
| **Per-table `ALTER TABLE ... SWAP`** | Not atomic across a unit, so consumers could see inconsistent tables within one source |
| **Blue/green schemas behind pointer views** | Reintroduces the view-binding problem, and repointing many views is not atomic |
| **Publishing tables by transactional DML** (Option B, Section 10.10) | Preserves object identity, at the cost of physical writes and more complex incrementals. Not the baseline; evaluated for selected Tier 1 marts in spike S11 |
| **Separate internal and published schemas per unit** | Requires two swaps with a partial-failure state between them. Replaced by one schema per unit with per-object grants |
| **Dynamic tables as published Silver objects** | Snowflake-managed refresh bypasses the gate. Internal use is optional spike S7 |
| **Reading upstream with time travel for consistent reads** | Not viable: published objects are recreated on each publication (Section 10.9) |
| **Event-driven task graph from the start** | Viable, since tasks can share one ops schema while executing project objects anywhere, but graph failure semantics must be neutralised and a dispatcher adds state. Scheduled gated tasks first; a graph or stream-driven dispatcher later if latency requires it |
| **Separate `dbt source freshness` step in the publish path** | Duplicates the gate's check against the same records and costs an extra execution per run. Kept for observability only |

---

## 16. Open Items for the Detailed Design

- Outcomes of spikes S1 to S13, and the contingency decision if S1 fails.
- Agreement of the ingestion contract with the platform team (Section 4).
- Whether the raw and analytics databases are replicated for disaster recovery, and if so, a shared replication or failover group (Section 10.10).
- The changed-key pattern for incremental entity models (Section 6.6).
- CDC history model specification: validity semantics, deletes, late-arriving changes (Section 6.7).
- Durable-key management in crosswalks: issuance, merges, splits (Section 9.3).
- Service-level targets per source, and the steward review turnaround, agreed with the platform team and consumers.
- Generator for base models and classification tags at source onboarding (Section 6.2, 6.3).
- Development and CI environments: per-developer and per-pull-request databases, how frozen inputs behave there, and canonical-state import.
- Role catalogue: per-unit service roles owning their schema pairs, steward, contributor and consumer roles; database roles for Silver readers; grant behaviour in managed-access schemas.
- Warehouse sizing per unit, resource monitors, and cost attribution by object and query tags.
- PII classification, tag carry-over and tag-based masking through frozen inputs, views and materialised outputs.
- Alerting on run-control views (Snowflake alerts or task notifications).
- The custom uniqueness test for batch-restricted source testing (Section 8).
- dbt Fusion migration criteria and path (Section 3.3).

---

## 17. References

### 17.1 dbt-Labs documentation

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

### 17.2 Snowflake documentation

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
- [AT | BEFORE (Time Travel)](https://docs.snowflake.com/en/sql-reference/constructs/at-before)
- [Object name resolution](https://docs.snowflake.com/en/sql-reference/name-resolution)
- [Task graphs and dependencies](https://docs.snowflake.com/en/user-guide/tasks-graphs) and [Triggered tasks](https://docs.snowflake.com/en/user-guide/tasks-triggered)
- [Streams](https://docs.snowflake.com/en/user-guide/streams-intro)
- [OBJECT_DEPENDENCIES](https://docs.snowflake.com/en/sql-reference/account-usage/object_dependencies) and [ACCESS_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/access_history)
- [Known limitations for dynamic tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-limitations)
- [Tag-based masking policies](https://docs.snowflake.com/en/user-guide/tag-based-masking-policies)
- [Resource monitors](https://docs.snowflake.com/en/user-guide/resource-monitors)

### 17.3 Tooling and community

- [dbt-utils](https://github.com/dbt-labs/dbt-utils)
- [Performing a blue/green deploy of your dbt project on Snowflake](https://discourse.getdbt.com/t/performing-a-blue-green-deploy-of-your-dbt-project-on-snowflake/1349)
- [Databricks: medallion lakehouse architecture](https://docs.databricks.com/aws/en/lakehouse/medallion)

---

---
