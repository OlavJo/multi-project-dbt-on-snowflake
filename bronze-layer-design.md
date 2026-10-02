# Bronze Layer Design

## The Shared Foundation for Multi-Project dbt on Snowflake

### Design Document for Bronze Stewards and Data Engineering

| | |
|---|---|
| **Version** | 1.1 |
| **Date** | 2026-10-01 |
| **Status** | **Proposed** for review |
| **Proposed By** | Lead Data Engineer, PSM CAI |
| **Companion document** | *Multi-Project dbt on Snowflake: Architecture Overview* v1.1 (referred to below as the **Overview**) |

---

## 1. Purpose and Scope

### 1.1 The Bronze Layer as analytics infrastructure

The Bronze Layer (Tier 0 Source Aggregate modules and the Tier 0.5 Core module) is the foundation every analytics project builds on. Its transformations are deliberately trivial: shaping, never business logic. Yet it is where the hardest properties of the system meet:

- **Untrusted data enters.** Freshness, ordering, landing completeness and source defects are all handled once, here.
- **Many teams share one artefact.** Tier 1 and Tier 2 projects each have one owner; the Bronze Layer has contributions from all of them.
- **Everything inherits its guarantees.** If Bronze provides them, the Tiers above get them almost for free. If it does not, no care above can recover them.
- **Its failures have the largest blast radius.**

The Bronze Layer is therefore designed and operated as **infrastructure**: with a steward team (analytics support data engineers), service levels, a release process and an operational control plane. This design can easily accomodate new sources and new analytics teams.

### 1.2 Guarantees and how they are met

The Overview (Section 3.2) promises consumers the following. Each is delivered by a specific mechanism here:

| # | Guarantee | Mechanism | Section |
|---|---|---|---|
| G1 | **Gated**: nothing published failed its tests | Clone, freeze, build and test in isolation; atomic `SWAP` on success only | 10 |
| G2 | **Consistent**: one point in time per source | Frozen inputs: all raw tables of a source cloned at one timestamp | 7 |
| G3 | **Dated**: every unit carries a watermark | Watermarks from landing-complete records, propagated as a minimum | 11, 12 |
| G4 | **Contracted and documented** | Enforced contracts, `persist_docs`, generated source package | 6.3, 13.1 |
| G5 | **Versioned** | dbt model versions with deprecation dates; impact queries | 13.2, 13.3 |
| G6 | **Shaping only** | Structural join rule, scope-creep test, steward review gate | 5.3, 6.5 |
| G7 | **Stable keys** | Durable-key rules in the Core module | 9.3 |

**Service levels** (per-source freshness targets, publication windows, time to restore) are to be agreed with the platform team (responsible for ingestion into Snowflake) and consumers and recorded in run-control configuration (Section 12.3). This document defines the mechanisms that measure them, not the targets.

### 1.3 Scope

In scope: the ingestion contract, Bronze project structure and stewardship, Source Aggregate and Core modules, frozen inputs, test placement, publication, freshness, run control, and the steward's duties towards consumers.

Publication and run control are designed here because Bronze's constraints drive them, but Tier 1 and Tier 2 projects use the same mechanism (Overview Section 9).

Open items for the detailed design are listed in Section 16.

---

## 2. Snowflake Features This Design Relies On

The architecture uses embedded dbt deliberately, and replaces missing dbt-Labs capabilities with Snowflake features (Overview Section 10). The Bronze Layer depends on these:

| Snowflake feature | Used for | Section |
|---|---|---|
| dbt project objects, `EXECUTE DBT PROJECT`, live version | Running the Bronze project inside Snowflake; persisting dbt state between runs | 3 |
| Zero-copy schema clone | Building each unit in a copy of its last good state | 10.4 |
| Zero-copy table clone with Time Travel (`CLONE ... AT`) | Frozen inputs: raw tables cloned at the unit watermark | 7 |
| `ALTER SCHEMA ... SWAP WITH` | Atomic publication of a unit | 10.4 |
| Snowflake Tasks | One gated, scheduled task per unit | 12.6 |
| Caller's rights stored procedures | The generic publish procedure | 10.4 |
| Database roles and per-object grants | Consumer access to published objects only | 10.7 |
| `persist_docs` (object and column comments) | Documentation inside Snowflake | 6.3 |
| `OBJECT_DEPENDENCIES`, `ACCESS_HISTORY`, Snowflake lineage | Impact analysis across project boundaries | 13.2 |
| Telemetry event table | dbt execution logs and traces | 12.1 |
| Tag-based masking, row access policies | PII protection, carried through frozen inputs (to verify, S9) | 7.4 |
| Resource monitors, object and query tags | Cost control and attribution | 16 |

---

## 3. Snowflake Constraints and Prerequisites

### 3.1 Constraints of dbt Projects on Snowflake

These documented Snowflake behaviours shape the design:

- **Only listed dbt commands are supported.** `build`, `run`, `test`, `seed`, `snapshot`, `run-operation`, `compile`, `list`, `parse`, `show`, `deps`, `clean`, `retry` and `source freshness` are supported through `EXECUTE DBT PROJECT`. **`source freshness`, `retry` and `clean` require a live-version project object** (Section 3.2). The newer `dbt freshness` command is not listed, so this design uses `dbt source freshness`.
- **Concurrent executions use distinct artifact paths.** Several executions of one project object may run at once. With writeback enabled, each must use distinct, non-overlapping `--target-path` and `--log-path` directories inside the project. Bronze units therefore share one project object (Section 10.3).
- **Caller's rights procedures.** Stored procedures that call `EXECUTE DBT PROJECT` must be caller's rights procedures.
- **User-managed warehouses.** Tasks that execute `EXECUTE DBT PROJECT` must specify a user-managed warehouse; serverless tasks cannot be used.
- **Dependencies are installed in CI.** Running `dbt deps` in CI and deploying with `dbt_packages` included avoids the need for an external access integration.
- **Managed dbt runtime.** Snowflake manages the dbt runtime. Arbitrary Python plugins cannot be installed.

### 3.2 The live version of dbt project objects

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
- **Opting in.** An account opts in by enabling the 2026_06 bundle (check with `SYSTEM$BEHAVIOR_CHANGE_BUNDLE_STATUS('2026_06')`), or by asking the Snowflake account representative to enable the separate single-live-version feature. New or replaced project objects are then live-version objects; existing objects migrate with `SYSTEM$MIGRATE_DBT_PROJECT`. Once the bundle is fully released, remaining versioned objects are migrated automatically, so this is Snowflake's direction of travel regardless.

---

## 4. The Ingestion Contract

The platform team is resourced to land enterprise source data in Snowflake, not to work in dbt. This is an enterprise structural reality rather than a design choice.

The boundary is treated as a **contract rather than a handoff**. Every raw object has a declared freshness expectation, monitored by analytics, and failures are attributable to ingestion before any dependent build is attempted.

The platform team provides:

> - A `_loaded_at` (or equivalent) column on every landed object.
> - For change-data-capture feeds, a **source ordering column** (sequence number, log sequence number, or source commit timestamp plus operation order). `_loaded_at` alone cannot order changes landed in the same batch, and CDC deduplication is impossible without a reliable ordering.
> - A **landing-complete record** written to a platform-owned control table per source system and batch, including batches with zero new rows. This record is the authoritative "data as of" signal for each source. It drives freshness (Section 11.3), the freeze timestamp (Section 7) and the raw watermark (Section 12.4). `max(_loaded_at)` is not a substitute, because a quiet source with no new rows would appear stale.
> - **Time Travel retention** on raw tables long enough to clone at the watermark (minutes in normal operation; to be agreed).

Analytics is granted read access to the landing-complete table. The platform team needs no access to analytics objects.

**Attribution.** Every Bronze failure is classed as `source` (ingestion owns it) or `transform` (stewards own it), from where the failing test sits (Section 8). Run control records the class (Section 12.2).

---

## 5. Structure and Stewardship

### 5.1 One project, one repository

The Bronze Layer is a single dbt project in a single repository ("monorepo"):

- One folder (module) per source system, plus the Core module.
- Shared macros and generic tests.
- One live-version dbt project object in Snowflake, executed per unit (Section 10.3).

A single project gives compile-time `ref()` between Core and the Source Aggregate modules, one CI pipeline, one set of conventions, and full in-project lineage. It reinforces the Bronze Layer as one catalogue of analysis-ready sources.

### 5.2 Database and schema layout

| Object | Example | Visible to consumers |
|---|---|---|
| Bronze database | `bronze` | Analytics roles only |
| Published schema per source module | `bronze.erp`, `bronze.crm` | Yes (published objects only, Section 10.7) |
| Build schema per source module | `bronze.erp__build` | No |
| Published and build schemas for Core | `bronze.core`, `bronze.core__build` | Published only |
| Run control | `analytics_ops.control` | Read access for stewards and project teams |

Frozen inputs live **inside** each published schema, alongside the objects that read them, but are never granted to consumers (Section 7.3).

### 5.3 Stewardship

The no-business-logic rule for Tier 0 is the load-bearing rule of this architecture. It erodes one reasonable-looking pull request at a time unless it is actively defended.

- A named **steward team (of data engineers)** owns the repository, CI, releases, run control and on-call for the Bronze Layer.
- `CODEOWNERS` assigns each source module, and the Core module, to stewards. Contributions from any analytics team are welcome; merges require steward approval.
- Stewards review against the structural join rule (Section 6.5), the scope-creep test (Section 6.5.2) and test placement (Section 8).

### 5.4 Splitting later

If the repository becomes too large, splitting by source system is straightforward and yields essentially independent repositories, because Source Aggregate modules have no cross-source joins. Different load cadences do **not** require a split: each module is already an independent publication unit with its own schedule. Splitting is not expected, and this document treats the Bronze Layer as one project.

---

## 6. Source Aggregate Modules (Tier 0)

### 6.1 Definition

A Source Aggregate module reconstructs one source system's **logical data models** as denormalised, contract-enforced, tested aggregates. It is the single source of truth for operational data entering the analytics warehouse, and **it contains no business logic**.

Each module is a source-named folder in the Bronze project. **Modules use sources, staging models, snapshots, seeds, macros and tests.** Intermediate and mart models are excluded: they live in Tiers 1 and 2.

### 6.2 What a module contains, and how it is materialised

| Type | dbt Folder | Purpose | Materialisation |
|------|-------|---------|-----------------|
| Frozen inputs (`raw__*`) | n/a (created by the freeze step) | Zero-copy clones of the source's raw tables, all at the unit watermark. Declared as a dbt source; never granted to consumers | Clone (Section 7) |
| Base models | `staging/<source>/base/` | 1:1 with source tables: cast, rename, reshape columns, filter soft deletes | View over the frozen input |
| CDC current state | `staging/<source>/base/` | Deduplicate change-data-capture rows to one current row per key | Incremental table |
| Aggregates | `staging/<source>/` | Denormalise normalised source tables into logical entities (DDD aggregates without business logic); union tables split in the source | Incremental table (or table) |
| Snapshots | `snapshots/<source>/` | SCD2 history for sources lacking change capture | Snapshot (table) |
| Helpers | `staging/<source>/` | Internal logic shared by the above | Ephemeral |

**Publish on demand.** A base model is published only when a Tier 1 project consumes it. Base models that only feed aggregates are ephemeral.

### 6.3 Required declarations

- **Two source declarations per raw source.** One for the live raw tables, used only by `dbt source freshness` and the freeze step. One for the frozen inputs, used by every model and source test, resolving to the build schema of the unit being published.
- **Source freshness** on every live raw source: `warn_after` and `error_after`, with thresholds agreed with the platform team, measured with `loaded_at_query` against the landing-complete records (Section 11.3).
- **Contracts:** `contract: enforced: true` on every published model.
- **Documentation:** `persist_docs` enabled for relations and columns. Every published column is described and typed, and descriptions state which tests the column has passed, so consumers can make consistent assumptions.
- **CDC deduplication** uses the source ordering column (Section 4), not `_loaded_at` alone.
- **Grants:** dbt `grants` config on published models only (Section 10.7).

### 6.4 What a module does NOT contain

- Any join or filter driven by a **business decision**.
- Any **cross-source-system** join. Cross-source joins for identity and conformance belong in the Core module; all others belong in Tier 1.
- Any derived metric or KPI resting on a business rule.
- Any model whose grain changes based on a business filter.

### 6.5 The structural join rule

Joins are permitted in an aggregate **if and only if** they are:

- Driven by the source's own **foreign key relationship**.
- Grain-preserving for the aggregate root, or a strict many:1 enrichment.
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

"Strict many:1" is a claim, and an unnoticed 1:N join silently inflates every metric downstream of it. Fan-out is a defect in Bronze code, so these tests sit on outputs (Section 8). Every published aggregate carries, at `error` severity:

- `unique` or `dbt_utils.unique_combination_of_columns` on its declared grain.
- `dbt_utils.relationships_where` on each FK used for enrichment.
- A row-count guard asserting the aggregate's row count equals its root table's row count (for grain-preserving aggregates).

### 6.6 Incremental aggregates: the joined-model trap

A denormalised aggregate materialised incrementally on its root key **will silently miss updates to joined child tables** if the incremental filter only looks at the root. For example, on the `orders` aggregate with `unique_key: order_id` and `merge`, filtered on the order's own timestamp, a change to an order *item* is never noticed.

The required pattern is to collect changed keys from **every** contributing frozen input, then rebuild those roots in full. The pattern is specified in the detailed design.

Snowflake dynamic tables handle incremental refresh across joins natively and would remove this trap. Under Snowflake-managed refresh, however, they change data outside any gate, and consumers of a dynamic table are not recorded in `ACCESS_HISTORY`. They are not used for published objects; using them as an internal, orchestrator-refreshed engine is optional spike S7.

### 6.7 Snapshots: capture at base grain

Snapshotting the *denormalised aggregate* is tempting. However, any change to any joined-in column spawns a new SCD2 row, so history becomes high-churn and expensive.

**Rule:** snapshots capture base models at their natural grain. Denormalisation happens downstream of the snapshot, not upstream of it.

### 6.8 Aggregate design decisions

Per source system, consider the analytical use cases to decide:

- Whether to emit the **parent with child aggregations** (`orders` with item count, total quantity).
- Whether to emit the **child with parent attributes** (`order_items` enriched with order header fields).
- Whether to emit **both**, which is common for highly normalised sources.
- Whether to snapshot the underlying entities, and at what grain.

---

## 7. Frozen Inputs

### 7.1 Principle

**Nothing in the Bronze Layer reads live raw data.** Each build of a Source Aggregate unit first clones every raw table of the source, **all at the same timestamp** (the unit watermark from the latest landing-complete record), into the unit's build schema. Every model and every source test reads these frozen inputs.

### 7.2 The freeze step

The freeze is a dbt `run-operation` macro, invoked by the publish procedure (Section 10.4). It reads the live-raw source declaration, so the list of tables lives in dbt alongside the models, and development and CI run the same logic. For each raw table it performs the equivalent of:

```sql
CREATE OR REPLACE TABLE bronze.erp__build.raw__orders
  CLONE raw.erp.orders AT (TIMESTAMP => :watermark);
```

The Core module has no frozen inputs: it reads published Source Aggregate outputs and seeds, never raw.

### 7.3 Consequences

- **Gateable views without copying data.** A view over a frozen input cannot show data newer than the input, so 1:1 base models are views. Raw data is not duplicated into Bronze.
- **A consistent snapshot across tables (G2).** All tables of a source are read at one point in time, so a batch landing mid-build can never split a join.
- **One point in time.** The frozen inputs, the unit watermark and the freshness check all refer to the same landing-complete record.
- **Frozen inputs publish with the unit.** They live in the published schema and swap with the views that read them. They are never granted to consumers (Section 10.7).
- **Incremental models get stable inputs.** Their change detection reads the same frozen inputs as everything else.

### 7.4 Costs and risks

- **Query-time shaping.** Base views re-execute their casts and renames on every consumer query. This is cheap for light 1:1 shaping; heavier work stays in incremental tables.
- **Retained storage.** A clone shares micro-partitions with raw at creation. A replaced clone's unique micro-partitions (those raw has since rewritten) are retained for Time Travel and then Fail-safe. On high-churn raw tables, this accumulates; a short retention period on Bronze schemas, or transient clones, should contain it (spike S9).
- **Clone time.** Cloning is metadata-only but not instantaneous for tables with many micro-partitions (spike S5).
- **Governance policies.** Masking and row access policies on raw must carry over to the clones, or be re-applied (spike S9).
- **View name binding.** Base views reference frozen inputs in their own schema, which depends on spike S1 (Section 10.5). A fallback exists (Section 10.6).

---

## 8. Test Placement

**Each assertion is tested where it can first fail, and only there.**

- **Source defects** are problems in what landed. Bronze code cannot cause or repair them. They are tested once, on the frozen inputs.
- **Transformation defects** can only be introduced by Bronze code. They are tested on the outputs.

A property that passes unchanged through a 1:1 base view (a key not null, an accepted value) is not re-tested on the view.

| Assertion | Tested on | Owner of a failure |
|---|---|---|
| Freshness | Live raw source (`dbt source freshness`) | Ingestion |
| Not null, accepted values, castability of raw columns | Frozen input | Ingestion |
| Uniqueness at the source's own grain (full-load tables) | Frozen input | Ingestion |
| Referential integrity within the source | Frozen input | Ingestion |
| One current row per key after CDC deduplication | Output | Stewards |
| Aggregate grain, row-count guards, enrichment FKs (6.5.3) | Output | Stewards |
| Snapshot validity (one current record per key, no overlapping validity ranges) | Output | Stewards |
| Contracts (names, types) | Output | Stewards |
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
2. **Cross-source identity resolution:** a customer in the ERP and the same customer in the CRM. Source Aggregate modules forbid cross-source joins, and Tier 1 is domain-scoped, so neither can own this.

Identity resolution is honestly **business logic**: matching rules are judgements, and two competent analysts could write them differently. It fails the scope-creep test. The Core module exists because this logic must be shared and must have exactly one owner. It is the **one governed exception** where shared business logic lives below Tier 1, and it is governed accordingly.

The Core module is expected to remain small, with critical but niche datasets.

### 9.2 What it contains

| Layer | Purpose | Materialisation |
|-------|---------|-----------------|
| Conformed dimensions | `dim_core__date`, `dim_core__currency`, `dim_core__fiscal_period` | Table |
| Crosswalks | `xref_core__customer` mapping source-system keys to a durable `customer_key` | Table (snapshot-backed) |
| Reference seeds | Curated lists, mappings, hierarchies under version control | Seed (Table) |

### 9.3 Governance

The Core module is the **only** Bronze module permitted to join across source systems, and that privilege is narrowly scoped:

- It may join across sources **for identity resolution or conformance only**, not to answer a business question.
- Every crosswalk publishes its **match rate and unmatched-key counts as tested metrics**, with an `error`-severity floor. An entity-resolution rule that quietly stops matching is worse than one that fails.
- **Durable keys are never reassigned (G7).** Once a `customer_key` is issued, it continues to identify the same real-world entity when matching rules change. Merges and splits are recorded explicitly (for example, a superseded-key mapping), never by silently re-pointing a key. Key stability is harder than match rate and is specified in the detailed design.
- Matching rules are documented in prose next to the model, and any change to them is treated as breaking (Overview Section 8.4).
- It is contracted and versioned exactly as Source Aggregate outputs are.
- It is its own publication unit (`bronze.core`).

### 9.4 Why not simply relax Tier 0's rule?

The no-cross-source-join rule is the clearest, most enforceable statement in this architecture, and a carve-out inside Tier 0 could be used to justify a genuine business join. Isolating the exception in a separate module with its own review gate keeps Tier 0's rule absolute.

---

## 10. Publication (Write-Audit-Publish)

### 10.1 Two requirements in tension

With a plain `dbt build`, tests run after models are replaced. By the time a test fails, published tables have already changed. **Tests observe the incident rather than prevent it.**

Gating publication on tests fixes this. But a single gate over the whole Bronze Layer means one failing source blocks every analytics team. Both are required:

> **(a)** Nothing ever builds on data that failed its tests.  
> **(b)** A failure blocks only what depends on it.  

### 10.2 Principle: failures block by staleness, not by status

A failed build never replaces published data. The last good version keeps serving, and the failure shows up downstream as **staleness**: the unit's watermark stops advancing.

Downstream units are never gated on an upstream *failure*. They are gated on upstream *freshness* against their own declared tolerance (Section 11). This satisfies both requirements:

- Failed data is never published, so nothing can read it (a).
- Unrelated units are unaffected (b).
- Dependent units either proceed on last-good data within tolerance, or skip.

### 10.3 Publication units

A **publication unit** is the smallest set of objects published atomically. Each unit has exactly one consumer-visible (published) schema and one build schema, and optionally one internal schema.

| Tier | Unit | Example schemas |
|---|---|---|
| Tier 0 | One per source system | `bronze.erp`, `bronze.erp__build` |
| Tier 0.5 | One unit | `bronze.core`, `bronze.core__build` |
| Tier 1 / 2 | One per project | `finance.published`, `finance.published__build`, plus internal `finance.internal`, `finance.internal__build` |

Source systems work as units because Source Aggregate modules forbid cross-source joins: each is an independent subgraph. Within a unit, publication is all-or-nothing. Across units, publication is independent, and each unit keeps its own schedule.

All Bronze units share one live-version dbt project object and run concurrently, each with its own `--target-path` and `--log-path` (Section 3.1).

### 10.4 Publish procedure

One generic, caller's rights procedure, parameterised by unit and driven by run-control configuration (Section 12), performs:

```sql
-- 0. Gate (Section 12.6). For Bronze units this includes dbt source freshness.
--    If the gate fails, log SKIPPED and stop.

-- 1. Clone last good state (zero-copy; preserves incremental and snapshot state)
CREATE OR REPLACE SCHEMA bronze.erp__build CLONE bronze.erp;

-- 2. Freeze (Source Aggregate units only): replace the frozen inputs with
--    clones of raw at the new watermark (Section 7.2)
EXECUTE DBT PROJECT bronze.ops.bronze_layer
  ARGS = 'run-operation freeze_sources --args "{unit: erp, watermark: ...}"
          --target-path target/erp --log-path logs/erp';

-- 3. Test sources, build, test outputs: one dbt build
EXECUTE DBT PROJECT bronze.ops.bronze_layer
  ARGS = 'build --select erp --vars {publish_unit: erp}
          --target-path target/erp --log-path logs/erp';

-- 4. On success only: atomic publish, then restore container grants
ALTER SCHEMA bronze.erp__build SWAP WITH bronze.erp;
GRANT USAGE ON SCHEMA bronze.erp TO DATABASE ROLE bronze_reader;

-- 5. Log PUBLISHED with watermark, or FAILED with failure_class (Section 12)
```

The order is **freeze, test sources, build, test outputs, publish**. Tests always run against the exact frozen copy that will be published. Testing live raw and cloning afterwards would publish anything that landed between the two steps untested.

If step 3 fails, nothing published has changed. The previous frozen inputs, views and tables keep serving together, out of date but correct. There is no rollback, because there is nothing to roll back. The build schema is discarded and re-cloned at the next run.

For units with an internal schema (Tier 1 and Tier 2), both schemas are cloned and built, then swapped **internal first, published second**. Only the published schema is consumer-visible, so atomicity is only required there.

**Schema redirection.** A custom `generate_schema_name` sends only the models of the unit being published (`var('publish_unit')`) into `__build` schemas. All other references resolve to published schemas. This is how the Core module reads last-good Source Aggregate outputs through `ref()` while keeping in-project lineage. Sketch (to be validated, and extended for dev and CI targets):

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

dbt renders fully qualified names, so a view in `bronze.erp__build` referencing a sibling object would still point at `bronze.erp__build` after the swap, reading the stale objects now living under that name.

If Snowflake resolves an unqualified identifier inside a view relative to the view's own schema, then rendering **same-schema references from views** as bare identifiers makes views swap-safe. References to other schemas stay fully qualified; they follow the swap by name. Tables, incrementals and tests stay fully qualified, because their SQL resolves against the session. Sketch:

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

The same treatment applies to `source()` references from base views to frozen inputs. Base views depend on this, so **spike S1 is load-bearing.**

### 10.6 Fallback if spike S1 fails: in-place frozen inputs

Each Source Aggregate unit gets a separate, permanent source schema (for example `bronze.erp_src`) holding the frozen inputs and base views. The views never move schema, so the binding question disappears. Once source tests pass against newly cloned inputs in a staging area, the inputs are replaced in place in `erp_src`. Aggregates and CDC tables continue to use build-and-swap in `bronze.erp`. The costs:

- **Unit atomicity is lost.** DDL auto-commits, so replacing N inputs happens at N separate moments; a consumer joining two base views mid-sequence can see mismatched watermarks.
- **Base views and aggregates can drift.** If source tests pass but an output test fails, base views advance while aggregates stay on the previous watermark.

The fallback is still preferable to materialising base models as tables.

### 10.7 Rules

1. **Nothing published reads live raw data.** Bronze models read frozen inputs. A view over live raw changes the moment data lands, so it cannot be gated. This holds regardless of spike S1.
2. **Views in published schemas** reference other units' objects fully qualified, and same-unit objects only through bare identifiers (Section 10.5). Snowflake semantic views are included in this rule and in spike S1.
3. **Tests are placed by Section 8, and severity carries meaning.**
4. **Retries are idempotent.** Every run starts from a clone of the last good state, so a failed run can simply be re-run. This holds for incrementals and snapshots alike.
5. **Consumers are granted object by object, never on build schemas.** Grants on child objects travel with a schema clone and a swap; grants on the schema itself do not, so the procedure re-applies schema USAGE after every swap. Object grants come from dbt's `grants` config on published models only, never from future grants on tables. This keeps frozen inputs invisible to consumers.
6. **Each unit has its own artifact paths** on the shared project object.

### 10.8 Failure behaviour

| Failure | Published result | Downstream |
|---|---|---|
| Raw ERP batch does not land | No new landing-complete record; raw ERP watermark stops advancing; ERP gate skips | Staleness visible in run control, owned by ingestion |
| ERP source test fails on frozen inputs | `bronze.erp` stays at last good; no ERP models built | FAILED with `failure_class: source`, owned by ingestion. As below |
| ERP output test fails | `bronze.erp` stays at last good | `crm` unaffected. `core` and ERP-dependent Tier 1 projects proceed on last-good ERP if within tolerance, otherwise skip; their watermarks inherit ERP's older watermark |
| Core crosswalk match rate below floor | Previous crosswalk keeps serving | All consumers stay on the last accepted identity mapping |
| Finance build fails | Finance marts stay at last good | Tier 2 gated by finance freshness |

### 10.9 Open issue: consistent reads across publication units

Frozen inputs make each Source Aggregate unit read raw at a single point in time. Units that read *other units* (the Core module, Tier 1 and Tier 2 projects) read published schemas directly. A swap is atomic, but a downstream build runs many queries over several minutes. If an upstream unit publishes mid-build, early models may read the old version and later models the new one.

The watermark recorded at gate time understates freshness in this case (the safe direction), but models within one downstream build can disagree. Candidate mitigations, for spike S10:

- **Generalise frozen inputs.** Each downstream unit clones the upstream published tables it reads at gate time. This is cheap for tables, but published base views cannot be cloned as data.
- **Read upstream with Time Travel** at the gate timestamp, if Time Travel applies to the objects read (confirm for views).
- **Publication windows.** Run control prevents an upstream unit from swapping while a dependent build that read it is in progress. This couples schedules and adds state to run control.

---

## 11. Freshness and Watermarks

### 11.1 The silent-wrongness problem

Suppose a Tier 2 project is triggered after all upstream Tier 1 tasks have completed. Finance completed this morning; digital last succeeded three days ago. Tier 2 builds successfully, publishes a cross-domain fact mixing fresh and stale data, and **no test anywhere fails.** The dashboard is wrong with no warning.

Completion is not freshness. Every cross-unit dependency needs a freshness assertion, not just a completion signal.

### 11.2 Propagated watermarks

Every publication unit carries a **watermark**: the time its data is current as of.

- A raw source's watermark is the time of its latest landing-complete record (Section 4).
- A Source Aggregate unit's watermark is the freeze timestamp of its frozen inputs.
- Any other unit's watermark is the **minimum of its upstream watermarks**, read at gate time.

Staleness therefore propagates automatically. In the example above, the Tier 2 unit's watermark is digital's three-day-old watermark, not finance's fresh one.

Each unit declares a **tolerance per upstream dependency** (Section 12.3). Before building, the unit's gate checks every upstream watermark against its tolerance, and skips if any is exceeded. Skipping leaves the last good version published and the watermark unchanged, so the staleness remains visible. Dashboards display the watermark as **"data as of"**.

### 11.3 Source freshness at the raw boundary

Every live raw source declares dbt source freshness, with thresholds agreed with the platform team. `dbt source freshness` runs as the first step of each Source Aggregate unit's gate. A breach of `error_after` skips the unit's build, with an alert whose owner is unambiguously ingestion. This requires a live-version project object (Section 3.2).

Freshness is measured with `loaded_at_query` (dbt v1.10 and later) rather than `loaded_at_field`. By default, dbt measures freshness as `max(loaded_at_field)`, which makes a quiet source with no new rows look stale. Pointing `loaded_at_query` at the landing-complete records measures when the source was last confirmed, including batches with zero new rows. Illustrative sketch:

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

Run control reads the same landing-complete records to derive raw watermarks (Section 12.4), so the freshness check, the freeze timestamp and the watermark cannot disagree.

---

## 12. Run Control

### 12.1 Design principle: record facts, not state

Run control coordinates publication, freshness and full-refresh events across all units, in all Tiers. Its main simplicity lever is that it **records facts rather than maintaining state**: rows are appended, never updated, and all current state is derived by views. The complete design is:

- one append-only event log
- two small configuration tables, maintained under version control
- a view of platform landing records
- two status views
- the single publish procedure (Section 10.4)
- one scheduled task per unit

Run control operates at **unit granularity only**. Per-model results stay in dbt's artifacts (written back to the live-version project object) and Snowflake's telemetry event table, and are not duplicated.

Run control lives in its own database or schema (for example `analytics_ops.control`), owned by the steward role.

### 12.2 The event log

```sql
create table analytics_ops.control.publish_log (
  event_ts    timestamp_ltz default current_timestamp(),
  unit        varchar,        -- 'bronze.erp', 'tier1.finance'
  event_type  varchar,        -- STARTED | PUBLISHED | FAILED | SKIPPED | FULL_REFRESH
  watermark   timestamp_ltz,  -- data as-of, set on PUBLISHED
  run_id      varchar,
  detail      variant         -- includes failure_class: source | transform on FAILED
);
```

The log is append-only: no updates, no `MERGE` contention, and a complete audit trail by construction. Writers are restricted to:

- the publish procedure (STARTED, PUBLISHED, FAILED, SKIPPED)
- a small steward procedure for FULL_REFRESH events

### 12.3 Configuration

Static configuration is maintained in a small ops repository and deployed as seeds or tables. Changes go through pull request review.

```sql
-- One row per publication unit, including raw units owned by the platform team
units (unit, tier, owner, dbt_project_object, schedule)

-- One row per dependency edge; this is the cross-project dependency graph
unit_dependencies (unit, upstream_unit, max_age_hours)
```

The dependency graph is declared, never inferred at runtime. Service-level targets (Section 1.2) are recorded here as tolerances.

### 12.4 Raw units and the landing boundary

The platform team keeps writing its own landing-complete table (Section 4); analytics has read access only. A view, `v_landing_complete`, presents each landing-complete record as a PUBLISHED event for the corresponding raw unit (for example `raw.erp`). Raw units therefore appear in status and gating exactly like any other unit, with the platform team as owner.

The same view is the target of the `loaded_at_query` used by `dbt source freshness` (Section 11.3), so there is a single source of truth for raw freshness.

### 12.5 Status views

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
  max_by(event_type, event_ts)                                       as last_event,
  max(event_ts)                                                      as last_event_at
from events
group by unit;
```

A second view, `unit_freshness`, joins `unit_status` to `unit_dependencies` and flags every edge whose upstream watermark exceeds its tolerance. This is the operational dashboard for stewards and project teams, and the natural target for Snowflake alerts.

### 12.6 Gating and scheduling

As an initial approach, each unit runs as a **scheduled task with a gate**, not as part of an event-driven cross-project task graph. The gate proceeds only when both conditions hold:

1. Every upstream watermark is within its tolerance. For Source Aggregate units, this includes `dbt source freshness` on the unit's raw sources, where an `error_after` breach counts as a failed tolerance.
2. At least one upstream has published since this unit last published. Otherwise there is nothing new to build.

If either condition fails, the procedure logs SKIPPED and exits. On success, the new watermark is the minimum of the upstream watermarks read at gate time (for Source Aggregate units, the freeze timestamp).

This avoids cross-database task graphs and a dispatcher entirely. 

If latency later matters, the intial approach can be enhanced by using a stream on `publish_log` feeding one dispatcher triggered task can replace the schedules, and the log design does not change.

### 12.7 Freshness visible in dbt

`unit_status` is included as a source in the generated source package, together with a generic `upstream_fresh(unit, max_age)` test. Tier 1 and Tier 2 developers then see upstream freshness in their own dbt output, without querying run control directly.

---

## 13. Serving Consumers

### 13.1 The generated source package

Bronze CI generates the consumers' source definitions from the Bronze manifest (published relations, column names, types and descriptions, model versions) and releases a new package version automatically with each Bronze release. The package also carries shared macros and generic tests, including `upstream_fresh`. It contains no models. Generation removes source-sync drift as a failure mode. Frozen inputs and other unpublished objects are excluded.

### 13.2 Impact analysis

Before any breaking change, a steward runs the standing **"who consumes this?" query**:

- **`SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES`**: which objects reference a given object, including across databases.
- **`SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY`**: which queries, roles and users touched a given object, with column-level granularity. This catches BI tools and ad hoc consumers that no dbt manifest knows about.

This is strictly better than manifest stitching for impact analysis, because it sees consumers entirely outside dbt.

### 13.3 Versioning duty

Every breaking change to a published Bronze model is made through a new dbt model version with a `deprecation_date`, following the process in Overview Section 8.2. Both versions publish side by side until the date. Additive changes need no version.

### 13.4 Full-refresh coordination

A full refresh of a published Bronze incremental is a coordinated event (Overview Section 8.5). The stewards announce it to consumers found by the impact query, run it through the normal publication procedure so it is still gated, and log a FULL_REFRESH event in run control.

---

## 14. Spikes Required Before the Detailed Design

| # | Spike | Question |
|---|---|---|
| S1 | View-relative resolution (**load-bearing**) | Create view `s1.v` as `select * from t`, swap `s1` with `s2`, check which `t` the view reads. Confirm `CREATE VIEW` accepts the bare identifier at creation; repeat for semantic views. Failure invokes the fallback in Section 10.6 |
| S2 | Swap and grants | Measure the privilege gap between `SWAP` and `GRANT USAGE`; confirm per-object grants travel with the swap |
| S3 | Failure signalling | Does `EXECUTE DBT PROJECT` raise, or return a status, on test failure inside a procedure? Repeat for a `source freshness` `error_after` breach |
| S4 | Snapshots | Snapshot correctness under clone-build-discard-retry |
| S5 | Clone cost | Clone time and storage for the largest incremental models, and for cloning the largest raw tables `AT` a timestamp |
| S6 | Macros across targets | `generate_schema_name`, `ref` and `source` overrides across dev, CI and prod |
| S7 | Dynamic tables (optional) | Whether an orchestrator-refreshed internal dynamic table, cloned to a regular table at publish, can remove the joined-model trap without reinitialising every cycle |
| S8 | Concurrent units | Two Bronze units concurrently on one live-version project object with distinct artifact paths: writeback, partial parsing and `--state` behave independently |
| S9 | Frozen inputs | Masking and row access policies carry over to table clones; frozen inputs stay invisible under per-object grants; retained storage from replaced clones on high-churn raw, and whether short retention or transient clones contain it |
| S10 | Consistent reads across units | Which mitigation in Section 10.9 gives consistent upstream reads for Core, Tier 1 and Tier 2 builds |

---

## 15. Alternatives Considered

| Alternative | Assessment |
|---|---|
| **Single gate over the whole Bronze Layer** | One failing source would block every analytics team. Replaced by per-source publication units |
| **Base models as tables** (copying raw) | Duplicates raw storage and build compute for 1:1 shaping. Replaced by views over frozen inputs |
| **Pinned views over live raw** (watermark filter, or Time Travel `AT` in the view) | A watermark filter leaks in-place updates; Time Travel in a view is unconfirmed and fails outright once staleness exceeds raw's retention period |
| **Test live raw, then clone** | Anything landing between the tests and the clone would be published untested. Frozen inputs are cloned first and tested in place |
| **Database-level blue/green swap**, with `ref` rendering schema-relative names | A known community pattern that fixes view binding. Rejected in favour of schema-level units: per-source isolation would need a database per unit, and the relative-reference side effects are the same. Revisit if spike S1 fails |
| **Per-table `ALTER TABLE ... SWAP`** | Not atomic across a unit, so consumers could see inconsistent tables within one source |
| **Blue/green schemas behind pointer views** | Reintroduces the view-binding problem |
| **Dynamic tables as published Bronze objects** | Snowflake-managed refresh bypasses the gate, and their consumers are invisible to `ACCESS_HISTORY`. Internal use is optional spike S7 |
| **Event-driven cross-project task graph** | Cross-database task graphs carry placement constraints (to confirm), and a dispatcher adds state. Scheduled gated tasks first; a stream-driven dispatcher later if latency requires it |

---

## 16. Open Items for the Detailed Design

- Outcomes of spikes S1 to S10, and the fallback decision if S1 fails.
- The changed-key pattern for incremental aggregates (Section 6.6).
- Durable-key management in crosswalks: issuance, merges, splits (Section 9.3).
- Service-level targets per source, agreed with the platform team and consumers.
- Development and CI environments: zero-copy clones of production Bronze, per-pull-request databases, and how frozen inputs behave there.
- Role model: steward, contributor, consumer and service roles; database roles for Bronze readers.
- Warehouse sizing per unit, resource monitors, and cost attribution by object and query tags.
- PII classification and tag-based masking through frozen inputs and views.
- Alerting on run-control views (Snowflake alerts or task notifications).
- The custom uniqueness test for batch-restricted source testing (Section 8).

---

## 17. References

### 17.1 dbt-Labs documentation

- [dbt: Staging](https://docs.getdbt.com/best-practices/how-we-structure/2-staging)
- [dbt: Snapshots](https://docs.getdbt.com/docs/build/snapshots)
- [dbt: Data contracts](https://docs.getdbt.com/docs/collaborate/govern/model-contracts)
- [dbt: Model versions](https://docs.getdbt.com/docs/collaborate/govern/model-versions)
- [dbt: Incremental models](https://docs.getdbt.com/docs/build/incremental-models)
- [dbt: persist_docs](https://docs.getdbt.com/reference/resource-configs/persist_docs)
- [dbt: freshness (including `loaded_at_query`)](https://docs.getdbt.com/reference/resource-configs/freshness)

### 17.2 Snowflake documentation

- [dbt Projects on Snowflake](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake)
- [dbt Projects on Snowflake: supported commands](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-supported-commands)
- [dbt Projects on Snowflake: limitations](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-limitations)
- [dbt project objects migrate to a single mutable live version (2026_06 bundle)](https://docs.snowflake.com/en/release-notes/bcr-bundles/2026_06/bcr-2362)
- [Cloning considerations](https://docs.snowflake.com/en/user-guide/object-clone) and [SWAP WITH](https://docs.snowflake.com/en/sql-reference/sql/alter-schema)
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
