# Multi-Project dbt Architecture on Snowflake

## A Tiered Governance Model for Independent Teams

### High Level Design: Engineering Discussion Document

| | |
|---|---|
| **Version** | 2.2 |
| **Date** | 2026-09-29 |
| **Status** | **Proposed** for review |
| **Author** | Olav Jordens, Lead Data Engineer |


---

## 1. Executive Summary

This document describes a tiered multi-project dbt architecture for Snowflake environments where dbt Mesh is unavailable. 

It balances **enterprise governance** (consistent naming, contracts, tests, access control) with **team independence** (separate CI, separate build cadences, separate ownership).

This multi-project architecture **classifies dbt Projects by Tier**:


| Tier | Name | Purpose | Owner |
|------|------|---------|-------|
| — | Raw / Landing | Source data loaded into Snowflake | Platform team (ingestion only) |
| 0 | Source Aggregate Project(s) | Reconstruct source systems' logical models as ready-to-consume aggregates | Analytics |
| 0.5 | Core / Conformance Project | Conformed dimensions, shared spines, cross-source identity resolution | Analytics |
| 1 | Domain Project(s) | Business transformation: dims, facts, marts, semantic definitions | Analytics/ Domain |
| 2 | Cross-Domain Project(s) | Analysis spanning two or more domains | Analytics/ Business-question owner |

Projects reference each other's production outputs via `source()`, with source definitions distributed as a **version-pinned dbt package** (§8.2) rather than copied by hand.

---

## 2. Design Constraints

- **Snowflake** as warehouse; **dbt-Labs dbt Mesh / Cloud Enterprise unavailable**
- **dbt Projects on Snowflake** as the execution mechanism — projects are Snowflake objects created from a Git repository and invoked with `EXECUTE DBT PROJECT` (§12.1)
- **Multiple independent teams** with separate sprints, CI, and ownership
- **Snowflake Tasks** for scheduling and cross-project coordination
- **No Git packages containing models** as the inter-project mechanism. This avoids accidental package rebuild costs, and decouples development. Dependency packages carry *only* source definitions, macros and tests, ensuring projects are only coupled through their contracted outputs (§8.2)
- **Platform team owns ingestion only.** Analytics owns everything from raw onward (§13)

### 2.1 The ingestion boundary

The platform team is resourced to land source data in Snowflake, not to work in dbt. This is an enterprise structural reality rather than a design choice.

This architecture creates the boundary as a **contract rather than a handoff** (§13.4): declared freshness expectations on every raw object, monitored by analytics, with failures attributable to ingestion before any Tier 0 build is attempted. If a raw table is late or malformed, Tier 0 should fail loudly at the boundary with an unambiguous owner, rather than produce a partially-correct aggregate that domain teams then debug independently.

We expect: 
> -  the platform team to ensure that a `_loaded_at` (or equivalent) column appears on every landed object. Without it, freshness monitoring and CDC deduplication are both impossible. 
> - A landing-complete signal written to a control table per source system and batch.

---

## 3. Project Tiering

Cross-project references are constrained by a fixed tiering structure. This provides an acyclicity guarantee on dependencies. The purpose is to support cross project collaboration and to ease sharing of common datasets. Each dbt project at every Tier is expected to provide contracted (production) outputs which are made available as sources for subsequent projects.

```
Raw / Landing
  │
  └──► Tier 0 (Source Aggregates)           ◄─ may read Raw only              ┐
        │                                                                     ├─  Bronze Layer 
        ├──► Tier 0.5 (Core)                ◄─ may read Tier 0 (+ seeds)      ┘     (Single dbt project)
        │
        └──► Tier 1 (Domains)               ◄─ may read Tier 0 and Tier 0.5   ┐   Silver (internal/ intermediate) 
              │                                                               ├─    and Gold (exposure) Layer
              └──► Tier 2 (Cross-Domain)    ◄─ may read Tier 1 marts          ┘     (Multiple dbt projects)
```

Analytics projects will tend to focus on Tier 1 dbt projects. Each analytic project initially needs to assess the existing Tier 0 and Tier 0.5 (production) contracted outputs as a base. The analytics project may be required to contribute to Tier 0 and Tier 0.5 where the base is lacking. This contribution supports shared Source Aggregates or Core outputs. So Tier 0 and 0.5 form shared analytics *internal* resources, receiving contributions from across all analytics teams. Tier 1 and Tier 2 provide contracted outputs for exposure to the business.

NOTE: 

 > - Both dbt projects and the Snowflake platform itself capture lineage. Cross Tier lineage (cross dbt project lineage) is captured within Snowflake itself.
 > - Tier 0 is purely about data shaping and requires no business logic.
 > - Tier 0.5 is a consolidation layer taking care of common cross source aggregate joins.
 > - Tiers 1 and 2 capture business logic - this is where data modelling comes to the fore. These tiers expose the Gold layer dims, facts, aggs, semantic views
 

**Tier assignment**:

> 1. All Tier 0 and Tier 0.5 projects (Bronze Layer) operate initially from a single "monorepo" using the standard dbt folder structure to separate source systems. This monorepo includes macros and tests. All outputs are contracted, i.e column names, data types and constraints are specified. Documentation is pushed to Snowflake. All outputs are written to a single Snowflake DB accessible only to Analytics teams building out Tier 1 and 2. Using a single dbt project and repo facilitates the perception that the Bronze Layer is an internal 'catalog' providing the starting point for Analytics business projects.
>
> 2. If Tier 0 projects are based on sources that have different load cadences, it may make sense to split the Bronze Layer repo according to cadence.
>
> 3. Once (and if) the monorepo is deemed to be overwhelmingly large, splits by source system should be very straight forward, yielding essentially independent repos. This is straight forward because Tier 0 is purely about data shaping with no business logic joins. However, splitting is not a necessity, and this document treats the Bronze Layer as a single dbt project and repo.
>
> 4. Tier 0 includes classical dbt staging, dbt snapshots, as well as source system structural aggregates such as child field aggregations to parent records, or parent field attribution to child records. We borrow the terminology "aggregate" from Data Driven Design theory. Such aggregates require no business logic. They are merely the re-build of logical constructs from the source system physical layer. Where aggregation is seen to include business logic, this aggregation must be implemented in Tier 1.
>
> 5. Tier 1 projects use dbt sources which are contracted production outputs of the Bronze Layer. This is a natural and very simple split of lineage as these Bronze outputs map to raw sources in a very straight forward way. The Tier 1 projects use these common clean contracted sources and can immediately focus on building intermediate and marts models, i.e. dims, facts, aggregates, and semantic models. A Tier 1 project requiring specific data tests on sources, should provide those data tests to the Tier 0 project.

---

## 4. Tier 0: Source Aggregate Projects

### 4.1 Definition

A Source Aggregate Project reconstructs one or more source systems' **logical data models** as denormalized, contract-enforced, tested aggregates. It is the single source of truth for operational data entering the warehouse, and it contains no business logic.

### 4.2 What it contains

| Layer | Purpose | Materialization |
|-------|---------|-----------------|
| `base/` | 1:1 with source table: cast, rename, 're-shape' columns, deduplicate (§4.5) | View/ Incremental Table |
| Aggregate models | Denormalize the source's normalized tables into logical entities (DDD aggregates) | View/ Incremental Table |
| Snapshots | SCD2 history, captured at **base grain** (§4.7) | Table |

#### Required settings and declarations:

 - Source freshness: Declared `loaded_at_field` and thresholds on every raw object (§13.4) 
 - Contracts: Declared  `contract: enforced: true` on all published aggregates
 - Column documentation: Use the `persist-docs: true` setting to persist to Snowflake. Every column described and typed (§21). Documentation describes all tests that have been undertaken so that subsequent projects can make informed and consistent assumptions relevant for their transformations.

### 4.3 What it does NOT contain

- Any join or filter driven by a **business decision**
- Any **cross-source-system** join (`erp.orders` to `crm.customers`) — that is Tier 0.5
- Any derived metric or KPI resting on a business rule
- Any model whose grain changes based on a business filter

### 4.4 The Structural Join Rule

Joins are permitted **if and only if** they are:

- Driven by the source's own **foreign key relationship**
- Grain-preserving for the aggregate root, or a strict many:1 enrichment
- Present **regardless of what any analyst wants to do** with the data. This is usually (but not always) evidenced by the foreign key being only useful for one particular join - e.g. order items joined to an order.

**Allowed:** `order_items LEFT JOIN orders ON order_items.order_id = orders.order_id`  [FK-driven enrichment].

**Not allowed:** `order_items JOIN orders WHERE orders.status = 'completed'` [business filter].

#### 4.4.1 Carve-out: source-semantic filters

Three filters are *structural* rather than business-driven and are explicitly permitted.

1. **Soft deletes.** Excluding rows the source system itself considers deleted (`is_deleted`, `deleted_at is not null`). A soft-deleted row is not data the source believes in.
2. **Deduplication of change-data-capture rows.** Selecting the current version per key (§4.5).
3. **Tenant or instance scoping** where the source is multi-tenant and only one tenant is in scope for the warehouse.

Non-structural filters (i.e. based on a business rule) belong in Tier 1.

#### 4.4.2 The scope-creep test

The boundary between structural and business logic erodes at child aggregations. `order_item_count` is structural; `net_revenue` is not. The distinguishing test:

> **If two competent analysts could compute it differently, it belongs in Tier 1.**

`order_item_count` has one reasonable definition. `net_revenue` requires decisions about discounts, returns, tax and cancellations — so it is business logic, however obvious it looks in one source system. Apply this test in review; it is the reason Tier 0 stays thin.

#### 4.4.3 Fan-out must be tested, not asserted

"Strict many:1" is a claim, and an unnoticed 1:N join silently inflates every metric downstream of it. Every published aggregate therefore carries, as a blocking test:

- `unique` or `dbt_utils.unique_combination_of_columns` on its declared grain
- `dbt_utils.relationships` on each FK used for enrichment
- A row-count guard asserting the aggregate's row count equals its root table's row count (for grain-preserving aggregates)

These tests provide critical guarantees.

### 4.5 Incremental aggregates: the joined-model trap

A denormalized aggregate materialized incrementally on its root key **will silently miss updates to joined child tables** if the incremental filter only looks at the root. For example, on the `orders` table, `unique_key: order_id` with `merge`, filtered on the order's own timestamp, never notices that an order *item* changed.

The required pattern is to collect changed keys from **every** contributing table, then rebuild those roots in full. See **Appendix C** for the implementation. Any incremental model in Tier 0 that joins more than one table must follow it, and this is a review gate rather than a suggestion.

### 4.6 Snapshots: capture at base grain

Snapshotting the *denormalized aggregate* is tempting. However, note that any change to any joined-in column spawns a new SCD2 row, so history potentially becomes high-churn and expensive.

**Rule:** snapshots capture `base/` models at their natural grain. Denormalization happens downstream of the snapshot, not upstream of it. Snapshots live in the configured `snapshot-paths` directory, never under `models/`.

### 4.7 Aggregate design decisions

Per source system, consider the analytical use cases to decide:

- Whether to emit the **parent with child aggregations** (`orders` with item count, total quantity)
- Whether to emit the **child with parent attributes** (`order_items` enriched with order header fields)
- Whether to emit **both** — common for highly normalized sources
- Whether to snapshot the underlying entities, and at what grain

### 4.8 Naming

See §20 for the consolidated scheme. Tier 0 aggregates are **not** prefixed `stg_`: in dbt convention staging is 1:1 with source and join-free, which describes the `base/` layer here. 

---

## 5. Tier 0.5: The Core / Conformance Project

### 5.1 Why it exists

Two needs have no home in a strict tier model:

1. **Conformed dimensions and spines:** Cross domain `dim_date`and fiscal calendar, currency and geography lookups held in seed datasets.
2. **Cross-source identity resolution:** A customer in the ERP and the same customer in the CRM. Tier 0 forbids cross-source joins; Tier 1 is domain-scoped.

Tier 0.5 is likely to remain small with niche data sets.

### 5.2 What it contains

| Layer | Purpose | Materialization |
|-------|---------|-----------------|
| Conformed dimensions | `dim_core__date`, `dim_core__currency`, `dim_core__fiscal_period` | Table |
| Crosswalks | `xref_core__customer` mapping source-system keys to a durable `customer_key` | Table (snapshot-backed) |
| Reference seeds | Curated lists, mappings, hierarchies under version control | Seed |

### 5.3 Governance

Tier 0.5 is the **only** tier permitted to join across source systems, and that privilege is narrowly scoped:

- It may join across sources **for the purpose of identity resolution or conformance only** — not to answer a business question
- Every crosswalk publishes its **match rate and unmatched-key counts as tested metrics**, because an entity-resolution rule that quietly stops matching is worse than one that fails
- Matching rules are documented in prose next to the model
- It is contracted and versioned exactly as Tier 1 marts are (§9)
- Tier 0.5 shares the "monorepo" with the Tier 0 source aggregates. It forms a special case within the Bronze Layer. 

### 5.4 Why not simply relax Tier 0's rule?

Because the no-cross-source-join rule is the clearest, most enforceable statement in this architecture, and a carve-out inside Tier 0 could be used to justify a genuine business join. Isolating the exception in a separate Tier with its own review gate keeps Tier 0's rule absolute.

### 5.5 Readable by everyone

Every Tier 1 and Tier 2 project may read all of the Bronze Layer: Tier 0 and Tier 0.5. This is the one shared-data route (for analytics team usage) managed by access to the single database target for source aggregates.

---

## 6. Tier 1: Domain Projects

### 6.1 Definition

Each Tier 1 project is owned by one (virtual) domain team (finance, marketing, operations, digital, ...) and contains all business logic for that domain. Unlike Tiers 0 and 0.5 which are combined within the source aggregate monorepo, **each Tier 1 domain level project has it's own repo and dbt project.**

Tier 1 projects, **by definition**, may only have sources pointing to the contracted outputs of the Bronze Layer.

### 6.2 What each Tier 1 dbt project may contain

| Layer | Purpose | Published? | Medallion |
|-------|---------|------------|-----------|
| `intermediate/` | Business-logic transformations, joins, filters, aggregations | No: intermediate = internal | Silver |
| `marts/` | dims and facts: contracted, documented, tested outputs | **Yes** | Gold |
| `semantic/` | Metric definitions (§6.5) | Yes | Gold |
| `presentation/` | Dashboard-optimized denormalizations | Yes | Gold |
| `features/` | ML feature tables | Yes | Gold |

### 6.3 Published versus internal

Only `marts/`, `semantic/`, `presentation/` and `features/` are consumable by anything outside the project (Gold Layer). This is enforced by schema separation and grants (§15.2), not convention: internal models (Silver) build into a schema no other role can read. A downstream project that needs an intermediate model has found a modelling gap, and the answer is to promote the model to a mart, not to grant access.

### 6.4 Contracts

All published models enforce contracts. Contracts are declared in the model's YAML alongside full column specifications:

```yaml
models:
  - name: fct_revenue
    config:
      contract:
        enforced: true
    columns:
      - name: revenue_date
        data_type: date
        constraints:
          - type: not_null
      - name: recognised_amount
        data_type: number(38,2)
      ...
  ...
```

### 6.5 One semantic layer, not two

- Metrics are defined once, in the domain that owns the concept
- Cross-domain metrics are defined in the Tier 2 project that owns the question, and may not redefine a Tier 1 metric

---

## 7. Tier 2: Cross-Domain Projects

- The business question(s) **span two or more Tier 1 domains**
- It supports an **ongoing dashboard or application**, not an ad hoc investigation. Ad hoc means merely writing queries against existing marts, even if the query is run regularly. Tier 2 is only for materializing cross domain marts. Materializing transforms is the purpose of dbt.
- It is complex enough to warrant tests, contracts and documentation.
- It belongs naturally to neither source domain.

**Example:** "Impact of digital journey on financial performance" consumes the finance mart and the digital mart and belongs in neither. It gets its own project.

**NOTE:** Governance is **identical** to Tier 1: owned, contracted, tested, documented.

---

## 8. Inter-Project Reference Mechanism

Projects reference each other's production outputs through `source()`. This section states the choice and its trade-offs, and does not relitigate the community's naming of the pattern as the *source-hack*. Suffice it to say that this approach *together with Tiering* as described here mitigates against the risks of a free-for-all source-referencing approach. 

### 8.1 The mechanism

A downstream (higher Tier) project declares an upstream project's published relations as sources, resolved per environment (§18.2):

```yaml
sources:
  - name: source_agg_erp
    database: "{{ env_var('DBT_SOURCE_AGG_DB') }}"
    schema: published
    tables:
      - name: ent_erp__orders
        description: "One row per order..."
        columns:
            - name: order_id
              description: "Unique identifier..."
            - name: channel_id
              description: ...
            ...
      - name: ent_erp__order_items
        description: ...
        ...
```

> **Never hard-code a production database name in `sources.yml`.** Resolution is by `env_var` or by target, always. This simplifies dbt project usage in dev or test environments 

### 8.2 Distribute source definitions as a package

To eliminate manual copying of `sources.yml` into multiple projects it may be useful to collect this information into a common "analytics sources" dbt project that can be imported as a package. This package contains only its `sources.yml`, plus shared macros and generic tests, but no models. Consumers pin a version in `packages.yml`:

```yaml
packages:
  - git: "https://git.internal/dbt-sources-source-agg-erp.git"
    revision: "1.4.0"
```

This ensures that common documentation travels with the package and is available in the project lineage.

**NOTE:** 
> - All Tier 1 dbt projects include the Bronze Layer (Tier 0 and Tier 0.5) as a single source. 
> - Tier 2 projects use the appropriate Tier 1 outputs as source.

### 8.3 Why not import packages containing models?

A package containing *models* is a different proposition and remains out of scope.


| Risk | Severity |
|------|----------|
| Accidental rebuild of upstream models (`dbt build --select +model`) | **Potential production incident** |
| Schema resolution defaults to the consumer's target schema | Wrong schema, wrong role |
| Per-environment `+schema` / `+database` overrides in every consumer | Operational surface area |


For a stable, thin, contract-enforced foundation layer the risk asymmetry favours `source()` clearly.

### 8.4 Known limitations and mitigations

| Limitation | Mitigation | Residual risk |
|-----------|------------|---------------|
| No compile-time validation of upstream schema | Contracts in the producing project; version-pinned source package; downstream CI builds against the pinned version | A producer can still break a consumer between version bumps. By separating the Bronze Layer which is mostly stable and simply structured, producers tend not to make breaking changes. |
| Broken lineage at project boundary (upstream direction) | Sources-only package carries column docs; `persist_docs` puts descriptions in Snowflake (§21) | Visual DAG is still severed even though upstream lineage is simple. |
| Broken lineage (**downstream** direction) | Snowflake `OBJECT_DEPENDENCIES` and `ACCESS_HISTORY` (§8.5) | Requires a query, not a click. |
| `state:modified` does not cross the boundary | Each project runs its own state comparison against its own prod manifest | Cross-project impact is not automatic. |
| Manual source sync | Solved by using a source definitions package (§8.2) | — |

### 8.5 Downstream impact analysis is the direction that matters

Broken lineage at the Tier 0 - Tier 1 border is trivial from the perspective of the Tier 1 user as the upstream DAG is very simple given the Tier 0 restrictions. Going the other way, i.e. from the perspective of the Tier 0 steward asking: **"What breaks if I change this column?"**, Snowflake answers this natively, without any manifest stitching:

- **`SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES`**: which objects reference a given object, including across databases. Catches view and table dependencies declaratively.
- **`SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY`**: which queries, roles and users touched a given object, with **column-level granularity**. Catches BI tools and ad hoc consumers that no dbt manifest knows about.

**Requirement:** a standing "who consumes this?" query, available to every steward, run as a mandatory step before any breaking change. Appendix D includes the query. This is strictly better than manifest stitching for the impact-analysis use case, because it sees consumers completely outside dbt entirely. Use dbt versions to manage changes.

**NOTE:** Changes to Tier 0, concerned with data shaping and being devoid of business logic are likely to be quite rare.

---

## 9. Change Management

### 9.1 What contracts actually do


**A contract asserts that a model's output matches its own YAML specification.** It does not protect consumers in a downstream dbt project built on the upstream sources.

Specifically, rename the column in both the SQL and the YAML of the upstream project and the build succeeds. Downstream `source()` references break at runtime. Awareness of this possibility is key. 

What contracts *do* provide:

- An upstream model cannot silently change shape relative to its declared specification — a mis-typed column or dropped field fails the producer's build
- With state comparison against the producer's own prod manifest, dbt **flags breaking contract changes** in the producer's CI. This is real and should be enabled (§18.3), but it tells the producing team they made a breaking change; it does not know or care whether any consumer minds, and the team can proceed by bumping a version
- For the Bronze -> Tier 1 boundary, a machine-readable schema that travels to consumers via the source package (§8.2). This requires a focused synchronisation. The source package could include tests to validate schema and properties. The source package could also include trivial staging models with contracts - some duplication from the Bronze Layer code. 

### 9.2 Model versions and deprecation dates

Protection for consumers comes from versioning, not enforcement. Bronze Layer changes, as rare as they are, would require strictly ensuring that versions are used. 

dbt's `versions` and `deprecation_date` are the critical mechanism, and they fit a `source()`-based architecture **better** than they fit `ref()`:

```yaml
models:
  - name: erp_revenue
    latest_version: 2
    config:
      contract:
        enforced: true
    columns:
      - name: revenue_date
        data_type: date
      - name: recognised_amount
        data_type: number(38,2)
    versions:
      - v: 1
        deprecation_date: 2027-03-31
        columns:
          - name: revenue_date
            data_type: date
          - name: revenue_amount      # renamed in v2
            data_type: number(38,2)
      - v: 2
```

Because versioned models materialize to **distinct physical relations** (`erp_revenue_v1`, `erp_revenue_v2`), both exist in the warehouse simultaneously. That is exactly the migration window a `source()`-based consumer needs:

1. Producer ships `v2` alongside `v1` and sets `v1`'s `deprecation_date`
2. Producer bumps the source package minor version; the new relation appears in consumers' source definitions
3. Each consumer migrates its `source()` reference on its own schedule, within the window
4. After the deprecation date, the producer drops `v1`

Under `ref()` with Mesh this requires coordination machinery. Here, physical relation names do the work. 

### 9.3 Change management matrix

| Change | Process |
|--------|---------|
| Tier 0 additive (new column, new aggregate) | Deploy. Bump source package minor version. Consumers unaffected |
| Tier 0 **breaking** (rename, drop, retype) | Run the impact query (§8.5). New model version with `deprecation_date`. Notify named consumers. Drop old version after the date |
| Tier 0.5 crosswalk rule change | **Always treated as breaking**, even when the schema is unchanged — match-rate changes alter downstream numbers silently. Requires stewardship review and consumer notification |
| Tier 1 additive in a published mart | Deploy. Bump package minor version |
| Tier 1 **breaking** in a published mart | As Tier 0 breaking. Tier 2 consumers are enumerable via §8.5 |
| New source system | Add a Tier 0 project or extend an existing one. Available to all consumers once future grants apply (§15.3) |
| **Full refresh of a Tier 0 incremental** | See §9.4 — not a routine operation |

### 9.4 Full-refresh coordination

If a Tier 0 model is fully refreshed, every downstream incremental built from it may now rest on shifted history — rows that changed, or disappeared, outside any downstream watermark.

**Policy:** a full refresh of any published Tier 0 or Tier 0.5 model is a **coordinated event**, not a routine `--full-refresh` flag:

1. Announce to consumers identified by §8.5, with a stated window
2. Full-refresh the upstream model
3. Downstream projects full-refresh any incremental model depending on it, in reference-graph order
4. Record the event in the build audit table (Appendix D) so discrepancies can later be correlated

Consider revoking `--full-refresh` in the production role's permitted arguments so that it cannot be invoked casually.

---

## 10. Write-Audit-Publish

### 10.1 The problem with dbt test

By the time a test fails, the published tables have already been replaced and downstream projects have already been triggered. So tests observe the incident rather than prevent it.

### 10.2 Recommended standard for Bronze Layer (Tier 0 and Tier 0.5): clone-test-swap

For the foundation tiers, publication is gated behind a passing test suite:

```sql
-- 1. Clone the published schema (zero-copy, preserves incremental state)
CREATE OR REPLACE SCHEMA source_agg.build CLONE source_agg.published;

-- 2. Build into the clone
EXECUTE DBT PROJECT source_agg_erp ARGS = 'build --target build';

-- 3. Only on success, swap
ALTER SCHEMA source_agg.build SWAP WITH source_agg.published;

-- 4. Emit the completion event (§12.3) — only now is data published
```

Zero-copy cloning makes step 1 nearly free and preserves existing table state, so incremental models behave correctly rather than reverting to full refresh.

### 10.3 Two gotchas worth knowing before you build this

- **Grants do not follow the swap the way you expect.** `SWAP WITH` exchanges the objects between schemas. Object-level grants travel with the objects, so after a swap the newly-published objects carry whatever grants the *build* schema conferred. Apply **identical future grants to both schemas** (§15.3) so consumer access is correct regardless of which physical objects currently occupy the published schema. Verify this in a non-production account before relying on it.
- **Cloning and swapping is per-schema.** A project writing to several schemas needs each cloned and swapped, and the swaps are not atomic with respect to one another. Prefer a single published schema per project; where that is impossible, ensure consumers tolerate a brief window of mixed state, or gate reads on the completion event.

---

## 11. Freshness and Staleness Contracts

### 11.1 The silent-wrongness problem in Tier 2

Consider what happens when  Tier 2 is triggered after all upstream Tier 1 tasks have completed. Suppose this example: finance completed this morning, digital last succeeded three days ago. Tier 2 builds successfully, publishes a cross-domain fact mixing fresh and stale data, and **no test anywhere fails.** The dashboard is wrong with no warning.

Completion is not freshness. Every cross-tier dependency needs a freshness assertion, not just a completion signal.

#### Watermarks

One way to surface this is to ensure that every project's final build step writes to a shared watermark table (Appendix D):

| Column | Meaning |
|--------|---------|
| `project_name` | Producing project |
| `build_id` | Unique run identifier |
| `status` | `SUCCESS` / `FAILED` |
| `published_at` | When the publish completed |
| `source_max_loaded_at` | Newest source timestamp represented in this build |
| `full_refresh` | Whether this run was a full refresh (§9.5) |

`source_max_loaded_at` is the important one: it describes how current the *data* is, not when the job ran. A job that runs every hour against a feed that stopped yesterday looks healthy by `published_at` and is stale by `source_max_loaded_at`.

#### Declared tolerances

On top of the Watermarks, every Tier 2 project declares, per upstream dependency, a maximum acceptable staleness, and **refuses to build** outside it:

```yaml
# staleness.yml — read by the pre-build gate
upstream:
  - project: source_agg_erp
    max_staleness: 26 hours
    on_breach: fail
  - project: finance
    max_staleness: 26 hours
    on_breach: fail
  - project: digital
    max_staleness: 26 hours
    on_breach: fail
```

The gate runs before the build (Appendix D), and `on_breach: fail` means no build rather than a build with a warning. A skipped build leaves yesterday's correct numbers in place; a completed build replaces them with wrong ones. Skipping is the safer failure.

### 11.2 Source freshness at the raw boundary

Independently of watermarks, Tier 0 declares dbt source freshness on **every** raw object, with thresholds agreed with the platform team (§13.4). `dbt source freshness` runs before the Tier 0 build and fails the run at the boundary, producing an alert whose owner is unambiguously ingestion rather than analytics.

This matters disproportionately given §2.1: when Analytics cannot fix the underlying problem, the least it can do is prove precisely where the problem is.

---

## 12. Orchestration on Snowflake

### 12.1 Execution: dbt Projects on Snowflake

dbt projects are Snowflake objects created from a Git repository integration and executed in Snowflake compute:

```sql
CREATE OR REPLACE GIT REPOSITORY dbt_repos.source_agg_erp
  API_INTEGRATION = git_api_integration
  ORIGIN = 'https://git.internal/source_agg_erp.git';

CREATE OR REPLACE DBT PROJECT source_agg_erp
  FROM '@dbt_repos.source_agg_erp/branches/main';

EXECUTE DBT PROJECT source_agg_erp 
   ARGS = 'build --target prod';
```

Scheduling wraps that call in a Task:

```sql
CREATE OR REPLACE TASK t_source_agg_erp_build
  WAREHOUSE = wh_source_agg
  AS EXECUTE DBT PROJECT source_agg_erp ARGS = 'build --target prod';
```

This removes the need for external orchestration infrastructure and keeps compute, credentials and scheduling in one place. The recommended multi-project orchestration mechanism is to deploy multiple DBT PROJECT objects alongside their corresponding Snowflake Tasks into a single, centralized schema - the *management layer*. Each dbt project points to its own separate target database for actual model materialization.

The **clone-test-swap** strategy of §10.2 is solid for Tiers 1 and 2 as well as the Bronze Layer. An additional benefit is that rollback is never required.

It is also good practice to write summary results of all `EXECUTE DBT PROJECT` calls to a control table. 

---

## 13. References

### 13.1 dbt documentation

- [dbt: How we structure our projects](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview)
- [dbt: Staging](https://docs.getdbt.com/best-practices/how-we-structure/2-staging)
- [dbt: Snapshots](https://docs.getdbt.com/docs/build/snapshots)
- [dbt: Data contracts](https://docs.getdbt.com/docs/collaborate/govern/model-contracts)
- [dbt: Model versions](https://docs.getdbt.com/docs/collaborate/govern/model-versions) 
- [dbt: Model access](https://docs.getdbt.com/docs/collaborate/govern/model-access)
- [dbt: Incremental models](https://docs.getdbt.com/docs/build/incremental-models)
- [dbt: Packages](https://docs.getdbt.com/docs/build/packages)
- [dbt: persist_docs](https://docs.getdbt.com/reference/resource-configs/persist_docs)
- [dbt: Mesh project dependencies](https://docs.getdbt.com/docs/mesh/govern/project-dependencies)

### 13.2 Snowflake documentation

- [dbt Projects on Snowflake](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake)
- [Best practices for dbt projects on Snowflake](https://docs.snowflake.com/en/developer-guide/dt/dbt-best-practices)
- [Task graphs and dependencies](https://docs.snowflake.com/en/user-guide/tasks-graphs)
- [Triggered tasks](https://docs.snowflake.com/en/user-guide/tasks-triggered)
- [Streams](https://docs.snowflake.com/en/user-guide/streams-intro)
- [Cloning](https://docs.snowflake.com/en/user-guide/object-clone) and [SWAP WITH](https://docs.snowflake.com/en/sql-reference/sql/alter-schema)
- [OBJECT_DEPENDENCIES](https://docs.snowflake.com/en/sql-reference/account-usage/object_dependencies) and [ACCESS_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/access_history)
- [Tag-based masking policies](https://docs.snowflake.com/en/user-guide/tag-based-masking-policies)
- [Resource monitors](https://docs.snowflake.com/en/user-guide/resource-monitors) 

### 13.3 Tooling and community

- [dbt-project-evaluator](https://dbt-labs.github.io/dbt-project-evaluator/latest/)
- [dbt-utils](https://github.com/dbt-labs/dbt-utils) 
- [dbt-core discussion #5244: cross-project lineage](https://github.com/dbt-labs/dbt-core/discussions/5244)


---
---

*End of document.*

