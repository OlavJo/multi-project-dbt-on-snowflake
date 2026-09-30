# Multi-Project dbt Architecture on Snowflake

## A Tiered Governance Model for Independent Teams

### High Level Design: Engineering Discussion Document

| | |
|---|---|
| **Version** | 2.3 |
| **Date** | 2026-09-29 |
| **Status** | **Proposed** for review |
| **Proposed By** | Lead Data Engineer, PSM CAI |


---

## 1. Executive Summary

This document describes a tiered multi-project dbt architecture for Snowflake environments, using Snowflake embedded open source dbt where dbt Mesh is unavailable. [dbt Mesh is the proprietary component of dbt-Labs designed to support multi-project dbt architecture.]

It balances **enterprise governance** (consistent naming, contracts, tests, access control) with **team independence** (separate CI, separate development, separate ownership, but integrated production pipelines).

This multi-project architecture **classifies dbt Projects by Tier**, and expects the project to follow the rules, recommendations and guidance set for each Tier:


| Tier | Name | Purpose | Owner |
|------|------|---------|-------|
| — | Raw / Landing | Source data loaded into Snowflake | Platform team (ingestion only) |
| 0 | Source Aggregate Project(s) | Reconstruct source systems' logical models as ready-to-consume aggregates | Analytics |
| 0.5 | Core / Consolidation Project | Cross-source identity resolution, conformed dimensions across sources, shared spines  | Analytics |
| 1 | Domain Project(s) | Business transformation: dims, facts, marts, semantic views | Analytics/ Domain |
| 2 | Cross-Domain Project(s) | Analytics spanning two or more domains | Analytics/ Business-question owner |

Business driven analytics projects will tend to focus on Tier 1 (Domain Projects). Rather than supporting end-to-end projects which include classical dbt data sourcing and staging layers within the project, this architecture splits out Tier 0 and Tier 0.5 as a shared layer, providing a convenient starting point for multiple projects using common source data sets. 

After defining the Tiers, this document discusses dependencies between projects as being managed by `source()` reference to prior Tier production outputs. This approach replaces the use of **cross-project references** provided in dbt Mesh.

### Scope

This document describes the multi-project architecture at a high level from the perspective of the individual contributing analytics teams. It describes how we can operate multiple dbt projects without relying on dbt Mesh. 

A more detailed design document will capture the recommended setup, orchestration, role assignments, access control, daily governance and control plane, project naming conventions, data testing, tagging and classification, PII handling, materialisation strategies, environment recommendations, and cost attribution. Most of this will rely on standard Snowflake recommended practice, which aligns closely with dbt-Labs.

---

## 2. Design Constraints

- **Snowflake** as warehouse including first class support for data lineage and cloning
- **Snowflake** embedded open source dbt. Projects are Snowflake objects created from a Git repository and invoked with Snowflake command `EXECUTE DBT PROJECT`
- **dbt-Labs dbt Mesh / Cloud Enterprise unavailable**
- **Multiple independent analytics teams** with separate sprints, CI, and ownership, but using a common set of enterprise data sources
- **Snowflake Tasks** for scheduling and cross-project coordination
- **Platform team owns ingestion only.** Analytics owns everything beyond the raw layer

### The ingestion boundary

The platform team is resourced to land enterprise source data in Snowflake, not to work in dbt. This is an enterprise structural reality rather than a design choice.

This architecture treats the boundary as a **contract rather than a handoff**: declared freshness expectations on every raw object, monitored by analytics, with failures attributable to ingestion before any dependent Tier 0 build is attempted. 

This is no different than a single dbt project architecture which also seeks to treat it's raw sources as a contracted boundary. 

We expect: 
> -  the platform team to ensure that a `_loaded_at` (or equivalent) column appears on every landed object. Without it, freshness monitoring and CDC deduplication are both impossible. 
> - A landing-complete record written to a control table per source system and batch for historical review.

---

## 3. Project Tiering

In this architecture, cross-project references are constrained by a fixed tiering structure. This provides an acyclicity guarantee on dependencies. The purpose is to support cross project collaboration and to ease sharing of common datasets. Each dbt project at every Tier is expected to provide contracted (production) outputs which are made available as sources for subsequent projects.

```
Raw / Landing
  │
  └──► Tier 0 (Source Aggregates)           ◄─ may read Raw only              ┐
        │                                                                     ├─  Bronze Layer 
        ├──► Tier 0.5 (Core/ Consolidation) ◄─ may read Tier 0 (+ seeds)      ┘     (Single dbt project)
        │
        └──► Tier 1 (Domains)               ◄─ may read Tier 0 and Tier 0.5   ┐   Silver (internal/ intermediate) 
              │                                                               ├─    and Gold (exposure) Layer
              └──► Tier 2 (Cross-Domain)    ◄─ may read Tier 1 marts          ┘     (Multiple dbt projects)
```

Note immediately that **the entire Bronze Layer (Tier 0 and Tier 0.5) is managed as a single dbt project.** This project produces all available sources for the multiple Tier 1 projects. All analytics teams contribute to this dbt project as new analytic sources are required. The Bronze Layer is built out conceptually as a catalog of available analytic sources.

Analytics projects will tend to focus on Tier 1 dbt projects. **Each domain-based Tier 1 project is managed as a single dbt project.**  Each analytic project initially needs to assess the existing Bronze Layer (production) contracted outputs as a base. The analytics project may be required to contribute to Tier 0 and Tier 0.5 where the base is lacking. This contribution supports shared Source Aggregates or Core outputs. So the Bronze Layer provides shared analytics *internal* resources, receiving contributions from across all analytics teams. Tier 1 and Tier 2 provide contracted outputs for exposure to the business.

NOTE: 

 > - Both dbt projects and the Snowflake platform itself capture lineage. Cross Tier lineage (cross dbt project lineage) is captured within Snowflake itself.
 > - Tier 0 is purely about data shaping and requires no business logic.
 > - Tier 0.5 is a consolidation layer taking care of common cross source aggregate joins.
 > - Tiers 1 and 2 capture business logic - this is where data modelling comes to the fore. These tiers expose the Gold layer dims, facts, aggs, semantic views
 

**Tier assignment**:

> 1. All Tier 0 and Tier 0.5 projects (Bronze Layer) operate initially from a single "monorepo" using the standard dbt folder structure to separate source systems. This monorepo includes macros and tests. All outputs are contracted, i.e column names, data types and constraints (although limited in Snowflake) are specified. Tests are performed. Documentation is pushed to Snowflake. All outputs are written to a single Snowflake DB accessible only to Analytics teams building out Tier 1 and 2. Using a single dbt project and repo facilitates the perception that the Bronze Layer is an internal 'catalog' providing the starting point for Analytics business projects in Tier 1.
>
> 2. If Tier 0 projects are based on sources that have different load cadences, it may make sense to split the Bronze Layer repo according to cadence.
>
> 3. Once (and if) the monorepo is deemed to be overwhelmingly large, splits by source system should be very straight forward, yielding essentially independent repos. This is straight forward because Tier 0 is purely about data shaping with no business logic joins. However, splitting is not a necessity, and this document treats the Bronze Layer as a single dbt project and repo.
>
> 4. Tier 0 includes classical dbt staging, dbt snapshots, dbt seeds, as well as source system structural aggregates such as child field aggregations to parent records, or parent field attribution to child records. We borrow the terminology "aggregate" from Domain Driven Design theory where such aggregates require no business logic. They are merely the re-build of logical constructs from the source system physical layer, which might be highly normalised. Where aggregation is seen to include business logic, such aggregation must be implemented in Tier 1.
>
> 5. Tier 1 projects use dbt sources which are contracted production outputs of the Bronze Layer. This is a natural and very simple split of lineage as these Bronze outputs map to raw sources in a very straight forward way. The Tier 1 projects use these common clean contracted sources and can immediately focus on building intermediate and marts models, i.e. dims, facts, aggregates, and semantic models. A Tier 1 project requiring specific data tests on sources, should provide those data tests to the Bronze Layer project.

---

## 4. Tier 0: Source Aggregate Projects

### 4.1 Definition

A Source Aggregate Project reconstructs one source system's **logical data models** as denormalized, contract-enforced, tested aggregates. It is the single source of truth for operational data entering the Analytics warehouse, and **it contains no business logic**.

Each Source Aggregate project contributes to a source-named folder in the single Bronze Layer dbt project. Source aggregate projects focus on these dbt concepts: source, staging, snapshot, macro, test. Note that 'models' are explicitly excluded - no intermediates, no marts as these live in Tiers 1 and 2.

### 4.2 What it contains

| Type | dbt Folder | Purpose | Materialization |
|------|-------|---------|-----------------|
|Source Tables | `staging/` or `staging/base/` | 1:1 with source tables: cast, rename, 're-shape' columns, deduplicate, filter soft deletes | View/ Incremental Table |
| Aggregates | `staging/` | Denormalize the source's normalized tables into logical entities (DDD aggregates without business logic); unions of tables split in the source | View/ Incremental Table |
| Snapshots | `snapshots/` and `staging/` | SCD2 history computed for sources lacking change capture | Table |

#### Required settings and declarations:

 - Source freshness: Declared `loaded_at_field` and thresholds on every raw object 
 - Contracts: Declared  `contract: enforced: true` on all published aggregates
 - Column documentation: Use the `persist-docs: true` setting to persist to Snowflake. Every column described and typed. Documentation describes all tests that have been undertaken so that subsequent projects can make informed and consistent assumptions relevant for their transformations.

### 4.3 What it does NOT contain

- Any join or filter driven by a **business decision**
- Any **cross-source-system** join - that is Tier 0.5 as long as no business logic is expressed
- Any derived metric or KPI resting on a business rule
- Any model whose grain changes based on a business filter

### 4.4 The Structural Join Rule

Joins are permitted in an aggregate **if and only if** they are:

- Driven by the source's own **foreign key relationship**
- Grain-preserving for the aggregate root, or a strict many:1 enrichment
- Present **regardless of what any analyst wants to do** with the data. This is usually (but not always) evidenced by the foreign key being only useful for one particular join. For example order items joined to an order.

**Allowed:** `order_items LEFT JOIN orders ON order_items.order_id = orders.order_id`  [FK-driven enrichment].

**Not allowed:** `order_items JOIN orders WHERE orders.status = 'completed'` [business filter].

#### 4.4.1 Carve-out: source-semantic filters

Three filters are *structural* rather than business-driven and are explicitly permitted.

1. **Soft deletes.** Excluding rows the source system itself considers deleted (`is_deleted`, `deleted_at is not null`). A soft-deleted row is not data the source believes in.
2. **Deduplication of change-data-capture rows.** Selecting the current version per key.
3. **Tenant or instance scoping** where the source is multi-tenant and only one tenant is in scope for the warehouse. For example, a source audit table includes records of various kinds, where only one kind is relevant for analytics.

Non-structural filters (i.e. based on a business rule) belong in Tier 1.

#### 4.4.2 The scope-creep test

The boundary between structural and business logic erodes at child aggregations. `order_item_count` is structural; `net_revenue` is not. The distinguishing test:

> **If two competent analysts could compute it differently, it belongs in Tier 1.**

`order_item_count` has one reasonable definition. `net_revenue` requires decisions about discounts, returns, tax and cancellations. Hence it is business logic, however obvious it looks in one source system.

#### 4.4.3 Fan-out must be tested, not asserted

"Strict many:1" is a claim, and an unnoticed 1:N join silently inflates every metric downstream of it. Every published aggregate therefore carries, as a blocking test:

- `unique` or `dbt_utils.unique_combination_of_columns` on its declared grain
- `dbt_utils.relationships_where` on each FK used for enrichment
- A row-count guard asserting the aggregate's row count equals its root table's row count (for grain-preserving aggregates)

These tests provide critical guarantees, and are to be discussed at length in the detailed design document.

### 4.5 Incremental aggregates: the joined-model trap

A denormalized aggregate materialized incrementally on its root key **will silently miss updates to joined child tables** if the incremental filter only looks at the root. For example, on the `orders` table, `unique_key: order_id` with `merge`, filtered on the order's own timestamp, never notices that an order *item* changed.

The required pattern is to collect changed keys from **every** contributing table, then rebuild those roots in full. Such design patterns belong in the detailed design document.

### 4.6 Snapshots: capture at base grain

Snapshotting the *denormalized aggregate* is tempting. However, note that any change to any joined-in column spawns a new SCD2 row, so history potentially becomes high-churn and expensive.

**Rule:** snapshots capture `base/` models at their natural grain. Denormalization happens downstream of the snapshot, not upstream of it. 

### 4.7 Aggregate design decisions

Per source system, consider the analytical use cases to decide:

- Whether to emit the **parent with child aggregations** (`orders` with item count, total quantity)
- Whether to emit the **child with parent attributes** (`order_items` enriched with order header fields)
- Whether to emit **both** — common for highly normalized sources
- Whether to snapshot the underlying entities, and at what grain

---

## 5. Tier 0.5: Core/ Consolidation

### 5.1 Why it exists

Prior to any domain business logic, there are often some loose ends requiring special consideration:

1. **Conformed dimensions and spines:** Cross domain `dim_date`and fiscal calendar, currency and geography lookups held in seed datasets.
2. **Cross-source identity resolution:** A customer in the ERP and the same customer in the CRM. Tier 0 forbids cross-source joins; Tier 1 is domain-scoped.

Tier 0.5 is likely to remain small with critical but niche data sets.

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
- It is contracted and versioned (if necessary) exactly as Tier 0 outputs are.
- Tier 0.5 shares the "monorepo" with the Tier 0 source aggregates. It forms a special case within the Bronze Layer, and has it's own folder in the Bronze Layer dbt project.

### 5.4 Why not simply relax Tier 0's rule?

Because the no-cross-source-join rule is the clearest, most enforceable statement in this architecture, and a carve-out inside Tier 0 could be used to justify a genuine business join. Isolating the exception in a separate Tier with its own review gate keeps Tier 0's rule absolute.

### 5.5 Readable by everyone in Analytics

Every Tier 1 and Tier 2 project may read all of the Bronze Layer: Tier 0 and Tier 0.5. This is the one shared-data route (for analytics team usage) managed by access to the single database target for source aggregates.

**No access to the Bronze Layer for the wider business.** The business consumes from Tier 1 and Tier 2.

---

## 6. Tier 1: Domain Projects

### 6.1 Definition

Each Tier 1 project is owned by one (virtual) domain team (finance, marketing, operations, digital, ...) and contains all business logic for that domain. Unlike Tiers 0 and 0.5 which are combined within the single Bronze Layer dbt project, **each Tier 1 domain level project has it's own repo and dbt project.**

Tier 1 projects, **by definition**, may only have sources pointing to the contracted (production) outputs of the Bronze Layer.

It may also be necessary to include a 'Shared Domain' project, but this should be as a last resort if it becomes a pre-requisite for multiple Tier 2 projects. 

### 6.2 What each Tier 1 dbt project may contain

| dbt Folder | Purpose | Published? | Medallion |
|-------|---------|------------|-----------|
| `intermediate/` | Business-logic transformations, joins, filters, aggregations | No: intermediate = internal | Silver |
| `marts/` | dims and facts: contracted, documented, tested outputs | Yes | Gold |
| `semantic/` | Metric definitions | Yes | Gold |
| `presentation/` | Dashboard-optimized denormalizations | Yes | Gold |
| `features/` | ML feature tables | Yes | Gold |

### 6.3 Published versus internal

Only `marts/`, `semantic/`, `presentation/` and `features/` are consumable outside the project (Gold Layer). This is enforced by schema separation and grants, not by convention: internal models (Silver) build into a schema no other role can read. A downstream project that needs an intermediate model has found a modelling gap, and the answer is to promote the model to a mart, not to grant access.

### 6.4 Contracts

All published models in Tier 1 enforce contracts - also true for the Bronze Layer. Contracts are declared in the model's YAML alongside full column specifications:

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

### 6.5 One semantic layer

- Metrics are defined once, in the domain that owns the concept
- Cross-domain metrics are defined in the Tier 2 project that owns the question, and may not redefine a Tier 1 metric

---

## 7. Tier 2: Cross-Domain Projects

- The business questions **span two or more Tier 1 domains**
- It supports an **ongoing dashboard or application**, not an ad hoc investigation. Ad hoc means merely writing queries against existing marts, even if the query is run regularly. Tier 2 is only for materializing cross domain marts. Materializing transforms is the purpose of dbt.
- It is complex enough to warrant tests, contracts and documentation.
- It belongs naturally to neither source domain.

**Example:** "Impact of digital journey on financial performance" consumes the finance mart and the digital mart and belongs in neither. It gets its own project.

**NOTE:** Governance is **identical** to Tier 1: owned, contracted, tested, documented.

Tier 2 has dependencies only on Tier 1 outputs. This may imply that analysts working on a Tier2 project may need to first contribute to a Tier 1 project, even if that is a skeleton.

---

## 8. Inter-Project Reference Mechanism

The critical consequence of the Tiering is the clean dependency structure whereby projects reference each other's production outputs through `source()`. This section reviews the mechanism and its trade-offs, and does not relitigate the community's naming of the pattern as the *source-hack*. Suffice it to say that this approach *together with Tiering* and with *Snowflake's advanced capabilites in data lineage and cloning* mitigates against the risks of a free-for-all source-referencing approach. 

### 8.1 The mechanism

A downstream (higher Tier) project declares an upstream project's published relations as sources, resolved per environment:

```yaml
sources:
  - name: bronze_layer
    database: "{{ env_var('DBT_BRONZE_DB') }}"
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
  - git: "https://git.internal/bronze-layer-dbt-sources.git"
    revision: "1.4.0"
```

This ensures that common documentation travels with the package and is available in the project lineage. This would include all available sources. There are multiple available ways to manage unused source definitions, even though they have no impact on the execution of the dbt project. Recommendations belong to the detailed design document.

**SUMMARY:** 
> - All Tier 1 dbt projects include the Bronze Layer (Tier 0 and Tier 0.5) as a single source. 
> - Tier 2 projects use the appropriate Tier 1 outputs as source.

### 8.3 Why not import packages containing models?

A package containing *models* is a different proposition and remains out of scope. This is an explicit restriction to ensuring projects are only coupled through their contracted outputs


| Risk | Severity |
|------|----------|
| Accidental rebuild of upstream models (`dbt build --select +model`) | **Potential production incident** |
| Schema resolution defaults to the consumer's target schema | Wrong schema, wrong role |
| Per-environment `+schema` / `+database` overrides in every consumer | Operational surface area |

### 8.4 Lineage: Downstream impact analysis is the direction that matters

Broken lineage at the Tier 0 - Tier 1 border is trivial from the perspective of the Tier 1 user as the upstream DAG is very simple given the Bronze Layer restrictions. 

Going the other way, i.e. from the perspective of the Bronze Layer steward asking: **"What breaks if I change this column?"**, Snowflake answers this natively, without any manifest stitching:

- **`SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES`**: which objects reference a given object, including across databases. Catches view and table dependencies declaratively.
- **`SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY`**: which queries, roles and users touched a given object, with **column-level granularity**. Catches BI tools and ad hoc consumers that no dbt manifest knows about.

**Requirement:** 
 - A standing "who consumes this?" query, available to every steward, run as a mandatory step before any breaking change. This is strictly better than manifest stitching for the impact-analysis use case, because it sees consumers completely outside dbt entirely. 
 - Use dbt versions to manage changes.

**NOTE:** Changes to the Bronze Layer, concerned with data shaping and being devoid of business logic are likely to be quite rare.

### 8.5 Known limitations and mitigations

| Limitation | Mitigation | Residual risk |
|-----------|------------|---------------|
| No compile-time validation of upstream schema | Contracts in the producing project; version-pinned source package; downstream CI builds against the pinned version; all breaking changes managed through dbt versioning | A producer can still break a consumer between version bumps. (By separating the Bronze Layer which is mostly stable and simply structured, producers tend not to make breaking changes.) |
| Broken lineage at project boundary (upstream direction) | Sources-only package carries column docs; `persist_docs` puts descriptions in Snowflake | Visual DAG is still severed even though upstream lineage is simple. |
| Broken lineage (**downstream** direction) | Snowflake `OBJECT_DEPENDENCIES` and `ACCESS_HISTORY`  | Requires a query, not a click. |
| `state:modified` does not cross the boundary | Each project runs its own state comparison against its own prod manifest | Cross-project impact is not automatic. |
| Manual source sync | Solved by using a source definitions package | — |

---

## 9. Change Management

### 9.1 What contracts actually do


**A contract asserts that a model's output matches its own YAML specification.** It does not protect consumers in a downstream dbt project built on the upstream sources, from changes made in the upstream project. This is because upstream, the model specification including the contract might be changed. 

Specifically, rename the column in both the SQL and the YAML of the upstream project and the build succeeds. Downstream `source()` references break at runtime. 

**Awareness of this possibility is key, and so all upstream changes require analysis and dbt versioning.**

What contracts *do* provide:

- An upstream model cannot silently change shape relative to its declared specification — a mis-typed column or dropped field fails the producer's build.
- With state comparison against the producer's own prod manifest, dbt **flags breaking contract changes** in the producer's CI. It tells the producing team they made a breaking change.I It cannot inform if any consumer minds, and the upstream change can proceed by bumping a version.
- For the Bronze -> Tier 1 boundary, a machine-readable schema that travels to consumers via the Bronze Layer source definition package. This requires a focused synchronisation. The source package could include tests to validate schema and properties. The source package could also include trivial staging models with contracts - some duplication from the Bronze Layer code which can easily be automated. Recommendations are expected in the detailed design document.

### 9.2 Model versions and deprecation dates

Protection for consumers comes from versioning, not enforcement. Bronze Layer changes, as rare as they are, would require strictly ensuring that versions are used. 

dbt's `versions` and `deprecation_date` are the critical mechanism, and they work well with a `source()`-based architecture:

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


### 9.3 Change management matrix

| Change | Process |
|--------|---------|
| Tier 0 additive (new column, new aggregate) | Deploy. Bump source package minor version. Consumers unaffected |
| Tier 0 **breaking** (rename, drop, retype) | Run the Snowflake impact query. New model version with `deprecation_date`. Notify named consumers. Drop old version after the date |
| Tier 0.5 crosswalk rule change | **Always treated as breaking**, even when the schema is unchanged — match-rate changes alter downstream numbers silently. Requires stewardship review and consumer notification |
| Tier 1 additive in a published mart | Deploy. Bump package minor version |
| Tier 1 **breaking** in a published mart | As Tier 0 breaking. Tier 2 consumers are enumerable. May also require investigating metadata in `exposures` |
| New source system | Add a Tier 0 project (i.e. a folder in the Bronze Layer project) or extend an existing one. Available to all consumers once future grants apply |
| **Full refresh of a Tier 0 incremental** | Not a routine operation, and requires coordination |

### 9.4 Full-refresh coordination

If a Tier 0 model is fully refreshed, every downstream incremental built from it may now rest on shifted history: rows that changed, or disappeared, outside any downstream watermark.

**Policy:** a full refresh of any published Bronze Layer model is a **coordinated event**, not a routine `--full-refresh` flag:

1. Announce to consumers identified by Snowflake impact query, with a stated window
2. Full-refresh the upstream model
3. Downstream projects full-refresh any incremental model depending on it, in reference-graph order (i.e. Tier 1 before Tier 2)
4. Record the event in the build control table so that potential discrepancies can later be correlated

---

## 10. Write-Audit-Publish

### 10.1 The problem with dbt test

By the time a test fails, the published tables have already been replaced and downstream projects have already been triggered. So tests observe the incident rather than prevent it.

### 10.2 Recommended standard for Bronze Layer (at least): clone-test-swap

In this approach, publication is gated behind a passing test suite:

```sql
-- 1. Clone the published schema (zero-copy, preserves incremental state)
CREATE OR REPLACE SCHEMA bronze_layer.build CLONE bronze_layer.published;

-- 2. Build into the clone
EXECUTE DBT PROJECT bronze_layer ARGS = 'build --target build';

-- 3. Only on success, atomic swap
ALTER SCHEMA bronze_layer.build SWAP WITH bronze_layer.published;

-- 4. Emit the completion event to the control table
```

Zero-copy cloning makes step 1 nearly free and preserves existing table state, so incremental models behave correctly rather than reverting to full refresh.

The clone-test-swap strategy may be useful for Tiers 1 and 2 as well as the Bronze Layer. An additional benefit is that rollback is never required if the `SWAP` hasn't occurred yet.

### 10.3 Well known gotchas

- **Snowflake's `SWAP WITH` has issues with `VIEW` definitions.** This is a critical issue to verify, and may require a more detailed procedure. The detailed design doc needs to cover this carefully. 
- **Grants do not follow the swap the way you expect.** `SWAP WITH` exchanges the objects between schemas. Object-level grants travel with the objects, so after a swap the newly-published objects carry whatever grants the *build* schema conferred. Apply **identical future grants to both schemas** so consumer access is correct regardless of which physical objects currently occupy the published schema. 
- **Cloning and swapping is per-schema.** A project writing to several schemas needs each cloned and swapped, and the swaps are not atomic with respect to one another. Prefer a single published schema per project.
 - How should test failures in the Bronze Layer be handled? One failed table from one source system should not prevent the entire analytics build. At the same time it is critical that nothing builds on top of failure. This may require the control table - certainly more detailed design is required. 

#### Conclusion: 
This build process needs careful testing, verification and documenting in the detailed design document.

---

## 11. Freshness and Staleness Contracts

### 11.1 The silent-wrongness problem in Tier 2

Consider what happens when  Tier 2 is triggered after all upstream Tier 1 tasks have completed. Suppose this example: finance completed this morning, digital last succeeded three days ago. Tier 2 builds successfully, publishes a cross-domain fact mixing fresh and stale data, and **no test anywhere fails.** The dashboard is wrong with no warning.

Completion is not freshness. Every cross-tier dependency needs a freshness assertion, not just a completion signal. A recommendation for this situation is needed in the detailed design document. One obvious way would be to utilise the control table to record watermarks.

### 11.2 Source freshness at the raw boundary

Tier 0 declares dbt source freshness on **every** raw object, with thresholds agreed with the platform team. `dbt source freshness` runs before the Tier 0 build and fails the run at the boundary, producing an alert whose owner is unambiguously ingestion rather than analytics.

---

## 12. References

### 12.1 dbt-Labs documentation

- [dbt-Labs Best practices](https://docs.getdbt.com/best-practices?version=2)
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


