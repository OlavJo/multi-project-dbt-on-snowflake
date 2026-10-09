# Multi-Project dbt on Snowflake

## Architecture Overview for Analytics Teams

### High Level Design: Engineering Discussion Document

| | |
|---|---|
| **Version** | 1.5 |
| **Date** | 2026-10-09 |
| **Status** | **Proposed** |
| **Proposed By** | Olav Jordens (Lead Data Engineer, PSM CAI) dbt Certified Developer |
| **Companion document** | *Silver Layer Design* v1.5 (referred to below as **SLD**) |

---

## 1. Executive Summary

The organisation has adopted Snowflake and intends to use open source dbt across multiple independent analytics teams, without proprietary dbt-Labs products. This document describes a multi-project dbt architecture built on **dbt Projects on Snowflake** (dbt Core, executed inside Snowflake). It uses **Snowflake's own feature set to replace dbt Mesh and dbt Cloud** wherever possible (Section 10).

It balances **enterprise governance** (version control, contracts, tests, access control) with **team independence** (separate repositories, CI, development and ownership, with integrated production pipelines).

dbt work is classified into Tiers, mapped onto the medallion layers:

| Tier | Name | Purpose | Ownership | Medallion |
|------|------|---------|-------|-----------|
|  | Raw/ Landing | Source data loaded into Snowflake | Platform engineers: Ingestion team | Bronze |
| 0 | Source Entity modules | Reconstruct source systems' logical models as typed, deduplicated, ready-to-consume base and entity models, with no business logic | Data engineers: Silver stewards | Silver |
| 0.5 | Core/ Consolidation module | Cross-source identity resolution, conformed dimensions, shared spines | Data engineers: Silver stewards | Silver |
| 1 | Domain projects | Business transformation: dims, facts, marts, semantic views | Analytics team + Business domain | Gold (published outputs) |
| 2 | Cross-Domain projects | Analytics spanning two or more domains | Analytics team + Business stakeholders | Gold (published outputs) |

Tiers 0 and 0.5 together form the **Silver Layer**: 

> - operated as **analytics infrastructure** by a steward team (of analytics support data engineers)
>
> - managed as a single dbt project and git repository
>
> - shared by every analytics team/ project. 
>
Tiers 1 and 2 are standard analytic dbt projects:

> - one per domain or cross-domain focus area
>
> - each with its own git repository
>
> - owned by the teams that build them
>
> - published outputs form the **Gold layer**

Projects connect only through each other's **published, contracted outputs**, referenced with `source()`. This replaces dbt Mesh's cross-project `ref()`. 

Every published model is versioned from v1, changes are managed with dbt model versions, and every published output carries a **watermark** stating how current its data is.

### 1.1 Why two documents

The Silver Layer's transformations are deliberately simple, yet it is where the hardest properties of the system meet:

- untrusted source data enters
- many teams share one artefact
- every downstream project inherits its guarantees
- its failures have the largest blast radius

The Silver Layer needs engineering features designed to deal robustly and effectively with these issues (amongst many others). Its design has its own documentation, starting with the SLD, written for data engineers contributing to Silver Layer implementation as well as data engineers in the role of Silver stewards.

The Silver Layer is designed for **safe change for consumers**: contracts, model versions, independent publication units and a phased scope let it evolve without disrupting consumers. 

**This document is for everyone building Tier 1 and Tier 2 projects.** It covers the Tiers, how to consume and contribute to the Silver Layer, change management, and what publication and freshness mean for your project. Within a Tier 1 or Tier 2 project, standard dbt-Labs practice applies and is not restated here.

### 1.2 Scope

This document and the companion SLD are High Level Design documents. 

In scope: the Tier structure, inter-project coupling, consumer and contributor responsibilities, change management, the Snowflake features replacing dbt Mesh and dbt Cloud, and the principles for runtime, security roles and PII that shape the architecture.

> **Deferred to the Low Level Design:** the role catalogue and access control, naming conventions, data testing standards, tagging, classification and PII handling, environment setup (dev, CI, prod), CI/CD pipelines, and cost attribution. Silver specific open items are listed in SLD Appendix C.

---

## 2. Design Constraints

- **Snowflake** as warehouse, including first class support for lineage, cloning and time travel. 
- **dbt Projects on Snowflake.** Projects are Snowflake objects deployed from a Git repository and invoked with `EXECUTE DBT PROJECT`. Snowflake manages the dbt runtime.
- **dbt runtime: dbt Core 1.12.3** (with dbt-snowflake 1.12.0), pinned explicitly with `DBT_VERSION` on every project object (otherwise `EXECUTE DBT PROJECT` defaults, currently to 1.9.4). dbt Fusion is the expected next runtime, adopted once its stability and migration path are confirmed.
- **dbt Mesh and dbt Cloud are unavailable.** Cross-project `ref` requires dbt-Labs Enterprise plans and metadata service; it is not available in dbt Core or Fusion standalone.
- **Multiple independent analytics teams** with separate sprints, CI and ownership, using a common set of enterprise data sources.
- **Snowflake Tasks** for scheduling and cross-project coordination.
- **Platform team owns ingestion only.** Analytics owns everything beyond the raw Bronze layer.
- **Batch ingestion.** Source data, including change-data-capture records, lands in batches on a daily or hourly cadence. The design is batch-oriented; continuous ingestion is a future extension (SLD Section 3.2).

### 2.1 Snowflake prerequisite: live-version dbt project objects

The architecture assumes **live-version** dbt project objects (Snowflake 2026_06 behaviour change bundle). A live-version object keeps dbt's run artifacts between executions. This enables:

- `dbt source freshness`
- `dbt retry`
- partial parsing
- `--state` comparisons, including Slim CI and defer to production

Version history of project code then lives in Git only. Because publication redirects models into build schemas, the artifacts of an ordinary production run are **not** a valid state for CI; a canonical state run is recorded after each publication (SLD Appendix A.2). Details, and how to opt in, are in SLD Appendix A.

---

## 3. Project Tiering

Cross-project references are constrained by a fixed Tier structure, which guarantees that dependencies are acyclic:

```
                                                                              ┐
Raw / Landing                                                                 ├─  Bronze (platform-owned)
  │                                                                           ┘
  └──► Tier 0 (Source Entities)             ◄─ may read Raw only              ┐
        │                                                                     ├─  Silver Layer
        ├──► Tier 0.5 (Core/ Consolidation) ◄─ may read Tier 0 (+ seeds)      ┘     (Single dbt project)
        │
        └──► Tier 1 (Domains)               ◄─ may read Tier 0 and Tier 0.5   ┐   Gold Layer (published outputs)
              │                                                               ├─    plus internal models
              └──► Tier 2 (Cross-Domain)    ◄─ may read Tier 1 and Tier 0.5   ┘     (Multiple dbt projects)
```

- **Tier 0 is purely data shaping.** It contains no business logic and no cross-source joins.
- **Tier 0.5 is the one governed exception.** It holds shared identity resolution and conformed dimensions (e.g. date), which must have exactly one owner.
- **Tiers 1 and 2 hold all business logic.** This is where data modelling comes to the fore: dims, facts, aggregates and semantic views.
- **Tier 2 may read Tier 0.5 conformed dimensions** (date, fiscal calendar, currency, geography) directly, so Tier 1 projects need not re-publish them. Tier 2 never reads Tier 0.
- **Lineage across project boundaries** is captured by Snowflake itself (Section 10).

### 3.1 The Silver Layer as analytics infrastructure

The Silver Layer is one dbt project in one repository, organised as one folder (module) per source system, plus the Core module. It writes to a single Silver database with one published schema per module. **The wider business has no access to it; only analytics teams building Tiers 1 and 2 can read it.**

Every analytics project starts by assessing the Silver Layer's published outputs. When a source system is onboarded, **base models for all of its tables are generated and published** (typed, renamed by convention, soft deletes and change-data-capture handled), so most needs are met without a contribution. Where something is still missing, such as an entity model or a source test, the team contributes it to the Silver Layer (Section 7) rather than rebuilding it in their own project. **Over time the Silver Layer becomes the organisation's catalogue of analysis-ready sources.**

The Silver Layer's scope is **phased**. Release 1 publishes base models (1:1 with source tables), change-data-capture current state and SCD2 history. Entity models (denormalised logical entities) follow once the publication machinery is proven, and are admitted by a rule of two (Section 7.1).

A named **steward team** owns the Silver Layer's repository, CI/CD, releases and operations, and approves every contribution within an agreed review service level.

### 3.2 What the Silver Layer guarantees to consumers

|  | Data Engineering Guarantee| Benefit for a Tier 1 or Tier 2 project |
|---|---|---|
| G1 | **Gated** | Nothing is published unless it passed its tests. A failed build leaves the previous good version in place |
| G2 | **Consistent** | All published outputs of one source reflect a single point in time; a join between two of them never sees half a batch |
| G3 | **Dated** | Every published unit carries a watermark ("data as of"). Staleness is visible, never silent |
| G4 | **Contracted, documented and classified** | Column names and types are enforced; descriptions are persisted into Snowflake and travel in the *source package*, a dedicated dbt package enabling Silver Layer referencing; every column derived from a classified source column carries its classification tag, so masking applies |
| G5 | **Versioned** | Every published model is versioned from v1. Breaking changes arrive as a new model version, alongside the old one, with a stated deprecation date |
| G6 | **Shaping only** | No business logic, except the governed identity resolution and conformance in Tier 0.5 |
| G7 | **Stable keys** | Durable keys issued by Tier 0.5 (for example `customer_key`) are never reassigned |
| G8 | **Historicised** | Where a source changes records in place, history is published with system-time validity taken from the source's own change ordering, so as-at queries are possible |

How each guarantee is achieved is described in the SLD.

---

## 4. Tier 1: Domain Projects

### 4.1 Definition

Each Tier 1 project is owned by one (virtual) domain team (finance, marketing, operations, digital, ...) and contains all business logic for that domain. **Each Tier 1 domain has its own repository and dbt project.**

Tier 1 projects, **by definition**, may only have sources pointing to the published outputs of the Silver Layer.

A "Shared Domain" project may be needed, but only as a last resort, if it becomes a prerequisite for multiple Tier 2 projects.

### 4.2 What a Tier 1 project may contain

In addition to `sources/` provided through the Silver Layer (using the source package):

| dbt Folder | Purpose | Published? | Medallion |
|-------|---------|------------|-----------|
| `intermediate/` | Business-logic transformations, joins, filters, aggregations | No: internal | none (internal) |
| `marts/` | Dims and facts: contracted, documented, tested outputs | Yes | Gold |
| `semantic/` | Metric definitions, as Snowflake semantic views | Yes | Gold |
| `presentation/` | Dashboard-optimised denormalisations | Yes | Gold |
| `features/` | ML feature tables | Yes | Gold |

### 4.3 Published versus internal

Only `marts/`, `semantic/`, `presentation/` and `features/` are consumable outside the project. This is enforced by grants, not by convention. Models are published in groups called *units*.

- **One schema per publication unit.** Published and internal models of a unit build into the same schema, so the whole unit publishes with a single atomic swap (Section 9). Published models are distinguished by naming prefix (`dim_`, `fct_`, `sem_`, `pres_`, `feat_`).
- **Internal models are never granted.** Consumer access is granted per published model, using dbt's `grants` config. Internal models carry no grants, so no other role can read them. This is the same mechanism that keeps the Silver Layer's frozen inputs invisible (SLD Section 9.6).
- **A project may define more than one publication unit** when parts of it have independent consumers (for example, ML features separate from finance marts), so that a failing test in one part does not hold back the other. One unit per project is the default.

A downstream project that needs an intermediate model has found a modelling gap. The answer is to promote the model to a mart, not to grant access.

### 4.4 Contracts

All published models enforce contracts and are **versioned from v1**, declared in the model's YAML alongside full column specifications:

```yaml
models:
  - name: fct_revenue
    latest_version: 1
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
    versions:
      - v: 1
  ...
```

Notes:

- On Snowflake only `not_null` constraints are enforced; other constraints are declarative.
- Contracted incremental models require `on_schema_change: append_new_columns` or `fail`.
- Versioning from v1 gives consumers stable relation names (`fct_revenue_v1`) from the start, and makes dbt treat breaking changes as errors rather than warnings (Section 8.1).

### 4.5 One semantic layer

- Metrics are defined once, in the domain that owns the concept, as Snowflake semantic views.
- Cross-domain metrics are defined in the Tier 2 project that owns the analysis, and may not redefine a Tier 1 metric.

### 4.6 Conformed keys pass through unaltered

Conformed dimensions only conform if domains do not re-derive them. Finance and digital may each publish their own `dim_customer`, but both **must carry the Tier 0.5 durable key (`customer_key`) unaltered** and must not apply their own identity matching. This is what makes Tier 2 joins across domain marts valid.

---

## 5. Tier 2: Cross-Domain Projects

A Tier 2 project is warranted when:

- The business focus areas **span two or more Tier 1 domains**.
- It supports an **ongoing dashboard or application**, not an ad hoc investigation. Ad hoc means writing queries against existing marts, even if the query runs regularly. Tier 2 exists to materialise cross-domain marts; materialising transforms is the purpose of dbt.
- It is complex enough to warrant tests, contracts and documentation.
- It does not naturally belong to either source domain.

**Example:** "Impact of digital journey on financial performance" consumes the finance mart and the digital mart and belongs in neither. It gets its own project.

Governance is **identical** to Tier 1: owned, contracted, versioned, tested, documented.

Tier 2 depends on Tier 1 outputs and on Tier 0.5 conformed dimensions only. Analysts starting a Tier 2 project may first need to contribute to a Tier 1 project, even if only a skeleton.

---

## 6. Consuming Upstream Outputs

### 6.1 The mechanism: `source()` to published outputs

A higher-Tier project declares the published relations it reads as dbt `sources`, resolved per environment:

```yaml
sources:
  - name: silver_erp
    database: "{{ env_var('DBT_SILVER_DB') }}"
    schema: erp
    tables:
      - name: ent_erp__orders
        identifier: ent_erp__orders_v1
        description: "One row per order..."
        columns:
            - name: order_id
              description: "Unique identifier..."
            ...
```

> **Never hard-code a production database name in source definitions.** Resolution is by `env_var` or by target, always. `EXECUTE DBT PROJECT` supplies these through its `ENV_VARS` parameter. This lets the same project run against development, CI and production.

The dbt community calls this pattern the *source-hack* as it extends the original intention of the `source`. Combined with the Tier structure and Snowflake's lineage,  cloning, and schema swapping, it gives most of what dbt Mesh offers (Section 10). Note that cross-project references in dbt Mesh resolve in a similar way, but the dbt-Labs platform provides a far greater level of integration across multiple projects. The remaining gaps are listed in Section 6.4.

**NOTE: Development and CI environments of Tier 1 and Tier 2 projects read the production Silver Layer directly**, read-only. No clone of Silver is needed for downstream development, just as no clone of Raw is needed for Silver development.

### 6.2 The Silver sources package

Tier 1 projects do not write Silver source definitions by hand. Silver CI **generates** them from the Silver manifest (relations, columns, types, descriptions, classification tags, model versions and deprecation dates) and releases them as a versioned dbt package with every Silver release. The package also carries shared macros and generic tests, including:

- `upstream_fresh` (Section 9.2), at `warn` severity
- a deprecation test that warns as a consumed model version approaches its `deprecation_date`, since dbt's own deprecation warnings only reach `ref()` consumers, never `source()` consumers

**It contains no models.**

Consumers reference a moving major-version tag and rely on dbt's `package-lock.yml` for reproducibility:

```yaml
packages:
  - git: "https://git.internal/silver-layer-dbt-sources.git"
    revision: "v1"          # moving major tag; the exact commit is locked in package-lock.yml
```

A consumer picks up new Silver tables by refreshing its lock file (`dbt deps --upgrade`), which can be automated. Additive column changes need no package update at all, since dbt Core does not validate selected columns against source declarations. CI runs `dbt deps` and deploys the project with `dbt_packages` included, so no external network access is needed from Snowflake.

Tier 2 projects declare the Tier 1 outputs they read in the same way. A generated package per Tier 1 project is optional, since Tier 2 consumers are few and named.

### 6.3 Why not import packages containing models?

A package containing *models* would couple projects through code rather than through contracted outputs. **This is explicitly out of scope**:

| Risk | Severity |
|------|----------|
| Accidental rebuild of upstream models (`dbt build --select +model`) | Potential production incident |
| Schema resolution defaults to the consumer's target schema | Wrong schema, wrong role |
| Per-environment `+schema` / `+database` overrides in every consumer | Operational surface area |

Snowflake has documented an approach to cross-project dependencies using a copy of the referenced project in the consuming project. This is a package-with-models approach and is rejected for the above reasons. dbt-Labs has built dbt Mesh precisely to avoid cross-project dependency management using dbt packages.

### 6.4 Known limitations and mitigations

| Limitation | Mitigation | Residual risk |
|-----------|------------|---------------|
| No compile-time validation of upstream schema | Contracts and versioning from v1 in the producing project; generated source package; breaking changes only through model versions. dbt Fusion's strict static analysis may later add compile-time column validation | A producer can still break a consumer between version bumps; rare for the shaping-only Silver Layer |
| dbt DAG severed at the project boundary | Snowflake lineage spans the boundary; column descriptions travel in the source package and in Snowflake | The dbt DAG view alone stops at the boundary |
| `state:modified` does not cross the boundary | Each project compares against its own canonical production state | Cross-project impact is found with an impact query (Section 8.3), not dbt |
| Freshness of upstream projects is not visible in dbt by default | Watermarks in run control and the `upstream_fresh` test (Section 9) | None significant |

---

## 7. Contributing to the Silver Layer

The Silver Layer does not take much effort to set up once the raw Bronze Layer is populating. It is phased, i.e. it evolves as we commit more structure (tests, column transformations, entities, etc.).

### 7.1 When to contribute

Contribute when your project needs a source system, an entity model or a source test that the Silver Layer does not yet publish. **Base models for onboarded sources are generated** (Silver stewards can easily set this up), so a contribution is rarely needed just to read a table. Do not rebuild shaping logic inside a Tier 1 project: every other team would have to rebuild it too. The organisation manages and monitors consistent source data structure, quality and presentation into the analytics community through the Silver Layer.

**Entity models are admitted by a rule of two:** an entity model enters the Silver Layer when two consumers need it, or when the stewards judge the source too normalised to consume through base models alone. Until then, the join belongs in the Tier 1 project that needs it.

### 7.2 How

- Open a pull request against the Silver repository. `CODEOWNERS` routes it to the Silver stewards for the affected source module, and a steward approval is required to merge. Stewards respond within the review service level recorded with the Silver Layer's other service levels (SLD Section 1.2).
- A new source system becomes a new module (folder), with its own publication unit, run-control entries and generated base models. Stewards set these up with you.
- Say which outputs you need, so the right entity models and tests are prioritised.

### 7.3 The rules your contribution is reviewed against

- **The structural join rule.** Joins follow the source's own foreign keys, preserve the entity root's grain or enrich many:1, and would be made regardless of any analysis. `order_items LEFT JOIN orders ON order_items.order_id = orders.order_id` is allowed; `... WHERE orders.status = 'completed'` is not.
- **Permitted filters.** Only source-semantic filters are allowed: soft deletes, CDC deduplication, and tenant scoping.
- **The scope-creep test.** **If two competent analysts could compute it differently, it belongs in Tier 1.** `order_item_count` is shaping; `net_revenue` is business logic.
- **Test placement.** Tests are placed where the defect can first occur. Source assertions (not null, accepted values, castability, uniqueness at the source's grain) go on the Silver **frozen inputs**. Tests of shaping logic (entity grain, fan-out, deduplication, history validity) go on Silver outputs. Most tests a Tier 1 team contributes are source assertions. See SLD Section 7.
- **Classification.** Classification tags propagate automatically from raw through the Silver Layer to derived columns, and the tag's masking policy applies to them. A derived column that is genuinely not sensitive is declassified by an explicit, reviewed tag value, never by removing the tag (SLD Section 5.3). CI checks derived columns against the raw tags.

---

## 8. Change Management

### 8.1 What contracts actually do

**A contract asserts that a model's output matches its own YAML specification.** It does not protect consumers in another project, because the producer can change the specification itself. Rename a column in both the SQL and the YAML and the producer's build succeeds; downstream `source()` references break at runtime.

**All changes to published outputs therefore require impact analysis and dbt model versioning.**

What contracts *do* provide:

- A published model cannot silently drift from its declared specification. A mis-typed column or dropped field fails the producer's build.
- With state comparison against the producer's canonical production manifest, dbt **detects breaking contract changes** in the producer's CI: an error for versioned models, only a warning for unversioned ones. Versioning every published model from v1 (Section 4.4) makes the check blocking. It cannot tell whether any consumer minds.
- A machine-readable schema that travels to consumers in the generated source package.

### 8.2 Model versions and deprecation dates

Protection for consumers comes from versioning, not enforcement. In this example, v2 renames `total_amount` to `order_total_amount`:

```yaml
models:
  - name: ent_erp__orders
    latest_version: 2
    config:
      contract:
        enforced: true
    columns:
      - name: order_id
        data_type: varchar
      - name: order_date
        data_type: date
      - name: order_total_amount
        data_type: number(38,2)
    versions:
      - v: 1
        deprecation_date: 2027-03-31
        columns:
          - include: all
            exclude: [order_total_amount]
          - name: total_amount          # pre-rename name
            data_type: number(38,2)
      - v: 2
```

A version inherits all top-level columns by default, so v1 must exclude the renamed column and declare its old name. Without the `include`/`exclude`, v1 would carry both columns and its contract would fail.

Versioned models materialise to **distinct relations** (`ent_erp__orders_v1`, `ent_erp__orders_v2`), so both exist simultaneously. That is the migration window a `source()`-based consumer needs:

1. The producer ships `v2` alongside `v1` and sets `v1`'s `deprecation_date`.
2. The source package is regenerated with a new minor version; the new relation and the deprecation date appear in consumers' source definitions.
3. Each consumer migrates its `source()` reference on its own schedule, within the window. The packaged deprecation test warns as the date approaches.
4. After the deprecation date, the producer drops `v1`.

**Cadence.** Following dbt-Labs guidance, breaking changes to widely used models are batched into a predictable version cadence (once or twice a year, announced in advance) rather than released one at a time. This applies to the Silver Layer in particular.

dbt's `latest_version_pointer` (an unversioned view onto the latest version) requires dbt 1.12, which is the pinned runtime, so it is available. This design keeps consumers on an explicit version: a pointer would move consumers to a new version without their migration, which is what the deprecation window exists to avoid.

### 8.3 Impact analysis

Before any breaking change, the producer runs the standing **"who consumes this?" query**, which unions two sources:

- **The consumer manifest registry.** Each Tier 1 and Tier 2 project's CI writes the sources it declares into a shared run-control table on every production deploy. This is deterministic, immediate, and covers every dbt consumer, including tables and incrementals.
- **`ACCESS_HISTORY`** (Enterprise Edition; up to 3 hours latency; 365 days retention), which sees every other consumer, including BI tools and ad hoc users that no dbt manifest knows about, with column-level detail. It omits failed queries and intermediate views.

`OBJECT_DEPENDENCIES` adds view-to-object dependencies, but it does not record tables built by `CREATE TABLE AS SELECT`, `INSERT` or `MERGE`, so on its own it misses most dbt consumers. Because published objects are replaced on each publication (Section 9.4), access-history queries match objects by name, not by object id.

Together these replace dbt-Labs dbt Explorer's cross-project impact view.

### 8.4 Change management matrix

| Change | Process |
|--------|---------|
| Silver additive (new column, new entity model) | Deploy. Source package minor version bumped by CI. Consumers unaffected |
| Silver **breaking** (rename, drop, retype) | Impact query. New model version with `deprecation_date`, in the next scheduled version window. Notify named consumers. Drop old version after the date |
| Tier 0.5 crosswalk rule change | **Always treated as breaking**, even when the schema is unchanged, because match-rate changes alter downstream numbers silently. Steward review and consumer notification. Durable keys are never reassigned |
| Tier 1 additive in a published mart | Deploy. Bump package or consumer documentation minor version |
| Tier 1 **breaking** in a published mart | As Silver breaking. Tier 2 consumers are enumerable from the manifest registry; also check `exposures` metadata |
| New source system | New Silver module, with publication unit, run-control entries and generated base models (Section 7.2) |
| **Full refresh of a Silver incremental** | Coordinated event (Section 8.5) |

### 8.5 Full-refresh coordination

If a Silver model is fully refreshed, downstream incrementals may rest on shifted history. A full refresh is therefore a **coordinated event**:

1. The stewards announce it to consumers identified by impact query, with a stated window.
2. The upstream model is fully refreshed through the normal publication procedure, so it is still gated by tests.
3. Downstream projects fully refresh dependent incrementals in reference-graph order (Tier 1 before Tier 2).
4. The event is recorded in run control, so later discrepancies can be correlated.

---

## 9. Publication and Freshness in Your Project

Tier 1 and Tier 2 projects use the same publication and freshness mechanism as the Silver Layer (SLD Sections 9 to 11). What it means for a project team:

### 9.1 Publication

- **Each project is a publication unit** by default (Section 4.3). A unit has one schema, holding published and internal models, and a build counterpart.
- **Builds happen in a zero-copy clone** of the last good state. Only if the build reports success and every `error`-severity test passes is the clone swapped into place, atomically.
- **A failed build changes nothing.** Consumers keep the last good version, so a failure shows downstream as staleness, not as wrong data. There is nothing to roll back. Resolving the failure leads to recovery on the next batch.
- **A unit that reads other units (Silver or Tier 1) reads consistent upstream versions.** At the gate, the unit takes zero-copy clones of the upstream published schemas it reads, and builds against those, so an upstream publication mid-build cannot split its reads between two versions (SLD Section 9.4 and 9.6).
- **A code release triggers a build.** Deploying a new version of the project records a `DEPLOYED` event, which lets the next gated run publish even if no upstream data has changed. Deployment is an in-place `ALTER DBT PROJECT ... ADD VERSION`, which keeps the project object's grants and canonical state (SLD Appendix A.2).
- **Published outputs are materialised.** Views are permitted in published schemas only under the rules in SLD Sections 9.5 and 9.6.
- **Test severity carries meaning.** `error` blocks publication; `warn` publishes and is logged. Use `error` only for tests that evidence wrong data.

### 9.2 Freshness

- **Every published unit has a watermark**: the time its data is current as of. A project's watermark is the **minimum** of its upstream watermarks, so staleness propagates automatically through the Tiers.
- **Each project declares a tolerance per upstream.** Before building, the project's gate checks every upstream watermark against its tolerance and skips the build if any is exceeded. This prevents the silent-wrongness problem: a cross-domain fact mixing fresh finance data with three-day-old digital data, with no failing test anywhere.
- **Dashboards should display the watermark as "data as of".**
- **Freshness is visible in dbt** through the `upstream_fresh` test in the source package. It runs at `warn` severity, because the gate already enforces tolerance before any build starts.

### 9.3 What a project team configures

- the project's upstream dependencies and freshness tolerances (run-control configuration, reviewed by pull request)
- the project's schedule, and any additional publication units
- `grants` on published models
- test severities
- classification tags on published columns

### 9.4 Published objects are replaced on every publication

Publishing by clone and swap means every published table and view is a **new Snowflake object** after each publication. This is an accepted consequence of the design (SLD Section 9.8). For consumers it means:

- **Do not create streams or dynamic tables on published objects.** A stream loses its offset, and a dynamic table must reinitialise, whenever the object is replaced.
- **Time travel on a published object reaches back only to its current publication.** For history, use the published history models (G8); for "what changed", use the run-control log.
- **The change signal is the watermark and the `PUBLISHED` event in run control**, not object metadata. Downstream incrementals filter on data columns (for example `_loaded_at`), never on table versions.
- **Monitoring that is keyed to object ids** (data metric function history, lineage, access history) restarts with each publication; query by name.

---

## 10. Snowflake in Place of dbt Mesh and dbt Cloud

Embedded dbt on Snowflake was chosen deliberately, so this architecture uses Snowflake's feature set to the fullest to replace what dbt-Labs products would otherwise provide. 

*Status*: **Documented** means the capability is documented by Snowflake and was checked while preparing this design; **Design** means it is a construct of this architecture; **To confirm** means availability or fit must be confirmed for our account and edition.

| Capability | dbt-Labs offering | Replacement in this architecture | Status |
|---|---|---|---|
| Cross-project references | dbt Mesh cross-project `ref` | `source()` to published outputs; generated source package (Section 6) | Design |
| Cross-project access control | Mesh model access (`public`, `protected`, `private`) | One schema per unit, database roles, per-model grants via dbt `grants`; internal models never granted | Design |
| Hosted execution and IDE | dbt Cloud | dbt project objects, `EXECUTE DBT PROJECT`, Snowsight Workspaces; dbt Core 1.12.3 pinned with `DBT_VERSION` | Documented |
| Job scheduling | dbt Cloud jobs | Snowflake Tasks, one gated task per publication unit | Documented |
| State-aware orchestration | dbt Cloud (Fusion) | Freshness gates and propagated watermarks in run control (SLD Section 11) | Design |
| Source freshness scheduling and history | dbt Cloud jobs and dbt Explorer (the `dbt source freshness` command itself is dbt Core) | Freshness gate on landing-complete records in run control; `dbt source freshness` on live-version objects for observability | Documented |
| Slim CI and defer to production | dbt Cloud CI | Live-version `--state`, imported from the canonical state run (SLD Appendix A.2); per-pull-request databases (Snowflake has a published Slim CI tutorial) | Documented, with design caveat |
| Development environments | dbt Cloud environments | Per-developer and per-pull-request databases; downstream projects read production Silver read-only | Design |
| Write-audit-publish | (none built in) | Clone, build, test, atomic `SWAP` (SLD Section 9) | Design |
| Documentation and DAG | dbt Explorer | `persist_docs` into Snowflake; Snowsight dbt project details page; `dbt docs generate --static` | Documented |
| Cross-project lineage and impact | dbt Explorer | Snowflake lineage (including column-level lineage in the dbt DAG), `ACCESS_HISTORY`, consumer manifest registry (Section 8.3) | Documented |
| Run metadata | dbt Discovery API | dbt artifacts written back to live-version objects; Snowflake telemetry event table; run-control log | Documented |
| Semantic layer | dbt Semantic Layer (MetricFlow) | Snowflake semantic views, built from dbt (Snowflake-Labs `dbt_semantic_view` package) | To confirm |
| Alerting | dbt Cloud notifications | Task failure notifications; Snowflake alerts on run-control views | To confirm |
| Continuous data quality monitoring | (partner tools) | Snowflake data metric functions on published outputs (history restarts per publication, Section 9.4) | To confirm |
| PII protection | (none built in) | Classification tags that propagate through views and tables (`PROPAGATE = ON_DEPENDENCY_AND_DATA_MOVEMENT`), tag-based masking policies, and a reserved tag value for declassification (SLD Section 5.3) | Documented |
| Cost attribution | (none built in) | Warehouse per Tier or unit, query and object tags, resource monitors | Design |

One dbt Mesh capability has no Snowflake equivalent today: **compile-time validation of references across projects**. It is mitigated by contracts, the generated source package and versioning (Section 6.4).

---

## 11. Alternatives Considered

| Alternative | Assessment |
|---|---|
| **dbt Mesh / dbt Cloud Enterprise** | Native cross-project `ref` through the dbt platform's metadata service. Unfunded; excluded by constraint |
| **Single project for all Tiers, with dbt groups and model access** | Compile-time boundary enforcement, full lineage and working `state:modified` across all teams. Rejected because independent teams would share one CI pipeline, one dbt version and one deployment, and one broken merge would block everyone. Adopted *within* the Silver Layer, where shared stewardship is the intent |
| **Multi-project monorepo** (many dbt projects in one repository, path-filtered CI) | Keeps independent projects, deployments and dbt versions, and adds atomic producer-and-consumer pull requests for breaking changes. Not chosen as the default because repository permissions and CI scale become shared concerns; remains an option for closely related Tier 1 and Tier 2 projects |
| **dbt-loom** (open-source cross-project `ref` from upstream manifests) | Requires a Python plugin installed into the dbt runtime, which Snowflake's managed runtime does not allow |
| **Snowflake's native cross-project dependency** (copy the referenced project into the consumer) | A package-with-models approach; rejected for the reasons in Section 6.3 |
| **End-to-end projects, each with its own staging layer** | The conventional single-project layout. Rejected because every team would rebuild the same sourcing and shaping, with divergent results, and none would get the Silver guarantees in Section 3.2 |
| **dbt Fusion runtime from the start** | Faster parsing and strict static analysis, generally available on Snowflake. Deferred until its stability and the migration path for this design's custom macros are confirmed. The custom macros and the freeze `run-operation` are Core-specific, so this is a precondition of any move |

Alternatives for the Silver Layer's internal mechanisms are recorded in SLD Appendix B.

---

## 12. References

### 12.1 dbt-Labs documentation

- [dbt-Labs Best practices](https://docs.getdbt.com/best-practices?version=2)
- [dbt: How we structure our projects](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview)
- [dbt: Data contracts](https://docs.getdbt.com/docs/mesh/govern/model-contracts)
- [dbt: Model versions](https://docs.getdbt.com/docs/mesh/govern/model-versions)
- [dbt: Model access](https://docs.getdbt.com/docs/collaborate/govern/model-access)
- [dbt: Packages](https://docs.getdbt.com/docs/build/packages)
- [dbt: freshness](https://docs.getdbt.com/reference/resource-configs/freshness)
- [dbt: Mesh project dependencies](https://docs.getdbt.com/docs/mesh/govern/project-dependencies)
- [dbt: Snowflake configurations](https://docs.getdbt.com/reference/resource-configs/snowflake-configs)
- [dbt: About static analysis (Fusion)](https://docs.getdbt.com/docs/build/about-static-analysis)

### 12.2 Snowflake documentation

- [dbt Projects on Snowflake](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake)
- [dbt Projects on Snowflake: supported commands](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-supported-commands)
- [dbt Projects on Snowflake: supported dbt versions](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-dbt-core-versions)
- [dbt Projects on Snowflake: Slim CI and defer to production](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-slim-ci-defer-to-prod)
- [EXECUTE DBT PROJECT](https://docs.snowflake.com/en/sql-reference/sql/execute-dbt-project)
- [dbt project objects migrate to a single mutable live version (2026_06 bundle)](https://docs.snowflake.com/en/release-notes/bcr-bundles/2026_06/bcr-2362)
- [Task graphs and dependencies](https://docs.snowflake.com/en/user-guide/tasks-graphs)
- [OBJECT_DEPENDENCIES](https://docs.snowflake.com/en/sql-reference/account-usage/object_dependencies) and [ACCESS_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/access_history)
- [Cloning considerations](https://docs.snowflake.com/en/user-guide/object-clone)
- [Tag-based masking policies](https://docs.snowflake.com/en/user-guide/tag-based-masking-policies)
- [Resource monitors](https://docs.snowflake.com/en/user-guide/resource-monitors)

### 12.3 Tooling and community

- [dbt-utils](https://github.com/dbt-labs/dbt-utils)
- [dbt-core discussion #5244: cross-project lineage](https://github.com/dbt-labs/dbt-core/discussions/5244)
- [Databricks: medallion lakehouse architecture](https://docs.databricks.com/aws/en/lakehouse/medallion)

---

---
