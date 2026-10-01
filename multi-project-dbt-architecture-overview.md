# Multi-Project dbt on Snowflake

## Architecture Overview for Analytics Teams

### High Level Design: Engineering Discussion Document

| | |
|---|---|
| **Version** | 3.0 |
| **Date** | 2026-10-01 |
| **Status** | **Proposed** for review |
| **Proposed By** | Lead Data Engineer, PSM CAI |
| **Companion document** | *Bronze Layer Design* v1.0 (referred to below as **BLD**) |

> **Revision note** *(remove before circulation)*
>
> This document and the Bronze Layer Design together supersede HLD v2.4. Everything an analytics team needs to build, consume and contribute is here. The design of the Bronze Layer itself, including publication, freshness and run control, is in the BLD. No design decisions changed in the split; the new material is the Snowflake feature map (Section 10) and the Bronze guarantees (Section 3.2).

---

## 1. Executive Summary

The organisation has adopted Snowflake and intends to use dbt across multiple independent analytics teams, without funding for dbt-Labs products. This document describes a multi-project dbt architecture built on **dbt Projects on Snowflake** (open source dbt Core, executed inside Snowflake). It uses **Snowflake's own feature set to replace dbt Mesh and dbt Cloud** wherever possible (Section 10).

It balances **enterprise governance** (consistent naming, contracts, tests, access control) with **team independence** (separate repositories, CI, development and ownership, with integrated production pipelines).

dbt work is classified into Tiers:

| Tier | Name | Purpose | Owner |
|------|------|---------|-------|
| n/a | Raw / Landing | Source data loaded into Snowflake | Platform team (ingestion only) |
| 0 | Source Aggregate modules | Reconstruct source systems' logical models as ready-to-consume aggregates, with no business logic | Bronze stewards |
| 0.5 | Core / Consolidation module | Cross-source identity resolution, conformed dimensions, shared spines | Bronze stewards |
| 1 | Domain projects | Business transformation: dims, facts, marts, semantic views | Domain teams |
| 2 | Cross-Domain projects | Analytics spanning two or more domains | Business-question owners |

Tiers 0 and 0.5 together form the **Bronze Layer**: a single dbt project, operated as a **platform product** by a steward team and shared by every analytics team. Tiers 1 and 2 are ordinary dbt projects, one per domain or cross-domain question, owned by the teams that build them.

Projects connect only through each other's **published, contracted outputs**, referenced with `source()`. This replaces dbt Mesh's cross-project `ref()`. Changes are managed with dbt model versions, and every published output carries a **watermark** stating how current its data is.

### 1.1 Why two documents

The Bronze Layer's transformations are deliberately trivial, yet it is where the hardest properties of the system meet:

- untrusted source data enters
- many teams share one artefact
- every downstream project inherits its guarantees
- its failures have the largest blast radius

Getting it right first time, without later overhaul, is the single most important outcome of this design. Its design therefore has its own document, the BLD, written for Bronze stewards and platform engineers.

This document is for everyone building Tier 1 and Tier 2 projects. It covers the Tiers, how to consume and contribute to the Bronze Layer, change management, and what publication and freshness mean for your project. Within a Tier 1 or Tier 2 project, standard dbt-Labs practice applies and is not restated here.

### 1.2 Scope

In scope: the Tier structure, inter-project coupling, consumer and contributor responsibilities, change management, and the Snowflake features replacing dbt Mesh and dbt Cloud.

A detailed design document will cover: role assignments and access control, naming conventions, data testing standards, tagging, classification and PII handling, environment setup (dev, CI, prod), CI/CD pipelines, and cost attribution.

---

## 2. Design Constraints

- **Snowflake** as warehouse, including first class support for lineage, cloning and Time Travel.
- **dbt Projects on Snowflake.** Projects are Snowflake objects deployed from a Git repository and invoked with `EXECUTE DBT PROJECT`. Snowflake manages the dbt runtime.
- **dbt Mesh and dbt Cloud are unavailable.**
- **Multiple independent analytics teams** with separate sprints, CI and ownership, using a common set of enterprise data sources.
- **Snowflake Tasks** for scheduling and cross-project coordination.
- **Platform team owns ingestion only.** Analytics owns everything beyond the raw layer.

### 2.1 Platform prerequisite: live-version dbt project objects

The architecture assumes **live-version** dbt project objects (Snowflake 2026_06 behaviour change bundle). A live-version object keeps dbt's run artifacts between executions. This enables:

- `dbt source freshness`
- `dbt retry`
- partial parsing
- `--state` comparisons, including Slim CI

Version history of project code then lives in Git only. Details, and how to opt in, are in BLD Section 3.

---

## 3. Project Tiering

Cross-project references are constrained by a fixed Tier structure, which guarantees that dependencies are acyclic:

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

- **Tier 0 is purely data shaping.** It contains no business logic and no cross-source joins.
- **Tier 0.5 is the one governed exception.** It holds shared identity resolution and conformed dimensions, which must have exactly one owner.
- **Tiers 1 and 2 hold all business logic.** This is where data modelling comes to the fore: dims, facts, aggregates and semantic views.
- **Lineage across project boundaries** is captured by Snowflake itself (Section 10).

### 3.1 The Bronze Layer as a platform product

The Bronze Layer is one dbt project in one repository, organised as one folder (module) per source system plus the Core module. It writes to a single Bronze database with one published schema per module. The wider business has no access to it; only analytics teams building Tiers 1 and 2 can read it.

Every analytics project starts by assessing the Bronze Layer's published outputs. Where something is missing, the team contributes it to the Bronze Layer (Section 7) rather than rebuilding it in their own project. Over time the Bronze Layer becomes the organisation's catalogue of analysis-ready sources.

A named **steward team** owns the Bronze Layer's repository, CI, releases and operations, and approves every contribution.

### 3.2 What the Bronze Layer guarantees to consumers

| # | Guarantee | Meaning for a Tier 1 or Tier 2 project |
|---|---|---|
| G1 | **Gated** | Nothing is published unless it passed its tests. A failed build leaves the previous good version in place |
| G2 | **Consistent** | All published outputs of one source reflect a single point in time; a join between two of them never sees half a batch |
| G3 | **Dated** | Every published unit carries a watermark ("data as of"). Staleness is visible, never silent |
| G4 | **Contracted and documented** | Column names and types are enforced; descriptions are persisted into Snowflake and travel in the source package |
| G5 | **Versioned** | Breaking changes arrive as a new model version, alongside the old one, with a stated deprecation date |
| G6 | **Shaping only** | No business logic, except the governed identity resolution and conformance in Tier 0.5 |
| G7 | **Stable keys** | Durable keys issued by Tier 0.5 (for example `customer_key`) are never reassigned |

How each guarantee is achieved is described in the BLD.

---

## 4. Tier 1: Domain Projects

### 4.1 Definition

Each Tier 1 project is owned by one (virtual) domain team (finance, marketing, operations, digital, ...) and contains all business logic for that domain. **Each Tier 1 domain has its own repository and dbt project.**

Tier 1 projects, **by definition**, may only have sources pointing to the published outputs of the Bronze Layer.

A "Shared Domain" project may be needed, but only as a last resort, if it becomes a prerequisite for multiple Tier 2 projects.

### 4.2 What a Tier 1 project may contain

| dbt Folder | Purpose | Published? | Medallion |
|-------|---------|------------|-----------|
| `intermediate/` | Business-logic transformations, joins, filters, aggregations | No: internal | Silver |
| `marts/` | Dims and facts: contracted, documented, tested outputs | Yes | Gold |
| `semantic/` | Metric definitions, as Snowflake semantic views | Yes | Gold |
| `presentation/` | Dashboard-optimised denormalisations | Yes | Gold |
| `features/` | ML feature tables | Yes | Gold |

### 4.3 Published versus internal

Only `marts/`, `semantic/`, `presentation/` and `features/` are consumable outside the project. This is enforced by schemas and grants, not by convention:

- All published folders build into **one published schema per project**, distinguished by naming prefix (`dim_`, `fct_`, `sem_`, `pres_`, `feat_`). A single published schema is what allows the project to publish atomically (Section 9).
- Internal models build into a separate internal schema that no other role can read.
- Consumer access is granted per published model, using dbt's `grants` config.

A downstream project that needs an intermediate model has found a modelling gap. The answer is to promote the model to a mart, not to grant access.

### 4.4 Contracts

All published models enforce contracts, declared in the model's YAML alongside full column specifications:

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

### 4.5 One semantic layer

- Metrics are defined once, in the domain that owns the concept, as Snowflake semantic views.
- Cross-domain metrics are defined in the Tier 2 project that owns the question, and may not redefine a Tier 1 metric.

### 4.6 Conformed keys pass through unaltered

Conformed dimensions only conform if domains do not re-derive them. Finance and digital may each publish their own `dim_customer`, but both **must carry the Tier 0.5 durable key (`customer_key`) unaltered** and must not apply their own identity matching. This is what makes Tier 2 joins across domain marts valid.

---

## 5. Tier 2: Cross-Domain Projects

A Tier 2 project is warranted when:

- The business questions **span two or more Tier 1 domains**.
- It supports an **ongoing dashboard or application**, not an ad hoc investigation. Ad hoc means writing queries against existing marts, even if the query runs regularly. Tier 2 exists to materialise cross-domain marts; materialising transforms is the purpose of dbt.
- It is complex enough to warrant tests, contracts and documentation.
- It belongs naturally to neither source domain.

**Example:** "Impact of digital journey on financial performance" consumes the finance mart and the digital mart and belongs in neither. It gets its own project.

Governance is **identical** to Tier 1: owned, contracted, tested, documented.

Tier 2 depends only on Tier 1 outputs. Analysts starting a Tier 2 project may first need to contribute to a Tier 1 project, even if only a skeleton.

---

## 6. Consuming Upstream Outputs

### 6.1 The mechanism: `source()` to published outputs

A higher-Tier project declares the published relations it reads as dbt sources, resolved per environment:

```yaml
sources:
  - name: bronze_erp
    database: "{{ env_var('DBT_BRONZE_DB') }}"
    schema: erp
    tables:
      - name: ent_erp__orders
        description: "One row per order..."
        columns:
            - name: order_id
              description: "Unique identifier..."
            ...
```

> **Never hard-code a production database name in source definitions.** Resolution is by `env_var` or by target, always. This lets the same project run against development, CI and production.

The dbt community calls this pattern the *source-hack*. Combined with the Tier structure and Snowflake's lineage and cloning, it gives most of what dbt Mesh offers (Section 10). The remaining gaps are listed in Section 6.4.

### 6.2 The Bronze sources package

Tier 1 projects do not write Bronze source definitions by hand. Bronze CI **generates** them from the Bronze manifest (relations, columns, types, descriptions) and releases them as a versioned dbt package with every Bronze release. The package also carries shared macros and generic tests, including an `upstream_fresh` test (Section 9.2). It contains no models.

Consumers pin a version:

```yaml
packages:
  - git: "https://git.internal/bronze-layer-dbt-sources.git"
    revision: "1.4.0"
```

CI runs `dbt deps` and deploys the project with `dbt_packages` included, so no external network access is needed from Snowflake.

Tier 2 projects declare the Tier 1 outputs they read in the same way. A generated package per Tier 1 project is optional, since Tier 2 consumers are few and named.

### 6.3 Why not import packages containing models?

A package containing *models* would couple projects through code rather than through contracted outputs. This is explicitly out of scope:

| Risk | Severity |
|------|----------|
| Accidental rebuild of upstream models (`dbt build --select +model`) | **Potential production incident** |
| Schema resolution defaults to the consumer's target schema | Wrong schema, wrong role |
| Per-environment `+schema` / `+database` overrides in every consumer | Operational surface area |

Snowflake's documented approach to cross-project dependencies (copying the referenced project into the consuming project) is a package-with-models approach and is excluded for the same reasons.

### 6.4 Known limitations and mitigations

| Limitation | Mitigation | Residual risk |
|-----------|------------|---------------|
| No compile-time validation of upstream schema | Contracts in the producing project; generated, version-pinned source package; CI builds against the pinned version; breaking changes only through model versions | A producer can still break a consumer between version bumps; rare for the shaping-only Bronze Layer |
| dbt DAG severed at the project boundary | Snowflake lineage spans the boundary; column descriptions travel in the source package and in Snowflake | The dbt DAG view alone stops at the boundary |
| `state:modified` does not cross the boundary | Each project compares against its own production state, kept in its live-version project object | Cross-project impact is found with Snowflake's impact queries, not dbt |
| Freshness of upstream projects is not visible in dbt by default | Watermarks in run control and the `upstream_fresh` test (Section 9) | None significant |

---

## 7. Contributing to the Bronze Layer

### 7.1 When to contribute

Contribute when your project needs a source system, table, aggregate or source test that the Bronze Layer does not yet publish. Do not rebuild shaping logic inside a Tier 1 project: every other team would have to rebuild it too.

### 7.2 How

- Open a pull request against the Bronze repository. `CODEOWNERS` routes it to the stewards for the affected source module, and a steward approval is required to merge.
- A new source system becomes a new module (folder), with its own publication unit and run-control entries. Stewards set these up with you.
- Base models are published only when a Tier 1 project consumes them, so say which outputs you need.

### 7.3 The rules your contribution is reviewed against

- **The structural join rule.** Joins follow the source's own foreign keys, preserve the aggregate root's grain or enrich many:1, and would be made regardless of any analysis. `order_items LEFT JOIN orders ON order_items.order_id = orders.order_id` is allowed; `... WHERE orders.status = 'completed'` is not.
- **Permitted filters.** Only source-semantic filters are allowed: soft deletes, CDC deduplication, and tenant scoping.
- **The scope-creep test.** **If two competent analysts could compute it differently, it belongs in Tier 1.** `order_item_count` is shaping; `net_revenue` is business logic.
- **Test placement.** Tests are placed where the defect can first occur. Source assertions (not null, accepted values, castability, uniqueness at the source's grain) go on the Bronze **frozen inputs**. Tests of shaping logic (aggregate grain, fan-out, deduplication, snapshot validity) go on Bronze outputs. Most tests a Tier 1 team contributes are source assertions. See BLD Section 8.

---

## 8. Change Management

### 8.1 What contracts actually do

**A contract asserts that a model's output matches its own YAML specification.** It does not protect consumers in another project, because the producer can change the specification itself. Rename a column in both the SQL and the YAML and the producer's build succeeds; downstream `source()` references break at runtime.

**All changes to published outputs therefore require impact analysis and dbt model versioning.**

What contracts *do* provide:

- A published model cannot silently drift from its declared specification. A mis-typed column or dropped field fails the producer's build.
- With state comparison against the producer's own production manifest, dbt **flags breaking contract changes** in the producer's CI. It cannot tell whether any consumer minds.
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
2. The source package is regenerated with a new minor version; the new relation appears in consumers' source definitions.
3. Each consumer migrates its `source()` reference on its own schedule, within the window.
4. After the deprecation date, the producer drops `v1`.

### 8.3 Impact analysis

Before any breaking change, the producer runs the standing **"who consumes this?" query** against Snowflake's `OBJECT_DEPENDENCIES` and `ACCESS_HISTORY`. These see every consumer, including BI tools and ad hoc users that no dbt manifest knows about, with column-level detail for access. This replaces dbt Explorer's cross-project impact view.

### 8.4 Change management matrix

| Change | Process |
|--------|---------|
| Bronze additive (new column, new aggregate) | Deploy. Source package minor version bumped by CI. Consumers unaffected |
| Bronze **breaking** (rename, drop, retype) | Impact query. New model version with `deprecation_date`. Notify named consumers. Drop old version after the date |
| Tier 0.5 crosswalk rule change | **Always treated as breaking**, even when the schema is unchanged, because match-rate changes alter downstream numbers silently. Steward review and consumer notification. Durable keys are never reassigned |
| Tier 1 additive in a published mart | Deploy. Bump package or consumer documentation minor version |
| Tier 1 **breaking** in a published mart | As Bronze breaking. Tier 2 consumers are enumerable; also check `exposures` metadata |
| New source system | New Bronze module, with publication unit and run-control entries (Section 7.2) |
| **Full refresh of a Bronze incremental** | Coordinated event (Section 8.5) |

### 8.5 Full-refresh coordination

If a Bronze model is fully refreshed, downstream incrementals may rest on shifted history. A full refresh is therefore a **coordinated event**:

1. The stewards announce it to consumers identified by the impact query, with a stated window.
2. The upstream model is fully refreshed through the normal publication procedure, so it is still gated by tests.
3. Downstream projects fully refresh dependent incrementals in reference-graph order (Tier 1 before Tier 2).
4. The event is recorded in run control, so later discrepancies can be correlated.

---

## 9. Publication and Freshness in Your Project

Tier 1 and Tier 2 projects use the same publication and freshness mechanism as the Bronze Layer (BLD Sections 10 to 12). What it means for a project team:

### 9.1 Publication

- **Each project is a publication unit.** It has one published schema and one internal schema, each with a build counterpart.
- **Builds happen in a zero-copy clone** of the last good state. Only if every `error`-severity test passes is the clone swapped into place, atomically.
- **A failed build changes nothing.** Consumers keep the last good version, so a failure shows downstream as staleness, not as wrong data. There is nothing to roll back.
- **Published outputs are materialised.** Views are permitted in published schemas only under the rules in BLD Section 10.
- **Test severity carries meaning.** `error` blocks publication; `warn` publishes and is logged. Use `error` only for tests that evidence wrong data.

### 9.2 Freshness

- **Every published unit has a watermark**: the time its data is current as of. A project's watermark is the **minimum** of its upstream watermarks, so staleness propagates automatically through the Tiers.
- **Each project declares a tolerance per upstream.** Before building, the project's gate checks every upstream watermark against its tolerance and skips the build if any is exceeded. This prevents the silent-wrongness problem: a cross-domain fact mixing fresh finance data with three-day-old digital data, with no failing test anywhere.
- **Dashboards should display the watermark as "data as of".**
- **Freshness is visible in dbt** through the `upstream_fresh` test in the source package.

### 9.3 What a project team configures

- the project's upstream dependencies and freshness tolerances (run-control configuration, reviewed by pull request)
- the project's schedule
- `grants` on published models
- test severities

---

## 10. Snowflake in Place of dbt Mesh and dbt Cloud

Embedded dbt on Snowflake was chosen deliberately, so this architecture uses Snowflake's feature set to the fullest to replace what dbt-Labs products would otherwise provide. *Status*: **Documented** means the capability is documented by Snowflake and was checked while preparing this design; **Design** means it is a construct of this architecture; **To confirm** means availability or fit must be confirmed for our account and edition.

| Capability | dbt-Labs offering | Replacement in this architecture | Status |
|---|---|---|---|
| Cross-project references | dbt Mesh cross-project `ref` | `source()` to published outputs; generated source package (Section 6) | Design |
| Cross-project access control | Mesh model access (`public`, `protected`, `private`) | Published and internal schemas, database roles, per-model grants via dbt `grants` | Design |
| Hosted execution and IDE | dbt Cloud | dbt project objects, `EXECUTE DBT PROJECT`, Snowsight Workspaces | Documented |
| Job scheduling | dbt Cloud jobs | Snowflake Tasks, one gated task per publication unit | Documented |
| State-aware orchestration | dbt Cloud (Fusion) | Freshness gates and propagated watermarks in run control (BLD Section 12) | Design |
| Freshness checks | `dbt source freshness` in dbt Cloud | `dbt source freshness` on live-version project objects | Documented |
| Slim CI and defer to production | dbt Cloud CI | Live-version `--state`; per-pull-request databases (Snowflake has a published Slim CI tutorial) | Documented |
| Development environments | dbt Cloud environments | Zero-copy clones of production databases | Design |
| Write-audit-publish | (none built in) | Clone, build, test, atomic `SWAP` (BLD Section 10) | Design |
| Documentation and DAG | dbt Explorer | `persist_docs` into Snowflake; Snowsight dbt project details page; `dbt docs generate --static` | Documented |
| Cross-project lineage and impact | dbt Explorer | Snowflake lineage (including column-level lineage in the dbt DAG), `OBJECT_DEPENDENCIES`, `ACCESS_HISTORY` | Documented |
| Run metadata | dbt Discovery API | dbt artifacts written back to live-version objects; Snowflake telemetry event table; run-control log | Documented |
| Semantic layer | dbt Semantic Layer (MetricFlow) | Snowflake semantic views, built from dbt (Snowflake-Labs `dbt_semantic_view` package) | To confirm |
| Alerting | dbt Cloud notifications | Task failure notifications; Snowflake alerts on run-control views | To confirm |
| Continuous data quality monitoring | (partner tools) | Snowflake data metric functions on published outputs | To confirm |
| PII protection | (none built in) | Tag-based masking policies | Documented |
| Cost attribution | (none built in) | Warehouse per Tier or unit, query and object tags, resource monitors | Design |

One Mesh capability has no Snowflake equivalent: **compile-time validation of references across projects**. It is mitigated by contracts, the generated source package and versioning (Section 6.4).

---

## 11. Alternatives Considered

| Alternative | Assessment |
|---|---|
| **dbt Mesh / dbt Cloud Enterprise** | Native cross-project `ref`. Unfunded; excluded by constraint |
| **Single monorepo for all Tiers, with dbt groups and model access** | Compile-time boundary enforcement, full lineage and working `state:modified` across all teams. Rejected because independent teams would share one CI pipeline, one dbt version and one deployment, and one broken merge would block everyone. Adopted *within* the Bronze Layer, where shared stewardship is the intent |
| **dbt-loom** (open-source cross-project `ref` from upstream manifests) | Requires a Python plugin, which Snowflake's managed dbt runtime does not allow |
| **Snowflake's native cross-project dependency** (copy the referenced project into the consumer) | A package-with-models approach; rejected for the reasons in Section 6.3 |
| **End-to-end projects, each with its own staging layer** | The conventional single-project layout. Rejected because every team would rebuild the same sourcing and shaping, with divergent results, and none would get the Bronze guarantees in Section 3.2 |

Alternatives for the Bronze Layer's internal mechanisms are recorded in BLD Section 15.

---

## 12. References

### 12.1 dbt-Labs documentation

- [dbt-Labs Best practices](https://docs.getdbt.com/best-practices?version=2)
- [dbt: How we structure our projects](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview)
- [dbt: Data contracts](https://docs.getdbt.com/docs/collaborate/govern/model-contracts)
- [dbt: Model versions](https://docs.getdbt.com/docs/collaborate/govern/model-versions)
- [dbt: Model access](https://docs.getdbt.com/docs/collaborate/govern/model-access)
- [dbt: Packages](https://docs.getdbt.com/docs/build/packages)
- [dbt: freshness](https://docs.getdbt.com/reference/resource-configs/freshness)
- [dbt: Mesh project dependencies](https://docs.getdbt.com/docs/mesh/govern/project-dependencies)

### 12.2 Snowflake documentation

- [dbt Projects on Snowflake](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake)
- [dbt Projects on Snowflake: supported commands](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-supported-commands)
- [dbt project objects migrate to a single mutable live version (2026_06 bundle)](https://docs.snowflake.com/en/release-notes/bcr-bundles/2026_06/bcr-2362)
- [Task graphs and dependencies](https://docs.snowflake.com/en/user-guide/tasks-graphs)
- [OBJECT_DEPENDENCIES](https://docs.snowflake.com/en/sql-reference/account-usage/object_dependencies) and [ACCESS_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/access_history)
- [Tag-based masking policies](https://docs.snowflake.com/en/user-guide/tag-based-masking-policies)
- [Resource monitors](https://docs.snowflake.com/en/user-guide/resource-monitors)

### 12.3 Tooling and community

- [dbt-utils](https://github.com/dbt-labs/dbt-utils)
- [dbt-core discussion #5244: cross-project lineage](https://github.com/dbt-labs/dbt-core/discussions/5244)
