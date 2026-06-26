# Tech COE Allocation — Dashboard Blueprint

A build-ready spec for the OAC workbook (**Tech COE Allocation Workbook**). Six canvases (tabs), the datasets and calculations behind each, and layout wireframes.

---

## 0. Foundations (do these first)

### Datasets (already imported)
| Dataset (OAC name) | Grain | Tabs it feeds |
|---|---|---|
| `ALLOC DEPT WEEK` | dept × month × week | Exec, Utilization, Bench |
| `UTILIZATION PERSON WEEK` | arganaut × month × week | Bench, Over-Allocation |
| `PROJECT RISK` | project | Projects, Risk |
| `ALLOC PROJECT WEEK` | project × week | Projects |
| `DEPT FORECAST` | dept × week (+future) | Forecast |
| `ALLOC DETAIL` | dept × arganaut × project × week | Drill-down (all tabs) |

### ⚠️ Critical: utilization must be a ratio-of-sums
The stored `Gross/Net Utilization Pct` columns are **pre-computed per row** — never average or SUM them across weeks/people. Build these **workbook calculations** instead and use them everywhere:

| Calc name | Formula (on `ALLOC DEPT WEEK`) |
|---|---|
| `Gross Util %` | `SUM("Billable Hours") / SUM("Total Capacity Hours") * 100` |
| `Net Util %` | `SUM("Billable Hours") / SUM("Available Capacity Hours") * 100` |
| `Bench Hours` | `SUM("Nonbillable Hours")` |
| `Time Off Hours` | `SUM("Total Time Off Hours")` |
| `Capacity Hours` | `SUM("Total Capacity Hours")` |
| `Billable Hours` | `SUM("Billable Hours")` |

Person-level equivalents on `UTILIZATION PERSON WEEK` use `"Capacity Hours"` / `"Available Capacity Hours"`.

### Global filters (pin to every canvas)
- **Department** (multi-select)
- **Week Start Date** — range; **default to next 8–12 weeks** so the coverage-cliff tail doesn't drag the headline numbers.
- **Year Month** (for monthly rollups)
- **Arganaut** (for drill-down)

### Utilization color thresholds (conditional formatting, reuse everywhere)
| Band | Range | Color |
|---|---|---|
| Over-allocated | > 100% | 🔴 Red |
| Healthy | 75–100% | 🟢 Green |
| Soft | 50–75% | 🟡 Amber |
| Under-utilized | < 50% | 🔴 Red |

---

## Tab 1 — Executive Summary
**Audience:** leadership. **Question:** "How utilized are we, and where's the open capacity?"

```
┌── KPI row ───────────────────────────────────────────────┐
│ Net Util %  │ Gross Util % │ Bench Hours │ Over-Alloc # │ Roster │
└──────────────────────────────────────────────────────────┘
┌── Coverage cliff (line) ─────────┬── Util by Department (bar) ─┐
│ Net Util % by Year Month         │ Net Util % by Department    │
│ (target line at 80%)             │ (color = threshold)         │
└──────────────────────────────────┴─────────────────────────────┘
┌── Bench hours by month (bar) ────────────────────────────┐
└──────────────────────────────────────────────────────────┘
```
- **KPIs:** `Net Util %`, `Gross Util %`, `Bench Hours`, `Over-Alloc #` (= `SUM("Is Over Allocated")`, filtered to selected window), `SUM(DISTINCT Arganaut)` roster. Source: `ALLOC DEPT WEEK` / `UTILIZATION PERSON WEEK`.
- **Coverage cliff:** line, X=`Year Month`, Y=`Net Util %`, reference line at 80%.
- **Util by Department:** bar, X=`Department`, Y=`Net Util %`, color by threshold.
- **Bench by month:** bar, X=`Year Month`, Y=`Bench Hours`.

## Tab 2 — Utilization & Capacity
**Audience:** practice leads. **Question:** "Gross vs net, and where is capacity going?"

```
┌── Dept × Month heat map (Net Util %) ────────────────────┐
└──────────────────────────────────────────────────────────┘
┌── Gross vs Net trend (combo) ────┬── Capacity breakdown (stacked) ─┐
│ two lines by Year Month          │ Billable / Time Off / Bench     │
│                                  │ hours by Year Month             │
└──────────────────────────────────┴─────────────────────────────────┘
```
- **Heat map:** pivot, rows=`Department`, cols=`Year Month`, color=`Net Util %` (threshold palette).
- **Gross vs Net:** two-line combo by `Year Month`.
- **Capacity breakdown:** stacked bar, X=`Year Month`, segments = `Billable Hours`, `Time Off Hours`, `Bench Hours` (sums to capacity).

## Tab 3 — Bench & Availability  *(sales / staffing)*
**Audience:** sales, resourcing. **Question:** "Who can take on new work, and when?"

```
┌── Bench hours by Dept × Week (next 12 wks) ──────────────┐
└──────────────────────────────────────────────────────────┘
┌── Available people (table) ──────────────────────────────┐
│ Arganaut │ Dept │ Week │ Billable │ Avail Cap │ Net Util%│
│ filter: Is On Bench = 1  OR  Net Util % < 50            │
└──────────────────────────────────────────────────────────┘
```
- **Bench trend:** bar/area, X=`Week Start Date`, Y=`Bench Hours`, color=`Department`; filter to next ~12 weeks.
- **Available people:** table from `UTILIZATION PERSON WEEK`, filter `Is On Bench = 1` or `Net Util % < 50`, sort by `Available Capacity Hours` desc.

## Tab 4 — Over-Allocation & Risk  *(delivery)*
**Audience:** delivery managers. **Question:** "Who's overbooked and what's fragile?"

```
┌── Over-allocated people (table) ─────────────────────────┐
│ Arganaut │ Dept │ Week │ Billable │ Avail Cap │ Net Util%│
│ filter: Is Over Allocated = 1   (Net Util% color red)   │
└──────────────────────────────────────────────────────────┘
┌── Over-alloc count by Dept×Week ─┬── Key-person-risk projects ─┐
│ bar                              │ table: Is Key Person Risk=1 │
└──────────────────────────────────┴─────────────────────────────┘
```
- **Over-allocated:** table from `UTILIZATION PERSON WEEK`, filter `Is Over Allocated = 1`.
- **Count trend:** bar, X=`Week Start Date`, Y=`SUM("Is Over Allocated")`, color=`Department`.
- **Key-person risk:** table from `PROJECT RISK`, filter `Is Key Person Risk = 1`, sorted by `Total Scheduled Hours` desc.

## Tab 5 — Projects & Demand
**Audience:** leadership / delivery. **Question:** "Where is the work, and how is it ramping?"

```
┌── Top 15 projects (bar) ─────────┬── Project demand over time (area) ─┐
│ PROJECT RISK: Total Hours        │ ALLOC PROJECT WEEK: Scheduled Hours│
│ desc, risk flag as color         │ by Week, color/stack by project    │
└──────────────────────────────────┴─────────────────────────────────────┘
┌── Project detail (table, drill) ─────────────────────────┐
│ Project │ Dept │ People │ Active Weeks │ Total Hours │Risk│
└──────────────────────────────────────────────────────────┘
```
- **Top projects:** bar from `PROJECT RISK`, Y=`Total Scheduled Hours`, top 15, color by `Is Key Person Risk`.
- **Demand over time:** area/line from `ALLOC PROJECT WEEK`, X=`Week Start Date`, Y=`Scheduled Hours`, breakdown by `Project Name` (top N filter).

## Tab 6 — Forecast
**Audience:** leadership. **Question:** "Which way is booked demand trending?"

```
┌── Billable demand: actual + trend + MA by Dept ──────────┐
│ DEPT FORECAST: lines for Actual / Trend / MA by Week     │
│ split or color by Department; mark Is Forecast = 1       │
└──────────────────────────────────────────────────────────┘
```
- Line, X=`Week Start Date`, Y= `Actual Billable Hours`, `Trend Hours`, `MA 3Wk Hours`; trellis by `Department`.
- **Note:** the trend line collapses over the long horizon — annotate that **`MA 3Wk Hours` is the reliable near-term signal** (see notebook §7; a recent-window trend fix is a pending enhancement).

---

## Build order & tips
1. Create the **6 workbook calculations** (above) first — every visual depends on them.
2. Build the **global filters**, set the Week Start Date default, then duplicate the filter bar across canvases.
3. Build **Tab 1**, validate the KPIs against the notebook's §8 numbers, then reuse calcs/filters on the rest.
4. Save **conditional-format threshold rules** once and apply to each utilization visual.
5. Use `ALLOC DETAIL` as the drill-through target (`HOURS_TYPE` lets you include/exclude time off).

## Open considerations
- **Over-Alloc / Bench KPI counts** are per *person-week*; pin them to a single selected week (or use `COUNT DISTINCT Arganaut` with the flag filter) so they read as headcount, not instances.
- **Default window:** near-term (8–12 weeks) for exec tabs; full horizon only on Forecast/Projects.
- **Straddle weeks** now appear as two month-rows per `Week Start Date` (correct) — when showing raw weeks, expect 6/28-style dates twice (one Jun, one Jul). Group by `Year Month` + `Week Start Date` to keep them distinct.

*Source datasets built by `tech_coe_allocation_gold.ipynb`. Capacity = 40 hrs/week, split by workday across month boundaries.*
