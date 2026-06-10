# Tech COE Allocation Analytics — One-Pager

*Turning the weekly allocation spreadsheet into a live view of team utilization, capacity, and demand.*

---

## What this is

Every week the Tech COE produces an **allocation spreadsheet** — who is scheduled on which project, for how many hours, for each week ahead. On its own it's a hard-to-read pivot. This pipeline turns it into a small set of clean tables that power a leadership dashboard, answering:

- **How busy is each team?** (utilization)
- **Who has capacity we can sell?** (bench / availability)
- **Where is the work concentrated, and what's at risk?** (projects & key-person risk)
- **Is committed billable work trending up or down?** (forecast)

## How it works

```
Weekly Excel  →  Bronze notebook  →  TECH_COE_ALLOCATIONS  →  Gold notebook  →  6 analytics tables  →  OAC dashboard
(drop file)       (normalises)        (one clean fact)          (this notebook)    (utilization, demand, …)
```

- **Drop & run:** drop the latest `Tech COE Allocation Summary …xlsx` into object storage — the bronze notebook auto-selects the newest file (no renaming). The gold notebook then rebuilds the analytics tables.
- **Capacity baseline:** **40 hours/week** per person (one adjustable setting).
- **Time off is handled correctly:** Holidays and PTO are booked on project lines, so they are split out and **excluded from billable utilization** and from project rankings.

## What leadership sees

| Metric | Plain-English meaning |
|---|---|
| **Gross utilization** | Billable hours ÷ 40. Overall how loaded the team is. |
| **Net utilization** | Billable hours ÷ (40 − time off). Fairer — a holiday week doesn't look like idle time. |
| **Bench / available hours** | Capacity that's free and sellable right now. |
| **Time off hours** | Holiday + PTO removing capacity that week. |
| **Top projects & demand** | Where billable hours are going, and the staffing curve over time. |
| **Key-person risk** | Projects staffed by a single person (bus-factor of one). |
| **Demand forecast** | Directional trend of billable hours per department, projected 8 weeks out. |

## How to use it

- **Staffing & sales:** read **bench hours** and **net utilization** for the next 8–12 weeks to see who can take on new work.
- **Delivery risk:** watch **over-allocated people** (booked beyond available capacity) and **key-person-risk** projects.
- **Planning:** use the **forecast** as a booking-pace signal — is the pipeline filling or thinning?
- **Self-serve:** slice the detail table by **Department** and **Project** in OAC for any drill-down.

## What it is — and isn't

- ✅ A **forward view of scheduled allocations**, refreshed weekly from the source of truth.
- ✅ A correct billable-vs-time-off split, with utilization measured two defensible ways.
- ⚠️ **Not actuals** — these are planned/forecast hours, so far-future weeks naturally look light (the "coverage cliff"); the **near-term weeks are the actionable ones**.
- ⚠️ The forecast is a **directional trend**, not a statistical model.

---

*Source: `tech_coe_allocation_gold.ipynb` over `TECH_COE_ALLOCATIONS`. Capacity assumption: 40 hrs/week. Refresh: weekly, on new spreadsheet drop.*
