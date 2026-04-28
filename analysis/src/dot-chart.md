---
title: Hispanic Enforcement Rates
theme: light
---

# Hispanic enforcement rates over 11 years

Four of five per-stop enforcement rates have *declined* for Hispanic drivers since 2014 — arrests, searches, investigative stops, and equipment stops. License stops are the one rate that rose, climbing to an 11-year high in 2024.

**Important caveat:** the same four-down-one-up pattern shows up for white and Black drivers. The rate changes are not unique to Hispanic drivers — they're a statewide composition shift toward license stops and away from discretionary categories. What *is* unique to Hispanic drivers is volume: stops of white and Black drivers fell 25–28% over the decade, while Hispanic stops rose 52%, which means the absolute count of Hispanic license stops doubled while white license-stop counts barely grew and Black license-stop counts fell. See [the main story](./) for the comparative view.

```js
const dotLineData = await FileAttachment("data/hispanic_dot_line.csv").csv({typed: true});
const raceCompare = await FileAttachment("data/race_comparison.csv").csv({typed: true});
```

```js
const metricColors = {
  "License Stop Rate": "#c22d2d",
  "Arrest Rate": "#7b3fa0",
  "Equipment Stop Rate": "#2a7b9b",
  "Investigative Stop Rate": "#5a8a3f",
  "Search Rate": "#888",
};

const data = dotLineData.map(d => {
  const base = dotLineData.find(b => b.Metric === d.Metric && b.Year === 2014)?.Rate;
  const pctChange = base ? ((d.Rate - base) / base) * 100 : 0;
  return {...d, pctChange, absChange: Math.abs(pctChange)};
});
```

```js
Plot.plot({
  title: "Hispanic enforcement rates, 2014–2024",
  subtitle: "Lines show raw rates each year. Dot size = how far that point is from the 2014 baseline.",
  width,
  height: 520,
  marginRight: 180,
  x: {label: null, tickFormat: "d", ticks: 6},
  y: {label: "Rate (% of Hispanic stops)", grid: true, nice: true},
  color: {
    domain: Object.keys(metricColors),
    range: Object.values(metricColors),
    legend: true
  },
  marks: [
    Plot.line(data, {
      x: "Year", y: "Rate", z: "Metric",
      stroke: "Metric", strokeWidth: 2, strokeOpacity: 0.55,
    }),
    Plot.dot(data, {
      x: "Year", y: "Rate",
      r: "absChange",
      fill: "Metric", fillOpacity: 0.85,
      stroke: "#fff", strokeWidth: 1.5,
      channels: {metric: "Metric", rate: "Rate", change: "pctChange"},
      tip: {format: {x: false, y: false, r: false, fill: false, metric: true, rate: d => d + "%", change: d => (d >= 0 ? "+" : "") + d.toFixed(0) + "% from 2014"}},
    }),
    Plot.text(data.filter(d => d.Year === 2024), {
      x: "Year", y: "Rate",
      text: d => `${d.Metric.replace(" Rate", "").replace(" Stop", "")} ${d.Rate}%`,
      fill: "Metric",
      dx: 24, textAnchor: "start", fontWeight: 600, fontSize: 11,
    }),
  ],
  style: {fontSize: 13},
})
```

**What this chart shows (Hispanic drivers in isolation):**

- **License stops** climbed from 19.65% of Hispanic stops in 2014 to 25.89% in 2024 — an 11-year high.
- **Arrest rates** fell from 8.19% (2014) to 4.80% (2020), then climbed back to 6.17% (2024) — still well below the 2014 level.
- **Search rates** declined steadily, from 9.91% (2014) to 6.40% (2024).
- **Investigative and equipment stop rates** dropped through the decade before a modest recent rebound.

## The same four-down-one-up pattern for every race

The chart above shows Hispanic rates in isolation. Looking at white and Black drivers over the same 11 years, the pattern is the same: license stops up, everything else down. The rate changes are a statewide composition shift, not something that was applied only to Hispanic drivers.

```js
const rateTable = ["White", "Black", "Hispanic"].map(race => {
  const r14 = raceCompare.find(d => d.race === race && d.year === 2014);
  const r24 = raceCompare.find(d => d.race === race && d.year === 2024);
  return {
    Race: race,
    "Arrest Δ%": ((r24.arrest_rate_pct - r14.arrest_rate_pct) / r14.arrest_rate_pct * 100).toFixed(0) + "%",
    "Search Δ%": ((r24.search_rate_pct - r14.search_rate_pct) / r14.search_rate_pct * 100).toFixed(0) + "%",
    "Investigative Δ%": ((r24.investigative_rate_pct - r14.investigative_rate_pct) / r14.investigative_rate_pct * 100).toFixed(0) + "%",
    "Equipment Δ%": ((r24.equipment_rate_pct - r14.equipment_rate_pct) / r14.equipment_rate_pct * 100).toFixed(0) + "%",
    "License Δ%": "+" + ((r24.license_rate_pct - r14.license_rate_pct) / r14.license_rate_pct * 100).toFixed(0) + "%",
  };
});
Inputs.table(rateTable)
```

## What *is* unique to Hispanic drivers

The per-stop rate story is not uniquely Hispanic. The *volume* story is. Hispanic stops rose 52% across the decade while white and Black stops each fell 25–28%. That volume divergence, combined with the statewide shift toward license stops, meant the absolute count of Hispanic license stops doubled — while white license stops barely grew and Black license stops fell.

```js
const countTable = ["White", "Black", "Hispanic"].map(race => {
  const r14 = raceCompare.find(d => d.race === race && d.year === 2014);
  const r24 = raceCompare.find(d => d.race === race && d.year === 2024);
  return {
    Race: race,
    "License stops 2014": d3.format(",")(r14.license_stops),
    "License stops 2024": d3.format(",")(r24.license_stops),
    "Count change": d3.format("+,")(r24.license_stops - r14.license_stops),
    "Count change %": d3.format("+.0%")((r24.license_stops - r14.license_stops) / r14.license_stops),
  };
});
Inputs.table(countTable)
```

The story isn't that Hispanic drivers are treated more harshly *per stop* than they were a decade ago — by most measures, they're not. The story is that Hispanic drivers are the only group whose stop counts rose, and because the statewide enforcement mix is now tilted toward license stops, most of that volume surge landed in the category most likely to surface immigration status.

```js
Inputs.table(
  data.filter(d => d.Year === 2014 || d.Year === 2024)
    .sort((a, b) => a.Metric.localeCompare(b.Metric) || a.Year - b.Year)
    .map(d => ({
      Metric: d.Metric,
      Year: d.Year,
      "Rate": d.Rate + "%",
      "Change from 2014": d.Year === 2014 ? "baseline" : (d.pctChange >= 0 ? "+" : "") + d.pctChange.toFixed(0) + "%",
    }))
)
```
