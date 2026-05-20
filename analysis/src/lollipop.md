---
title: 2014 vs 2024 — Lollipop
theme: light
---

# The decade-long rate shift — and why it isn't Hispanic-specific

Each lollipop below shows how a Hispanic per-stop enforcement rate moved from 2014 to 2024. The stems point left (decrease) or right (increase) from the 2014 baseline. License stops are the one rate heading up; the other four are down.

**But the same pattern shows up for white and Black drivers too.** This isn't a Hispanic-specific rate story — it's a statewide composition shift toward license stops and away from discretionary categories. The lollipops below are still accurate for Hispanic drivers in isolation, but the headline "license stops are the one rate that rose" is equally true for every race. Compare below.

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

const metrics = Object.keys(metricColors);

const lollipopData = metrics.map(metric => {
  const d2014 = dotLineData.find(d => d.Metric === metric && d.Year === 2014);
  const d2024 = dotLineData.find(d => d.Metric === metric && d.Year === 2024);
  const change = d2024.Rate - d2014.Rate;
  const pctChange = (change / d2014.Rate) * 100;
  return {
    metric,
    label: metric.replace(" Rate", "").replace(" Stop", ""),
    start: d2014.Rate,
    end: d2024.Rate,
    change,
    pctChange,
    absChange: Math.abs(pctChange),
    direction: change >= 0 ? "up" : "down",
  };
}).sort((a, b) => b.pctChange - a.pctChange);
```

```js
Plot.plot({
  title: "Hispanic enforcement rates: 2014 → 2024",
  subtitle: "Percent change in each rate over the decade. Red = increase. Blue = decrease.",
  width,
  height: 320,
  marginLeft: 150,
  marginRight: 100,
  x: {label: "% change from 2014", grid: true, nice: true},
  y: {label: null, domain: lollipopData.map(d => d.label)},
  marks: [
    Plot.ruleX([0], {stroke: "#333", strokeWidth: 1.5}),
    Plot.link(lollipopData, {
      x1: d => 0, x2: "pctChange",
      y1: "label", y2: "label",
      stroke: d => d.pctChange >= 0 ? "#c22d2d" : "#2a7b9b",
      strokeWidth: 4,
      strokeLinecap: "round",
    }),
    Plot.dot(lollipopData, {
      x: "pctChange", y: "label",
      r: d => 8 + d.absChange * 0.5,
      fill: d => d.pctChange >= 0 ? "#c22d2d" : "#2a7b9b",
      fillOpacity: 0.9,
      stroke: "#fff", strokeWidth: 2,
    }),
    Plot.text(lollipopData, {
      x: "pctChange", y: "label",
      text: d => (d.pctChange >= 0 ? "+" : "") + d.pctChange.toFixed(0) + "%",
      dx: d => d.pctChange >= 0 ? d.absChange * 0.5 + 16 : -(d.absChange * 0.5 + 16),
      textAnchor: d => d.pctChange >= 0 ? "start" : "end",
      fontSize: 12, fontWeight: 600,
      fill: d => d.pctChange >= 0 ? "#c22d2d" : "#2a7b9b",
    }),
  ],
  style: {fontSize: 13},
})
```

---

## The actual rates — Hispanic

```js
Inputs.table(lollipopData.map(d => ({
  Metric: d.metric,
  "2014": `${d.start}%`,
  "2024": `${d.end}%`,
  "Change (pp)": (d.change >= 0 ? "+" : "") + d.change.toFixed(2),
  "Change (%)": (d.pctChange >= 0 ? "+" : "") + d.pctChange.toFixed(0) + "%",
})))
```

## The same pattern for every race

License rose and everything else fell — for white and Black drivers too. The direction is uniform across the state. The *magnitudes* differ, but no group uniquely experienced the license-stop rise or the decline in other categories.

```js
const metricsList = [
  {key: "arrest_rate_pct", label: "Arrest Rate"},
  {key: "search_rate_pct", label: "Search Rate"},
  {key: "investigative_rate_pct", label: "Investigative Stop Rate"},
  {key: "equipment_rate_pct", label: "Equipment Stop Rate"},
  {key: "license_rate_pct", label: "License Stop Rate"},
];

const allRaceRates = ["White", "Black", "Hispanic"].flatMap(race => {
  const r14 = raceCompare.find(d => d.race === race && d.year === 2014);
  const r24 = raceCompare.find(d => d.race === race && d.year === 2024);
  return metricsList.map(m => ({
    race,
    metric: m.label,
    pctChange: ((r24[m.key] - r14[m.key]) / r14[m.key]) * 100,
  }));
});

Plot.plot({
  title: "2014 → 2024 rate changes, by race",
  subtitle: "Same direction for every race — license stops up, everything else down",
  width,
  height: 340,
  marginLeft: 170,
  x: {label: "% change from 2014", grid: true, nice: true},
  y: {label: null, domain: metricsList.map(m => m.label)},
  color: {
    domain: ["White", "Black", "Hispanic"],
    range: ["#888", "#333", "#e8a820"],
    legend: true,
  },
  marks: [
    Plot.ruleX([0], {stroke: "#333"}),
    Plot.dot(allRaceRates, {
      x: "pctChange", y: "metric",
      fill: "race", r: 7, fillOpacity: 0.9,
      stroke: "#fff", strokeWidth: 1.5,
    }),
  ],
})
```

## Where Hispanic drivers actually are unique — absolute counts

The rate story is statewide. The absolute count story is Hispanic-specific. Because Hispanic stop volume grew 52% while white and Black stop volume each fell 25–28%, the composition shift landed on very different populations:

```js
const licTable = ["White", "Black", "Hispanic"].map(race => {
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
Inputs.table(licTable)
```

**The one rate that's up for every race:** License stops — the one enforcement category that can surface immigration status through a missing or invalid license. The difference is that Hispanic is the only group whose absolute license-stop count actually doubled.
