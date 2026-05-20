---
title: The Hispanic Surge
theme: light
---

# Eleven years of mandatory state data show Missouri police scaled back traffic enforcement for almost everyone — except Hispanic drivers.

Between 2014 and 2024, traffic stops of white drivers fell 25%. Stops of Black drivers fell 28%. Stops of Hispanic drivers rose 52%. Hispanic population grew roughly in step with that increase (+49%), so the per-capita stop rate for Hispanic drivers is essentially flat across the decade — but white and Black per-capita stop rates each fell by about a quarter. Hispanic drivers are the only group in the data who didn't share in the statewide enforcement pullback, and the gap widened sharply after 2021. This isn't a bounce-back from the COVID dip in 2020. Pre-pandemic, Hispanic stops were already at 35,526 in 2019. By 2024 they hit 46,655 — the highest count on record.

```js
const raw = await FileAttachment("data/story_analysis_results.csv").csv({typed: true});
const pop = await FileAttachment("data/missouri_population.csv").csv({typed: true});
const agencyPop = await FileAttachment("data/agency_population.csv").csv({typed: true});
const censusByYear = await FileAttachment("data/census_population_by_year.csv").csv({typed: true});
const contrabandData = await FileAttachment("data/contraband_hit_rates.csv").csv({typed: true});
const agencyContraband = await FileAttachment("data/agency_contraband_2024.csv").csv({typed: true});
const agencyComments = await FileAttachment("data/agency_comments.csv").csv({typed: true});
const dotLineData = await FileAttachment("data/hispanic_dot_line.csv").csv({typed: true});
const raceCompare = await FileAttachment("data/race_comparison.csv").csv({typed: true});
```

```js
const iceMonthly = await FileAttachment("data/ice_detainers_monthly.csv").csv({typed: true});
const iceArrestsMonthly = await FileAttachment("data/ice_arrests_monthly.csv").csv({typed: true});
const iceFacilities = await FileAttachment("data/ice_detainers_by_facility.csv").csv({typed: true});
const licArrestProxy = await FileAttachment("data/license_arrest_proxy.csv").csv({typed: true});
const vsrIceCrossref = await FileAttachment("data/vsr_ice_crossref.csv").csv({typed: true});
```

```js
const statewide = raw.filter(d => d.section === "statewide");
const byAgency = raw.filter(d => d.section === "by_agency");

function sw(metric) { return statewide.filter(d => d.metric === metric); }
function ag(metric) { return byAgency.filter(d => d.metric === metric); }

const storyRaces = ["White", "Black", "Hispanic"];
const raceColors = {domain: storyRaces, range: ["#888", "#333", "#e8a820"]};

const swStopsData = sw("total_stops");

const perCapita = storyRaces.flatMap(race => {
  return pop.map(p => {
    const stops = swStopsData.find(d => d.race === race && d.year === p.year)?.value;
    const population = p[race];
    if (!stops || !population) return null;
    return {year: p.year, race, value: +(stops / population * 100).toFixed(1), stops, population};
  }).filter(d => d);
});

function trendChart(data, {title, ylabel, fmt = ".1f", height = 380, annotateYear = 2024, baseline} = {}) {
  const filtered = data.filter(d => storyRaces.includes(d.race));
  const marks = [
    Plot.lineY(filtered, {x: "year", y: "value", stroke: "race", strokeWidth: 2.5}),
    Plot.dot(filtered, {x: "year", y: "value", fill: "race", r: 4}),
    Plot.text(filtered.filter(d => d.year === annotateYear), {
      x: "year", y: "value",
      text: d => `${d.race} ${d3.format(fmt)(d.value)}`,
      fill: "race", dx: 10, textAnchor: "start", fontWeight: 600, fontSize: 12
    }),
  ];
  if (baseline != null) marks.unshift(Plot.ruleY([baseline], {stroke: "#ddd", strokeDasharray: "4,3"}));
  return Plot.plot({title, width, height, x: {label: null, tickFormat: "d", ticks: 6}, y: {label: ylabel, grid: true, nice: true}, color: raceColors, marks});
}
```

---

<div class="grid grid-cols-3">
  <div class="card">
    <h2>−24%</h2>
    <p>White per-capita stop rate, 2014 → 2024</p>
  </div>
  <div class="card">
    <h2>≈0%</h2>
    <p>Hispanic per-capita stop rate over the same period</p>
  </div>
  <div class="card">
    <h2>×2</h2>
    <p>Hispanic license stops: 6,048 → 12,080</p>
  </div>
</div>

---

## The per-capita rate

Missouri's Hispanic population grew **48.6%** over the decade — from 233,325 in 2014 to 346,700 in 2024 (ACS). White population barely moved (−2%). Black population declined (−8%). So the raw stop counts need to be read against very different population trajectories.

Once you divide out population, the picture sharpens into a different story than the raw count suggests. The per-capita Hispanic stop rate is essentially **flat** across the decade (+2%). But Missouri dialed back enforcement for white and Black drivers by roughly a quarter: white per-capita stops fell 24%, Black per-capita stops fell 22%. Hispanic drivers are the one group who didn't share in that pullback.

Black drivers are still the most over-stopped group in Missouri per capita — 33.6 stops per 100 residents in 2024, compared with 20.7 for white and 13.5 for Hispanic. This story is not about Hispanic drivers being the most-stopped population. It's about *direction of change*: everyone else got a meaningful per-capita enforcement reduction over the decade, and Hispanic drivers didn't.

```js
trendChart(perCapita, {title: "Stops per 100 residents, 2014–2024", ylabel: "Stops per 100 residents"})
```

```js
const pcDelta = storyRaces.map(race => {
  const p14 = pop.find(d => d.year === 2014)?.[race];
  const p24 = pop.find(d => d.year === 2024)?.[race];
  const s14 = swStopsData.find(d => d.race === race && d.year === 2014)?.value;
  const s24 = swStopsData.find(d => d.race === race && d.year === 2024)?.value;
  const pc14 = s14 / p14 * 100;
  const pc24 = s24 / p24 * 100;
  return {race, pctChange: (pc24 - pc14) / pc14 * 100, pc2014: pc14, pc2024: pc24};
});

Plot.plot({
  title: "Per-capita stop rate change, 2014 → 2024",
  subtitle: "Missouri cut enforcement ~23% per capita for white and Black drivers. Hispanic: no change.",
  width, height: 200,
  marginLeft: 100, marginRight: 80,
  x: {label: "% change in stops per 100 residents", grid: true, nice: true},
  y: {label: null, domain: storyRaces},
  marks: [
    Plot.ruleX([0], {stroke: "#333", strokeWidth: 1.5}),
    Plot.link(pcDelta, {
      x1: 0, x2: "pctChange", y1: "race", y2: "race",
      stroke: d => d.pctChange >= 0 ? "#e8a820" : "#2a7b9b",
      strokeWidth: 6, strokeLinecap: "round",
    }),
    Plot.dot(pcDelta, {
      x: "pctChange", y: "race",
      r: 10,
      fill: d => d.pctChange >= 0 ? "#e8a820" : "#2a7b9b",
      stroke: "#fff", strokeWidth: 2,
    }),
    Plot.text(pcDelta, {
      x: "pctChange", y: "race",
      text: d => `${d.pctChange >= 0 ? "+" : ""}${d.pctChange.toFixed(0)}%`,
      dx: d => d.pctChange >= 0 ? 24 : -24,
      textAnchor: d => d.pctChange >= 0 ? "start" : "end",
      fontWeight: 700, fontSize: 13,
      fill: d => d.pctChange >= 0 ? "#e8a820" : "#2a7b9b",
    }),
  ],
})
```

```js
const popTable = storyRaces.map(race => {
  const p2014 = pop.find(d => d.year === 2014)?.[race];
  const p2024 = pop.find(d => d.year === 2024)?.[race];
  const s2014 = swStopsData.find(d => d.race === race && d.year === 2014)?.value;
  const s2024 = swStopsData.find(d => d.race === race && d.year === 2024)?.value;
  return {
    Race: race,
    "Pop 2014": d3.format(",")(p2014), "Pop 2024": d3.format(",")(p2024),
    "Pop change": d3.format("+.0%")((p2024 - p2014) / p2014),
    "Stop change": d3.format("+.0%")((s2024 - s2014) / s2014),
    "Per capita 2014": (s2014 / p2014 * 100).toFixed(1),
    "Per capita 2024": (s2024 / p2024 * 100).toFixed(1),
    "Per capita change": d3.format("+.0%")((s2024 / p2024 - s2014 / p2014) / (s2014 / p2014)),
  };
});
Inputs.table(popTable)
```

Population: U.S. Census Bureau, American Community Survey. 2014–2019 and 2021–2024 use 1-year estimates; 2020 uses 5-year estimates (1-year not released due to COVID). "White" and "Black" are non-Hispanic.

---

## The 2020 baseline isn't the whole story

An earlier version of this analysis started from 2020 — the year the state last published a full format update of its traffic stop data. That baseline raised concerns: 2020 was a COVID year, so maybe the increases were just a bounce-back from artificially low stop counts.

With 11 years of data, we can answer that directly. Hispanic stops in 2019 — before COVID — were already at 35,526. By 2024 they'd risen to 46,655. That's a 31% increase from the pre-pandemic high, not a rebound. Meanwhile, White stops in 2019 were 1.16 million; by 2024 they'd fallen to 984,000. Everyone else's stop counts went down. Hispanic stops went up.

```js
const baselines = storyRaces.map(race => {
  const r2014 = swStopsData.find(d => d.race === race && d.year === 2014)?.value;
  const r2019 = swStopsData.find(d => d.race === race && d.year === 2019)?.value;
  const r2020 = swStopsData.find(d => d.race === race && d.year === 2020)?.value;
  const r2024 = swStopsData.find(d => d.race === race && d.year === 2024)?.value;
  return {
    Race: race,
    "2014": d3.format(",")(r2014),
    "2019": d3.format(",")(r2019),
    "2020 (COVID)": d3.format(",")(r2020),
    "2024": d3.format(",")(r2024),
    "vs 2014": d3.format("+.0%")((r2024 - r2014) / r2014),
    "vs 2019": d3.format("+.0%")((r2024 - r2019) / r2019),
  };
});
Inputs.table(baselines)
```

---

## The 11-year trend

```js
trendChart(sw("stop_index_2014_eq_100"), {title: "Traffic stops, indexed to 2014 = 100", ylabel: "Index", baseline: 100})
```

Indexed to 2014, the Hispanic line climbs above 150 while the white and Black lines drop below 80. The gap grows every year after 2021. Missouri's Hispanic population grew 49% over the same period (233,000 → 347,000 per the ACS), roughly matching the 52% stop increase — so the per-capita Hispanic stop rate is essentially flat across the decade. White and Black populations barely moved, but their stop counts fell roughly 25%. In per-capita terms, white and Black drivers got a 22–24% enforcement reduction; Hispanic drivers got none of it.

```js
const swStopsYoy = storyRaces.flatMap(race => {
  const sorted = swStopsData.filter(d => d.race === race).sort((a, b) => a.year - b.year);
  return sorted.slice(1).map((d, i) => ({
    year: d.year, race,
    value: +((d.value - sorted[i].value) / sorted[i].value * 100).toFixed(1),
    label: d3.format("+.1f")((d.value - sorted[i].value) / sorted[i].value * 100) + "%",
  }));
});

Plot.plot({
  title: "Year-over-year change in stops, 2015–2024",
  subtitle: "White and Black stops drift downward; Hispanic stops turn sharply up after 2021",
  width, height: 380,
  x: {label: null, tickFormat: "d"},
  y: {label: "% change from prior year", grid: true},
  color: raceColors,
  marks: [
    Plot.ruleY([0], {stroke: "#ddd"}),
    Plot.lineY(swStopsYoy, {x: "year", y: "value", stroke: "race", strokeWidth: 2.5, marker: "circle"}),
    Plot.text(swStopsYoy.filter(d => d.race === "Hispanic" && d.year >= 2022), {
      x: "year", y: "value", text: "label", fill: "#e8a820", dy: -12, fontWeight: 600, fontSize: 12,
    }),
  ],
})
```

---

## The composition shift: license stops doubled for Hispanic drivers

Not all traffic stops are created equal. Moving violations leave little room for discretion. But license stops — pulling a driver over for a missing, expired, or invalid license — are also the category of stop most likely to surface immigration status. A driver without a license can't prove who they are, and that opens the door to everything that happens next.

Missouri's overall enforcement mix has shifted toward license stops for every race: as moving violations and equipment stops declined, the *share* of stops that are license stops rose for white, Black, and Hispanic drivers alike. That's a statewide composition change, not a Hispanic-specific one. But because Hispanic stop volume grew 52% while stops of white and Black drivers fell 25–28%, that composition shift landed very differently on different populations. Look at the absolute counts:

```js
const licenseByRace = raceCompare.filter(d => [2014, 2024].includes(d.year));
const licTable = ["White", "Black", "Hispanic"].map(race => {
  const r14 = licenseByRace.find(d => d.race === race && d.year === 2014);
  const r24 = licenseByRace.find(d => d.race === race && d.year === 2024);
  return {
    Race: race,
    "License stops 2014": d3.format(",")(r14.license_stops),
    "License stops 2024": d3.format(",")(r24.license_stops),
    "Change": d3.format("+,")(r24.license_stops - r14.license_stops),
    "Change %": d3.format("+.0%")((r24.license_stops - r14.license_stops) / r14.license_stops),
  };
});
Inputs.table(licTable)
```

White license stops grew 7% (a near-flat count). Black license stops actually fell 4%. **Hispanic license stops doubled** — from 6,048 to 12,080 — even though the underlying *rate* rose by about the same proportion for everyone. The volume explosion is what channels the composition shift disproportionately onto Hispanic drivers.

```js
const licRateByYear = raceCompare.filter(d => d.license_rate_pct !== "");
Plot.plot({
  title: "License stop rate by race (% of all stops), 2014–2024",
  subtitle: "The rate rose for every race. The absolute count did not.",
  width, height: 340,
  x: {label: null, tickFormat: "d", ticks: 6},
  y: {label: "License stops as % of all stops", grid: true, nice: true},
  color: raceColors,
  marks: [
    Plot.lineY(licRateByYear, {x: "year", y: "license_rate_pct", stroke: "race", strokeWidth: 2.5}),
    Plot.dot(licRateByYear, {x: "year", y: "license_rate_pct", fill: "race", r: 4}),
    Plot.text(licRateByYear.filter(d => d.year === 2024), {
      x: "year", y: "license_rate_pct",
      text: d => `${d.race} ${d.license_rate_pct}%`,
      fill: "race", dx: 10, textAnchor: "start", fontWeight: 600, fontSize: 12,
    }),
  ],
})
```

The rate-based chart shows all three lines trending up. The count-based story shows only one group being drawn into that category in meaningful new volume. That's the finding that matters.

```js
const reasonMetrics = [
  {metric: "reason_license", label: "License", color: "#c22d2d"},
  {metric: "reason_equipment", label: "Equipment", color: "#2a7b9b"},
  {metric: "reason_investigative", label: "Investigative", color: "#5a8a3f"},
  {metric: "reason_moving", label: "Moving violation", color: "#888"},
];

const reasonData = reasonMetrics.flatMap(({metric, label, color}) => {
  const hispRows = sw(metric).filter(d => d.race === "Hispanic").sort((a, b) => a.year - b.year);
  const stopsRows = sw("total_stops").filter(d => d.race === "Hispanic").sort((a, b) => a.year - b.year);
  return hispRows.map(d => {
    const stops = stopsRows.find(s => s.year === d.year)?.value;
    return {
      year: d.year,
      reason: label,
      color,
      rate: stops ? +(d.value / stops * 100).toFixed(2) : null,
    };
  }).filter(d => d.rate !== null);
});

Plot.plot({
  title: "Hispanic stops by reason — % of all Hispanic stops",
  subtitle: "License stops climb to an 11-year high; other categories fall or hold flat",
  width, height: 420,
  marginRight: 130,
  x: {label: null, tickFormat: "d", ticks: 6},
  y: {label: "% of Hispanic stops", grid: true, nice: true},
  color: {domain: reasonMetrics.map(r => r.label), range: reasonMetrics.map(r => r.color), legend: true},
  marks: [
    Plot.lineY(reasonData, {x: "year", y: "rate", stroke: "reason", strokeWidth: 2.5}),
    Plot.dot(reasonData, {x: "year", y: "rate", fill: "reason", r: 4, stroke: "#fff", strokeWidth: 1}),
    Plot.text(reasonData.filter(d => d.year === 2024), {
      x: "year", y: "rate",
      text: d => `${d.reason} ${d.rate}%`,
      dx: 10, textAnchor: "start", fontWeight: 600, fontSize: 11, fill: "reason",
    }),
  ],
})
```

License stops aren't random. A Missouri driver without a valid license is someone whose identification the officer can't verify — the exact scenario that made the O'Fallon traffic stop of Victor López Delara in February 2026 end with his transfer to ICE custody.

---

## It's not just one agency

The Missouri State Highway Patrol accounts for roughly a quarter of all traffic stops. Is the statewide surge just an MSHP story? No. MSHP's own Hispanic stop numbers were flat for most of the past decade — hovering around 10,000 a year from 2014 through 2021 — and only started climbing in 2022.

```js
const mshpData = byAgency.filter(d => d.agency === "Missouri State Highway Patrol");
const mshpStops = mshpData.filter(d => d.metric === "total_stops");

const mshpYears = [...new Set(mshpStops.map(d => d.year))].sort();

const mshpVsRest = storyRaces.flatMap(race => {
  return mshpYears.map(year => {
    const swVal = swStopsData.find(d => d.race === race && d.year === year)?.value;
    const mshpVal = mshpStops.find(d => d.race === race && d.year === year)?.value;
    if (!swVal || !mshpVal) return null;
    return {year, race, mshp: mshpVal, rest: swVal - mshpVal};
  }).filter(d => d);
});

const hispOnly = storyRaces.filter(r => r === "Hispanic").flatMap(race => {
  const rows = mshpVsRest.filter(d => d.race === race);
  const base_mshp = rows.find(d => d.year === 2014)?.mshp;
  const base_rest = rows.find(d => d.year === 2014)?.rest;
  if (!base_mshp || !base_rest) return [];
  return rows.flatMap(d => [
    {year: d.year, group: "MSHP", value: +(d.mshp / base_mshp * 100).toFixed(1)},
    {year: d.year, group: "All other agencies", value: +(d.rest / base_rest * 100).toFixed(1)},
  ]);
});

Plot.plot({
  title: "Hispanic stops indexed to 2014 — MSHP vs. everyone else",
  subtitle: "MSHP was flat through 2021; the surge starts in 2022",
  width, height: 380,
  x: {label: null, tickFormat: "d", ticks: 6},
  y: {label: "Index (2014 = 100)", grid: true},
  color: {legend: true},
  marks: [
    Plot.ruleY([100], {stroke: "#ddd", strokeDasharray: "4,3"}),
    Plot.lineY(hispOnly, {x: "year", y: "value", stroke: "group", strokeWidth: 2.5}),
    Plot.dot(hispOnly, {x: "year", y: "value", fill: "group", r: 4}),
    Plot.text(hispOnly.filter(d => d.year === 2024), {
      x: "year", y: "value", text: d => `${d.group} ${d.value}`,
      dx: 10, textAnchor: "start", fontWeight: 600, fontSize: 12
    }),
  ],
})
```

```js
const mshpSummary = storyRaces.map(race => {
  const rows = mshpVsRest.filter(d => d.race === race);
  const r14 = rows.find(d => d.year === 2014);
  const r24 = rows.find(d => d.year === 2024);
  if (!r14 || !r24) return null;
  return {
    Race: race,
    "MSHP 2014": d3.format(",")(r14.mshp), "MSHP 2024": d3.format(",")(r24.mshp),
    "MSHP change": d3.format("+.0%")((r24.mshp - r14.mshp) / r14.mshp),
    "Other 2014": d3.format(",")(r14.rest), "Other 2024": d3.format(",")(r24.rest),
    "Other change": d3.format("+.0%")((r24.rest - r14.rest) / r14.rest),
  };
}).filter(d => d);
Inputs.table(mshpSummary)
```

```js
const mshpSearch = mshpData.filter(d => d.metric === "search_rate_pct" && storyRaces.includes(d.race));
const mshpArrest = mshpData.filter(d => d.metric === "arrest_rate_pct" && storyRaces.includes(d.race));
```

<div class="grid grid-cols-2">

```js
trendChart(mshpSearch, {title: "MSHP search rate (%)", ylabel: "Search rate %", fmt: ".1f", height: 300})
```

```js
trendChart(mshpArrest, {title: "MSHP arrest rate (%)", ylabel: "Arrest rate %", fmt: ".2f", height: 300})
```

</div>

MSHP: 312,976 stops in 2024. Hispanic search rate 6.2% vs. white 3.2% (1.9×). Hispanic arrest rate 5.1% vs. white 2.7% (1.9×). MSHP has never submitted comments to the AG explaining its disparities.

---

## Hispanic enforcement rates, 11 years

Looking only at Hispanic drivers, four of five per-stop rates *declined* since 2014 — arrests, searches, investigative stops, and equipment stops are all lower than they were 11 years ago. License stop rate is the one that rose, from 19.65% to 25.89% of Hispanic stops. The 2020 COVID year was the low point across the board; most metrics have partially rebounded since, but none of the declining ones have returned to 2014 levels.

A crucial caveat: the same per-stop rate declines show up for white and Black drivers. The rate changes aren't unique to Hispanic drivers (see [the composition section](#the-composition-shift-license-stops-doubled-for-hispanic-drivers) above). What *is* unique is the volume surge: Hispanic stops rose 52% while stops of every other group fell, so the per-stop rate softening got overwhelmed by raw count growth. The arrest rate dropped for Hispanic drivers, but more Hispanic drivers were still being arrested in absolute terms in 2024 than in 2014.

```js
const metricColors = {
  "License Stop Rate": "#c22d2d",
  "Arrest Rate": "#7b3fa0",
  "Equipment Stop Rate": "#2a7b9b",
  "Investigative Stop Rate": "#5a8a3f",
  "Search Rate": "#888",
};

const metricOrder = Object.keys(metricColors);

const dotDataWithChange = dotLineData.map(d => {
  const base = dotLineData.find(b => b.Metric === d.Metric && b.Year === 2014)?.Rate;
  const pctChange = base ? ((d.Rate - base) / base) * 100 : 0;
  return {...d, pctChange, absChange: Math.abs(pctChange)};
});

Plot.plot({
  title: "Hispanic enforcement rates, 2014–2024",
  subtitle: "Only license stops are at an 11-year high. Every other rate is below 2014 levels.",
  width,
  height: 500,
  marginRight: 160,
  x: {label: null, tickFormat: "d", ticks: 6},
  y: {label: "Rate (%)", grid: true, nice: true},
  color: {domain: metricOrder, range: metricOrder.map(m => metricColors[m]), legend: true},
  marks: [
    Plot.line(dotDataWithChange, {
      x: "Year", y: "Rate", z: "Metric",
      stroke: "Metric", strokeWidth: 2, strokeOpacity: 0.6,
    }),
    Plot.dot(dotDataWithChange, {
      x: "Year", y: "Rate",
      r: "absChange",
      fill: "Metric", fillOpacity: 0.85,
      stroke: "#fff", strokeWidth: 1.5,
    }),
    Plot.text(dotDataWithChange.filter(d => d.Year === 2024), {
      x: "Year", y: "Rate",
      text: d => `${d.Metric.replace(" Rate", "").replace(" Stop", "")} ${d.Rate}%`,
      fill: "Metric",
      dx: 24, textAnchor: "start", fontWeight: 600, fontSize: 11,
    }),
  ],
  style: {fontSize: 13},
})
```

```js
Inputs.table(
  metricOrder.map(metric => {
    const v2014 = dotLineData.find(d => d.Metric === metric && d.Year === 2014)?.Rate;
    const v2024 = dotLineData.find(d => d.Metric === metric && d.Year === 2024)?.Rate;
    const pctChange = v2014 ? ((v2024 - v2014) / v2014 * 100) : 0;
    return {
      Metric: metric,
      "2014": `${v2014}%`,
      "2024": `${v2024}%`,
      "Change": `${pctChange >= 0 ? "+" : ""}${pctChange.toFixed(0)}%`,
    };
  }),
  {sort: "Change", reverse: true}
)
```

---

## After the stop: the persistent disparity

Per-stop harshness declined for every race over the 11-year window. The Hispanic arrest rate fell from 8.19% to 6.17% (−25%). White arrest rate fell from 4.14% to 3.38% (−18%). Black arrest rate fell from 7.90% to 5.82% (−26%). Searches, investigative stops, and equipment stops all tell a similar story — all three races saw the per-stop rate decline, and Black drivers actually saw the biggest declines on most metrics.

But two things make Hispanic drivers the exception. First, the disparity never closed: Hispanic drivers were still arrested at roughly **1.8 times the white rate** in 2024 (6.17% vs. 3.38%), the same ratio as 2014. Second, because Hispanic stop volume rose 52% while everyone else's fell, the absolute arrest count tells a different story than the rate:

```js
const arrestCounts = ["White", "Black", "Hispanic"].map(race => {
  const r14 = raceCompare.find(d => d.race === race && d.year === 2014);
  const r24 = raceCompare.find(d => d.race === race && d.year === 2024);
  return {
    Race: race,
    "Arrests 2014": d3.format(",")(r14.arrest_count),
    "Arrests 2024": d3.format(",")(r24.arrest_count),
    "Change": d3.format("+,")(r24.arrest_count - r14.arrest_count),
    "Change %": d3.format("+.0%")((r24.arrest_count - r14.arrest_count) / r14.arrest_count),
  };
});
Inputs.table(arrestCounts)
```

White arrests fell by about 21,000 over the decade. Black arrests fell by about 11,000. **Hispanic is the only race whose absolute arrest count rose** — by about 360, or +14%. Every other group saw their raw arrest numbers drop by nearly half. Hispanic drivers experienced a softer per-stop arrest rate in 2024 than in 2014, but more of them were being arrested in absolute terms anyway, because there were so many more stops to arrest from.

<div class="grid grid-cols-2">

```js
trendChart(sw("search_rate_pct"), {title: "Search rate (% of stops)", ylabel: "Search rate %", fmt: ".1f", height: 320})
```

```js
trendChart(sw("arrest_rate_pct"), {title: "Arrest rate (% of stops)", ylabel: "Arrest rate %", fmt: ".2f", height: 320})
```

</div>

The search rate is worth a methodological note. Missouri law requires a search incident to every arrest, so year-over-year search and arrest rates move together mechanically. Hispanic search rates have declined across the 11-year window — from 9.91% in 2014 to 6.40% in 2024 — and so have Black (9.00% → 5.35%) and white (5.21% → 4.68%) rates. The story isn't the search rate trajectory. It's the persistent 1.8× arrest disparity on top of a 52% stop-volume increase.

### The license stop → arrest proxy

The VSR data doesn't cross-tabulate stop reason by outcome, so we can't directly measure "what fraction of license stops end in arrest." But we can compute a rough proxy: total arrests divided by total license stops, for each race. This isn't causal — arrests come from all stop types — but the ratio shows how much arrest activity accompanies the license-stop volume for each group.

```js
Plot.plot({
  title: "Arrests per 100 license stops, by race (proxy)",
  subtitle: "Not causal, but the gap is persistent: Hispanic drivers face ~2× the arrest intensity per license stop",
  width, height: 380,
  x: {label: null, tickFormat: "d", ticks: 6},
  y: {label: "Arrests per 100 license stops", grid: true, nice: true},
  color: raceColors,
  marks: [
    Plot.lineY(licArrestProxy, {x: "year", y: "arrests_per_100_lic", stroke: "race", strokeWidth: 2.5}),
    Plot.dot(licArrestProxy, {x: "year", y: "arrests_per_100_lic", fill: "race", r: 4, stroke: "#fff", strokeWidth: 1}),
    Plot.text(licArrestProxy.filter(d => d.year === 2024), {
      x: "year", y: "arrests_per_100_lic",
      text: d => `${d.race} ${d.arrests_per_100_lic}`,
      fill: "race", dx: 10, textAnchor: "start", fontWeight: 600, fontSize: 12,
    }),
  ],
})
```

In 2024, for every 100 Hispanic license stops there were about 24 arrests happening in the same population. For white drivers: about 10. That's a 2.3× ratio, and it's been roughly that wide for the entire decade. The combination of doubling license-stop volume *and* maintaining a 2× arrest-intensity gap means the absolute number of Hispanic drivers cycling through the stop → arrest → jail pipeline grew substantially — even as rates softened.

### Stops that went nowhere

Another signal: in 2024, 4.0% of Hispanic stops ended in "no action" — no citation, no warning, nothing. For white drivers: 2.6%. Hispanic drivers are more likely to be stopped and released with nothing to show for it, which is consistent with more pretextual or exploratory stops.

---

## The volume increase isn't a numbers game

Rates can decline while raw counts rise — because the underlying stop volume matters. Hispanic stops rose from 30,782 in 2014 to 46,655 in 2024. So even as rates softened, more Hispanic drivers were being affected in absolute terms. The number of Hispanic arrests, for example, was roughly 2,500 in 2014 and roughly 2,900 in 2024 — higher, despite a lower per-stop arrest rate.

---

## The contraband hit rate

Across 11 years, the Hispanic contraband hit rate barely moved: 19.5% in 2014 and 19.2% in 2024. It spiked during the COVID years (peaking at 35.2% in 2020, when overall search volume cratered) and then fell back to near its starting point. Hispanic searches are no more productive today than they were a decade ago, even though search rates for every other group followed the same boom-and-bust pattern.

```js
trendChart(contrabandData.map(d => ({...d, value: d.hit_rate})), {
  title: "Contraband hit rate (% of searches), 2014–2024",
  ylabel: "Hit rate %", fmt: ".1f", height: 320,
})
```

The 2020–2022 spike coincides with a collapse in total search volume — fewer searches were happening, but the ones that did were more likely to find contraband. As search volume recovered, hit rates slid back to their pre-pandemic baseline for every group.

### Contraband hit rates by agency

Which agencies are searching Hispanic drivers the most and finding the least? The table below shows agencies with at least 20 Hispanic searches in 2024, sorted by hit rate. A low hit rate with a high search count suggests searches that aren't driven by strong probable cause.

```js
const hispContraband = agencyContraband
  .filter(d => d.race === "Hispanic" && d.searches >= 20)
  .sort((a, b) => a.hit_rate - b.hit_rate);

const whiteContraband = new Map(
  agencyContraband.filter(d => d.race === "White").map(d => [d.agency, d])
);

const contrabandAgTable = hispContraband.map(d => {
  const w = whiteContraband.get(d.agency);
  return {
    Agency: d.agency,
    "Hispanic searches": d.searches,
    "Hispanic hit rate": d.hit_rate + "%",
    "White hit rate": w ? w.hit_rate + "%" : "—",
    "Gap": w ? d3.format("+.1f")(d.hit_rate - w.hit_rate) + " pp" : "—",
  };
});
Inputs.table(contrabandAgTable, {sort: "Hispanic hit rate"})
```

---

## Where is this happening?

The surge isn't uniform. Some agencies saw Hispanic stops increase by hundreds of percent.

```js
const minStops = view(Inputs.range([50, 500], {label: "Min Hispanic stops in 2024", step: 25, value: 100}));
```

```js
// Build 11-year Hispanic stop change for each agency (2014 → 2024)
const agHisp = ag("total_stops").filter(d => d.race === "Hispanic");
const agencyChangeMap = new Map();
for (const d of agHisp) {
  if (!agencyChangeMap.has(d.agency)) agencyChangeMap.set(d.agency, {});
  agencyChangeMap.get(d.agency)[d.year] = d.value;
}

const agencyChanges = [...agencyChangeMap.entries()]
  .map(([agency, years]) => {
    const h2014 = years[2014];
    const h2024 = years[2024];
    if (!h2024 || h2024 < minStops) return null;
    if (!h2014 || h2014 < 5) return null;
    const agencyRow = agHisp.find(d => d.agency === agency);
    return {
      agency,
      type: agencyRow?.agency_type || "",
      pct_change: (h2024 - h2014) / h2014 * 100,
      stops_2014: h2014,
      stops_2024: h2024,
    };
  })
  .filter(d => d)
  .sort((a, b) => b.pct_change - a.pct_change);
```

```js
Plot.plot({
  title: "Change in Hispanic stops, 2014–2024",
  width, height: Math.max(300, agencyChanges.slice(0, 25).length * 24),
  marginLeft: 240,
  x: {label: "% change since 2014", grid: true},
  y: {label: null},
  marks: [
    Plot.ruleX([0], {stroke: "#ddd"}),
    Plot.barX(agencyChanges.slice(0, 25), {
      y: "agency", x: "pct_change", fill: "#e8a820", sort: {y: "-x"},
      tip: true, title: d => `${d.agency}\n${d3.format("+,.0f")(d.pct_change)}%\n${d3.format(",")(d.stops_2014)} Hispanic stops in 2014 → ${d3.format(",")(d.stops_2024)} in 2024`,
    }),
    Plot.text(agencyChanges.slice(0, 25), {
      y: "agency", x: "pct_change",
      text: d => d3.format("+,.0f")(d.pct_change) + "%",
      dx: 4, textAnchor: "start", fontSize: 11
    }),
  ],
})
```

```js
Inputs.table(agencyChanges, {
  columns: ["agency", "type", "stops_2014", "stops_2024", "pct_change"],
  header: {agency: "Agency", type: "Type", stops_2014: "Hispanic stops 2014", stops_2024: "Hispanic stops 2024", pct_change: "% change"},
  format: {pct_change: d => d3.format("+,.0f")(d) + "%", stops_2014: d => d3.format(",")(d), stops_2024: d => d3.format(",")(d)},
  sort: "pct_change", reverse: true,
})
```

---

## Stops vs. local population

If police stopped people in proportion to the local population, the "Hispanic share of stops" and "Hispanic share of population" would be roughly equal. Agencies above the diagonal line are stopping Hispanic drivers more than their share would predict.

```js
const censusYearLookup = new Map();
for (const row of censusByYear) censusYearLookup.set(`${row.name}||${row.year}`, row);

const popLookup = new Map(agencyPop.map(d => [d.agency, d]));

const disparity = ag("total_stops")
  .filter(d => d.year === 2024 && (d.race === "Hispanic" || d.race === "Total"))
  .reduce((acc, d) => {
    if (!acc.has(d.agency)) acc.set(d.agency, {agency: d.agency, type: d.agency_type});
    const obj = acc.get(d.agency);
    if (d.race === "Hispanic") obj.hisp_stops = d.value;
    if (d.race === "Total") obj.total_stops = d.value;
    return acc;
  }, new Map());

const scatterData = [...disparity.values()]
  .filter(d => d.hisp_stops && d.total_stops && d.total_stops >= 100)
  .map(d => {
    const p = popLookup.get(d.agency);
    if (!p || !p.pop_hispanic || !p.pop_total) return null;
    return {
      ...d,
      hisp_stop_pct: +(d.hisp_stops / d.total_stops * 100).toFixed(1),
      hisp_pop_pct: +(p.pop_hispanic / p.pop_total * 100).toFixed(1),
      pop_hispanic: p.pop_hispanic, pop_total: p.pop_total,
      census_match_name: p.census_match_name,
    };
  })
  .filter(d => d);

Plot.plot({
  title: "Hispanic share of stops vs. Hispanic share of local population (2024)",
  width, height: 500,
  x: {label: "Hispanic % of local population →", grid: true, nice: true},
  y: {label: "Hispanic % of stops →", grid: true, nice: true},
  marks: [
    Plot.link([{x1: 0, y1: 0, x2: 50, y2: 50}], {x1: "x1", y1: "y1", x2: "x2", y2: "y2", stroke: "#ddd", strokeDasharray: "4,3"}),
    Plot.dot(scatterData, {
      x: "hisp_pop_pct", y: "hisp_stop_pct",
      r: d => Math.sqrt(d.total_stops) / 8,
      fill: "#e8a820", fillOpacity: 0.6, stroke: "#333", strokeWidth: 0.5,
      tip: true,
      title: d => `${d.agency}\nHispanic stops: ${d.hisp_stop_pct}% (${d3.format(",")(d.hisp_stops)})\nHispanic pop: ${d.hisp_pop_pct}%\nTotal stops: ${d3.format(",")(d.total_stops)}`,
    }),
    Plot.text(scatterData.filter(d => d.hisp_stops > 400 || d.hisp_stop_pct > 15), {
      x: "hisp_pop_pct", y: "hisp_stop_pct", text: "agency", fontSize: 9, dy: -10,
    }),
  ],
})
```

Circle size = total stops. 587 of 612 agencies matched to Census geography.

```js
const gapData = scatterData
  .map(d => ({
    Agency: d.agency, Type: d.type,
    "Hispanic pop %": d.hisp_pop_pct, "Hispanic stop %": d.hisp_stop_pct,
    "Gap (pp)": +(d.hisp_stop_pct - d.hisp_pop_pct).toFixed(1),
    "Hispanic stops": d.hisp_stops,
  }))
  .filter(d => d["Hispanic stops"] >= 50)
  .sort((a, b) => b["Gap (pp)"] - a["Gap (pp)"]);
Inputs.table(gapData, {sort: "Gap (pp)", reverse: true})
```

---

## Agency per-capita trends over time

Using year-by-year Census estimates, we can track how the per-capita stop rate for Hispanic drivers changed at specific agencies. Select an agency to see whether its local Hispanic stop rate outpaced local population growth.

```js
const agPerCapitaList = [...new Set(scatterData.map(d => d.agency))].sort();
const selectedPerCapita = view(Inputs.select(agPerCapitaList, {label: "Agency", value: agPerCapitaList.find(a => a === "Missouri State Highway Patrol") ?? agPerCapitaList[0]}));
```

```js
const selAgPop = popLookup.get(selectedPerCapita);
const censusName = selAgPop?.census_match_name;

const agPerCapita = storyRaces.flatMap(race => {
  return [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024].map(year => {
    const stops = byAgency.find(d => d.agency === selectedPerCapita && d.metric === "total_stops" && d.race === race && d.year === year)?.value;
    const censusRow = censusYearLookup.get(`${censusName}||${year}`);
    const population = censusRow?.[race];
    if (!stops || !population || population === 0) return null;
    return {year, race, value: +(stops / population * 100).toFixed(2), stops, population};
  }).filter(d => d);
});
```

```js
trendChart(agPerCapita, {title: `${selectedPerCapita} — stops per 100 residents`, ylabel: "Stops per 100 residents", fmt: ".1f"})
```

```js
const agPcTable = storyRaces.flatMap(race => {
  return [2014, 2024].map(year => {
    const d = agPerCapita.find(r => r.race === race && r.year === year);
    if (!d) return null;
    return {Race: race, Year: year, Stops: d3.format(",")(d.stops), Population: d3.format(",")(d.population), "Per capita": d.value};
  }).filter(d => d);
});
Inputs.table(agPcTable)
```

---

## Arrest rate ratios by agency

Where are Hispanic drivers most disproportionately arrested? The chart shows the Hispanic arrest rate divided by the white arrest rate for agencies with at least 100 Hispanic stops in 2024.

```js
const agStopsAll = ag("total_stops").filter(d => d.year === 2024);
const agArrestAll = ag("arrest_rate_pct").filter(d => d.year === 2024);
const stopsMap = d3.group(agStopsAll, d => d.agency);
const arrestMap = d3.group(agArrestAll, d => d.agency);

const ratioRows = [];
for (const [agency, sRows] of stopsMap) {
  const hispStops = sRows.find(d => d.race === "Hispanic")?.value;
  if (!hispStops || hispStops < 100) continue;
  const aRows = arrestMap.get(agency);
  if (!aRows) continue;
  const hispArrest = aRows.find(d => d.race === "Hispanic")?.value;
  const whiteArrest = aRows.find(d => d.race === "White")?.value;
  if (!hispArrest || !whiteArrest || whiteArrest === 0) continue;
  ratioRows.push({agency, hisp_stops: hispStops, hisp_arrest_pct: hispArrest, white_arrest_pct: whiteArrest, ratio: +(hispArrest / whiteArrest).toFixed(2)});
}
ratioRows.sort((a, b) => b.ratio - a.ratio);
const topRatios = ratioRows.slice(0, 20);
```

```js
Plot.plot({
  title: "Hispanic-to-white arrest ratio by agency (2024)",
  subtitle: "Dashed line = statewide ratio (1.8×)",
  width, height: Math.max(300, topRatios.length * 26),
  marginLeft: 260,
  x: {label: "Arrest ratio (Hispanic ÷ White)", grid: true},
  y: {label: null},
  marks: [
    Plot.ruleX([1], {stroke: "#ddd"}),
    Plot.ruleX([1.8], {stroke: "#e8a820", strokeDasharray: "4,3", strokeWidth: 1.5}),
    Plot.barX(topRatios, {
      y: "agency", x: "ratio", fill: d => d.ratio >= 1.8 ? "#e8a820" : "#ccc", sort: {y: "-x"},
      tip: true,
      title: d => `${d.agency}\nHispanic arrest rate: ${d.hisp_arrest_pct}%\nWhite arrest rate: ${d.white_arrest_pct}%\nHispanic stops: ${d3.format(",")(d.hisp_stops)}`,
    }),
    Plot.text(topRatios, {
      y: "agency", x: "ratio", text: d => d.ratio.toFixed(1) + "×",
      dx: 4, textAnchor: "start", fontSize: 11, fontWeight: 600,
    }),
  ],
})
```

```js
Inputs.table(ratioRows, {
  columns: ["agency", "hisp_stops", "hisp_arrest_pct", "white_arrest_pct", "ratio"],
  header: {agency: "Agency", hisp_stops: "Hispanic stops", hisp_arrest_pct: "Hispanic arrest %", white_arrest_pct: "White arrest %", ratio: "Ratio"},
  format: {hisp_stops: d => d3.format(",")(d), ratio: d => d.toFixed(1) + "×"},
  sort: "ratio", reverse: true,
})
```

---

## Who's been asked to explain — and who responded

Each year, the attorney general invites agencies to submit comments explaining their data. In 2020–2023, roughly 500+ agencies were included in the comment file. In 2024, just 18 responded. The agencies with the largest Hispanic stop increases and disparity ratios are overwhelmingly silent.

```js
// For each agency in our ratioRows (high-disparity agencies), check comment history
const commentsByAgency = d3.group(agencyComments, d => d.agency);

const commentCheck = ratioRows.map(d => {
  const comments = commentsByAgency.get(d.agency) ?? [];
  const withText = comments.filter(c => c.has_comment === "True" || c.has_comment === true);
  const years = withText.map(c => c.year).sort();
  return {
    Agency: d.agency,
    "Hispanic stops": d.hisp_stops,
    "Arrest ratio": d.ratio.toFixed(1) + "×",
    "Years commented": years.length > 0 ? years.join(", ") : "Never",
    "Commented in 2024": years.includes(2024) || years.includes("2024") ? "Yes" : "No",
  };
});
Inputs.table(commentCheck)
```

```js
// 2024 response rate
const commented2024 = new Set(
  agencyComments.filter(d => d.year === 2024 && (d.has_comment === "True" || d.has_comment === true)).map(d => d.agency)
);
const totalAgencies2024 = new Set(ag("total_stops").filter(d => d.year === 2024 && d.race === "Total").map(d => d.agency));
```

<div class="grid grid-cols-3">
<div class="card">
  <h2>${commented2024.size}</h2>
  <p>Agencies commented in 2024</p>
</div>
<div class="card">
  <h2>${totalAgencies2024.size}</h2>
  <p>Total reporting agencies</p>
</div>
<div class="card">
  <h2>${d3.format(".0%")(commented2024.size / totalAgencies2024.size)}</h2>
  <p>Response rate</p>
</div>
</div>

MSHP — the state's largest agency with 312,976 stops — has rows in the comment file for 2020–2023 but never submitted actual text. They did not respond in 2024 either.

---

## Agency deep dive

Pick an agency to see how its stop numbers, search rates, and arrest rates have changed over time.

```js
const agencyList = [...new Set(byAgency.filter(d => d.race === "Hispanic" && d.metric === "total_stops").map(d => d.agency))].sort();
const selectedAgency = view(Inputs.select(agencyList, {label: "Agency", value: "Missouri State Highway Patrol"}));
```

```js
const agencyStops = byAgency.filter(d => d.agency === selectedAgency && d.metric === "total_stops" && storyRaces.includes(d.race));

Plot.plot({
  title: `${selectedAgency} — stops by race`,
  width, height: 380,
  x: {label: null, tickFormat: "d"},
  y: {label: "Stops", grid: true},
  color: raceColors,
  marks: [
    Plot.lineY(agencyStops, {x: "year", y: "value", stroke: "race", strokeWidth: 2.5}),
    Plot.dot(agencyStops, {x: "year", y: "value", fill: "race", r: 4}),
  ],
})
```

<div class="grid grid-cols-2">

```js
const agencySearch = byAgency.filter(d => d.agency === selectedAgency && d.metric === "search_rate_pct" && storyRaces.includes(d.race));
trendChart(agencySearch, {title: "Search rate %", ylabel: "Search rate %", fmt: ".1f", height: 280})
```

```js
const agencyArrest = byAgency.filter(d => d.agency === selectedAgency && d.metric === "arrest_rate_pct" && storyRaces.includes(d.race));
trendChart(agencyArrest, {title: "Arrest rate %", ylabel: "Arrest rate %", fmt: ".2f", height: 280})
```

</div>

---

## Reporting context

### The 287(g) wave

The surge in Hispanic stops predates — but now intersects with — a rapid expansion of immigration enforcement agreements in Missouri.

The Missouri State Highway Patrol signed a 287(g) task force agreement with ICE on March 21, 2025. Under the task force model, troopers can ask about immigration status during routine traffic stops. The program went operational September 26, 2025. Since then, MSHP has contacted ICE about 53 people and obtained detainers on 44.

As of March 2026, more than 60 agencies across Missouri have signed 287(g) agreements. At least 28 use the task force model — the most aggressive type, which embeds immigration enforcement into everyday traffic policing.

The data in this notebook shows the Hispanic stop pattern was already escalating from 2022–2024, before most agreements were signed. The question: was the enforcement culture already shifting before the formal agreements made it policy?

### ICE enforcement in Missouri: the detainer data

Using ICE's own detainer records (released via FOIA, Sept 2023 – Oct 2025), we can see how the enforcement apparatus grew. The dashed line marks February 2025 — when 287(g) agreements began going operational.

```js
const parseMonth = d3.timeParse("%Y-%m");
const iceDetainersParsed = iceMonthly.map(d => ({...d, date: parseMonth(d.month)}));
const iceArrestsParsed = iceArrestsMonthly.map(d => ({...d, date: parseMonth(d.month)}));
const cutoff = parseMonth("2025-02");
```

```js
Plot.plot({
  title: "ICE detainers + admin arrests in Missouri, by month",
  subtitle: "Dashed line = Feb 2025, when 287(g) agreements began going operational",
  width, height: 380,
  x: {label: null, type: "utc"},
  y: {label: "Monthly count", grid: true, nice: true},
  color: {legend: true, domain: ["Detainers", "Admin arrests"], range: ["#c22d2d", "#e8a820"]},
  marks: [
    Plot.ruleX([cutoff], {stroke: "#333", strokeDasharray: "6,3", strokeWidth: 2}),
    Plot.areaY(iceDetainersParsed, {x: "date", y: "detainers", fill: "#c22d2d", fillOpacity: 0.12, curve: "catmull-rom"}),
    Plot.lineY(iceDetainersParsed, {x: "date", y: "detainers", stroke: "#c22d2d", strokeWidth: 2.5, curve: "catmull-rom"}),
    Plot.dot(iceDetainersParsed, {x: "date", y: "detainers", fill: "#c22d2d", r: 3.5, stroke: "#fff", strokeWidth: 1}),
    Plot.areaY(iceArrestsParsed, {x: "date", y: "arrests", fill: "#e8a820", fillOpacity: 0.12, curve: "catmull-rom"}),
    Plot.lineY(iceArrestsParsed, {x: "date", y: "arrests", stroke: "#e8a820", strokeWidth: 2.5, curve: "catmull-rom"}),
    Plot.dot(iceArrestsParsed, {x: "date", y: "arrests", fill: "#e8a820", r: 3.5, stroke: "#fff", strokeWidth: 1}),
    Plot.text([{date: cutoff, y: Math.max(...iceMonthly.map(d => d.detainers)) * 0.95}], {
      x: "date", y: "y", text: d => "287(g) →", dx: 8, textAnchor: "start",
      fontSize: 11, fontWeight: 700,
    }),
  ],
})
```

Both lines climb together after the 287(g) cutoff. Pre-287(g) (Sept 2023 – Jan 2025): detainers averaged ~85/month, arrests ~90/month. Post-287(g) (Feb 2025 onward): detainers averaged ~205/month, arrests ~198/month. Both roughly doubled.

### Which facilities produce the most detainers?

The top detainer-producing facilities map directly onto the agencies with the highest Hispanic stop growth.

```js
const topFacilities = iceFacilities.filter(d => d.total >= 40).sort((a, b) => b.total - a.total);
const facLong = topFacilities.flatMap(d => [
  {facility: d.facility, period: "Pre-287(g)", count: d.pre_287g, total: d.total},
  {facility: d.facility, period: "Post-287(g)", count: d.post_287g, total: d.total},
]);

Plot.plot({
  title: "Top Missouri ICE detainer facilities (≥40 detainers)",
  width, height: Math.max(300, topFacilities.length * 26),
  marginLeft: 250,
  x: {label: "Detainers (Sept 2023 – Oct 2025)", grid: true},
  y: {label: null, domain: topFacilities.map(d => d.facility)},
  color: {legend: true, domain: ["Pre-287(g)", "Post-287(g)"], range: ["#2a7b9b", "#c22d2d"]},
  marks: [
    Plot.barX(facLong, {
      y: "facility", x: "count", fill: "period",
      sort: {y: {value: "x", reduce: "sum", reverse: true}},
      tip: true,
    }),
    Plot.text(topFacilities, {
      y: "facility", x: "total",
      text: d => d.total,
      dx: 4, textAnchor: "start", fontSize: 10,
    }),
  ],
  style: {fontSize: 11},
})
```

Greene County Jail (237), Jasper County Jail (219), St. Charles County Jail (90), Taney County Sheriff (61), O'Fallon PD (59) — the same jurisdictions that show the sharpest Hispanic stop increases in the VSR data are also the biggest producers of ICE detainers. 1,178 of the 3,205 Missouri ICE arrests came from local jails (CAP Local Incarceration) — the pipeline from traffic stop to arrest to jail to ICE contact.

### The crossover: VSR agencies ↔ ICE facilities

Matching ICE detainer records to the corresponding VSR agencies. This table puts both datasets on the same row.

```js
Inputs.table(vsrIceCrossref.map(d => ({
  Agency: d.agency,
  "ICE detainers": d.detainers,
  "Hispanic stops 2014": d.hisp_stops_2014 ? d3.format(",")(d.hisp_stops_2014) : "—",
  "Hispanic stops 2024": d3.format(",")(d.hisp_stops_2024),
  "Stop Δ 2014→2024": d.change_pct ? d3.format("+,.0f")(d.change_pct) + "%" : "—",
  "Arrest ratio 2024": d.arrest_ratio_2024 ? d.arrest_ratio_2024.toFixed(1) + "×" : "—",
})), {sort: "ICE detainers", reverse: true})
```

The agencies producing the most ICE detainers are the same agencies where Hispanic traffic stops surged. O'Fallon PD: 59 detainers, Hispanic stops +251%. Taney County Sheriff: 61 detainers, +461%. Christian County Sheriff: 80 detainers, +514%. This is the traffic stop → jail → ICE pipeline with names attached.

**Key agencies with 287(g) task force agreements:**
- Missouri State Highway Patrol (signed March 2025)
- Pulaski County Sheriff's Office (July 2025)
- Phelps County Sheriff's Office (October 2025)
- St. Charles County (considering, March 2026)

**Documented incidents where traffic stops led to ICE transfers:**
- O'Fallon PD: traffic stop → former DACA recipient transferred to ICE, held in rural jail
- St. Ann PD: traffic-related detention → transfer after officer found outstanding warrants

Austin Kocher, a Syracuse University researcher studying 287(g), warned that the task force model "can exacerbate community concerns around racial profiling, can lead to unlawful arrests, and can generate backlash of lawsuits that can and have cost taxpayers in these jurisdictions a lot of money."

*Sources: [KBIA](https://www.kbia.org/missouri-news/2026-03-18/ice-researcher-warns-of-risks-for-local-police-signing-onto-new-agreements-with-agency), [STLPR](https://www.stlpr.org/government-politics-issues/2026-03-11/immigration-enforcement-researcher-287-agreements), [KCUR](https://www.kcur.org/news/2026-03-18/missouri-police-ice-deportation-immigration-enforcement)*

### The AG tried to stop publishing the disparity data

In May 2025, the Missouri NAACP sued Attorney General Andrew Bailey's office for removing the disparity index — the core measure of whether police are stopping people in proportion to their population — from the 2023 vehicle stops report. The AG called it of "limited analytical value."

The NAACP settled in December 2025. The AG's office agreed to restore the disparity index to the 2023 and 2024 reports.

The disparity index is essentially what the "Stops vs. local population" scatter plot in this notebook computes. The state's own attorney general tried to stop publishing it.

*Sources: [Missouri Independent](https://missouriindependent.com/2025/05/29/naacp-lawsuit-accuses-missouri-ag-of-illegally-withholding-info-on-police-vehicle-stops/), [ABC17](https://abc17news.com/news/missouri/2025/12/29/naacp-settles-lawsuit-with-missouri-attorney-general-over-vehicle-stops-report/)*

### The advocacy landscape

Empower Missouri and the Coalition for Fair Policing have tracked the VSR for 19 years. They are pushing for the Fourth Amendment Affirmation Act, which would move beyond data collection to consequences for officers who engage in racial profiling. Their analysis has consistently flagged that non-white drivers — particularly Black and Hispanic — are searched at rates far exceeding their white counterparts, and that the contraband hit rate for those searches is lower.

*Source: [Empower Missouri](https://empowermissouri.org/by-the-numbers-the-missouri-vehicle-stops-report/)*

### Reporting targets

Agencies to contact, cross-referenced across multiple disparity metrics:

```js
// Cross-reference: which agencies appear in the worst lists across multiple metrics?
const stopChangeMap = new Map(agencyChanges.map(d => [d.agency, d.pct_change]));
const gapMap = new Map(gapData.map(d => [d.Agency, d["Gap (pp)"]]));
const ratioMap = new Map(ratioRows.map(d => [d.agency, d.ratio]));
const commentMap = new Map(commentCheck.map(d => [d.Agency, d["Years commented"]]));

// Agencies with 100+ Hispanic stops
const targetAgencies = [...new Set([
  ...agencyChanges.filter(d => d.pct_change > 50).map(d => d.agency),
  ...ratioRows.filter(d => d.ratio > 2).map(d => d.agency),
  ...gapData.filter(d => d["Gap (pp)"] > 3).map(d => d.Agency),
])];

const targets = targetAgencies.map(agency => {
  const hispContra = agencyContraband.find(d => d.agency === agency && d.race === "Hispanic");
  return {
    Agency: agency,
    "Stop change": stopChangeMap.has(agency) ? d3.format("+,.0f")(stopChangeMap.get(agency)) + "%" : "—",
    "Arrest ratio": ratioMap.has(agency) ? ratioMap.get(agency).toFixed(1) + "×" : "—",
    "Pop gap (pp)": gapMap.has(agency) ? gapMap.get(agency).toFixed(1) : "—",
    "Hisp hit rate": hispContra ? hispContra.hit_rate + "%" : "—",
    "Commented": commentMap.get(agency) ?? "Unknown",
    "Has 287(g)": ["Missouri State Highway Patrol", "Pulaski County Sheriff's Dept", "Phelps County Sheriff's Dept", "Pettis County Sheriff's Dept", "Callaway County Sheriff's Dept"].includes(agency) ? "Yes" : "—",
  };
}).sort((a, b) => {
  const aScore = (parseFloat(a["Stop change"]) || 0) + (parseFloat(a["Arrest ratio"]) || 0) * 20;
  const bScore = (parseFloat(b["Stop change"]) || 0) + (parseFloat(b["Arrest ratio"]) || 0) * 20;
  return bScore - aScore;
});

Inputs.table(targets)
```

### Past reporting

Nearly all prior coverage of the Missouri VSR has focused on Black-white disparities. The Hispanic trend is essentially unreported.

- **[Decades of Data Suggest Racial Profiling is Getting Worse, Not Better](https://flatlandkc.org/stopped-profiling-the-police/decades-of-data-suggest-racial-profiling-is-getting-worse-not-better/)** — Flatland KC, 2020. The most comprehensive investigation. Analyzed 20 years and 30+ million stops. Found Black drivers stopped 59% above expected rate. Hispanic drivers mentioned only in passing (arrested at 9.9% of stops — actually higher than Black drivers' 9.1%). Key finding: two decades of data collection produced no meaningful policy reform.

- **[Report shows Black drivers in Missouri more likely to be ticketed, arrested](https://missouriindependent.com/2023/06/03/report-shows-black-drivers-in-missouri-ticketed-arrested-at-much-higher-numbers-than-whites/)** — Missouri Independent, 2023. Covered the 2022 VSR. Entirely Black-white framing. Noted Hispanic citation rate was 50.4% vs. 38.2% for white but didn't investigate further.

- **[Missouri report on traffic stops revives call for anti-discrimination policies](https://www.stlpr.org/government-politics-issues/2018-06-04/missouri-report-on-traffic-stops-revives-call-for-anti-discrimination-policies)** — STLPR, 2018. Black drivers stopped 85% above white rate. Hispanic mentioned only as "searched at higher rates than average." Coalition for Fair Policing pushed the 4th Amendment Affirmation Act.

- **[Missouri Police Stop Black Drivers At Higher Rates](https://www.kcur.org/news/2019-06-04/missouri-police-stop-black-drivers-at-higher-rates-than-white-drivers)** — KCUR, 2019. Annual VSR coverage. Black-focused.

- **[Traffic stop report shows racial disparities as officials downplay numbers](https://www.columbiamissourian.com/news/local/traffic-stop-report-shows-racial-disparities-as-officials-downplay-numbers/article_6018752a-65ac-11e8-b3c1-d319bcc3cee9.html)** — Columbia Missourian, 2018. Boone County angle. Officials dismissed the data.

- **[NAACP lawsuit accuses Missouri AG of illegally withholding info on police vehicle stops](https://missouriindependent.com/2025/05/29/naacp-lawsuit-accuses-missouri-ag-of-illegally-withholding-info-on-police-vehicle-stops/)** — Missouri Independent, 2025. The AG removed the disparity index from the 2023 report. NAACP sued. Settled Dec 2025 — AG agreed to restore it.

- **[Missouri AG criticized for removing racial 'disparity index' from vehicle stops report](https://www.stltoday.com/news/local/government-politics/missouri-ag-criticized-for-removing-racial-disparity-index-from-vehicle-stops-report/article_0f8c58f6-5bdc-11ef-ba59-671894889699.html)** — St. Louis Post-Dispatch. The AG called the disparity index of "limited analytical value."

### Sources to pursue

**Agencies with the worst composite scores across multiple metrics** (11-year view, 2014 → 2024):
- **St. Ann PD** — 60 Hispanic stops in 2014 → 275 in 2024 (+358%). Search rate 32.7%, arrest rate 29.5%, 2.3× arrest ratio. Never commented.
- **St. Charles PD** — 212 → 358 (+69%), but the real climb is post-2020 (+426% from 2020). Arrest rate 22.9% vs 6.7% White (3.4×). Never commented.
- **Taney County Sheriff** — Branson tourism economy. 23 → 129 (+461%). 4.5× arrest ratio.
- **Perry County Sheriff** — 15 → 78 (+420%). Arrests and searches both 16.7% of Hispanic stops. 5.9× arrest ratio.
- **Crawford County Sheriff** — 5 → 106 (+2020%, tiny 2014 base). 4.4× arrest ratio.
- **Jefferson County Sheriff** — 31 → 107 (+245%). 2.7× arrest ratio.
- **St. Peters PD, Lake St. Louis PD** — same St. Charles County corridor as St. Charles PD. Possible regional pattern. None ever commented.

**Researchers and advocates:**
- **Austin Kocher**, Syracuse University — 287(g) researcher, already on the record in Missouri (KBIA, STLPR, KCUR). Can contextualize the traffic stop → immigration enforcement pipeline.
- **Empower Missouri / Coalition for Fair Policing** — 19 years of VSR advocacy, focused on Black disparities. Have not flagged the Hispanic trend.
- **ACLU of Missouri** — filed 287(g) challenges, can speak to the legal framework.
- **Missouri NAACP** — just settled the disparity index lawsuit with the AG. They know the data and the politics.

**Community sources:**
- **Hispanic community organizations** in Jackson County (KC metro), Greene County (Springfield), Jasper County (Joplin), St. Charles County — the counties with the fastest Hispanic population growth and agency-level disparities. Ask what drivers are experiencing.
- **Defense attorneys** in high-disparity jurisdictions — they see the probable cause affidavits. They know whether "equipment violation" means a real broken taillight or pretext.

**Government sources:**
- **MSHP public affairs** — 312,976 stops, never submitted a comment to the AG, signed the 287(g). Ask what changed.
- **The AG's office** — why did agency comment responses drop from 500+ to 18 in 2024? Is that a reporting change or did agencies just stop responding?
- **Individual agency chiefs/sheriffs** on the reporting targets list — the data is the opening question: "Your Hispanic stops rose X% while your contraband hit rate fell. Can you explain what changed?"

**Records to request:**
- MSHP 287(g) implementation documents — when it went operational, training materials, number of ICE referrals from traffic stops by month
- Agency policies on license stops and ID verification procedures — to understand whether the doubling of Hispanic license stops reflects a policy change, a training shift, or individual officer behavior

---

Data: Missouri Attorney General, Vehicle Stops Report, 2014–2024. Population: U.S. Census Bureau, American Community Survey (2014–2019 and 2021–2024: 1-year estimates; 2020: 5-year estimate). "White" and "Black" refer to non-Hispanic populations.
