---
title: Spiral — Hispanic Enforcement
theme: light
---

# The spiral

Each arm is a metric. The radius represents the rate. As years progress outward from the center (2020) to the edge (2024), arms that grew extend further out. Arms that shrank pull inward.

```js
const dotLineData = await FileAttachment("data/hispanic_dot_line.csv").csv({typed: true});
```

```js
const metricColors = {
  "License Stop Rate": "#3a8a3f",
  "Arrest Rate": "#c22d2d",
  "Equipment Stop Rate": "#2a7b9b",
  "Investigative Stop Rate": "#7b3fa0",
  "Search Rate": "#e07b24",
};

const metrics = Object.keys(metricColors);
const years = [2020, 2021, 2022, 2023, 2024];

const size = Math.min(width, 600);
const cx = size / 2;
const cy = size / 2;
const maxRadius = size / 2 - 60;

// Each metric gets an angle slice
const angleStep = (2 * Math.PI) / metrics.length;

// Normalize rates to 0-1 across ALL metrics for radius mapping
const allRates = dotLineData.map(d => d.Rate);
const minRate = Math.min(...allRates);
const maxRate = Math.max(...allRates);

function rScale(rate) {
  return 30 + ((rate - minRate) / (maxRate - minRate)) * (maxRadius - 30);
}

// Build spiral points
const spiralPoints = [];
const spiralPaths = [];

metrics.forEach((metric, mi) => {
  const baseAngle = mi * angleStep - Math.PI / 2; // start at top
  const metricData = years.map(year => {
    const d = dotLineData.find(d => d.Metric === metric && +d.Year === year);
    const yearIndex = years.indexOf(year);
    // Slight angle offset per year to create spiral effect
    const angle = baseAngle + (yearIndex - 2) * 0.08;
    const r = rScale(d.Rate);
    return {
      metric, year, rate: d.Rate,
      x: cx + Math.cos(angle) * r,
      y: cy + Math.sin(angle) * r,
      r,
      angle,
    };
  });
  
  metricData.forEach(d => spiralPoints.push(d));
  spiralPaths.push({metric, points: metricData});
});
```

```js
// Draw with raw SVG for full control
const svg = d3.create("svg")
  .attr("viewBox", `0 0 ${size} ${size}`)
  .attr("width", size)
  .attr("height", size)
  .style("font-family", "sans-serif");

// Background rings for years
years.forEach((year, i) => {
  const r = 30 + (i / (years.length - 1)) * (maxRadius - 30);
  svg.append("circle")
    .attr("cx", cx).attr("cy", cy).attr("r", r)
    .attr("fill", "none").attr("stroke", "#eee").attr("stroke-width", 1);
  svg.append("text")
    .attr("x", cx + 4).attr("y", cy - r - 4)
    .attr("font-size", 10).attr("fill", "#bbb")
    .text(year);
});

// Axis lines (one per metric, from center outward)
metrics.forEach((metric, mi) => {
  const angle = mi * angleStep - Math.PI / 2;
  svg.append("line")
    .attr("x1", cx).attr("y1", cy)
    .attr("x2", cx + Math.cos(angle) * maxRadius)
    .attr("y2", cy + Math.sin(angle) * maxRadius)
    .attr("stroke", "#eee").attr("stroke-width", 1);
  
  // Label at the end
  const lx = cx + Math.cos(angle) * (maxRadius + 20);
  const ly = cy + Math.sin(angle) * (maxRadius + 20);
  svg.append("text")
    .attr("x", lx).attr("y", ly)
    .attr("text-anchor", "middle")
    .attr("dominant-baseline", "middle")
    .attr("font-size", 11).attr("font-weight", 600)
    .attr("fill", metricColors[metric])
    .text(metric.replace(" Rate", "").replace(" Stop", ""));
});

// Draw paths (lines connecting years for each metric)
spiralPaths.forEach(({metric, points}) => {
  const line = d3.line().x(d => d.x).y(d => d.y).curve(d3.curveCatmullRom);
  svg.append("path")
    .attr("d", line(points))
    .attr("fill", "none")
    .attr("stroke", metricColors[metric])
    .attr("stroke-width", 2.5)
    .attr("stroke-opacity", 0.6);
});

// Draw dots (sized by cumulative change from 2020)
spiralPoints.forEach(d => {
  const base = spiralPoints.find(b => b.metric === d.metric && b.year === 2020)?.rate;
  const pctChange = base ? Math.abs((d.rate - base) / base) * 100 : 0;
  const dotR = Math.max(3 + pctChange * 0.4, 3);
  
  svg.append("circle")
    .attr("cx", d.x).attr("cy", d.y).attr("r", dotR)
    .attr("fill", metricColors[d.metric])
    .attr("fill-opacity", 0.85)
    .attr("stroke", "#fff").attr("stroke-width", 1.5);
});

// Rate labels at 2024 positions
spiralPoints.filter(d => d.year === 2024).forEach(d => {
  svg.append("text")
    .attr("x", d.x).attr("y", d.y - 14)
    .attr("text-anchor", "middle")
    .attr("font-size", 10).attr("font-weight", 600)
    .attr("fill", metricColors[d.metric])
    .text(`${d.rate}%`);
});

// Center label
svg.append("text")
  .attr("x", cx).attr("y", cy)
  .attr("text-anchor", "middle")
  .attr("dominant-baseline", "middle")
  .attr("font-size", 12).attr("fill", "#999")
  .text("2020");

display(svg.node());
```
