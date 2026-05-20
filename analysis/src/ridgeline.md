---
title: Ridgeline — Hispanic Enforcement
theme: light
---

# Eleven years, five metrics

Each ridge is one Hispanic enforcement rate over time. License stops climb to a new peak at the end; everything else is lower than where it started.

**A note before reading:** this visualization shows Hispanic rates in isolation. The same pattern — license stops rising, everything else falling — also holds for white and Black drivers over the same 11 years. The rate shift isn't Hispanic-specific; it's a statewide composition change. What *is* unique to Hispanic drivers is that their absolute stop volume rose 52% while every other group's fell 25–28%, so the count of Hispanic license stops doubled while the white license-stop count barely grew and Black license stops fell. See [the main story](./) for the comparative view and absolute-count breakdown.

```js
const dotLineData = await FileAttachment("data/hispanic_dot_line.csv").csv({typed: true});
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
const size = width;
const h = 560;
const marginLeft = 170;
const marginRight = 100;
const marginTop = 40;
const marginBottom = 40;
const plotWidth = size - marginLeft - marginRight;
const plotHeight = h - marginTop - marginBottom;

const years = [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024];
const xScale = d3.scaleLinear().domain([2014, 2024]).range([0, plotWidth]);

const bandHeight = plotHeight / metrics.length;
const ridgeHeight = bandHeight * 1.4;

function getNorm(metric, rate) {
  const vals = dotLineData.filter(d => d.Metric === metric).map(v => v.Rate);
  const min = Math.min(...vals);
  const max = Math.max(...vals);
  return max > min ? (rate - min) / (max - min) : 0.5;
}

const svg = d3.create("svg")
  .attr("viewBox", `0 0 ${size} ${h}`)
  .attr("width", size)
  .attr("height", h)
  .style("font-family", "sans-serif");

const g = svg.append("g").attr("transform", `translate(${marginLeft},${marginTop})`);

g.append("g")
  .attr("transform", `translate(0,${plotHeight})`)
  .call(d3.axisBottom(xScale).ticks(6).tickFormat(d3.format("d")))
  .call(g => g.select(".domain").remove())
  .call(g => g.selectAll(".tick line").attr("stroke", "#ddd"));

metrics.forEach((metric, i) => {
  const mData = dotLineData.filter(d => d.Metric === metric).sort((a, b) => a.Year - b.Year);
  
  const points = mData.map(d => ({
    x: xScale(d.Year),
    y: i * bandHeight + bandHeight - getNorm(metric, d.Rate) * ridgeHeight,
    rate: d.Rate,
    year: d.Year,
  }));
  
  const baselineY = i * bandHeight + bandHeight;
  
  const area = d3.area()
    .x(d => d.x)
    .y0(baselineY)
    .y1(d => d.y)
    .curve(d3.curveCatmullRom);
  
  g.append("path")
    .attr("d", area(points))
    .attr("fill", metricColors[metric])
    .attr("fill-opacity", 0.35);
  
  const line = d3.line().x(d => d.x).y(d => d.y).curve(d3.curveCatmullRom);
  
  g.append("path")
    .attr("d", line(points))
    .attr("fill", "none")
    .attr("stroke", metricColors[metric])
    .attr("stroke-width", 2.5);
  
  // Dots at every year
  points.forEach(p => {
    g.append("circle")
      .attr("cx", p.x).attr("cy", p.y).attr("r", 3)
      .attr("fill", metricColors[metric])
      .attr("fill-opacity", 0.9)
      .attr("stroke", "#fff").attr("stroke-width", 1);
  });
  
  // Metric label on left
  g.append("text")
    .attr("x", -12)
    .attr("y", i * bandHeight + bandHeight * 0.5)
    .attr("text-anchor", "end")
    .attr("dominant-baseline", "middle")
    .attr("font-size", 12).attr("font-weight", 600)
    .attr("fill", metricColors[metric])
    .text(metric.replace(" Rate", "").replace(" Stop", ""));
  
  const first = points[0];
  const last = points[points.length - 1];
  
  // 2014 value (gray, left)
  g.append("text")
    .attr("x", first.x - 8).attr("y", first.y)
    .attr("text-anchor", "end").attr("dominant-baseline", "middle")
    .attr("font-size", 10).attr("fill", "#999")
    .text(`${first.rate}%`);
  
  // 2024 value (colored, right)
  g.append("text")
    .attr("x", last.x + 8).attr("y", last.y)
    .attr("text-anchor", "start").attr("dominant-baseline", "middle")
    .attr("font-size", 11).attr("font-weight", 600)
    .attr("fill", metricColors[metric])
    .text(`${last.rate}%`);
  
  // Direction arrow (up/down) next to the 2024 label
  const pctChange = ((last.rate - first.rate) / first.rate * 100);
  const arrow = pctChange >= 5 ? "↑" : pctChange <= -5 ? "↓" : "→";
  g.append("text")
    .attr("x", last.x + 50).attr("y", last.y)
    .attr("text-anchor", "start").attr("dominant-baseline", "middle")
    .attr("font-size", 14).attr("font-weight", 700)
    .attr("fill", pctChange >= 5 ? "#c22d2d" : pctChange <= -5 ? "#2a7b9b" : "#999")
    .text(`${arrow} ${pctChange >= 0 ? "+" : ""}${pctChange.toFixed(0)}%`);
});

display(svg.node());
```

**How to read this:**

Each ridge is normalized to its own 0–1 range, so the peak of each shape marks that metric's highest year in the 11-year window. The gray number on the left is the 2014 value; the colored number on the right is the 2024 value.

- **License stops** peak at the end — their 2024 value is their 11-year high.
- **Arrest rate** peaks at the start — its 2014 value was the highest in the window.
- **Search rate** declines monotonically — its shape tilts downward from left to right.
- **Investigative** and **equipment** rates show U-shapes — high early, dip through 2020, partial rebound.

Only one ridge is still rising, and that's true for Hispanic drivers alone and for white and Black drivers as well. The per-stop rate shift is a statewide composition change that happened to all three groups in the same direction. The Hispanic-unique finding lives in absolute counts, not in the ridge shapes.
