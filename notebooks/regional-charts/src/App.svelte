<script>
  import { onMount } from 'svelte';
  import * as d3 from 'd3';
  import SmallMultiple from './lib/SmallMultiple.svelte';

  let data = $state([]);
  let loaded = $state(false);
  let loadError = $state('');
  let selectedMetric = $state('Indexed');

  const races = ['Hispanic', 'White', 'Black'];
  const regions = ['STL Metro', 'KC Metro', 'Southwest', 'Central', 'Southeast', 'Northwest', 'Northeast'];
  const raceColors = { Hispanic: '#e8a820', White: '#888', Black: '#333' };

  onMount(async () => {
    try {
      const [hisp, white, black] = await Promise.all([
        d3.csv('/hispanic.csv', d3.autoType),
        d3.csv('/white.csv', d3.autoType),
        d3.csv('/black.csv', d3.autoType),
      ]);
      data = [
        ...hisp.map(r => ({ ...r, race: 'Hispanic' })),
        ...white.map(r => ({ ...r, race: 'White' })),
        ...black.map(r => ({ ...r, race: 'Black' })),
      ];
      loaded = true;
    } catch(e) {
      loadError = String(e);
    }
  });

  function getChartData(race, region) {
    const rows = data.filter(d => d.race === race && d.Region === region);
    const agencies = [...new Set(rows.map(d => d.Agency))];
    return { rows, agencies };
  }
</script>

<main>
  <h1>Missouri Traffic Stops by Agency, 2014-2024</h1>
  <p class="subtitle">Each line is one agency. Indexed: above 100 = more stops than 2014, below = fewer. Agencies with &lt;10 stops in 2014 excluded.</p>

  <div class="controls">
    <label>
      <input type="radio" bind:group={selectedMetric} value="Indexed" /> Indexed (2014 = 100)
    </label>
    <label>
      <input type="radio" bind:group={selectedMetric} value="Stops" /> Raw counts
    </label>
  </div>

  {#if loaded}
    <div class="header-row">
      <div class="region-label"></div>
      {#each races as race}
        <div class="race-header" style="color: {raceColors[race]}">{race}</div>
      {/each}
    </div>

    {#each regions as region}
      <div class="grid-row">
        <div class="region-label">{region}</div>
        {#each races as race}
          {@const chartData = getChartData(race, region)}
          <div class="cell">
            <SmallMultiple
              rows={chartData.rows}
              agencies={chartData.agencies}
              color={raceColors[race]}
              metric={selectedMetric}
              label={String(chartData.agencies.length)}
            />
          </div>
        {/each}
      </div>
    {/each}
  {:else if loadError}
    <p style="color:red">Error loading data: {loadError}</p>
  {:else}
    <p>Loading data...</p>
  {/if}
</main>

<style>
  :global(body) {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    margin: 0;
    padding: 20px;
    background: #fafafa;
    color: #222;
  }
  main { max-width: 1400px; margin: 0 auto; }
  h1 { font-size: 22px; margin-bottom: 4px; }
  .subtitle { color: #666; font-size: 13px; margin-bottom: 16px; }
  .controls { margin-bottom: 20px; display: flex; gap: 16px; font-size: 14px; }
  .controls label { cursor: pointer; }
  .header-row {
    display: grid;
    grid-template-columns: 110px repeat(3, 1fr);
    gap: 6px;
    margin-bottom: 2px;
  }
  .race-header { font-weight: 700; font-size: 15px; text-align: center; }
  .grid-row {
    display: grid;
    grid-template-columns: 110px repeat(3, 1fr);
    gap: 6px;
    margin-bottom: 4px;
  }
  .region-label {
    font-weight: 600;
    font-size: 12px;
    display: flex;
    align-items: center;
    color: #555;
  }
  .cell {
    background: white;
    border: 1px solid #eee;
    border-radius: 3px;
    padding: 6px 8px;
    min-height: 120px;
  }
</style>
