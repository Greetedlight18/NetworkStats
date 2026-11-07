/**
 * NetworkStats — Real-Time PingPlotter-Style WebUI Controller
 */

const HOP_PALETTE = [
  '#22d3ee', // cyan-400
  '#a855f7', // purple-500
  '#38bdf8', // sky-400
  '#f59e0b', // amber-500
  '#10b981', // emerald-500
  '#ec4899', // pink-500
  '#6366f1', // indigo-500
  '#84cc16', // lime-500
  '#14b8a6', // teal-500
  '#f97316', // orange-500
];

const state = {
  targets: [],
  selectedTargetId: null,
  hopsByTarget: {},
  plottedHopsByTarget: {}, // targetId -> Set of hop_num
  historySeries: {},       // hop_num -> Array of { ts, ms, loss }
  windowSeconds: 300,
  ws: null,
  wsPingTimer: null,
  chart: null,
};

// DOM Elements
const elTargetList = document.getElementById('targetListContainer');
const elTargetCount = document.getElementById('targetCountBadge');
const elHopTableBody = document.getElementById('hopTableBody');
const elActiveBadge = document.getElementById('activeTargetBadge');
const elActiveLabel = document.getElementById('activeTargetLabel');
const elActiveHost = document.getElementById('activeTargetHost');
const elActiveInterval = document.getElementById('activeTargetInterval');
const elHeaderPing = document.getElementById('headerPingVal');
const elHeaderJitter = document.getElementById('headerJitterVal');
const elHeaderLoss = document.getElementById('headerLossVal');
const elBtnTogglePause = document.getElementById('btnTogglePause');
const elBtnTogglePauseText = document.getElementById('btnTogglePauseText');
const elBtnResetStats = document.getElementById('btnResetStats');
const elBtnDeleteTarget = document.getElementById('btnDeleteTarget');
const elPlottedSummary = document.getElementById('plottedHopsSummary');
const elWsDot = document.getElementById('wsStatusDot');
const elWsText = document.getElementById('wsStatusText');

// Modal Elements
const elAddModal = document.getElementById('addTargetModal');
const elAddForm = document.getElementById('addTargetForm');
const elAddError = document.getElementById('addTargetError');

function colorForHop(hopNum, isFinal) {
  if (isFinal) return '#22d3ee';
  return HOP_PALETTE[(hopNum - 1) % HOP_PALETTE.length];
}

function getTargetStatus(target) {
  if (!target.enabled) {
    return { color: 'bg-slate-500', text: 'Paused', badgeClass: 'text-slate-400 bg-slate-800 border-slate-700' };
  }
  const loss = Math.max(Number(target.rolling_loss_pct || 0), Number(target.loss_1h_pct || 0));
  const ping = target.current_ms;
  const jitter = Number(target.jitter_ms || 0);

  if (loss >= 5.0 || (target.hop_count > 0 && ping === null && loss > 0)) {
    return { color: 'bg-rose-500', text: 'Packet Loss', badgeClass: 'text-rose-300 bg-rose-950/70 border-rose-800' };
  }
  if (loss >= 1.0 || (ping !== null && ping > 120) || jitter > 25) {
    return { color: 'bg-amber-400', text: 'Degraded', badgeClass: 'text-amber-300 bg-amber-950/70 border-amber-800' };
  }
  return { color: 'bg-emerald-400', text: 'Healthy', badgeClass: 'text-emerald-300 bg-emerald-950/70 border-emerald-800' };
}

function formatMs(val) {
  if (val === null || val === undefined || Number.isNaN(Number(val))) return '---';
  return `${Number(val).toFixed(1)} ms`;
}

function formatTimeLabel(unixSec, windowSec) {
  const d = new Date(unixSec * 1000);
  if (windowSec > 21600) {
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  }
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

// ============================================================================
// Chart.js Real-Time Timeline Initialization & Updates
// ============================================================================
function initTimelineChart() {
  const ctx = document.getElementById('timelineChart').getContext('2d');
  state.chart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: [],
      datasets: [],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: {
        mode: 'index',
        intersect: false,
      },
      plugins: {
        legend: {
          display: true,
          position: 'top',
          align: 'end',
          labels: {
            color: '#cbd5e1',
            boxWidth: 12,
            boxHeight: 12,
            font: { family: 'ui-monospace, monospace', size: 11 },
          },
        },
        tooltip: {
          backgroundColor: 'rgba(15, 23, 42, 0.95)',
          titleColor: '#f8fafc',
          bodyColor: '#e2e8f0',
          borderColor: '#334155',
          borderWidth: 1,
          titleFont: { family: 'ui-monospace, monospace', size: 11 },
          bodyFont: { family: 'ui-monospace, monospace', size: 11 },
        },
        zoom: {
          pan: {
            enabled: true,
            mode: 'x',
          },
          zoom: {
            wheel: { enabled: true },
            pinch: { enabled: true },
            mode: 'x',
          },
        },
      },
      scales: {
        x: {
          grid: { color: 'rgba(51, 65, 85, 0.25)' },
          ticks: {
            color: '#94a3b8',
            maxRotation: 0,
            autoSkip: true,
            maxTicksLimit: 10,
            font: { family: 'ui-monospace, monospace', size: 10 },
          },
        },
        yLatency: {
          type: 'linear',
          position: 'left',
          beginAtZero: true,
          suggestedMax: 50,
          grid: { color: 'rgba(51, 65, 85, 0.35)' },
          title: {
            display: true,
            text: 'Latency (ms)',
            color: '#94a3b8',
            font: { family: 'ui-monospace, monospace', size: 11 },
          },
          ticks: {
            color: '#cbd5e1',
            font: { family: 'ui-monospace, monospace', size: 10 },
          },
        },
        yLoss: {
          type: 'linear',
          position: 'right',
          min: 0,
          max: 100,
          grid: { drawOnChartArea: false },
          title: {
            display: true,
            text: 'Packet Loss (%)',
            color: '#fb7185',
            font: { family: 'ui-monospace, monospace', size: 11 },
          },
          ticks: {
            color: '#fb7185',
            callback: (v) => `${v}%`,
            font: { family: 'ui-monospace, monospace', size: 10 },
          },
        },
      },
    },
  });
}

function getPlottedSet(targetId, hops) {
  if (!state.plottedHopsByTarget[targetId]) {
    state.plottedHopsByTarget[targetId] = new Set();
  }
  const set = state.plottedHopsByTarget[targetId];
  if (set.size === 0 && hops && hops.length > 0) {
    set.add(hops[hops.length - 1].hop_num);
  }
  return set;
}

function renderTimelineChart() {
  if (!state.chart || !state.selectedTargetId) return;

  const hops = state.hopsByTarget[state.selectedTargetId] || [];
  const finalHopNum = hops.length > 0 ? hops[hops.length - 1].hop_num : null;
  const plottedSet = getPlottedSet(state.selectedTargetId, hops);
  const plottedHops = Array.from(plottedSet).sort((a, b) => a - b);

  if (plottedHops.length === 0) {
    elPlottedSummary.textContent = '(No hops selected)';
  } else {
    elPlottedSummary.textContent = `(Plotting Hop ${plottedHops.map((h) => `#${h}`).join(', ')})`;
  }

  // Collect unified sorted timestamps across plotted hops
  const tsSet = new Set();
  for (const hNum of plottedHops) {
    const arr = state.historySeries[String(hNum)] || [];
    for (const pt of arr) {
      tsSet.add(pt.ts);
    }
  }
  const sortedTs = Array.from(tsSet).sort((a, b) => a - b);
  const labels = sortedTs.map((ts) => formatTimeLabel(ts, state.windowSeconds));

  const datasets = [];
  const primaryHop = plottedHops.includes(finalHopNum)
    ? finalHopNum
    : plottedHops[plottedHops.length - 1];

  // Add Latency Line Dataset for each toggled hop
  for (const hNum of plottedHops) {
    const hopMeta = hops.find((h) => h.hop_num === hNum);
    const hopLabel = hopMeta
      ? `Hop #${hNum} (${hopMeta.ip_address})`
      : `Hop #${hNum}`;
    const color = colorForHop(hNum, hNum === finalHopNum);

    const ptMap = new Map();
    for (const pt of state.historySeries[String(hNum)] || []) {
      ptMap.set(pt.ts, pt);
    }

    const latencyData = sortedTs.map((ts) => {
      const item = ptMap.get(ts);
      return item && item.ms !== null ? item.ms : null;
    });

    datasets.push({
      type: 'line',
      label: `${hopLabel} Latency`,
      data: latencyData,
      borderColor: color,
      backgroundColor: hNum === primaryHop ? 'rgba(34, 211, 238, 0.12)' : 'transparent',
      fill: hNum === primaryHop,
      borderWidth: hNum === primaryHop ? 2 : 1.5,
      pointRadius: 0,
      pointHoverRadius: 4,
      spanGaps: true,
      tension: 0.2,
      yAxisID: 'yLatency',
      order: 2,
    });
  }

  // Add Packet Loss Vertical Red Spikes/Bars for the primary plotted hop
  if (primaryHop !== undefined) {
    const ptMap = new Map();
    for (const pt of state.historySeries[String(primaryHop)] || []) {
      ptMap.set(pt.ts, pt);
    }
    const lossData = sortedTs.map((ts) => {
      const item = ptMap.get(ts);
      return item && item.loss > 0 ? item.loss : 0;
    });

    datasets.push({
      type: 'bar',
      label: `Hop #${primaryHop} Packet Loss %`,
      data: lossData,
      backgroundColor: 'rgba(244, 63, 94, 0.75)',
      borderColor: '#f43f5e',
      borderWidth: 1,
      barPercentage: 0.9,
      categoryPercentage: 0.95,
      yAxisID: 'yLoss',
      order: 1,
    });
  }

  state.chart.data.labels = labels;
  state.chart.data.datasets = datasets;
  state.chart.update('none');
}

// ============================================================================
// Sidebar & Hop Table Rendering
// ============================================================================
function renderTargetList() {
  elTargetCount.textContent = String(state.targets.length);
  elTargetList.innerHTML = '';

  for (const t of state.targets) {
    const isSelected = t.id === state.selectedTargetId;
    const status = getTargetStatus(t);
    const pingText = t.current_ms !== null && t.current_ms !== undefined
      ? `${Number(t.current_ms).toFixed(1)} ms`
      : '---';
    const loss1h = Number(t.loss_1h_pct || 0).toFixed(1);

    const item = document.createElement('div');
    item.className = `p-3 cursor-pointer transition border-l-4 ${
      isSelected
        ? 'bg-slate-850 border-cyan-400'
        : 'border-transparent hover:bg-slate-900/80'
    }`;
    item.onclick = () => selectTarget(t.id);

    item.innerHTML = `
      <div class="flex items-center justify-between mb-1">
        <div class="flex items-center space-x-2 min-w-0">
          <span class="w-2.5 h-2.5 rounded-full ${status.color} shrink-0"></span>
          <span class="font-semibold text-xs text-white truncate">${escapeHtml(t.label)}</span>
        </div>
        <span class="text-[10px] font-mono px-1.5 py-0.5 rounded border ${status.badgeClass}">
          ${status.text}
        </span>
      </div>
      <div class="flex items-center justify-between text-[11px] font-mono text-slate-400 mt-1.5">
        <span class="text-cyan-400/90 truncate">${escapeHtml(t.host)}</span>
        <div class="flex items-center space-x-2 shrink-0">
          <span class="text-slate-200">${pingText}</span>
          <span class="${Number(loss1h) >= 1 ? 'text-rose-400 font-semibold' : 'text-slate-400'}">${loss1h}% (1h)</span>
        </div>
      </div>
    `;
    elTargetList.appendChild(item);
  }

  updateSelectedTargetHeader();
}

function updateSelectedTargetHeader() {
  const target = state.targets.find((t) => t.id === state.selectedTargetId);
  if (!target) {
    elActiveLabel.textContent = 'No target selected';
    elActiveHost.textContent = '';
    elHeaderPing.textContent = '--';
    elHeaderJitter.textContent = '--';
    elHeaderLoss.textContent = '--';
    return;
  }

  const status = getTargetStatus(target);
  elActiveBadge.className = `w-2.5 h-2.5 rounded-full ${status.color} shrink-0`;
  elActiveLabel.textContent = target.label;
  elActiveHost.textContent = `(${target.host})`;
  elActiveInterval.textContent = `${Number(target.interval_sec).toFixed(1)}s interval`;

  elHeaderPing.textContent = formatMs(target.current_ms);
  elHeaderJitter.textContent = formatMs(target.jitter_ms);
  elHeaderLoss.textContent = `${Number(target.loss_1h_pct || 0).toFixed(1)}%`;

  elBtnTogglePauseText.textContent = target.enabled ? 'Pause' : 'Resume';
}

function buildSparklineSvg(sparkline, scaleMaxMs) {
  if (!sparkline || sparkline.length === 0) {
    return `<div class="text-[10px] text-slate-600">Waiting for probes...</div>`;
  }
  const width = 120;
  const height = 22;
  const maxSamples = 30;
  const step = width / maxSamples;
  const points = [];
  const lossBars = [];

  sparkline.slice(-maxSamples).forEach((val, idx) => {
    const x = Math.round(idx * step + step / 2);
    if (val === null || val === undefined) {
      lossBars.push(
        `<rect x="${x - 1.5}" y="2" width="3" height="${height - 4}" fill="#f43f5e" opacity="0.85" rx="1" />`
      );
    } else {
      const clamped = Math.min(1, Math.max(0.06, Number(val) / Math.max(10, scaleMaxMs)));
      const y = Math.round(height - 3 - clamped * (height - 6));
      points.push(`${x},${y}`);
    }
  });

  const polyline = points.length >= 2
    ? `<polyline fill="none" stroke="#22d3ee" stroke-width="1.5" points="${points.join(' ')}" />`
    : '';

  return `
    <svg width="${width}" height="${height}" class="overflow-visible shrink-0 bg-slate-900/90 rounded border border-slate-800">
      ${lossBars.join('')}
      ${polyline}
    </svg>
  `;
}

function buildLatencyRangeBar(hop, scaleMaxMs) {
  if (hop.min_ms === null || hop.max_ms === null) {
    return `<div class="flex-1 h-2.5 bg-slate-900 rounded border border-slate-800/80"></div>`;
  }
  const ceil = Math.max(20, scaleMaxMs);
  const minPct = Math.min(96, Math.max(0, (hop.min_ms / ceil) * 100));
  const maxPct = Math.min(100, Math.max(minPct + 2, (hop.max_ms / ceil) * 100));
  const avgPct = hop.avg_ms !== null ? Math.min(98, Math.max(0, (hop.avg_ms / ceil) * 100)) : minPct;
  const lastPct = hop.last_ms !== null ? Math.min(98, Math.max(0, (hop.last_ms / ceil) * 100)) : null;

  let barColor = 'bg-emerald-500/40 border-emerald-500/70';
  if (hop.rolling_loss_pct >= 5) {
    barColor = 'bg-rose-500/40 border-rose-500/70';
  } else if (hop.avg_ms > 100 || hop.jitter_ms > 20) {
    barColor = 'bg-amber-500/40 border-amber-500/70';
  }

  return `
    <div class="flex-1 h-3 bg-slate-900 rounded relative overflow-hidden border border-slate-800" title="Min: ${formatMs(hop.min_ms)} | Avg: ${formatMs(hop.avg_ms)} | Max: ${formatMs(hop.max_ms)}">
      <div class="absolute top-0.5 bottom-0.5 rounded border ${barColor}"
           style="left: ${minPct}%; width: ${Math.max(3, maxPct - minPct)}%;"></div>
      <div class="absolute top-0 bottom-0 w-0.5 bg-amber-300" style="left: ${avgPct}%;"></div>
      ${
        lastPct !== null
          ? `<div class="absolute top-0.5 w-2 h-2 rounded-full bg-cyan-300 shadow" style="left: calc(${lastPct}% - 4px);"></div>`
          : ''
      }
    </div>
  `;
}

function renderHopTable() {
  const targetId = state.selectedTargetId;
  const hops = (targetId && state.hopsByTarget[targetId]) ? state.hopsByTarget[targetId] : [];
  elHopTableBody.innerHTML = '';

  if (hops.length === 0) {
    elHopTableBody.innerHTML = `
      <tr>
        <td colspan="11" class="py-10 text-center text-slate-400 font-sans text-xs">
          Probing route hops via MTR Engine... results will appear momentarily.
        </td>
      </tr>
    `;
    return;
  }

  const plottedSet = getPlottedSet(targetId, hops);
  const finalHopNum = hops[hops.length - 1].hop_num;
  const scaleMaxMs = Math.max(
    25,
    ...hops.map((h) => Number(h.max_ms || 0))
  );

  for (const h of hops) {
    const isFinal = h.hop_num === finalHopNum;
    const isPlotted = plottedSet.has(h.hop_num);
    const hopColor = colorForHop(h.hop_num, isFinal);
    const lossVal = Number(h.rolling_loss_pct || 0);

    let lossBadgeClass = 'text-emerald-400 bg-emerald-950/50 border-emerald-800/60';
    if (lossVal >= 5 && h.ip_address !== '???') {
      lossBadgeClass = 'text-rose-300 bg-rose-950/80 border-rose-700 font-bold';
    } else if (lossVal > 0 && h.ip_address !== '???') {
      lossBadgeClass = 'text-amber-300 bg-amber-950/70 border-amber-700';
    } else if (h.ip_address === '???') {
      lossBadgeClass = 'text-slate-500 bg-slate-900 border-slate-800';
    }

    const rdnsDisplay =
      h.rdns_hostname && h.rdns_hostname !== h.ip_address && h.rdns_hostname !== '???'
        ? `<span class="text-slate-200 font-medium">${escapeHtml(h.rdns_hostname)}</span> <span class="text-slate-400">(${escapeHtml(h.ip_address)})</span>`
        : `<span class="${h.ip_address === '???' ? 'text-slate-500 italic' : 'text-slate-200'}">${
            h.ip_address === '???' ? 'No Response (ICMP Timeout)' : escapeHtml(h.ip_address)
          }</span>`;

    const tr = document.createElement('tr');
    tr.className = `hover:bg-slate-900/90 transition ${
      isFinal ? 'bg-cyan-950/15' : ''
    }`;

    tr.innerHTML = `
      <td class="py-1.5 px-3 text-center">
        <input type="checkbox" ${isPlotted ? 'checked' : ''}
               style="accent-color: ${hopColor}"
               class="w-3.5 h-3.5 rounded cursor-pointer"
               data-hop="${h.hop_num}" />
      </td>
      <td class="py-1.5 px-2 font-semibold ${isFinal ? 'text-cyan-400' : 'text-slate-400'}">
        #${h.hop_num}
      </td>
      <td class="py-1.5 px-3 truncate max-w-xs">
        ${rdnsDisplay}
      </td>
      <td class="py-1.5 px-2 text-right">
        <span class="px-1.5 py-0.5 rounded border text-[11px] ${lossBadgeClass}">
          ${h.ip_address === '???' ? '100%' : `${lossVal.toFixed(1)}%`}
        </span>
      </td>
      <td class="py-1.5 px-2 text-right text-slate-300">${h.sent}</td>
      <td class="py-1.5 px-2 text-right font-semibold ${
        h.last_ms !== null ? 'text-white' : 'text-rose-400'
      }">${h.last_ms !== null ? h.last_ms.toFixed(1) : 'TIMEOUT'}</td>
      <td class="py-1.5 px-2 text-right text-slate-300">${
        h.avg_ms !== null ? h.avg_ms.toFixed(1) : '---'
      }</td>
      <td class="py-1.5 px-2 text-right text-emerald-400/90">${
        h.min_ms !== null ? h.min_ms.toFixed(1) : '---'
      }</td>
      <td class="py-1.5 px-2 text-right text-amber-400/90">${
        h.max_ms !== null ? h.max_ms.toFixed(1) : '---'
      }</td>
      <td class="py-1.5 px-2 text-right ${
        h.jitter_ms > 20 ? 'text-amber-300 font-semibold' : 'text-slate-300'
      }">${h.jitter_ms !== null ? h.jitter_ms.toFixed(1) : '0.0'}</td>
      <td class="py-1.5 px-4">
        <div class="flex items-center space-x-2">
          ${buildLatencyRangeBar(h, scaleMaxMs)}
          ${buildSparklineSvg(h.sparkline, scaleMaxMs)}
        </div>
      </td>
    `;

    const checkbox = tr.querySelector('input[type="checkbox"]');
    checkbox.addEventListener('change', (e) => {
      const hopNum = Number(e.target.getAttribute('data-hop'));
      const set = getPlottedSet(targetId, hops);
      if (e.target.checked) {
        set.add(hopNum);
      } else {
        set.delete(hopNum);
      }
      renderTimelineChart();
    });

    elHopTableBody.appendChild(tr);
  }
}

// ============================================================================
// API Interactions & Target Selection
// ============================================================================
async function selectTarget(targetId) {
  state.selectedTargetId = targetId;
  renderTargetList();

  try {
    const [hopsRes, histRes] = await Promise.all([
      fetch(`/api/targets/${targetId}/hops`),
      fetch(`/api/targets/${targetId}/history?window=${state.windowSeconds}`),
    ]);
    if (hopsRes.ok) {
      const hopsData = await hopsRes.json();
      state.hopsByTarget[targetId] = hopsData.hops || [];
    }
    if (histRes.ok) {
      const histData = await histRes.json();
      state.historySeries = histData.series || {};
    }
  } catch (err) {
    console.error('Failed loading target details:', err);
  }

  renderHopTable();
  renderTimelineChart();
}

async function loadHistoryForCurrentTarget() {
  if (!state.selectedTargetId) return;
  try {
    const res = await fetch(
      `/api/targets/${state.selectedTargetId}/history?window=${state.windowSeconds}`
    );
    if (res.ok) {
      const data = await res.json();
      state.historySeries = data.series || {};
      renderTimelineChart();
    }
  } catch (err) {
    console.error('Error loading history window:', err);
  }
}

// ============================================================================
// WebSocket Client with Auto-Reconnect
// ============================================================================
function connectWebSocket() {
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = `${proto}//${window.location.host}/ws`;
  const ws = new WebSocket(wsUrl);
  state.ws = ws;

  ws.onopen = () => {
    elWsDot.className = 'w-2 h-2 rounded-full bg-emerald-400';
    elWsText.textContent = 'Live Connected';
    if (state.wsPingTimer) clearInterval(state.wsPingTimer);
    state.wsPingTimer = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send('ping');
      }
    }, 15000);
  };

  ws.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      handleWsMessage(msg);
    } catch (err) {
      console.error('WS parse error:', err);
    }
  };

  ws.onclose = () => {
    elWsDot.className = 'w-2 h-2 rounded-full bg-rose-500 animate-ping';
    elWsText.textContent = 'Reconnecting...';
    if (state.wsPingTimer) clearInterval(state.wsPingTimer);
    setTimeout(connectWebSocket, 2500);
  };
}

function handleWsMessage(msg) {
  if (msg.type === 'init' || msg.type === 'targets_changed') {
    state.targets = msg.targets || [];
    if (
      state.targets.length > 0 &&
      (!state.selectedTargetId || !state.targets.some((t) => t.id === state.selectedTargetId))
    ) {
      selectTarget(state.targets[0].id);
    } else {
      renderTargetList();
    }
    return;
  }

  if (msg.type === 'probe_update') {
    const tId = msg.target_id;
    state.hopsByTarget[tId] = msg.hops || [];

    // Update target summary in sidebar list
    const targetObj = state.targets.find((t) => t.id === tId);
    if (targetObj && msg.summary) {
      targetObj.current_ms = msg.summary.current_ms;
      targetObj.avg_ms = msg.summary.avg_ms;
      targetObj.jitter_ms = msg.summary.jitter_ms;
      targetObj.rolling_loss_pct = msg.summary.rolling_loss_pct;
      targetObj.hop_count = msg.summary.hop_count;
    }
    renderTargetList();

    // If this probe update is for the active target, update Hop Table and live Chart
    if (tId === state.selectedTargetId) {
      const cutoffTs = msg.timestamp - state.windowSeconds;
      for (const h of msg.hops || []) {
        const key = String(h.hop_num);
        if (!state.historySeries[key]) {
          state.historySeries[key] = [];
        }
        const seriesArr = state.historySeries[key];
        const isLoss = h.last_ms === null && h.ip_address !== '???';
        seriesArr.push({
          ts: msg.timestamp,
          ms: h.last_ms,
          loss: isLoss || h.ip_address === '???' ? 100.0 : 0.0,
        });
        while (seriesArr.length > 0 && seriesArr[0].ts < cutoffTs) {
          seriesArr.shift();
        }
      }
      renderHopTable();
      renderTimelineChart();
    }
  }
}

// ============================================================================
// UI Event Listeners
// ============================================================================
function bindEvents() {
  // Time window buttons
  document.querySelectorAll('.time-window-btn').forEach((btn) => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.time-window-btn').forEach((b) => {
        b.className =
          'time-window-btn px-2 py-1 rounded text-xs font-mono bg-slate-800 text-slate-300 hover:bg-slate-700';
      });
      btn.className = 'time-window-btn px-2 py-1 rounded text-xs font-mono bg-cyan-600 text-white';
      state.windowSeconds = Number(btn.getAttribute('data-window'));
      loadHistoryForCurrentTarget();
    });
  });

  // Reset Zoom
  document.getElementById('btnResetZoom').addEventListener('click', () => {
    if (state.chart) state.chart.resetZoom();
  });

  // Pause / Resume Target
  elBtnTogglePause.addEventListener('click', async () => {
    const target = state.targets.find((t) => t.id === state.selectedTargetId);
    if (!target) return;
    await fetch(`/api/targets/${target.id}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ enabled: !target.enabled }),
    });
  });

  // Reset Target Statistics
  elBtnResetStats.addEventListener('click', async () => {
    if (!state.selectedTargetId) return;
    await fetch(`/api/targets/${state.selectedTargetId}/reset`, { method: 'POST' });
    state.hopsByTarget[state.selectedTargetId] = [];
    state.historySeries = {};
    renderHopTable();
    renderTimelineChart();
  });

  // Delete Target
  elBtnDeleteTarget.addEventListener('click', async () => {
    const target = state.targets.find((t) => t.id === state.selectedTargetId);
    if (!target) return;
    if (!confirm(`Delete target "${target.label} (${target.host})"?`)) return;
    await fetch(`/api/targets/${target.id}`, { method: 'DELETE' });
  });

  // Add Target Modal open/close
  const openModal = () => {
    elAddError.classList.add('hidden');
    elAddForm.reset();
    elAddModal.classList.remove('hidden');
    elAddModal.classList.add('flex');
    document.getElementById('inputTargetHost').focus();
  };
  const closeModal = () => {
    elAddModal.classList.add('hidden');
    elAddModal.classList.remove('flex');
  };

  document.getElementById('btnOpenAddModal').addEventListener('click', openModal);
  document.getElementById('btnCloseAddModal').addEventListener('click', closeModal);
  document.getElementById('btnCancelAddModal').addEventListener('click', closeModal);

  elAddForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    elAddError.classList.add('hidden');
    const host = document.getElementById('inputTargetHost').value.trim();
    const label = document.getElementById('inputTargetLabel').value.trim();
    const interval_sec = Number(document.getElementById('inputTargetInterval').value || 2.0);

    try {
      const res = await fetch('/api/targets', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ host, label, interval_sec }),
      });
      const data = await res.json();
      if (!res.ok) {
        elAddError.textContent = data.detail || 'Failed to add target.';
        elAddError.classList.remove('hidden');
        return;
      }
      closeModal();
      if (data.target && data.target.id) {
        selectTarget(data.target.id);
      }
    } catch (err) {
      elAddError.textContent = String(err);
      elAddError.classList.remove('hidden');
    }
  });
}

function escapeHtml(str) {
  return String(str ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// ============================================================================
// Bootstrap
// ============================================================================
window.addEventListener('DOMContentLoaded', () => {
  initTimelineChart();
  bindEvents();
  connectWebSocket();
});
