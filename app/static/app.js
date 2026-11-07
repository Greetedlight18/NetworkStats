/**
 * NetworkStats — Clean Real-Time UI Controller
 */

const HOP_PALETTE = [
  '#22d3ee',
  '#a855f7',
  '#38bdf8',
  '#f59e0b',
  '#10b981',
  '#ec4899',
  '#6366f1',
  '#84cc16',
  '#14b8a6',
  '#f97316',
];

const state = {
  targets: [],
  selectedTargetId: null,
  hopsByTarget: {},
  plottedHopsByTarget: {},
  historySeries: {},
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
const elSelectInterval = document.getElementById('selectTargetInterval');
const elHeaderPing = document.getElementById('headerPingVal');
const elHeaderJitter = document.getElementById('headerJitterVal');
const elHeaderLoss = document.getElementById('headerLossVal');
const elBtnTogglePause = document.getElementById('btnTogglePause');
const elBtnTogglePauseText = document.getElementById('btnTogglePauseText');
const elBtnResetStats = document.getElementById('btnResetStats');
const elBtnDeleteTarget = document.getElementById('btnDeleteTarget');
const elPlottedSummary = document.getElementById('plottedHopsSummary');

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
    return { color: 'bg-slate-600' };
  }
  const loss = Math.max(Number(target.rolling_loss_pct || 0), Number(target.loss_1h_pct || 0));
  const ping = target.current_ms;
  const jitter = Number(target.jitter_ms || 0);

  if (loss >= 5.0 || (target.hop_count > 0 && ping === null && loss > 0)) {
    return { color: 'bg-rose-500' };
  }
  if (loss >= 1.0 || (ping !== null && ping > 120) || jitter > 25) {
    return { color: 'bg-amber-400' };
  }
  return { color: 'bg-emerald-400' };
}

function formatMs(val) {
  if (val === null || val === undefined || Number.isNaN(Number(val))) return '--';
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
// Chart.js Timeline
// ============================================================================
function initTimelineChart() {
  const ctx = document.getElementById('timelineChart').getContext('2d');
  state.chart = new Chart(ctx, {
    type: 'line',
    data: { labels: [], datasets: [] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: {
          display: true,
          position: 'top',
          align: 'end',
          labels: {
            color: '#94a3b8',
            boxWidth: 10,
            boxHeight: 10,
            font: { family: 'ui-monospace, monospace', size: 11 },
          },
        },
        tooltip: {
          backgroundColor: 'rgba(12, 18, 30, 0.95)',
          titleColor: '#f8fafc',
          bodyColor: '#cbd5e1',
          borderColor: '#1e293b',
          borderWidth: 1,
          titleFont: { family: 'ui-monospace, monospace', size: 11 },
          bodyFont: { family: 'ui-monospace, monospace', size: 11 },
        },
        zoom: {
          pan: { enabled: true, mode: 'x' },
          zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: 'x' },
        },
      },
      scales: {
        x: {
          grid: { color: 'rgba(30, 41, 59, 0.4)' },
          ticks: {
            color: '#64748b',
            maxRotation: 0,
            autoSkip: true,
            maxTicksLimit: 9,
            font: { family: 'ui-monospace, monospace', size: 10 },
          },
        },
        yLatency: {
          type: 'linear',
          position: 'left',
          beginAtZero: true,
          suggestedMax: 40,
          grid: { color: 'rgba(30, 41, 59, 0.5)' },
          ticks: {
            color: '#94a3b8',
            callback: (v) => `${v} ms`,
            font: { family: 'ui-monospace, monospace', size: 10 },
          },
        },
        yLoss: {
          type: 'linear',
          position: 'right',
          min: 0,
          max: 100,
          grid: { drawOnChartArea: false },
          ticks: {
            color: '#f43f5e',
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
  if (hops && hops.length > 0) {
    const validNums = new Set(hops.map((h) => h.hop_num));
    for (const hNum of Array.from(set)) {
      if (!validNums.has(hNum)) {
        set.delete(hNum);
      }
    }
    if (set.size === 0) {
      set.add(hops[hops.length - 1].hop_num);
    }
  }
  return set;
}

function renderTimelineChart() {
  if (!state.chart || !state.selectedTargetId) return;

  const hops = state.hopsByTarget[state.selectedTargetId] || [];
  const finalHopNum = hops.length > 0 ? hops[hops.length - 1].hop_num : null;
  const plottedSet = getPlottedSet(state.selectedTargetId, hops);
  const plottedHops = Array.from(plottedSet).sort((a, b) => a - b);

  elPlottedSummary.textContent =
    plottedHops.length > 0 ? `Hop ${plottedHops.map((h) => `#${h}`).join(', ')}` : '';

  const tsSet = new Set();
  for (const hNum of plottedHops) {
    for (const pt of state.historySeries[String(hNum)] || []) {
      tsSet.add(pt.ts);
    }
  }
  const sortedTs = Array.from(tsSet).sort((a, b) => a - b);
  const labels = sortedTs.map((ts) => formatTimeLabel(ts, state.windowSeconds));

  const datasets = [];
  const primaryHop = plottedHops.includes(finalHopNum)
    ? finalHopNum
    : plottedHops[plottedHops.length - 1];

  for (const hNum of plottedHops) {
    const hopMeta = hops.find((h) => h.hop_num === hNum);
    const hopLabel = hopMeta ? `#${hNum} ${hopMeta.ip_address}` : `#${hNum}`;
    const color = colorForHop(hNum, hNum === finalHopNum);

    const ptMap = new Map();
    for (const pt of state.historySeries[String(hNum)] || []) {
      ptMap.set(pt.ts, pt);
    }

    datasets.push({
      type: 'line',
      label: hopLabel,
      data: sortedTs.map((ts) => {
        const item = ptMap.get(ts);
        return item && item.ms !== null ? item.ms : null;
      }),
      borderColor: color,
      backgroundColor: hNum === primaryHop ? 'rgba(34, 211, 238, 0.08)' : 'transparent',
      fill: hNum === primaryHop,
      borderWidth: 1.75,
      pointRadius: 0,
      pointHoverRadius: 3,
      spanGaps: true,
      tension: 0.15,
      yAxisID: 'yLatency',
      order: 2,
    });
  }

  if (primaryHop !== undefined) {
    const ptMap = new Map();
    for (const pt of state.historySeries[String(primaryHop)] || []) {
      ptMap.set(pt.ts, pt);
    }
    datasets.push({
      type: 'bar',
      label: 'Loss %',
      data: sortedTs.map((ts) => {
        const item = ptMap.get(ts);
        return item && item.loss > 0 ? item.loss : 0;
      }),
      backgroundColor: 'rgba(244, 63, 94, 0.7)',
      borderWidth: 0,
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
// Sidebar & Hop Table
// ============================================================================
function renderTargetList() {
  elTargetCount.textContent = String(state.targets.length);
  elTargetList.innerHTML = '';

  for (const t of state.targets) {
    const isSelected = t.id === state.selectedTargetId;
    const status = getTargetStatus(t);
    const pingText =
      t.current_ms !== null && t.current_ms !== undefined
        ? `${Number(t.current_ms).toFixed(1)} ms`
        : '--';
    const loss1h = Number(t.loss_1h_pct || 0).toFixed(1);

    const item = document.createElement('div');
    item.className = `px-3 py-2.5 cursor-pointer transition border-l-2 ${
      isSelected
        ? 'bg-slate-850 border-cyan-400'
        : 'border-transparent hover:bg-slate-900/60'
    }`;
    item.onclick = () => selectTarget(t.id);

    item.innerHTML = `
      <div class="flex items-center justify-between">
        <div class="flex items-center space-x-2 min-w-0">
          <span class="w-2 h-2 rounded-full ${status.color} shrink-0"></span>
          <span class="font-medium text-xs text-slate-100 truncate">${escapeHtml(t.label)}</span>
        </div>
        <span class="text-xs font-mono text-slate-200">${pingText}</span>
      </div>
      <div class="flex items-center justify-between text-[11px] font-mono text-slate-500 mt-0.5 pl-4">
        <span class="truncate">${escapeHtml(t.host)}</span>
        <span class="${Number(loss1h) >= 1 ? 'text-rose-400' : 'text-slate-500'}">${loss1h}%</span>
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
  elActiveBadge.className = `w-2 h-2 rounded-full ${status.color} shrink-0`;
  elActiveLabel.textContent = target.label;
  elActiveHost.textContent = target.host !== target.label ? target.host : '';

  // Sync interval selector dropdown
  const intervalVal = String(Number(target.interval_sec));
  if ([...elSelectInterval.options].some((o) => o.value === intervalVal)) {
    elSelectInterval.value = intervalVal;
  } else {
    const opt = document.createElement('option');
    opt.value = intervalVal;
    opt.textContent = `${Number(target.interval_sec).toFixed(1)}s`;
    opt.className = 'bg-slate-900 text-slate-200';
    elSelectInterval.appendChild(opt);
    elSelectInterval.value = intervalVal;
  }

  elHeaderPing.textContent = formatMs(target.current_ms);
  elHeaderJitter.textContent = formatMs(target.jitter_ms);
  elHeaderLoss.textContent = `${Number(target.loss_1h_pct || 0).toFixed(1)}%`;
  elBtnTogglePauseText.textContent = target.enabled ? 'Pause' : 'Resume';
}

function buildSparklineSvg(sparkline, scaleMaxMs) {
  if (!sparkline || sparkline.length === 0) {
    return `<div class="text-[11px] text-slate-600">...</div>`;
  }
  const width = 110;
  const height = 20;
  const maxSamples = 30;
  const step = width / maxSamples;
  const points = [];
  const lossBars = [];

  sparkline.slice(-maxSamples).forEach((val, idx) => {
    const x = Math.round(idx * step + step / 2);
    if (val === null || val === undefined) {
      lossBars.push(
        `<rect x="${x - 1}" y="2" width="2.5" height="${height - 4}" fill="#f43f5e" opacity="0.85" />`
      );
    } else {
      const clamped = Math.min(1, Math.max(0.06, Number(val) / Math.max(10, scaleMaxMs)));
      const y = Math.round(height - 3 - clamped * (height - 6));
      points.push(`${x},${y}`);
    }
  });

  const polyline =
    points.length >= 2
      ? `<polyline fill="none" stroke="#22d3ee" stroke-width="1.4" points="${points.join(' ')}" />`
      : '';

  return `
    <svg width="${width}" height="${height}" class="overflow-visible shrink-0">
      ${lossBars.join('')}
      ${polyline}
    </svg>
  `;
}

function buildLatencyRangeBar(hop, scaleMaxMs) {
  if (hop.min_ms === null || hop.max_ms === null) {
    return `<div class="flex-1 h-2 bg-slate-900 rounded"></div>`;
  }
  const ceil = Math.max(20, scaleMaxMs);
  const minPct = Math.min(95, Math.max(0, (hop.min_ms / ceil) * 100));
  const maxPct = Math.min(100, Math.max(minPct + 2, (hop.max_ms / ceil) * 100));
  const lastPct =
    hop.last_ms !== null ? Math.min(98, Math.max(0, (hop.last_ms / ceil) * 100)) : null;

  return `
    <div class="flex-1 h-2 bg-slate-900 rounded relative overflow-hidden">
      <div class="absolute top-0 bottom-0 bg-slate-700 rounded"
           style="left: ${minPct}%; width: ${Math.max(2, maxPct - minPct)}%;"></div>
      ${
        lastPct !== null
          ? `<div class="absolute top-0 bottom-0 w-1.5 rounded-full bg-cyan-400" style="left: calc(${lastPct}% - 3px);"></div>`
          : ''
      }
    </div>
  `;
}

function renderHopTable() {
  const targetId = state.selectedTargetId;
  const hops = targetId && state.hopsByTarget[targetId] ? state.hopsByTarget[targetId] : [];
  elHopTableBody.innerHTML = '';

  if (hops.length === 0) {
    elHopTableBody.innerHTML = `
      <tr>
        <td colspan="11" class="py-8 text-center text-slate-500 font-sans text-xs">
          Tracing route hops...
        </td>
      </tr>
    `;
    return;
  }

  const plottedSet = getPlottedSet(targetId, hops);
  const finalHopNum = hops[hops.length - 1].hop_num;
  const scaleMaxMs = Math.max(25, ...hops.map((h) => Number(h.max_ms || 0)));

  for (const h of hops) {
    const isFinal = h.hop_num === finalHopNum;
    const isPlotted = plottedSet.has(h.hop_num);
    const hopColor = colorForHop(h.hop_num, isFinal);
    const lossVal = Number(h.rolling_loss_pct || 0);

    const rdnsDisplay =
      h.rdns_hostname && h.rdns_hostname !== h.ip_address && h.rdns_hostname !== '???'
        ? `<span class="text-slate-200">${escapeHtml(h.rdns_hostname)}</span> <span class="text-slate-500">(${escapeHtml(h.ip_address)})</span>`
        : `<span class="${h.ip_address === '???' ? 'text-slate-600' : 'text-slate-200'}">${
            h.ip_address === '???' ? '* * *' : escapeHtml(h.ip_address)
          }</span>`;

    const tr = document.createElement('tr');
    tr.className = `hover:bg-slate-900/60 transition ${isFinal ? 'bg-cyan-950/10' : ''}`;

    tr.innerHTML = `
      <td class="py-1.5 px-3 text-center">
        <input type="checkbox" ${isPlotted ? 'checked' : ''}
               style="accent-color: ${hopColor}"
               class="w-3.5 h-3.5 rounded cursor-pointer"
               data-hop="${h.hop_num}" />
      </td>
      <td class="py-1.5 px-2 ${isFinal ? 'text-cyan-400 font-semibold' : 'text-slate-500'}">
        ${h.hop_num}
      </td>
      <td class="py-1.5 px-3 truncate max-w-xs">${rdnsDisplay}</td>
      <td class="py-1.5 px-2 text-right ${
        lossVal >= 5 && h.ip_address !== '???'
          ? 'text-rose-400 font-semibold'
          : lossVal > 0 && h.ip_address !== '???'
          ? 'text-amber-400'
          : 'text-slate-400'
      }">
        ${h.ip_address === '???' ? '--' : `${lossVal.toFixed(1)}%`}
      </td>
      <td class="py-1.5 px-2 text-right text-slate-400">${h.sent}</td>
      <td class="py-1.5 px-2 text-right ${
        h.last_ms !== null ? 'text-slate-100' : 'text-slate-600'
      }">${h.last_ms !== null ? h.last_ms.toFixed(1) : '--'}</td>
      <td class="py-1.5 px-2 text-right text-slate-400">${
        h.avg_ms !== null ? h.avg_ms.toFixed(1) : '--'
      }</td>
      <td class="py-1.5 px-2 text-right text-slate-400">${
        h.min_ms !== null ? h.min_ms.toFixed(1) : '--'
      }</td>
      <td class="py-1.5 px-2 text-right text-slate-400">${
        h.max_ms !== null ? h.max_ms.toFixed(1) : '--'
      }</td>
      <td class="py-1.5 px-2 text-right text-slate-400">${
        h.jitter_ms !== null ? h.jitter_ms.toFixed(1) : '0.0'
      }</td>
      <td class="py-1.5 px-4">
        <div class="flex items-center space-x-3">
          ${buildLatencyRangeBar(h, scaleMaxMs)}
          ${buildSparklineSvg(h.sparkline, scaleMaxMs)}
        </div>
      </td>
    `;

    const checkbox = tr.querySelector('input[type="checkbox"]');
    checkbox.addEventListener('change', (e) => {
      const hopNum = Number(e.target.getAttribute('data-hop'));
      const set = getPlottedSet(targetId, hops);
      if (e.target.checked) set.add(hopNum);
      else set.delete(hopNum);
      renderTimelineChart();
    });

    elHopTableBody.appendChild(tr);
  }
}

// ============================================================================
// API & WebSocket
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

function connectWebSocket() {
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const ws = new WebSocket(`${proto}//${window.location.host}/ws`);
  state.ws = ws;

  ws.onopen = () => {
    if (state.wsPingTimer) clearInterval(state.wsPingTimer);
    state.wsPingTimer = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) ws.send('ping');
    }, 15000);
  };

  ws.onmessage = (event) => {
    try {
      handleWsMessage(JSON.parse(event.data));
    } catch (err) {
      console.error('WS parse error:', err);
    }
  };

  ws.onclose = () => {
    if (state.wsPingTimer) clearInterval(state.wsPingTimer);
    setTimeout(connectWebSocket, 2000);
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

    const targetObj = state.targets.find((t) => t.id === tId);
    if (targetObj && msg.summary) {
      targetObj.current_ms = msg.summary.current_ms;
      targetObj.avg_ms = msg.summary.avg_ms;
      targetObj.jitter_ms = msg.summary.jitter_ms;
      targetObj.rolling_loss_pct = msg.summary.rolling_loss_pct;
      targetObj.hop_count = msg.summary.hop_count;
    }
    renderTargetList();

    if (tId === state.selectedTargetId) {
      const cutoffTs = msg.timestamp - state.windowSeconds;
      for (const h of msg.hops || []) {
        const key = String(h.hop_num);
        if (!state.historySeries[key]) state.historySeries[key] = [];
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
// Event Listeners
// ============================================================================
function bindEvents() {
  document.querySelectorAll('.time-window-btn').forEach((btn) => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.time-window-btn').forEach((b) => {
        b.className =
          'time-window-btn px-2 py-0.5 rounded text-xs font-mono text-slate-400 hover:text-slate-200';
      });
      btn.className = 'time-window-btn px-2 py-0.5 rounded text-xs font-mono bg-cyan-600 text-white';
      state.windowSeconds = Number(btn.getAttribute('data-window'));
      loadHistoryForCurrentTarget();
    });
  });

  document.getElementById('btnResetZoom').addEventListener('click', () => {
    if (state.chart) state.chart.resetZoom();
  });

  // Live Adjustable Interval Dropdown
  elSelectInterval.addEventListener('change', async (e) => {
    const target = state.targets.find((t) => t.id === state.selectedTargetId);
    if (!target) return;
    const interval_sec = Number(e.target.value);
    await fetch(`/api/targets/${target.id}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ interval_sec }),
    });
  });

  elBtnTogglePause.addEventListener('click', async () => {
    const target = state.targets.find((t) => t.id === state.selectedTargetId);
    if (!target) return;
    await fetch(`/api/targets/${target.id}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ enabled: !target.enabled }),
    });
  });

  elBtnResetStats.addEventListener('click', async () => {
    if (!state.selectedTargetId) return;
    await fetch(`/api/targets/${state.selectedTargetId}/reset`, { method: 'POST' });
    state.hopsByTarget[state.selectedTargetId] = [];
    state.historySeries = {};
    renderHopTable();
    renderTimelineChart();
  });

  elBtnDeleteTarget.addEventListener('click', async () => {
    const target = state.targets.find((t) => t.id === state.selectedTargetId);
    if (!target) return;
    if (!confirm(`Delete target "${target.label}"?`)) return;
    await fetch(`/api/targets/${target.id}`, { method: 'DELETE' });
  });

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
    const interval_sec = Number(document.getElementById('inputTargetInterval').value || 1.0);

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
      if (data.target && data.target.id) selectTarget(data.target.id);
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

window.addEventListener('DOMContentLoaded', () => {
  initTimelineChart();
  bindEvents();
  connectWebSocket();
});
