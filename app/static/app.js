/**
 * NetworkStats — Real-Time UI & Packet Loss Log Controller
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
  activeTab: 'live',
  targets: [],
  selectedTargetId: null,
  hopsByTarget: {},
  plottedHopsByTarget: {},
  historySeries: {},
  windowSeconds: 300,
  bucketSec: 1,
  isZoomed: false,
  serverClockOffset: 0,
  ws: null,
  wsPingTimer: null,
  hasConnectedOnce: false,
  disconnectBannerTimer: null,
  lossRefreshTimer: null,
  lastLossRefreshAt: 0,
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
const elWsDisconnectBanner = document.getElementById('wsDisconnectBanner');

// View & Tab Elements
const elTabBtnLive = document.getElementById('tabBtnLive');
const elTabBtnLossLog = document.getElementById('tabBtnLossLog');
const elLossLogBadge = document.getElementById('lossLogBadge');
const elViewLive = document.getElementById('viewLiveMonitor');
const elViewLossLog = document.getElementById('viewLossLog');
const elOutagesTableBody = document.getElementById('outagesTableBody');
const elLossLogTableBody = document.getElementById('lossLogTableBody');
const elLossLogTotalOutages = document.getElementById('lossLogTotalOutages');
const elLossLogTotalIncidents = document.getElementById('lossLogTotalIncidents');
const elLossLogTotalPackets = document.getElementById('lossLogTotalPackets');
const elSelectLossTargetFilter = document.getElementById('selectLossTargetFilter');
const elSelectLossWindowFilter = document.getElementById('selectLossWindowFilter');
const elBtnClearLossLog = document.getElementById('btnClearLossLog');

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
  const loss = Number(target.rolling_loss_pct || 0);
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

function formatTooltipTime(unixSec) {
  const d = new Date(unixSec * 1000);
  return d.toLocaleString([], {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

function formatRelativeAgo(unixSec) {
  const diff = Math.max(0, Math.floor(Date.now() / 1000) - unixSec);
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ${Math.floor((diff % 3600) / 60)}m ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

function formatDuration(sec) {
  const s = Math.max(1, Number(sec || 1));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
}

// ============================================================================
// Tab Switching
// ============================================================================
function switchTab(tabName) {
  state.activeTab = tabName;
  if (tabName === 'live') {
    elTabBtnLive.className = 'px-3 py-1 rounded-md text-xs font-medium bg-cyan-600 text-white whitespace-nowrap transition';
    elTabBtnLossLog.className =
      'px-3 py-1 rounded-md text-xs font-medium text-slate-400 hover:text-slate-200 flex items-center space-x-1.5 whitespace-nowrap transition';
    elViewLive.classList.remove('hidden');
    elViewLossLog.classList.add('hidden');
    elViewLossLog.classList.remove('flex');
    renderTimelineChart();
  } else {
    elTabBtnLossLog.className =
      'px-3 py-1 rounded-md text-xs font-medium bg-cyan-600 text-white flex items-center space-x-1.5 whitespace-nowrap transition';
    elTabBtnLive.className =
      'px-3 py-1 rounded-md text-xs font-medium text-slate-400 hover:text-slate-200 whitespace-nowrap transition';
    elViewLive.classList.add('hidden');
    elViewLossLog.classList.remove('hidden');
    elViewLossLog.classList.add('flex');
    loadLossLog();
  }
}

// ============================================================================
// Chart.js Timeline
// ============================================================================
function initTimelineChart() {
  const ctx = document.getElementById('timelineChart').getContext('2d');
  state.chart = new Chart(ctx, {
    type: 'line',
    data: { datasets: [] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: { mode: 'x', intersect: false },
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
          filter: (item) => item.parsed && item.parsed.y !== null,
          callbacks: {
            title: (items) =>
              items.length > 0 ? formatTooltipTime(Math.round(items[0].parsed.x)) : '',
            label: (item) => {
              if (item.raw && item.raw.lostPkts !== undefined) {
                return `Packet Loss: ${item.raw.lostPkts}/${item.raw.totalPkts} pkts (${item.raw.lossPct}%)`;
              }
              return `${item.dataset.label}: ${Number(item.parsed.y).toFixed(1)} ms`;
            },
          },
        },
        zoom: {
          pan: {
            enabled: true,
            mode: 'x',
            onPanComplete: () => {
              state.isZoomed = true;
            },
          },
          zoom: {
            wheel: { enabled: true },
            pinch: { enabled: true },
            mode: 'x',
            onZoomComplete: () => {
              state.isZoomed = true;
            },
          },
        },
      },
      scales: {
        x: {
          type: 'linear',
          grid: { color: 'rgba(30, 41, 59, 0.4)' },
          ticks: {
            color: '#64748b',
            maxRotation: 0,
            autoSkip: true,
            maxTicksLimit: 8,
            callback: (val) => formatTimeLabel(Math.round(val), state.windowSeconds),
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
          display: false,
          min: 0,
          max: 100,
          grid: { drawOnChartArea: false },
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

  const datasets = [];
  const primaryHop = plottedHops.includes(finalHopNum)
    ? finalHopNum
    : plottedHops[plottedHops.length - 1];

  for (const hNum of plottedHops) {
    const hopMeta = hops.find((h) => h.hop_num === hNum);
    const hopLabel = hopMeta ? `#${hNum} ${hopMeta.ip_address}` : `#${hNum}`;
    const color = colorForHop(hNum, hNum === finalHopNum);
    const seriesArr = state.historySeries[String(hNum)] || [];

    datasets.push({
      type: 'line',
      label: state.bucketSec > 1 && hNum === primaryHop ? `${hopLabel} (Avg)` : hopLabel,
      data: seriesArr.map((pt) => ({
        x: pt.ts,
        y: pt.ms !== null && pt.ms !== undefined ? pt.ms : null,
      })),
      borderColor: color,
      backgroundColor: hNum === primaryHop ? 'rgba(34, 211, 238, 0.08)' : 'transparent',
      fill: hNum === primaryHop ? 'origin' : false,
      borderWidth: 1.75,
      pointRadius: 0,
      pointHoverRadius: 3,
      spanGaps: false,
      tension: 0.15,
      yAxisID: 'yLatency',
      order: 2,
    });

    if (state.bucketSec > 1 && hNum === primaryHop) {
      datasets.push({
        type: 'line',
        label: 'Peak (Max)',
        data: seriesArr.map((pt) => ({
          x: pt.ts,
          y: pt.max_ms !== null && pt.max_ms !== undefined ? pt.max_ms : null,
        })),
        borderColor: 'rgba(56, 189, 248, 0.45)',
        backgroundColor: 'rgba(56, 189, 248, 0.10)',
        fill: '-1',
        borderWidth: 1,
        borderDash: [3, 2],
        pointRadius: 0,
        pointHoverRadius: 3,
        spanGaps: false,
        tension: 0.15,
        yAxisID: 'yLatency',
        order: 3,
      });
    }
  }

  let lossSpikesInWindow = 0;
  if (primaryHop !== undefined) {
    const primaryArr = state.historySeries[String(primaryHop)] || [];
    const lossData = [];
    for (const pt of primaryArr) {
      if (pt.loss > 0) {
        lossSpikesInWindow += 1;
        lossData.push({
          x: pt.ts,
          y: 100,
          lossPct: pt.loss_pct !== undefined ? pt.loss_pct : pt.loss,
          lostPkts: pt.lost_pkts !== undefined ? pt.lost_pkts : 1,
          totalPkts: pt.total_pkts !== undefined ? pt.total_pkts : 1,
        });
      }
    }

    datasets.push({
      type: 'bar',
      label: 'Packet Loss',
      data: lossData,
      backgroundColor: 'rgba(244, 63, 94, 0.75)',
      borderColor: '#f43f5e',
      borderWidth: 1,
      barThickness: 3,
      grouped: false,
      yAxisID: 'yLoss',
      order: 1,
    });
  }

  if (plottedHops.length > 0) {
    const hopStr = `Hop ${plottedHops.map((h) => `#${h}`).join(', ')}`;
    elPlottedSummary.textContent =
      lossSpikesInWindow > 0 ? `${hopStr} • ${lossSpikesInWindow} loss bucket(s)` : hopStr;
  } else {
    elPlottedSummary.textContent = '';
  }

  if (!state.isZoomed) {
    const nowSec = Math.floor(Date.now() / 1000) + state.serverClockOffset;
    state.chart.options.scales.x.min = nowSec - state.windowSeconds;
    state.chart.options.scales.x.max = nowSec;
  }

  state.chart.data.datasets = datasets;
  state.chart.update('none');
}

// ============================================================================
// Sidebar & Hop Table
// ============================================================================
function renderTargetList() {
  elTargetCount.textContent = String(state.targets.length);
  elTargetList.innerHTML = '';

  const prevFilter = elSelectLossTargetFilter.value;
  elSelectLossTargetFilter.innerHTML = '<option value="">All Targets</option>';

  for (const t of state.targets) {
    const opt = document.createElement('option');
    opt.value = String(t.id);
    opt.textContent = `${t.label} (${t.host})`;
    elSelectLossTargetFilter.appendChild(opt);

    const isSelected = t.id === state.selectedTargetId;
    const status = getTargetStatus(t);
    const pingText =
      t.current_ms !== null && t.current_ms !== undefined
        ? `${Number(t.current_ms).toFixed(1)} ms`
        : '--';
    const totalLoss = Number(t.loss_pct ?? t.loss_1h_pct ?? 0).toFixed(2);
    const lostCnt = Number(t.lost_count || 0);

    const item = document.createElement('div');
    item.className = `px-3 py-2 cursor-pointer transition shrink-0 min-w-[170px] xl:min-w-0 border-b-2 xl:border-b-0 xl:border-l-2 ${
      isSelected
        ? 'bg-slate-850 border-cyan-400'
        : 'border-transparent hover:bg-slate-900/60'
    }`;
    item.onclick = () => {
      selectTarget(t.id);
      if (state.activeTab !== 'live') switchTab('live');
    };

    item.innerHTML = `
      <div class="flex items-center justify-between gap-2 whitespace-nowrap">
        <div class="flex items-center space-x-2 min-w-0">
          <span class="w-2 h-2 rounded-full ${status.color} shrink-0"></span>
          <span class="font-medium text-xs text-slate-100 truncate">${escapeHtml(t.label)}</span>
        </div>
        <span class="text-xs font-mono text-slate-200 shrink-0">${pingText}</span>
      </div>
      <div class="flex items-center justify-between gap-2 text-[11px] font-mono text-slate-500 mt-0.5 pl-4 whitespace-nowrap">
        <span class="truncate max-w-[110px]">${escapeHtml(t.host)}</span>
        <span class="shrink-0 ${lostCnt > 0 ? 'text-rose-400' : 'text-slate-500'}">
          ${totalLoss}%${lostCnt > 0 ? ` (${lostCnt})` : ''}
        </span>
      </div>
    `;
    elTargetList.appendChild(item);
  }

  elSelectLossTargetFilter.value = prevFilter;
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

  const totalLoss = Number(target.loss_pct ?? target.loss_1h_pct ?? 0).toFixed(2);
  const lostCnt = Number(target.lost_count || 0);

  elHeaderPing.textContent = formatMs(target.current_ms);
  elHeaderJitter.textContent = formatMs(target.jitter_ms);
  elHeaderLoss.textContent = lostCnt > 0 ? `${totalLoss}% (${lostCnt} lost)` : `${totalLoss}%`;
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
        `<rect x="${x - 1}" y="2" width="2.5" height="${height - 4}" fill="#f43f5e" opacity="0.9" />`
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
        <td colspan="12" class="py-8 text-center text-slate-500 font-sans text-xs">
          Tracing route hops...
        </td>
      </tr>
    `;
    return;
  }

  const plottedSet = getPlottedSet(targetId, hops);
  const finalHop = hops[hops.length - 1];
  const finalHopNum = finalHop.hop_num;
  const finalLossVal = Number(finalHop.loss_pct || 0);
  const scaleMaxMs = Math.max(25, ...hops.map((h) => Number(h.max_ms || 0)));

  for (const h of hops) {
    const isFinal = h.hop_num === finalHopNum;
    const isPlotted = plottedSet.has(h.hop_num);
    const hopColor = colorForHop(h.hop_num, isFinal);
    const lossVal = Number(h.loss_pct || 0);
    const lostCnt = Number(h.lost || 0);
    const carriedOver = isFinal || finalLossVal > 0;

    const rdnsDisplay =
      h.rdns_hostname && h.rdns_hostname !== h.ip_address && h.rdns_hostname !== '???'
        ? `<span class="text-slate-200">${escapeHtml(h.rdns_hostname)}</span> <span class="text-slate-500">(${escapeHtml(h.ip_address)})</span>`
        : `<span class="${h.ip_address === '???' ? 'text-slate-600' : 'text-slate-200'}">${
            h.ip_address === '???' ? '* * *' : escapeHtml(h.ip_address)
          }</span>`;

    const tr = document.createElement('tr');
    tr.className = `hover:bg-slate-900/60 transition ${isFinal ? 'bg-cyan-950/10' : ''}`;

    let lossTextClass = 'text-slate-400';
    if (lossVal > 0 && h.ip_address !== '???') {
      if (!carriedOver) {
        lossTextClass = 'text-slate-500';
      } else if (lossVal >= 5) {
        lossTextClass = 'text-rose-400 font-semibold';
      } else {
        lossTextClass = 'text-amber-400';
      }
    }

    tr.innerHTML = `
      <td class="py-1.5 px-2.5 text-center">
        <input type="checkbox" ${isPlotted ? 'checked' : ''}
               style="accent-color: ${hopColor}"
               class="w-3.5 h-3.5 rounded cursor-pointer"
               data-hop="${h.hop_num}" />
      </td>
      <td class="py-1.5 px-2 ${isFinal ? 'text-cyan-400 font-semibold' : 'text-slate-500'}">
        ${h.hop_num}
      </td>
      <td class="py-1.5 px-3 truncate max-w-[180px] sm:max-w-[260px] lg:max-w-sm">${rdnsDisplay}</td>
      <td class="py-1.5 px-2 text-right ${lossTextClass}"
          title="${!carriedOver && lossVal > 0 ? 'Hop ignored ping, but 0% loss reached final destination' : ''}">
        ${h.ip_address === '???' ? '--' : `${lossVal.toFixed(2)}%`}
      </td>
      <td class="py-1.5 px-2 text-right ${
        lostCnt > 0 ? (carriedOver ? 'text-rose-400 font-semibold' : 'text-slate-500') : 'text-slate-500'
      }">
        ${lostCnt}
      </td>
      <td class="py-1.5 px-2 text-right text-slate-400 hidden sm:table-cell">${h.sent}</td>
      <td class="py-1.5 px-2 text-right ${
        h.last_ms !== null ? 'text-slate-100' : 'text-rose-400'
      }">${h.last_ms !== null ? h.last_ms.toFixed(1) : 'LOST'}</td>
      <td class="py-1.5 px-2 text-right text-slate-400">${
        h.avg_ms !== null ? h.avg_ms.toFixed(1) : '--'
      }</td>
      <td class="py-1.5 px-2 text-right text-slate-400 hidden md:table-cell">${
        h.min_ms !== null ? h.min_ms.toFixed(1) : '--'
      }</td>
      <td class="py-1.5 px-2 text-right text-slate-400 hidden md:table-cell">${
        h.max_ms !== null ? h.max_ms.toFixed(1) : '--'
      }</td>
      <td class="py-1.5 px-2 text-right text-slate-400">${
        h.jitter_ms !== null ? h.jitter_ms.toFixed(1) : '0.0'
      }</td>
      <td class="py-1.5 px-3">
        <div class="flex items-center space-x-2.5">
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
// Packet Loss Log & Network-Wide Outages View
// ============================================================================
function updateLossLogBadge(summary) {
  const incidents = Number((summary && summary.total_incidents) || 0);
  if (incidents > 0) {
    elLossLogBadge.textContent = String(incidents);
    elLossLogBadge.classList.remove('hidden');
  } else {
    elLossLogBadge.classList.add('hidden');
  }
}

async function refreshLossBadge() {
  try {
    const res = await fetch('/api/loss-events?window=0');
    if (!res.ok) return;
    const data = await res.json();
    updateLossLogBadge(data.summary);
  } catch (err) {
    console.error('Failed refreshing loss badge:', err);
  }
}

function scheduleLossLogRefresh(immediate = false) {
  const run = async () => {
    state.lastLossRefreshAt = Date.now();
    if (state.activeTab === 'losslog') {
      await loadLossLog();
    } else {
      await refreshLossBadge();
    }
  };

  if (immediate) {
    if (state.lossRefreshTimer) {
      clearTimeout(state.lossRefreshTimer);
      state.lossRefreshTimer = null;
    }
    run();
    return;
  }

  const elapsed = Date.now() - state.lastLossRefreshAt;
  if (elapsed >= 2000) {
    if (state.lossRefreshTimer) {
      clearTimeout(state.lossRefreshTimer);
      state.lossRefreshTimer = null;
    }
    run();
  } else if (!state.lossRefreshTimer) {
    state.lossRefreshTimer = setTimeout(() => {
      state.lossRefreshTimer = null;
      run();
    }, 2000 - elapsed);
  }
}

async function loadLossLog() {
  const windowSec = Number(elSelectLossWindowFilter.value || 0);
  const targetFilter = elSelectLossTargetFilter.value;
  const qs = new URLSearchParams({ window: String(windowSec) });
  if (targetFilter) qs.set('target_id', targetFilter);

  try {
    const res = await fetch(`/api/loss-events?${qs.toString()}`);
    if (!res.ok) return;
    const data = await res.json();
    renderLossLogTable(data);
    if (windowSec === 0 && !targetFilter) {
      updateLossLogBadge(data.summary);
    } else {
      refreshLossBadge();
    }
  } catch (err) {
    console.error('Failed loading loss events:', err);
  }
}

function renderOutagesTable(outages) {
  elOutagesTableBody.innerHTML = '';

  if (!outages || outages.length === 0) {
    elOutagesTableBody.innerHTML = `
      <tr>
        <td colspan="6" class="py-5 text-center text-slate-500 font-sans text-xs">
          No network-wide internet outages recorded in this window.
        </td>
      </tr>
    `;
    return;
  }

  for (const o of outages) {
    const startD = new Date(o.start_ts * 1000);
    const endD = new Date(o.end_ts * 1000);
    const startStr = startD.toLocaleString([], {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
    const endStr = endD.toLocaleTimeString([], {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });

    const diagText = o.router_reachable
      ? 'ISP / WAN Outage (Local Router OK)'
      : 'Local Router / LAN Down';
    const diagClass = o.router_reachable
      ? 'text-rose-300 bg-rose-950/60 border-rose-800/70'
      : 'text-amber-300 bg-amber-950/60 border-amber-800/70';

    const tr = document.createElement('tr');
    tr.className = 'hover:bg-slate-900/60 transition';
    tr.innerHTML = `
      <td class="py-2.5 px-4 text-slate-200">${escapeHtml(startStr)} &rarr; ${escapeHtml(endStr)}</td>
      <td class="py-2.5 px-3 text-slate-500">${formatRelativeAgo(o.start_ts)}</td>
      <td class="py-2.5 px-3 text-right text-rose-400 font-semibold">${formatDuration(o.duration_sec)}</td>
      <td class="py-2.5 px-3 text-center text-slate-200">${o.affected_targets}/${o.total_external_targets}</td>
      <td class="py-2.5 px-3">
        <span class="px-2 py-0.5 rounded border text-[11px] font-sans ${diagClass}">
          ${escapeHtml(diagText)}
        </span>
      </td>
      <td class="py-2.5 px-4 text-slate-400 truncate max-w-md">${escapeHtml(o.affected_labels || '')}</td>
    `;
    elOutagesTableBody.appendChild(tr);
  }
}

function renderLossLogTable(data) {
  const events = data.events || [];
  const outages = data.outages || [];
  const summary = data.summary || {
    total_incidents: 0,
    total_packets_lost: 0,
    total_outages: 0,
  };

  if (elLossLogTotalOutages) {
    elLossLogTotalOutages.textContent = String(summary.total_outages || 0);
  }
  elLossLogTotalIncidents.textContent = String(summary.total_incidents || 0);
  elLossLogTotalPackets.textContent = String(summary.total_packets_lost || 0);

  renderOutagesTable(outages);
  elLossLogTableBody.innerHTML = '';

  if (events.length === 0) {
    elLossLogTableBody.innerHTML = `
      <tr>
        <td colspan="5" class="py-10 text-center text-slate-500 font-sans text-xs">
          No packet loss recorded for this filter.
        </td>
      </tr>
    `;
    return;
  }

  for (const ev of events) {
    const d = new Date(ev.start_ts * 1000);
    const timeStr = d.toLocaleString([], {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });

    let causeClass = 'text-rose-300 bg-rose-950/60 border-rose-800/70';
    if (ev.severity === 'router') {
      causeClass = 'text-amber-300 bg-amber-950/60 border-amber-800/70';
    } else if (ev.severity === 'isp') {
      causeClass = 'text-purple-300 bg-purple-950/60 border-purple-800/70';
    }

    const durationSec = Math.max(1, ev.end_ts - ev.start_ts + 1);
    const tr = document.createElement('tr');
    tr.className = 'hover:bg-slate-900/60 transition cursor-pointer';
    tr.onclick = () => {
      selectTarget(ev.target_id);
      switchTab('live');
    };

    tr.innerHTML = `
      <td class="py-2.5 px-4 text-slate-200">${escapeHtml(timeStr)}</td>
      <td class="py-2.5 px-3 text-slate-500">${formatRelativeAgo(ev.start_ts)}</td>
      <td class="py-2.5 px-3">
        <span class="text-slate-100 font-sans font-medium">${escapeHtml(ev.target_label)}</span>
        <span class="text-slate-500 ml-1">(${escapeHtml(ev.target_host)})</span>
      </td>
      <td class="py-2.5 px-3 text-right text-rose-400 font-semibold">
        ${ev.packets_lost} lost${durationSec > 1 ? ` (${formatDuration(durationSec)})` : ''}
      </td>
      <td class="py-2.5 px-4">
        <span class="px-2 py-0.5 rounded border text-[11px] font-sans ${causeClass}">
          ${escapeHtml(ev.cause)}
        </span>
      </td>
    `;
    elLossLogTableBody.appendChild(tr);
  }
}

// ============================================================================
// API & WebSocket
// ============================================================================
async function selectTarget(targetId) {
  state.selectedTargetId = targetId;
  state.isZoomed = false;
  if (state.chart) {
    delete state.chart.options.scales.x.min;
    delete state.chart.options.scales.x.max;
    state.chart.resetZoom('none');
  }
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
      state.bucketSec = histData.bucket_sec || 1;
      state.serverClockOffset = histData.server_now
        ? histData.server_now - Math.floor(Date.now() / 1000)
        : 0;
    }
  } catch (err) {
    console.error('Failed loading target details:', err);
  }

  renderHopTable();
  renderTimelineChart();
}

async function loadHistoryForCurrentTarget() {
  if (!state.selectedTargetId) return;
  state.isZoomed = false;
  if (state.chart) {
    delete state.chart.options.scales.x.min;
    delete state.chart.options.scales.x.max;
    state.chart.resetZoom('none');
  }
  try {
    const res = await fetch(
      `/api/targets/${state.selectedTargetId}/history?window=${state.windowSeconds}`
    );
    if (res.ok) {
      const data = await res.json();
      state.historySeries = data.series || {};
      state.bucketSec = data.bucket_sec || 1;
      state.serverClockOffset = data.server_now
        ? data.server_now - Math.floor(Date.now() / 1000)
        : 0;
      renderTimelineChart();
    }
  } catch (err) {
    console.error('Error loading history window:', err);
  }
}

async function refreshAfterReconnect() {
  try {
    const res = await fetch('/api/targets');
    if (res.ok) {
      const data = await res.json();
      state.targets = data.targets || [];
      renderTargetList();
    }
  } catch (err) {
    console.error('Failed refreshing targets after reconnect:', err);
  }
  if (state.selectedTargetId) {
    await selectTarget(state.selectedTargetId);
  }
  scheduleLossLogRefresh(true);
}

function connectWebSocket() {
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const ws = new WebSocket(`${proto}//${window.location.host}/ws`);
  state.ws = ws;

  ws.onopen = () => {
    if (state.disconnectBannerTimer) {
      clearTimeout(state.disconnectBannerTimer);
      state.disconnectBannerTimer = null;
    }
    if (elWsDisconnectBanner) {
      elWsDisconnectBanner.classList.add('hidden');
    }

    if (state.wsPingTimer) clearInterval(state.wsPingTimer);
    state.wsPingTimer = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) ws.send('ping');
    }, 15000);

    if (state.hasConnectedOnce) {
      refreshAfterReconnect();
    } else {
      state.hasConnectedOnce = true;
      refreshLossBadge();
    }
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
    if (!state.disconnectBannerTimer && elWsDisconnectBanner) {
      state.disconnectBannerTimer = setTimeout(() => {
        elWsDisconnectBanner.classList.remove('hidden');
      }, 4000);
    }
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

  if (msg.type === 'loss_update') {
    scheduleLossLogRefresh(false);
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
      targetObj.loss_pct = msg.summary.loss_pct;
      targetObj.loss_1h_pct = msg.summary.loss_pct;
      targetObj.lost_count = msg.summary.lost_count;
      targetObj.hop_count = msg.summary.hop_count;
    }
    renderTargetList();

    if (tId === state.selectedTargetId) {
      const bucketSec = Math.max(1, state.bucketSec || 1);
      const bucketTs = Math.floor(msg.timestamp / bucketSec) * bucketSec;
      const cutoffTs = msg.timestamp - state.windowSeconds - bucketSec;

      for (const h of msg.hops || []) {
        const key = String(h.hop_num);
        if (!state.historySeries[key]) state.historySeries[key] = [];
        const seriesArr = state.historySeries[key];
        const isLoss = h.last_ms === null;
        const msVal = isLoss ? null : Number(h.last_ms);
        const lastPt = seriesArr.length > 0 ? seriesArr[seriesArr.length - 1] : null;

        if (lastPt && lastPt.ts === bucketTs) {
          const prevTotal = Number(lastPt.total_pkts || 1);
          const prevLost = Number(lastPt.lost_pkts || (lastPt.loss > 0 ? 1 : 0));
          const prevValid = Math.max(0, prevTotal - prevLost);
          const newTotal = prevTotal + 1;
          const newLost = prevLost + (isLoss ? 1 : 0);

          lastPt.total_pkts = newTotal;
          lastPt.lost_pkts = newLost;
          lastPt.loss_pct = Math.round((newLost / newTotal) * 1000) / 10;
          if (isLoss) {
            lastPt.loss = 100.0;
          } else if (lastPt.ms === null || prevValid === 0) {
            lastPt.ms = msVal;
            lastPt.max_ms = msVal;
            lastPt.min_ms = msVal;
          } else {
            lastPt.ms = Math.round(((lastPt.ms * prevValid + msVal) / (prevValid + 1)) * 100) / 100;
            lastPt.max_ms = lastPt.max_ms !== null ? Math.max(lastPt.max_ms, msVal) : msVal;
            lastPt.min_ms = lastPt.min_ms !== null ? Math.min(lastPt.min_ms, msVal) : msVal;
          }
        } else if (!lastPt || bucketTs > lastPt.ts) {
          seriesArr.push({
            ts: bucketTs,
            ms: msVal,
            max_ms: msVal,
            min_ms: msVal,
            loss: isLoss ? 100.0 : 0.0,
            loss_pct: isLoss ? 100.0 : 0.0,
            lost_pkts: isLoss ? 1 : 0,
            total_pkts: 1,
          });
        }

        while (seriesArr.length > 0 && seriesArr[0].ts < cutoffTs) {
          seriesArr.shift();
        }
      }
      renderHopTable();
      if (state.activeTab === 'live') {
        renderTimelineChart();
      }
    }
  }
}

// ============================================================================
// Event Listeners
// ============================================================================
function bindEvents() {
  elTabBtnLive.addEventListener('click', () => switchTab('live'));
  elTabBtnLossLog.addEventListener('click', () => switchTab('losslog'));

  elSelectLossTargetFilter.addEventListener('change', loadLossLog);
  elSelectLossWindowFilter.addEventListener('change', loadLossLog);
  document.getElementById('btnRefreshLossLog').addEventListener('click', loadLossLog);

  if (elBtnClearLossLog) {
    elBtnClearLossLog.addEventListener('click', async () => {
      if (!confirm('Clear all recorded outages and packet loss logs?')) return;
      await fetch('/api/loss-events', { method: 'DELETE' });
      await loadLossLog();
      await refreshLossBadge();
    });
  }

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
    state.isZoomed = false;
    if (state.chart) {
      delete state.chart.options.scales.x.min;
      delete state.chart.options.scales.x.max;
      state.chart.resetZoom('none');
      renderTimelineChart();
    }
  });

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
    scheduleLossLogRefresh(true);
  });

  elBtnDeleteTarget.addEventListener('click', async () => {
    const target = state.targets.find((t) => t.id === state.selectedTargetId);
    if (!target) return;
    if (!confirm(`Delete target "${target.label}"?`)) return;
    await fetch(`/api/targets/${target.id}`, { method: 'DELETE' });
    scheduleLossLogRefresh(true);
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
