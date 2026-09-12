async function refreshInternals() {
  let d;
  try {
    const r = await fetch('/api/internals');
    d = await r.json();
  } catch(e) { return; }

  setRows('tb-int-health', (d.st_health||[]).map(r => {
    const dot = r.is_populated
      ? '<span style="color:var(--green)">●</span>'
      : '<span style="color:var(--yellow)">○</span>';
    const ts  = r.last_refresh  ? new Date(r.last_refresh).toLocaleTimeString()  : '—';
    const dur = r.duration_ms   != null ? r.duration_ms + ' ms' : '—';
    const rows = r.rows_affected != null ? fmt(r.rows_affected)  : '—';
    return `<tr>
       <td style="font-family:monospace">${esc(r.name)}</td>
       <td><code>${esc(r.refresh_mode)}</code></td>
       <td style="color:var(--muted);font-size:11px">${esc(r.schedule||'calc')}</td>
       <td>${dot}</td>
       <td style="color:var(--muted);font-size:11px">${ts}</td>
       <td style="text-align:right;color:var(--muted)">${dur}</td>
       <td style="text-align:right">${rows}</td>
       <td style="font-size:11px">${esc(r.status)}</td>
     </tr>`;
  }).join(''));

  document.getElementById('pre-dep-tree').textContent =
    (d.dep_tree||[]).join('\n') || '(no dependency data)';

  setRows('tb-int-history', (d.refresh_history||[]).map(r => {
    const fallback = r.was_full_fallback
      ? ' <span style="color:var(--yellow);font-size:10px" title="fell back to FULL">▲</span>' : '';
    const statusCol = r.status === 'FAILED' ? 'color:var(--red)' : 'color:var(--muted)';
    const ts = r.start_time ? new Date(r.start_time).toLocaleTimeString() : '—';
    return `<tr>
       <td style="font-family:monospace;font-size:12px">${esc(r.name)}</td>
       <td><code>${esc(r.refresh_mode)}</code>${fallback}</td>
       <td style="color:var(--muted);font-size:11px">${ts}</td>
       <td style="text-align:right">${r.duration_ms != null ? r.duration_ms : '—'}</td>
       <td style="text-align:right">${r.rows_affected != null ? fmt(r.rows_affected) : '—'}</td>
       <td style="${statusCol};font-size:11px">${esc(r.status)}</td>
     </tr>`;
  }).join(''));

  setRows('tb-int-eff', (d.efficiency||[]).map(r =>
    `<tr>
       <td style="font-family:monospace">${esc(r.name)}</td>
       <td style="text-align:right">${fmt(r.total_refreshes)}</td>
       <td style="text-align:right;color:var(--green)">${fmt(r.diff_count)}</td>
       <td style="text-align:right;color:var(--yellow)">${fmt(r.full_count)}</td>
       <td style="text-align:right">${r.avg_diff_ms != null ? Number(r.avg_diff_ms).toFixed(1) : '—'}</td>
       <td style="text-align:right">${r.avg_full_ms != null ? Number(r.avg_full_ms).toFixed(1) : '—'}</td>
       <td style="text-align:right;color:var(--green)">${esc(r.diff_speedup||'—')}</td>
       <td style="text-align:right">${r.avg_change_ratio != null ? Number(r.avg_change_ratio).toFixed(3) : '—'}</td>
     </tr>`).join(''));

  setRows('tb-int-advisor', (d.opt_hints||[]).map(r => {
    const keep = r.recommended_mode === 'KEEP';
    const recHtml = keep
      ? `<span class="score-keep">✓ KEEP <code>${esc(r.current_mode)}</code></span>`
      : `<span class="score-switch">→ SWITCH TO <code>${esc(r.recommended_mode)}</code></span>`;
    const confClass = r.confidence === 'high' ? 'conf-high'
                    : r.confidence === 'medium' ? 'conf-medium' : 'conf-low';
    return `<tr>
       <td style="font-family:monospace">${esc(r.name)}</td>
       <td><code>${esc(r.current_mode)}</code></td>
       <td style="color:var(--muted)"><code>${esc(r.effective_mode||'—')}</code></td>
       <td>${recHtml}</td>
       <td class="${confClass}" style="font-size:11px">${esc(r.confidence||'—')}</td>
       <td style="color:var(--muted);font-size:11px">${esc(r.reason||'')}</td>
     </tr>`;
  }).join(''));
}

document.getElementById('tab-internals-btn')
  .addEventListener('shown.bs.tab', refreshInternals);

document.getElementById('tab-internals-btn')
  .addEventListener('shown.bs.tab', refreshInternals);
setInterval(refreshInternals, 5000);
