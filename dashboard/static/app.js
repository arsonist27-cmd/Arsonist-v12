function loadBar(load, max = 1.25) {
  const v = Number(load) || 0;
  const pct = Math.min(100, Math.round((v / max) * 100));
  return `<div class="load-track" title="load ${v.toFixed(2)}"><span class="load-fill" style="width:${pct}%"></span></div>`;
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

async function refreshFederation() {
  const go = document.getElementById("fed-global-overview");
  const mapEl = document.getElementById("fed-map");
  const cards = document.getElementById("fed-cluster-cards");
  const health = document.getElementById("fed-health");
  const rt = document.getElementById("fed-routing");
  const fo = document.getElementById("fed-failover");
  if (!go || !cards || !health || !rt || !fo) return;
  try {
    const res = await fetch("/api/federation");
    const fed = await res.json();
    if (!fed.enabled) {
      go.innerHTML = `<span class="muted">Federation UI disabled (set dashboard federation URL + token).</span>`;
      if (mapEl) mapEl.innerHTML = "";
      cards.innerHTML = "";
      health.innerHTML = "";
      rt.textContent = "—";
      fo.textContent = "—";
      return;
    }
    const gh = fed.global_health || {};
    const fm = fed.federation_metrics || {};
    const cm = fed.cluster_metrics || {};
    const clusters = (fed.clusters && fed.clusters.clusters) || (cm.clusters || []);
    const rm = fed.routing_metrics || {};

    go.innerHTML = `
      <div class="fed-stat"><span class="lbl">Clusters</span><span class="val">${fm.total_clusters ?? gh.total_clusters ?? "—"}</span></div>
      <div class="fed-stat"><span class="lbl">Active</span><span class="val">${fm.active_clusters ?? "—"}</span></div>
      <div class="fed-stat"><span class="lbl">Offline</span><span class="val warn">${fm.failed_clusters ?? gh.offline_clusters ?? "—"}</span></div>
      <div class="fed-stat"><span class="lbl">Global queue</span><span class="val">${fm.global_queue_size ?? gh.global_queue_depth ?? "—"}</span></div>
      <div class="fed-stat"><span class="lbl">Transfers</span><span class="val">${fm.cross_cluster_transfers ?? "—"}</span></div>
      <div class="fed-stat"><span class="lbl">Failovers</span><span class="val">${fm.failover_events ?? "—"}</span></div>
    `;

    if (mapEl) {
      const hub =
        '<div class="fed-hub" title="Federation controller"><span class="hub-dot"></span><div>Federation</div></div>';
      const arms = clusters
        .map((c) => {
          const st = (c.health_state || "").toLowerCase();
          const cls = st === "offline" ? "offline" : st === "degraded" ? "degraded" : "";
          return `<div class="fed-map-arm ${cls}">
            <div class="fed-edge"></div>
            <div class="fed-node" title="${escapeHtml(c.cluster_id)}">${escapeHtml(c.cluster_id || "?")}</div>
          </div>`;
        })
        .join("");
      mapEl.innerHTML = `<div class="fed-map-inner">${hub}<div class="fed-map-arms">${arms}</div></div>`;
    }

    cards.innerHTML = clusters
      .map((c) => {
        const st = (c.health_state || "").toLowerCase();
        const cls = st === "offline" ? "bad" : st === "degraded" ? "warn" : "";
        const load = c.current_load ?? 0;
        return `<div class="fed-card ${cls}">
          <div class="fed-card-head"><strong>${escapeHtml(c.cluster_id || "")}</strong>
            <span class="health-dot ${cls}" title="${escapeHtml(st)}"></span></div>
          <div class="muted">${escapeHtml(c.region || "")}</div>
          <div class="fed-metrics">nodes ${c.node_count ?? 0} · gpu ${c.gpu_capacity ?? 0} · q ${c.queue_depth ?? 0}</div>
          <div class="fed-load"><span>load</span>${loadBar(load)}</div>
        </div>`;
      })
      .join("");

    health.innerHTML = `
      <div class="fed-stat"><span class="lbl">Healthy</span><span class="val ok">${gh.healthy_clusters ?? "—"}</span></div>
      <div class="fed-stat"><span class="lbl">Degraded</span><span class="val warn">${gh.degraded_clusters ?? "—"}</span></div>
      <div class="fed-stat"><span class="lbl">Reroutes</span><span class="val">${fm.failover_reroutes ?? rm.failover_reroutes ?? "—"}</span></div>
      <div class="fed-stat"><span class="lbl">Jobs by status</span><span class="val tiny">${escapeHtml(JSON.stringify(fm.jobs_by_status || {}))}</span></div>
    `;

    rt.textContent = JSON.stringify(
      {
        routing_metrics: rm,
        cluster_load_map: fm.cluster_load_map || {},
      },
      null,
      2
    );

    const events = (rm.recent_events || []).filter((e) => {
      const t = (e && e.event) || "";
      return t === "failover" || t === "cluster_offline" || t === "cluster_recovered";
    });
    fo.textContent = events.length ? JSON.stringify(events.slice(0, 20), null, 2) : JSON.stringify(rm.recent_events || [], null, 2);
  } catch (err) {
    go.innerHTML = `<div class="pill bad">Federation fetch failed</div>`;
  }
}

async function refresh() {
  try {
    await refreshFederation();
    const res = await fetch("/api/cluster");
    const data = await res.json();
    const metrics = data.metrics || {};
    const nodes = (data.nodes && data.nodes.nodes) || [];
    const jobs = (data.jobs && data.jobs.jobs) || [];

    document.getElementById("health-summary").innerHTML = `
      <div><strong>Nodes:</strong> ${metrics.active_nodes ?? nodes.length}</div>
      <div><strong>Queued:</strong> ${metrics.queued_jobs ?? 0}</div>
      <div><strong>Running:</strong> ${metrics.running_jobs ?? 0}</div>
      <div><strong>Failed:</strong> ${metrics.failed_jobs ?? 0}</div>
      <div><strong>Avg load:</strong> ${metrics.avg_cluster_load ?? 0}</div>
      <div><strong>GPU load:</strong> ${metrics.avg_gpu_load ?? 0}</div>
    `;

    document.getElementById("nodes-live").innerHTML = nodes
      .map(
        (n) =>
          `<div class="pill ${n.healthy ? "ok" : "bad"}">${n.node_id} | ${n.node_type} | load=${Number(
            n.current_load || 0
          ).toFixed(2)} | q=${n.queue_size ?? 0}</div>`
      )
      .join("");

    const counts = jobs.reduce((acc, j) => {
      acc[j.status] = (acc[j.status] || 0) + 1;
      return acc;
    }, {});
    document.getElementById("jobs-live").innerHTML = Object.entries(counts)
      .map(([k, v]) => `<div class="pill">${k}: ${v}</div>`)
      .join("");

    document.getElementById("snapshot").textContent = JSON.stringify(data, null, 2);
  } catch (err) {
    document.getElementById("snapshot").textContent = "Failed to fetch cluster state: " + err;
  }
}

refresh();
setInterval(refresh, 3000);
