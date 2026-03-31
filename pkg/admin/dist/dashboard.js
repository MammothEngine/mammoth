// Dashboard Page

route('/', async (el) => {
  const info = await api('/status');
  const eng = info.engine;
  const ops = info.operations;

  el.innerHTML = `
    <h2 class="mb-16">Dashboard</h2>
    <div class="card-grid mb-16">
      <div class="card">
        <h3>Version</h3>
        <div class="value">${esc(info.version)}</div>
      </div>
      <div class="card">
        <h3>Uptime</h3>
        <div class="value">${esc(info.uptime)}</div>
      </div>
      <div class="card">
        <h3>SSTables</h3>
        <div class="value">${formatNumber(eng.sstables)}</div>
        <small>${formatBytes(eng.sstableSize)}</small>
      </div>
      <div class="card">
        <h3>Memtables</h3>
        <div class="value">${eng.memtables}</div>
        <small>${formatBytes(eng.memtableSize)}</small>
      </div>
    </div>
    <div class="card-grid">
      <div class="card">
        <h3>WAL Segments</h3>
        <div class="value">${eng.walSegments}</div>
      </div>
      <div class="card">
        <h3>Compactions</h3>
        <div class="value">${formatNumber(eng.compactions)}</div>
      </div>
      <div class="card">
        <h3>Sequence</h3>
        <div class="value">${formatNumber(eng.sequenceNumber)}</div>
      </div>
      <div class="card">
        <h3>Status</h3>
        <div class="value"><span class="badge badge-success">Running</span></div>
      </div>
    </div>
    <div class="card mt-16">
      <h3>Operations</h3>
      <table>
        <tr><th>Puts</th><th>Gets</th><th>Deletes</th><th>Scans</th></tr>
        <tr>
          <td>${formatNumber(ops.puts)}</td>
          <td>${formatNumber(ops.gets)}</td>
          <td>${formatNumber(ops.deletes)}</td>
          <td>${formatNumber(ops.scans)}</td>
        </tr>
      </table>
    </div>
  `;
});
