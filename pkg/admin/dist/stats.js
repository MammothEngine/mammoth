// Statistics Page

route('/stats', async (el) => {
  const info = await api('/status');
  const eng = info.engine;
  const ops = info.operations;

  el.innerHTML = `
    <h2 class="mb-16">Statistics</h2>
    <div class="card mb-16">
      <h3>Storage Engine</h3>
      <table>
        <tr><th>Metric</th><th>Value</th></tr>
        <tr><td>Memtables</td><td>${eng.memtables} (${formatBytes(eng.memtableSize)})</td></tr>
        <tr><td>SSTables</td><td>${eng.sstables} (${formatBytes(eng.sstableSize)})</td></tr>
        <tr><td>WAL Segments</td><td>${eng.walSegments}</td></tr>
        <tr><td>Compactions</td><td>${formatNumber(eng.compactions)}</td></tr>
        <tr><td>Sequence Number</td><td>${formatNumber(eng.sequenceNumber)}</td></tr>
      </table>
    </div>
    <div class="card">
      <h3>Operations</h3>
      <table>
        <tr><th>Operation</th><th>Count</th></tr>
        <tr><td>Put</td><td>${formatNumber(ops.puts)}</td></tr>
        <tr><td>Get</td><td>${formatNumber(ops.gets)}</td></tr>
        <tr><td>Delete</td><td>${formatNumber(ops.deletes)}</td></tr>
        <tr><td>Scan</td><td>${formatNumber(ops.scans)}</td></tr>
      </table>
    </div>
  `;
});
