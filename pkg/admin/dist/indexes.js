// Index Management

route('/indexes', async (el) => {
  // Get all databases first
  const dbData = await api('/databases');
  const dbs = dbData.databases || [];

  let allIndexes = [];
  for (const db of dbs) {
    try {
      const colls = await api(`/databases/${db.name}/collections`);
      for (const c of (colls.collections || [])) {
        try {
          const idxData = await api(`/databases/${db.name}/collections/${c.name}/indexes`);
          for (const idx of (idxData.indexes || [])) {
            allIndexes.push({ ...idx, db: db.name, coll: c.name });
          }
        } catch (e) { /* skip */ }
      }
    } catch (e) { /* skip */ }
  }

  el.innerHTML = `
    <div class="flex-between mb-16">
      <h2>Indexes</h2>
      <button onclick="showCreateIndex()">Create Index</button>
    </div>
    <div class="card">
      ${allIndexes.length > 0 ? `
        <table>
          <tr><th>Database</th><th>Collection</th><th>Name</th><th>Key</th><th>Unique</th><th>Sparse</th><th></th></tr>
          ${allIndexes.map(idx => `
            <tr>
              <td>${esc(idx.db)}</td>
              <td>${esc(idx.coll)}</td>
              <td>${esc(idx.name)}</td>
              <td>${esc(JSON.stringify(idx.key))}</td>
              <td>${idx.unique ? 'Yes' : 'No'}</td>
              <td>${idx.sparse ? 'Yes' : 'No'}</td>
              <td><button class="btn-danger btn-sm" onclick="dropIndex('${esc(idx.db)}','${esc(idx.coll)}','${esc(idx.name)}')">Drop</button></td>
            </tr>
          `).join('')}
        </table>
      ` : '<p style="color:var(--text-dim)">No indexes found</p>'}
    </div>
  `;
});

async function dropIndex(db, coll, name) {
  if (!confirm(`Drop index ${name} on ${db}.${coll}?`)) return;
  try {
    await api(`/databases/${db}/collections/${coll}/indexes/${name}`, { method: 'DELETE' });
    navigate();
  } catch (e) {
    alert('Error: ' + e.message);
  }
}

function showCreateIndex() {
  openModal('Create Index', `
    <div class="form-row">
      <div class="form-group">
        <label>Database</label>
        <input id="idx-db" placeholder="mydb">
      </div>
      <div class="form-group">
        <label>Collection</label>
        <input id="idx-coll" placeholder="mycollection">
      </div>
    </div>
    <div class="form-group">
      <label>Index Name</label>
      <input id="idx-name" placeholder="idx_field1">
    </div>
    <div class="form-group">
      <label>Key (field:direction, e.g. name:1, age:-1)</label>
      <input id="idx-key" placeholder="field:1">
    </div>
    <div class="form-group">
      <label><input type="checkbox" id="idx-unique"> Unique</label>
    </div>
    <button onclick="doCreateIndex()">Create</button>
  `);
}

async function doCreateIndex() {
  const db = document.getElementById('idx-db').value;
  const coll = document.getElementById('idx-coll').value;
  const name = document.getElementById('idx-name').value;
  const keyStr = document.getElementById('idx-key').value;
  const unique = document.getElementById('idx-unique').checked;

  if (!db || !coll || !name || !keyStr) return alert('All fields required');

  const key = keyStr.split(',').map(k => {
    const [field, dir] = k.trim().split(':');
    return { field: field.trim(), descending: dir.trim() === '-1' };
  });

  try {
    await api(`/databases/${db}/collections/${coll}/indexes`, {
      method: 'POST',
      body: JSON.stringify({ name, key, unique })
    });
    closeModal();
    navigate();
  } catch (e) {
    alert('Error: ' + e.message);
  }
}
