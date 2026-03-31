// Databases & Collections Browser

let selectedDB = null;
let selectedColl = null;

route('/databases', async (el) => {
  const data = await api('/databases');
  const dbs = data.databases || [];

  let dbListHtml = dbs.map(db =>
    `<div class="tree-item ${selectedDB === db.name ? 'active' : ''}"
          onclick="selectDB('${esc(db.name)}')">${esc(db.name)}</div>
     <div class="tree-children" id="colls-${esc(db.name)}"></div>`
  ).join('');

  el.innerHTML = `
    <div class="flex-between mb-16">
      <h2>Databases</h2>
      <button onclick="showCreateDB()">Create Database</button>
    </div>
    <div class="flex gap-8">
      <div style="width:250px">
        <div class="card" style="padding:8px">
          ${dbListHtml || '<p style="color:var(--text-dim)">No databases</p>'}
        </div>
      </div>
      <div style="flex:1" id="coll-detail">
        <div class="card"><p style="color:var(--text-dim)">Select a database</p></div>
      </div>
    </div>
  `;

  if (selectedDB) {
    await loadCollections(selectedDB);
  }
});

async function selectDB(name) {
  selectedDB = name;
  selectedColl = null;
  navigate();
}

async function loadCollections(db) {
  try {
    const data = await api(`/databases/${db}/collections`);
    const colls = data.collections || [];
    const container = document.getElementById(`colls-${db}`);
    if (container) {
      container.innerHTML = colls.map(c =>
        `<div class="tree-item ${selectedColl === c.name ? 'active' : ''}"
              onclick="selectColl('${esc(db)}','${esc(c.name)}')">${esc(c.name)}</div>`
      ).join('');
    }
  } catch (e) { /* ignore */ }
}

async function selectColl(db, coll) {
  selectedDB = db;
  selectedColl = coll;
  const detail = document.getElementById('coll-detail');
  if (!detail) return;

  try {
    const stats = await api(`/databases/${db}/collections/${coll}/stats`);
    detail.innerHTML = `
      <div class="card">
        <div class="flex-between mb-16">
          <h3>${esc(db)}.${esc(coll)}</h3>
          <div>
            <button class="btn-sm" onclick="viewDocuments('${esc(db)}','${esc(coll)}')">View Documents</button>
            <button class="btn-danger btn-sm" onclick="dropColl('${esc(db)}','${esc(coll)}')">Drop</button>
          </div>
        </div>
        <table>
          <tr><th>Documents</th><th>Data Size</th><th>Indexes</th></tr>
          <tr>
            <td>${formatNumber(stats.count)}</td>
            <td>${formatBytes(stats.size)}</td>
            <td>${stats.indexes}</td>
          </tr>
        </table>
      </div>
    `;
  } catch (e) {
    detail.innerHTML = `<div class="card"><p style="color:var(--danger)">${esc(e.message)}</p></div>`;
  }
}

function viewDocuments(db, coll) {
  location.hash = `/documents/${db}/${coll}`;
}

async function dropColl(db, coll) {
  if (!confirm(`Drop collection ${db}.${coll}?`)) return;
  try {
    await api(`/databases/${db}/collections/${coll}`, { method: 'DELETE' });
    selectedColl = null;
    navigate();
  } catch (e) {
    alert('Error: ' + e.message);
  }
}

function showCreateDB() {
  openModal('Create Collection', `
    <div class="form-group">
      <label>Database</label>
      <input id="new-db" placeholder="mydb">
    </div>
    <div class="form-group">
      <label>Collection</label>
      <input id="new-coll" placeholder="mycollection">
    </div>
    <button onclick="doCreateColl()">Create</button>
  `);
}

async function doCreateColl() {
  const db = document.getElementById('new-db').value;
  const coll = document.getElementById('new-coll').value;
  if (!db || !coll) return alert('Both fields required');
  try {
    await api(`/databases/${db}/collections`, {
      method: 'POST',
      body: JSON.stringify({ name: coll })
    });
    closeModal();
    navigate();
  } catch (e) {
    alert('Error: ' + e.message);
  }
}
