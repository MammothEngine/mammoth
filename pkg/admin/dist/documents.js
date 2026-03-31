// Documents Browser

let docSkip = 0;
const docLimit = 20;

route('/documents', async (el, hash) => {
  const parts = hash.replace('/documents', '').split('/').filter(Boolean);
  if (parts.length < 2) {
    el.innerHTML = '<div class="card"><p>Select a collection from Databases page.</p></div>';
    return;
  }
  const [db, coll] = parts;
  await loadDocPage(el, db, coll);
});

async function loadDocPage(el, db, coll) {
  const data = await api(`/databases/${db}/collections/${coll}/documents?limit=${docLimit}&skip=${docSkip}`);
  const docs = data.documents || [];

  let tableHtml = '';
  if (docs.length > 0) {
    // Get field names from first doc
    const first = typeof docs[0] === 'string' ? JSON.parse(docs[0]) : docs[0];
    const fields = first ? Object.keys(first).slice(0, 6) : [];

    tableHtml = `
      <table>
        <tr>${fields.map(f => `<th>${esc(f)}</th>`).join('')}<th></th></tr>
        ${docs.map((doc, i) => {
          const obj = typeof doc === 'string' ? JSON.parse(doc) : doc;
          return `<tr>
            ${fields.map(f => `<td>${esc(String(obj[f] ?? ''))}</td>`).join('')}
            <td><button class="btn-sm" onclick="viewDoc(${i})">View</button></td>
          </tr>`;
        }).join('')}
      </table>
    `;

    // Store docs for detail view
    window._currentDocs = docs;
  }

  el.innerHTML = `
    <div class="flex-between mb-16">
      <h2>${esc(db)}.${esc(coll)}</h2>
      <div class="flex gap-8">
        <input id="doc-filter" placeholder="Filter (JSON)" style="width:200px">
        <button onclick="showInsertDoc('${esc(db)}','${esc(coll)}')">Insert</button>
      </div>
    </div>
    <div class="card">
      ${tableHtml || '<p style="color:var(--text-dim)">No documents found</p>'}
      <div class="pagination">
        <button ${docSkip === 0 ? 'disabled' : ''} onclick="docSkip=${Math.max(0, docSkip - docLimit)};navigate()">Prev</button>
        <span>${docSkip + 1} - ${docSkip + docs.length} of ${data.count}</span>
        <button ${docs.length < docLimit ? 'disabled' : ''} onclick="docSkip=${docSkip + docLimit};navigate()">Next</button>
      </div>
    </div>
  `;
}

function viewDoc(index) {
  const doc = window._currentDocs[index];
  const obj = typeof doc === 'string' ? JSON.parse(doc) : doc;
  openModal('Document', `<div class="json-display">${esc(JSON.stringify(obj, null, 2))}</div>`);
}

function showInsertDoc(db, coll) {
  openModal('Insert Document', `
    <div class="form-group">
      <label>JSON Document</label>
      <textarea id="insert-doc" rows="10" placeholder='{"key": "value"}'></textarea>
    </div>
    <button onclick="doInsertDoc('${esc(db)}','${esc(coll)}')">Insert</button>
  `);
}

async function doInsertDoc(db, coll) {
  const raw = document.getElementById('insert-doc').value;
  try {
    JSON.parse(raw); // validate
    await api(`/databases/${db}/collections/${coll}/documents`, {
      method: 'POST',
      body: JSON.stringify({ document: JSON.parse(raw) })
    });
    closeModal();
    docSkip = 0;
    navigate();
  } catch (e) {
    alert('Error: ' + e.message);
  }
}
