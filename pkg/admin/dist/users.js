// User Management

route('/users', async (el) => {
  const data = await api('/users');
  const users = data.users || [];

  el.innerHTML = `
    <div class="flex-between mb-16">
      <h2>Users</h2>
      <button onclick="showCreateUser()">Create User</button>
    </div>
    <div class="card">
      ${users.length > 0 ? `
        <table>
          <tr><th>Username</th><th>Database</th><th>Created</th><th></th></tr>
          ${users.map(u => `
            <tr>
              <td>${esc(u.Username || u.username || '')}</td>
              <td>${esc(u.AuthDB || u.authDB || '')}</td>
              <td>${new Date((u.CreatedAt || u.createdAt || 0) * 1000).toLocaleString()}</td>
              <td><button class="btn-danger btn-sm" onclick="deleteUser('${esc(u.Username || u.username)}')">Delete</button></td>
            </tr>
          `).join('')}
        </table>
      ` : '<p style="color:var(--text-dim)">No users found. Create one to get started.</p>'}
    </div>
  `;
});

function showCreateUser() {
  openModal('Create User', `
    <div class="form-group">
      <label>Username</label>
      <input id="user-name" placeholder="alice">
    </div>
    <div class="form-group">
      <label>Password</label>
      <input id="user-pass" type="password" placeholder="secret">
    </div>
    <div class="form-group">
      <label>Database</label>
      <input id="user-db" placeholder="admin">
    </div>
    <button onclick="doCreateUser()">Create</button>
  `);
}

async function doCreateUser() {
  const username = document.getElementById('user-name').value;
  const password = document.getElementById('user-pass').value;
  const db = document.getElementById('user-db').value || 'admin';
  if (!username || !password) return alert('Username and password required');
  try {
    await api('/users', {
      method: 'POST',
      body: JSON.stringify({ username, password, db })
    });
    closeModal();
    navigate();
  } catch (e) {
    alert('Error: ' + e.message);
  }
}

async function deleteUser(username) {
  if (!confirm(`Delete user "${username}"?`)) return;
  try {
    await api(`/users/${username}`, { method: 'DELETE' });
    navigate();
  } catch (e) {
    alert('Error: ' + e.message);
  }
}
