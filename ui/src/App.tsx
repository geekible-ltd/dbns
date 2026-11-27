import { useEffect, useState } from 'react';
import { api } from './api';

type Nav = 'overview' | 'data' | 'query' | 'indexes' | 'stress' | 'admin';

export default function App() {
  const [nav, setNav] = useState<Nav>('overview');
  return (
    <>
      <nav>
        <h1>NDB</h1>
        {(['overview', 'data', 'query', 'indexes', 'stress', 'admin'] as Nav[]).map((n) => (
          <button key={n} className={nav === n ? 'on' : ''} onClick={() => setNav(n)}>
            {n === 'data' ? 'Data browser' : n === 'query' ? 'Query console' : n === 'stress' ? 'Stress test' : n.charAt(0).toUpperCase() + n.slice(1)}
          </button>
        ))}
      </nav>
      <main>
        {nav === 'overview' && <Overview />}
        {nav === 'data' && <DataBrowser />}
        {nav === 'query' && <QueryConsole />}
        {nav === 'indexes' && <IndexAdmin />}
        {nav === 'stress' && <StressPanel />}
        {nav === 'admin' && <Admin />}
      </main>
    </>
  );
}

function useFetch<T>(fn: () => Promise<T>, deps: unknown[]): [T | null, string | null, () => void] {
  const [v, setV] = useState<T | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [tick, setTick] = useState(0);
  useEffect(() => {
    let live = true;
    fn().then((r) => live && setV(r)).catch((e) => live && setErr(String(e)));
    return () => { live = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tick, ...deps]);
  return [v, err, () => setTick((t) => t + 1)];
}

// ---- Overview: cluster + node health (§15.8) --------------------------------

function Overview() {
  const [cluster, cerr] = useFetch(() => api.get('/cluster/status'), []);
  const [metrics, merr] = useFetch(() => api.get('/admin/metrics'), []);
  const [info] = useFetch(() => api.get('/admin/info'), []);
  if (cerr) return <p className="err">{cerr}</p>;
  if (merr) return <p className="err">{merr}</p>;
  const node = (cluster as any)?.node ?? {};
  const m = (metrics as any) ?? {};
  return (
    <>
      <h2>Cluster &amp; node health</h2>
      <div className="grid">
        <div className="card"><div className="lbl">node</div><div className="stat">{node.id ?? '—'}</div><div className="lbl">{node.role} · {node.state}</div></div>
        <div className="card"><div className="lbl">nodes</div><div className="stat">{(cluster as any)?.nodes ?? '—'}</div></div>
        <div className="card"><div className="lbl">commit seq</div><div className="stat">{m.engine?.commit_seq ?? '—'}</div></div>
        <div className="card"><div className="lbl">active txns</div><div className="stat">{m.engine?.active_transactions ?? '—'}</div></div>
        <div className="card"><div className="lbl">buffer pool</div><div className="stat">{m.buffer_pool?.resident_pages ?? '—'}/{m.buffer_pool?.frames ?? '—'}</div><div className="lbl">fetches {m.buffer_pool?.fetches ?? 0} · evictions {m.buffer_pool?.evictions ?? 0}</div></div>
        <div className="card"><div className="lbl">version</div><div className="stat">{(info as any)?.version ?? '—'}</div><div className="lbl">{(((info as any)?.features) ?? []).join(' ')}</div></div>
      </div>
      <h2>WAL &amp; storage</h2>
      <pre>{JSON.stringify(m.wal ?? {}, null, 2)}</pre>
    </>
  );
}

// ---- Data browser -----------------------------------------------------------

function DataBrowser() {
  const [db, setDb] = useState('');
  const [coll, setColl] = useState('');
  const [dbs, , refresh] = useFetch<string[]>(() => api.get('/databases').then((j: any) => j.databases ?? []), []);
  const [colls] = useFetch<string[]>(
    () => (db ? api.get(`/databases/${db}/collections`).then((j: any) => j.collections ?? []) : Promise.resolve([])),
    [db],
  );
  const [docs, setDocs] = useState<any[] | null>(null);
  const [err, setErr] = useState('');
  const [notice, setNotice] = useState('');
  const [newDb, setNewDb] = useState('');
  const [newColl, setNewColl] = useState('');
  const [editor, setEditor] = useState('{\n  "name": "",\n}');
  const [editing, setEditing] = useState<number | null>(null);

  const browse = async () => {
    setErr('');
    try {
      const j = await api.post(`/collections/${db}/${coll}/query`, { filter: {}, limit: 50 });
      setDocs(j.docs ?? []);
    } catch (e) { setErr(String(e)); setDocs(null); }
  };

  const submitDoc = async () => {
    setErr(''); setNotice('');
    let body: unknown;
    try { body = JSON.parse(editor); } catch { setErr('invalid JSON'); return; }
    try {
      if (editing === null) {
        const j = await api.post(`/collections/${db}/${coll}/documents`, body);
        setNotice(`inserted _id ${j._id}`);
      } else {
        await api.put(`/collections/${db}/${coll}/documents/${editing}`, body);
        setNotice(`replaced _id ${editing}`);
        setEditing(null);
      }
      await browse();
    } catch (e) { setErr(String(e)); }
  };

  const editDoc = (d: any) => {
    const { _id, ...rest } = d;
    setEditing(_id ?? null);
    setEditor(JSON.stringify(rest, null, 2));
    setErr(''); setNotice('');
  };

  const deleteDoc = async (id: number) => {
    setErr(''); setNotice('');
    try {
      await api.del(`/collections/${db}/${coll}/documents/${id}`);
      setNotice(`deleted _id ${id}`);
      if (editing === id) setEditing(null);
      await browse();
    } catch (e) { setErr(String(e)); }
  };

  return (
    <>
      <h2>Data browser</h2>
      <div className="card">
        <input placeholder="new database" value={newDb} onChange={(e) => setNewDb(e.target.value)} />
        <button className="act" onClick={async () => { await api.post('/databases', { name: newDb }); setNewDb(''); refresh(); }}>Create db</button>
        {db && <>
          <input placeholder="new collection" value={newColl} onChange={(e) => setNewColl(e.target.value)} />
          <button className="act" onClick={async () => { await api.post(`/databases/${db}/collections`, { name: newColl }); setNewColl(''); }}>Create collection</button>
        </>}
      </div>
      <table>
        <thead><tr><th>database</th><th>collection</th></tr></thead>
        <tbody>
          {(dbs ?? []).map((d) => (
            <tr key={d}>
              <td>
                <a className="link" onClick={() => { setDb(d); setColl(''); setDocs(null); }}>{d}</a>
              </td>
              <td>{db === d ? (colls ?? []).map((c) => (
                <a key={c} className="link" style={{ marginRight: 8 }} onClick={() => { setColl(c); }}>{c}</a>
              )) : null}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {coll && <button className="act" onClick={browse}>Browse {db}/{coll}</button>}

      {coll && (
        <div className="card">
          <h2 style={{ marginTop: 0 }}>{editing === null ? 'Add document' : `Edit document _id ${editing}`}</h2>
          <textarea value={editor} onChange={(e) => setEditor(e.target.value)} spellCheck={false} />
          <div>
            <button className="act" onClick={submitDoc}>
              {editing === null ? 'Insert' : 'Save (replace)'}
            </button>
            {editing !== null && (
              <button className="act" onClick={() => { setEditing(null); setEditor('{\n  "name": "",\n}'); }}>
                Cancel edit
              </button>
            )}
          </div>
        </div>
      )}

      {err && <p className="err">{err}</p>}
      {notice && <p className="ok">{notice}</p>}
      {docs && <Docs docs={docs} onEdit={editDoc} onDelete={deleteDoc} />}
    </>
  );
}

function Docs({ docs, onEdit, onDelete }: { docs: any[]; onEdit: (d: any) => void; onDelete: (id: number) => void }) {
  return (
    <table>
      <thead><tr><th>_id</th><th>document</th><th></th></tr></thead>
      <tbody>
        {docs.map((d, i) => (
          <tr key={d._id ?? i}>
            <td>{d._id}</td>
            <td><pre style={{ margin: 0 }}>{JSON.stringify(d)}</pre></td>
            <td style={{ whiteSpace: 'nowrap' }}>
              <button className="act" onClick={() => onEdit(d)}>edit</button>
              <button className="act danger" onClick={() => onDelete(d._id)}>del</button>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

// ---- Query console -----------------------------------------------------------

function QueryConsole() {
  const [coll, setColl] = useState('app/users');
  const [kind, setKind] = useState('query');
  const [body, setBody] = useState('{"filter": {"age": {"$gte": 30}}}');
  const [out, setOut] = useState('');
  const [err, setErr] = useState('');

  const run = async () => {
    setErr(''); setOut('');
    const [db, c] = coll.split('/');
    const parsed = JSON.parse(body);
    try {
      let j: any;
      if (kind === 'explain') j = await api.post(`/collections/${db}/${c}/explain`, parsed);
      else if (kind === 'aggregate') j = await api.post(`/collections/${db}/${c}/aggregate`, parsed);
      else if (kind === 'search') j = await api.post(`/collections/${db}/${c}/search`, parsed);
      else if (kind === 'vector') j = await api.post(`/collections/${db}/${c}/vector-search`, parsed);
      else if (kind === 'hybrid') j = await api.post(`/collections/${db}/${c}/hybrid-search`, parsed);
      else j = await api.post(`/collections/${db}/${c}/query`, parsed);
      setOut(JSON.stringify(j, null, 2));
    } catch (e) { setErr(String(e)); }
  };

  return (
    <>
      <h2>Query console</h2>
      <div className="card">
        <input value={coll} onChange={(e) => setColl(e.target.value)} style={{ width: 180 }} />{' '}
        <select value={kind} onChange={(e) => setKind(e.target.value)}>
          <option value="query">find</option>
          <option value="aggregate">aggregate</option>
          <option value="search">text search</option>
          <option value="vector">vector search</option>
          <option value="hybrid">hybrid search</option>
          <option value="explain">explain</option>
        </select>
      </div>
      <textarea value={body} onChange={(e) => setBody(e.target.value)} spellCheck={false} />
      <div><button className="act" onClick={run}>Run</button></div>
      {err && <p className="err">{err}</p>}
      {out && <pre>{out}</pre>}
    </>
  );
}

// ---- Index admin ---------------------------------------------------------------

function IndexAdmin() {
  const [coll, setColl] = useState('app/users');
  const [indexes, setIndexes] = useState<any[]>([]);
  const [err, setErr] = useState('');
  const [def, setDef] = useState('{"name":"by_age","kind":"secondary","fields":["age"]}');

  const refresh = async () => {
    setErr('');
    const [db, c] = coll.split('/');
    try {
      const j = await api.get(`/collections/${db}/${c}/indexes`);
      setIndexes(j.indexes ?? []);
    } catch (e) { setErr(String(e)); setIndexes([]); }
  };

  return (
    <>
      <h2>Index administration</h2>
      <div className="card">
        <input value={coll} onChange={(e) => setColl(e.target.value)} style={{ width: 180 }} />
        <button className="act" onClick={refresh}>List indexes</button>
      </div>
      <table>
        <thead><tr><th>name</th><th>kind</th><th>fields</th><th>actions</th></tr></thead>
        <tbody>
          {indexes.map((i) => (
            <tr key={i.name}>
              <td>{i.name}</td><td>{i.kind}</td><td>{(i.fields ?? [i.field]).join(', ')}</td>
              <td>
                <button className="act" onClick={async () => {
                  const [db, c] = coll.split('/');
                  await api.post(`/collections/${db}/${c}/indexes/${i.name}/rebuild`, {}).catch((e) => setErr(String(e)));
                }}>rebuild</button>
                <button className="act danger" onClick={async () => {
                  const [db, c] = coll.split('/');
                  await api.del(`/collections/${db}/${c}/indexes/${i.name}`).then(refresh).catch((e) => setErr(String(e)));
                }}>drop</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <h2>Create index</h2>
      <textarea value={def} onChange={(e) => setDef(e.target.value)} spellCheck={false} />
      <div><button className="act" onClick={async () => {
        const [db, c] = coll.split('/');
        await api.post(`/collections/${db}/${c}/indexes`, JSON.parse(def)).then(refresh).catch((e) => setErr(String(e)));
      }}>Create</button></div>
      {err && <p className="err">{err}</p>}
    </>
  );
}

// ---- Admin ---------------------------------------------------------------------

function Admin() {
  const [j, err, refresh] = useFetch(async () => {
    const [metrics, storage, txn, info, config] = await Promise.all([
      api.get('/admin/metrics'), api.get('/admin/storage'), api.get('/admin/txn'),
      api.get('/admin/info'), api.get('/admin/config'),
    ]);
    return { metrics, storage, txn, info, config };
  }, []);
  if (err) return <p className="err">{err}</p>;
  if (!j) return <p>loading…</p>;
  return (
    <>
      <h2>Administration</h2>
      <button className="act" onClick={refresh}>Refresh</button>
      {(['metrics', 'storage', 'txn', 'info', 'config'] as const).map((k) => (
        <div key={k} className="card"><h2 style={{ marginTop: 0 }}>{k}</h2><pre>{JSON.stringify((j as any)[k], null, 2)}</pre></div>
      ))}
    </>
  );
}

// ---- Stress test ----------------------------------------------------------
// Drives a run on the server's own gRPC listener (full HTTP/2 + prost path)
// via POST /admin/stress; polls GET /admin/stress for live per-op
// throughput + log-bucketed latency percentiles.

const OP_KEYS = ['insert', 'read', 'update', 'delete', 'find', 'text', 'vector', 'hybrid'] as const;
const DEFAULT_MIX: Record<string, number> = { insert: 10, read: 30, update: 10, delete: 5, find: 20, text: 10, vector: 10, hybrid: 5 };

function StressPanel() {
  const [workers, setWorkers] = useState(8);
  const [duration, setDuration] = useState(30);
  const [seed, setSeed] = useState(1000);
  const [mix, setMix] = useState<Record<string, number>>({ ...DEFAULT_MIX });
  const [snap, setSnap] = useState<any>(null);
  const [err, setErr] = useState('');

  const running = snap && ['connecting', 'seeding', 'running'].includes(snap.phase);

  const poll = async () => {
    try { setSnap(await api.get('/admin/stress')); } catch (e) { setErr(String(e)); }
  };
  useEffect(() => {
    poll();
    if (!running) return;
    const t = setInterval(poll, 1000);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [running]);

  const start = async () => {
    setErr('');
    try {
      setSnap(await api.post('/admin/stress', {
        workers, duration_secs: duration, seed_docs: seed, mix,
      }));
    } catch (e) { setErr(String(e)); }
  };
  const stop = async () => {
    try { await api.post('/admin/stress/stop', {}); } catch (e) { setErr(String(e)); }
    await poll();
  };

  const rows: [string, any][] = snap?.ops ? Object.entries(snap.ops) : [];
  const maxOps = Math.max(1, ...rows.map(([, s]) => s.ops_per_sec ?? 0));

  return (
    <>
      <h2>Stress test <span className="lbl">— over the node&rsquo;s own gRPC listener</span></h2>
      <div className="card">
        {(['workers', 'duration (s)', 'seed docs'] as const).map((lbl, i) => {
          const [v, sv] = [workers, duration, seed][i] !== undefined
            ? [[workers, setWorkers], [duration, setDuration], [seed, setSeed]][i] as [number, (n: number) => void]
            : [0, () => {}];
          return (
            <span key={lbl} style={{ marginRight: 16 }}>
              <span className="lbl">{lbl} </span>
              <input type="number" value={v} style={{ width: 80 }}
                onChange={(e) => sv(Number(e.target.value))} />
            </span>
          );
        })}
      </div>
      <div className="card">
        <div className="lbl" style={{ marginBottom: 6 }}>op mix (weights — 0 disables)</div>
        {OP_KEYS.map((k) => (
          <span key={k} style={{ marginRight: 14 }}>
            <span className="lbl">{k} </span>
            <input type="number" value={mix[k]} style={{ width: 60 }}
              onChange={(e) => setMix({ ...mix, [k]: Number(e.target.value) })} />
          </span>
        ))}
      </div>
      <div>
        <button className="act" onClick={start} disabled={!!running}>Start</button>
        <button className="act danger" onClick={stop} disabled={!running}>Stop</button>
        <button className="act" onClick={poll}>Refresh</button>
      </div>
      {err && <p className="err">{err}</p>}

      {snap && snap.phase !== 'idle' && (
        <>
          <div className="card">
            <span className={running ? 'ok' : ''}>
              phase: <b>{snap.phase}</b> · elapsed {snap.elapsed_secs}s · total {snap.total_ops} ops
              {snap.total_errors ? ` · ${snap.total_errors} errors` : ''}
            </span>
            {snap.failure && <p className="err">{snap.failure}</p>}
            {snap.last_error && <p className="err">last error: {snap.last_error}</p>}
          </div>
          <table>
            <thead><tr>
              <th>op</th><th>count</th><th>errors</th><th>misses</th>
              <th>ops/s</th><th>avg ms</th><th>p50</th><th>p95</th><th>p99</th><th>max</th><th></th>
            </tr></thead>
            <tbody>
              {rows.map(([op, s]) => (
                <tr key={op}>
                  <td>{op}</td>
                  <td>{s.count}</td>
                  <td className={s.errors ? 'err' : ''}>{s.errors}</td>
                  <td>{s.misses}</td>
                  <td>{s.ops_per_sec}</td>
                  <td>{s.avg_ms?.toFixed?.(2) ?? s.avg_ms}</td>
                  <td>{s.p50_ms}</td>
                  <td>{s.p95_ms}</td>
                  <td>{s.p99_ms}</td>
                  <td>{s.max_ms}</td>
                  <td style={{ width: '30%' }}>
                    <div style={{ background: '#1f5fbf', height: 8, borderRadius: 3,
                      width: `${((s.ops_per_sec ?? 0) / maxOps) * 100}%` }} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="lbl">latency in ms (log-bucketed histogram upper bounds); misses = get/update/delete on already-deleted ids (real work, expected NotFound)</p>
        </>
      )}
      {(!snap || snap.phase === 'idle') && <p className="lbl">no run yet — configure the mix and hit Start</p>}
    </>
  );
}
