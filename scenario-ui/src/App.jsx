import { useEffect, useState } from 'react';
import { api } from './api.js';
import { awaitSettled } from './journey.js';
import { scenarios } from './scenarios.js';
import Journey from './Journey.jsx';

const SERVICES = ['order', 'inventory', 'notification', 'kafka-ui'];

function usePolling(fn, intervalMs) {
  const [value, setValue] = useState(null);
  useEffect(() => {
    let alive = true;
    const tick = () => fn().then((v) => alive && setValue(v)).catch(() => {});
    tick();
    const id = setInterval(tick, intervalMs);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, [fn, intervalMs]);
  return value;
}

const fetchHealth = () => Promise.all(SERVICES.map(api.health));
const fetchOrders = async () =>
  (await api.listOrders()).sort((a, b) => Date.parse(b.createdAt) - Date.parse(a.createdAt)).slice(0, 25);

// The selected order lives in the URL hash, so a path can be reloaded or shared.
function useSelectedOrder() {
  const read = () => decodeURIComponent(window.location.hash.slice(1)) || null;
  const [selected, setSelected] = useState(read);
  useEffect(() => {
    const onHash = () => setSelected(read());
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);
  const select = (id) => {
    window.location.hash = id;
    setSelected(id);
  };
  return [selected, select];
}

export default function App() {
  const [selected, setSelected] = useSelectedOrder();
  const [results, setResults] = useState({});
  const health = usePolling(fetchHealth, 3000);
  const orders = usePolling(fetchOrders, 2000);
  const running = Object.values(results).some((r) => r.state === 'running');

  const run = async (scenario) => {
    const set = (r) => setResults((all) => ({ ...all, [scenario.id]: r }));
    set({ state: 'running' });
    try {
      const productId = scenario.product();
      await api.initStock(productId, scenario.stock);
      const order = await api.createOrder(scenario.customer, [{ productId, quantity: scenario.quantity }]);
      setSelected(order.id);
      set({ state: 'running', orderId: order.id });
      const journey = await awaitSettled(order.id);
      const checks = scenario.checks(journey.summary);
      set({ state: checks.every(([, ok]) => ok) ? 'passed' : 'failed', orderId: order.id, checks });
    } catch (e) {
      set({ state: 'failed', error: e.message });
    }
  };

  return (
    <div className="page">
      <header>
        <div>
          <h1>Order flow</h1>
          <p className="muted">Run a scenario and follow the order through the services and Kafka topics.</p>
        </div>
        <div className="health">
          {SERVICES.map((svc, i) => {
            const status = health?.[i];
            return (
              <span key={svc} className={`pill ${status === 'UP' ? 'up' : status ? 'down' : ''}`}>
                {svc} {status ?? '…'}
              </span>
            );
          })}
        </div>
      </header>

      <section className="scenarios">
        {scenarios.map((s) => {
          const r = results[s.id] ?? { state: 'idle' };
          return (
            <div key={s.id} className={`card scenario ${r.state}`}>
              <div className="row">
                <h2>{s.title}</h2>
                <span className={`badge ${r.state}`}>{r.state}</span>
              </div>
              <p>{s.story}</p>
              <p className="muted">Expected: {s.expected}</p>
              {r.checks && (
                <ul className="checks">
                  {r.checks.map(([label, ok]) => (
                    <li key={label} className={ok ? 'pass' : 'fail'}>
                      {label}
                    </li>
                  ))}
                </ul>
              )}
              {r.error && <p className="error">{r.error}</p>}
              <div className="row actions">
                <button className="primary" onClick={() => run(s)} disabled={running}>
                  {r.state === 'running' ? 'Running…' : 'Run'}
                </button>
                {r.orderId && (
                  <button className="link" onClick={() => setSelected(r.orderId)}>
                    show path
                  </button>
                )}
              </div>
            </div>
          );
        })}
      </section>

      <main>
        <aside>
          <ManualOrder onPlaced={setSelected} disabled={running} />
          <div className="card">
            <h2>Orders</h2>
            <ul className="orders">
              {orders?.map((o) => (
                <li key={o.id}>
                  <button className={o.id === selected ? 'selected' : ''} onClick={() => setSelected(o.id)}>
                    <span className={`badge ${o.status.toLowerCase()}`}>{o.status}</span>
                    <span className="order-line">
                      <span>{o.customerId}</span>
                      <span className="mono muted">
                        {o.items.map((i) => `${i.quantity}×${i.productId}`).join(', ')}
                      </span>
                    </span>
                    <span className="mono muted">{o.id.slice(0, 8)}</span>
                  </button>
                </li>
              ))}
              {orders?.length === 0 && <li className="muted">No orders yet.</li>}
            </ul>
          </div>
        </aside>
        <Journey orderId={selected} />
      </main>
    </div>
  );
}

function ManualOrder({ onPlaced, disabled }) {
  const [form, setForm] = useState({ productId: 'PROD-1', stock: 5, customerId: 'alice', quantity: 1 });
  const [message, setMessage] = useState(null);
  const set = (k) => (e) => setForm({ ...form, [k]: e.target.value });

  const act = (fn) => async (e) => {
    e.preventDefault();
    try {
      setMessage({ ok: true, text: await fn() });
    } catch (err) {
      setMessage({ ok: false, text: err.message });
    }
  };

  return (
    <div className="card">
      <h2>Place an order</h2>
      <form className="manual">
        <label>
          Product <input value={form.productId} onChange={set('productId')} />
        </label>
        <label>
          Stock <input type="number" min="0" value={form.stock} onChange={set('stock')} />
        </label>
        <button
          disabled={disabled}
          onClick={act(async () => {
            const s = await api.initStock(form.productId, Number(form.stock));
            return `${s.productId}: ${s.availableQuantity} available`;
          })}
        >
          Set stock
        </button>
        <label>
          Customer <input value={form.customerId} onChange={set('customerId')} />
        </label>
        <label>
          Quantity <input type="number" min="1" value={form.quantity} onChange={set('quantity')} />
        </label>
        <button
          className="primary"
          disabled={disabled}
          onClick={act(async () => {
            const o = await api.createOrder(form.customerId, [
              { productId: form.productId, quantity: Number(form.quantity) },
            ]);
            onPlaced(o.id);
            return `order ${o.id.slice(0, 8)} placed`;
          })}
        >
          Order
        </button>
      </form>
      <p className="muted hint">
        Customer prefix <span className="mono">flaky_</span> fails the notification twice,{' '}
        <span className="mono">invalid_</span> sends it to the DLT.
      </p>
      {message && <p className={message.ok ? 'ok' : 'error'}>{message.text}</p>}
    </div>
  );
}
