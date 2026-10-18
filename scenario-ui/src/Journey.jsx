import { Fragment, useEffect, useState } from 'react';
import { describe, snapshot } from './journey.js';

const ICON = { done: '✓', failed: '✗', warn: '!', active: '', pending: '' };

// Step texts come from Kafka payloads; never let an unexpected shape crash the render.
const text = (v) => (v != null && typeof v === 'object' ? JSON.stringify(v) : v);

/** Polls the selected order until it settles, then stops. */
function useJourney(orderId) {
  const [journey, setJourney] = useState(null);
  const [error, setError] = useState(null);
  useEffect(() => {
    if (!orderId) return undefined;
    let alive = true;
    let timer;
    setJourney(null);
    setError(null);
    const tick = async () => {
      try {
        const j = describe(await snapshot(orderId));
        if (!alive) return;
        setJourney(j);
        setError(null);
        if (j.settled) return;
      } catch (e) {
        if (alive) setError(e.message);
      }
      if (alive) timer = setTimeout(tick, 700);
    };
    tick();
    return () => {
      alive = false;
      clearTimeout(timer);
    };
  }, [orderId]);
  return [journey, error];
}

function Step({ step }) {
  return (
    <div className={`step ${step.state} ${step.kind ?? ''}`}>
      <div className="step-head">
        <span className="dot">{ICON[step.state]}</span>
        <span className="service">{step.service}</span>
      </div>
      <div className="step-title">{text(step.title)}</div>
      {step.detail && <div className="step-detail">{text(step.detail)}</div>}
      {step.where && <div className="step-where">{step.where}</div>}
    </div>
  );
}

function Lane({ label, hint, steps, lead }) {
  return (
    <div className="lane">
      <div className="lane-label">
        {label}
        {hint && <span>{hint}</span>}
      </div>
      <div className="track">
        {lead}
        {steps.map((s, i) => (
          <Fragment key={s.key}>
            {(i > 0 || lead) && <div className={`arrow ${s.state}`} />}
            <Step step={s} />
          </Fragment>
        ))}
      </div>
    </div>
  );
}

const ms = (d) => (d == null ? '–' : d < 1000 ? `+${Math.round(d)} ms` : `+${(d / 1000).toFixed(2)} s`);

export default function Journey({ orderId }) {
  const [journey, error] = useJourney(orderId);

  if (!orderId) {
    return (
      <div className="card empty">
        <p>Run a scenario or place an order to see its path through the system.</p>
      </div>
    );
  }
  if (!journey) {
    return <div className="card empty">{error ? <p className="error">{error}</p> : <p className="muted">Loading…</p>}</div>;
  }

  const { order, saga, notification, timeline, summary } = journey;
  return (
    <div className="card journey">
      <div className="journey-head">
        <div>
          <h2>
            Order <span className="mono">{order.id.slice(0, 8)}</span>
          </h2>
          <p className="muted">
            {order.customerId} · {order.items.map((i) => `${i.quantity} × ${i.productId}`).join(', ')} · total{' '}
            {order.totalAmount.toFixed(2)}
          </p>
        </div>
        <div className="journey-status">
          <span className={`badge ${order.status.toLowerCase()}`}>{order.status}</span>
          <span className="muted">{journey.settled ? 'settled' : 'in flight…'}</span>
        </div>
      </div>

      {journey.kafkaError && (
        <p className="error">Kafka UI unreachable, Kafka steps are unknown: {journey.kafkaError}</p>
      )}

      <Lane label="Order saga" steps={saga} />
      <Lane
        label="Notification"
        hint="its own consumer group on orders.events, in parallel with the saga"
        steps={notification}
        lead={<div className={`step topic mini ${journey.ordersTopicStep.state}`}>orders.events</div>}
      />

      <div className="stock-now">
        {summary.stock.map((s) => (
          <span key={s.productId} className="mono">
            {s.productId}:{' '}
            {s.missing ? 'unknown product' : `${s.availableQuantity} available · ${s.reservedQuantity} reserved`}
          </span>
        ))}
      </div>

      <h3>What happened, in order</h3>
      <ol className="timeline">
        {timeline.map((s) => (
          <li key={s.key} className={s.state}>
            <span className="delta mono">
              {s.approx && s.delta != null ? '≈ ' : ''}
              {ms(s.delta)}
            </span>
            <span className="service">{s.service}</span>
            <span>
              <strong>{text(s.title)}</strong>
              {s.detail && <span className="muted"> · {text(s.detail)}</span>}
              {s.where && <span className="mono where"> {s.where}</span>}
            </span>
          </li>
        ))}
        {!journey.settled && <li className="active muted">waiting for the next step…</li>}
      </ol>
      <p className="muted hint">
        Times are relative to the order's creation. Kafka steps use the record timestamps; the final status
        is when this page first saw it (≈). A successful notification leaves no record: it is inferred from the
        notification-service consumer group's committed offset, so it has no time (–).
      </p>
    </div>
  );
}
