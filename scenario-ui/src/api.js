// Thin clients over the services' REST APIs and Kafka UI's API (all proxied by Vite, see vite.config.js).

async function request(method, url, body) {
  const res = await fetch(url, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    throw new Error(`${method} ${url} -> HTTP ${res.status} ${await res.text()}`);
  }
  return res.json();
}

const enc = encodeURIComponent;
const CLUSTER = '/kafka-ui/api/clusters/local';

// Avro's JSON encoding wraps a non-null union value in its type: {"string": "..."} for a nullable string,
// {"com.example.Rec": {...}} for a record. Unwrapped so the UI sees plain values.
const AVRO_TYPES = new Set(['string', 'int', 'long', 'float', 'double', 'boolean', 'bytes', 'array', 'map']);

function unwrapAvro(value) {
  if (Array.isArray(value)) return value.map(unwrapAvro);
  if (value === null || typeof value !== 'object') return value;
  const keys = Object.keys(value);
  if (keys.length === 1 && (AVRO_TYPES.has(keys[0]) || keys[0].includes('.'))) return unwrapAvro(value[keys[0]]);
  return Object.fromEntries(keys.map((k) => [k, unwrapAvro(value[k])]));
}

function parseJson(text) {
  try {
    return unwrapAvro(JSON.parse(text));
  } catch {
    return text;
  }
}

export const api = {
  initStock: (productId, quantity) =>
    request('POST', `/api/inventory/${enc(productId)}/init?quantity=${quantity}`),
  getStock: (productId) => request('GET', `/api/inventory/${enc(productId)}`),

  // items: [{ productId, quantity, unitPrice? }]
  createOrder: (customerId, items) =>
    request('POST', '/api/orders', {
      customerId,
      items: items.map((i) => ({ unitPrice: 10.0, ...i })),
    }),
  getOrder: (id) => request('GET', `/api/orders/${enc(id)}`),
  listOrders: () => request('GET', '/api/orders'),
  pendingOutbox: () => request('GET', '/api/orders/outbox/pending'),

  health: async (service) => {
    try {
      const res = await fetch(service === 'kafka-ui' ? '/kafka-ui/api/clusters' : `/health/${service}`);
      if (!res.ok) return 'DOWN';
      return service === 'kafka-ui' ? 'UP' : (await res.json()).status;
    } catch {
      return 'DOWN';
    }
  },

  /**
   * Records of `topic` whose key or value contains `filter`, oldest first. Kafka UI answers with a
   * server-sent event stream; the request ends once the topic has been scanned.
   */
  topicMessages: async (topic, filter) => {
    const res = await fetch(
      `${CLUSTER}/topics/${enc(topic)}/messages/v2?mode=EARLIEST&limit=100&stringFilter=${enc(filter)}`,
    );
    if (!res.ok) throw new Error(`Kafka UI: ${topic} -> HTTP ${res.status}`);
    return (await res.text())
      .split('\n')
      .filter((line) => line.startsWith('data:'))
      .map((line) => JSON.parse(line.slice(5)))
      .filter((event) => event.type === 'MESSAGE')
      .map(({ message: m }) => ({
        topic,
        partition: m.partition,
        offset: m.offset,
        timestamp: Date.parse(m.timestamp),
        key: m.key,
        value: parseJson(m.content),
        headers: m.headers ?? {},
      }))
      .sort((a, b) => a.timestamp - b.timestamp);
  },

  /** Committed offsets of a consumer group, as { 'topic:partition': nextOffsetToRead }. */
  groupOffsets: async (groupId) => {
    const res = await fetch(`${CLUSTER}/consumer-groups/${enc(groupId)}`);
    if (!res.ok) throw new Error(`Kafka UI: group ${groupId} -> HTTP ${res.status}`);
    const group = await res.json();
    return Object.fromEntries(
      (group.partitions ?? []).map((p) => [`${p.topic}:${p.partition}`, p.currentOffset ?? 0]),
    );
  },
};

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
