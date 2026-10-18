// Each run uses a fresh product id, so scenarios are independent and can be re-run in any order.
const suffix = () => Math.random().toString(36).slice(2, 6).toUpperCase();

const stockIs = (summary, available, reserved) =>
  summary.stock[0]?.availableQuantity === available && summary.stock[0]?.reservedQuantity === reserved;

/**
 * setup: stock to create before ordering. order: what the customer orders.
 * checks(summary): [label, passed] pairs, evaluated once the order has settled (see journey.js).
 */
export const scenarios = [
  {
    id: 'happy-path',
    title: 'Happy path',
    story: 'Stock is 5. alice orders 2.',
    expected: 'CONFIRMED, 3 left in stock, alice notified on the first try.',
    product: () => `MUG-${suffix()}`,
    stock: 5,
    customer: 'alice',
    quantity: 2,
    checks: (s) => [
      ['order CONFIRMED', s.status === 'CONFIRMED'],
      ['stock 3 available, 2 reserved', stockIs(s, 3, 2)],
      ['notified on the first attempt', s.notified && s.retries === 0],
    ],
  },
  {
    id: 'out-of-stock',
    title: 'Not enough stock',
    story: 'Stock is 1. bob orders 3.',
    expected: 'REJECTED by inventory, stock untouched. The order event is still notified.',
    product: () => `LAMP-${suffix()}`,
    stock: 1,
    customer: 'bob',
    quantity: 3,
    checks: (s) => [
      ['order REJECTED', s.status === 'REJECTED'],
      ['stock untouched (1 available, 0 reserved)', stockIs(s, 1, 0)],
      ['notified', s.notified],
    ],
  },
  {
    id: 'flaky-notification',
    title: 'Flaky notifications',
    story: 'flaky_carol orders 1. The notification provider fails twice.',
    expected: 'CONFIRMED. Notification retried twice via the retry topic, then sent. The saga is not delayed.',
    product: () => `BOOK-${suffix()}`,
    stock: 5,
    customer: 'flaky_carol',
    quantity: 1,
    checks: (s) => [
      ['order CONFIRMED', s.status === 'CONFIRMED'],
      ['2 failed attempts on orders.events-notification-retry', s.retries === 2],
      ['notified on attempt 3', s.notified && !s.deadLettered],
    ],
  },
];
