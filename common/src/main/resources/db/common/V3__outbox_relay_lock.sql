-- Single-row mutex for OutboxRelay: the relay instance holding this row's lock is the only one publishing.
CREATE TABLE outbox_relay_lock (
    id INTEGER PRIMARY KEY
);

INSERT INTO outbox_relay_lock (id) VALUES (1);
