-- Database per service: each service owns its tables (orders, stocks, outbox, processed_events).
CREATE DATABASE order_db;
CREATE DATABASE inventory_db;
CREATE DATABASE notification_db;

-- Used by `make test` only, so tests never share data (or outbox rows) with running services.
CREATE DATABASE order_test;
CREATE DATABASE inventory_test;
CREATE DATABASE notification_test;
