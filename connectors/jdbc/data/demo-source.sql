-- demo-source.sql — the source tables the JDBC page's screens read.
--
-- They live in the hub's OWN PostgreSQL so the page can be replayed with nothing but the
-- self-host stack (`deploy/selfhost/up.sh`). A real integration points at the ERP instead;
-- nothing else in the manifests changes.
--
--   docker exec -i lumnik-postgres-1 psql -U lumnik -d lumnik < docs/connectors/jdbc/data/demo-source.sql
--
-- The two GRANTs are the only ones a lumnik connection user ever needs: USAGE on the schema,
-- SELECT on the tables. Nothing in the jdbc connector writes to a source.

CREATE SCHEMA IF NOT EXISTS demo;

-- The view goes first: it reads demo.invoices, and Postgres refuses to drop a table a view
-- depends on — so a second run of this script would stop here.
DROP VIEW IF EXISTS demo.v_open_invoices;

DROP TABLE IF EXISTS demo.invoices;
CREATE TABLE demo.invoices (
    invoice_id bigint PRIMARY KEY,
    customer   text           NOT NULL,
    total      numeric(12,2)  NOT NULL,
    status     text           NOT NULL,
    updated_at timestamp      NOT NULL
);

INSERT INTO demo.invoices VALUES
    (1001, 'Northwind Ltd', 1250.00, 'paid',    '2026-09-01 09:15:00'),
    (1002, 'Contoso SA',     480.50, 'open',    '2026-09-01 11:40:00'),
    (1003, 'Fabrikam Oy',   2310.75, 'open',    '2026-09-02 08:05:00'),
    (1004, 'Adventure BV',   199.90, 'draft',   '2026-09-02 16:20:00');

-- A table whose primary key is called `id` — one of the seven names the hub reserves on its
-- own tables. See "Reserved column names" on the page.
DROP TABLE IF EXISTS demo.price_list;
CREATE TABLE demo.price_list (
    id    bigint PRIMARY KEY,
    sku   text    NOT NULL,
    price numeric(10,2) NOT NULL
);

INSERT INTO demo.price_list VALUES
    (1, 'PUMP-075', 349.00),
    (2, 'HOSE-32M',  59.90),
    (3, 'FILT-A1',  129.50);

-- Two more tables for the guided flow: `warehouses` is the one it adds, `audit_log` has no
-- primary key, which is what the guided add refuses.
DROP TABLE IF EXISTS demo.warehouses;
CREATE TABLE demo.warehouses (
    warehouse_id integer PRIMARY KEY,
    code         text      NOT NULL,
    city         text      NOT NULL,
    updated_at   timestamp NOT NULL
);

INSERT INTO demo.warehouses VALUES
    (1, 'W-LYS', 'Lyon',      '2026-09-01 08:00:00'),
    (2, 'W-MRS', 'Marseille', '2026-09-03 08:00:00');

DROP TABLE IF EXISTS demo.audit_log;
CREATE TABLE demo.audit_log (
    at     timestamp NOT NULL,
    actor  text,
    action text
);

INSERT INTO demo.audit_log VALUES ('2026-09-01 06:00:00', 'system', 'boot');

-- A COMPOSITE primary key: 100 orders of seven lines each, 700 rows. `pk_columns` is read
-- whole, so the pair (order_id, line_no) is what the reader orders and resumes on and no
-- order is split by a chunk boundary. See "Per-table keys" on the page.
DROP TABLE IF EXISTS demo.order_lines;
CREATE TABLE demo.order_lines (
    order_id integer NOT NULL,
    line_no  integer NOT NULL,
    sku      text,
    PRIMARY KEY (order_id, line_no)
);

INSERT INTO demo.order_lines
SELECT o, l, 'SKU-' || o || '-' || l
FROM generate_series(1, 100) AS o, generate_series(1, 7) AS l;

-- The view the page reads in "Reading a view": the invoices that are not paid.
CREATE VIEW demo.v_open_invoices AS
    SELECT invoice_id, customer, total, updated_at FROM demo.invoices WHERE status <> 'paid';

GRANT USAGE ON SCHEMA demo TO lumnik_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA demo TO lumnik_readonly;
ANALYZE demo.invoices;
ANALYZE demo.price_list;
ANALYZE demo.warehouses;
ANALYZE demo.audit_log;
ANALYZE demo.order_lines;
