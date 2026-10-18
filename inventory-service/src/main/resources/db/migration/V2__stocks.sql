CREATE TABLE stocks (
    product_id         VARCHAR(50) PRIMARY KEY,
    available_quantity INTEGER NOT NULL CHECK (available_quantity >= 0),
    reserved_quantity  INTEGER NOT NULL DEFAULT 0
);
