-- ─────────────────────────────────────────────────────────────
-- CDC Pipeline: Final Production Schema (Fixed & Optimized)
-- ─────────────────────────────────────────────────────────────

-- 1. EXTENSIONS
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- 2. SCHEMA & PERMISSIONS
CREATE SCHEMA IF NOT EXISTS ecommerce;

-- แก้ไข: ให้สิทธิ์ User 'admin' (ที่สร้างจาก Docker) ทำงานได้
ALTER USER admin WITH REPLICATION;
GRANT ALL PRIVILEGES ON SCHEMA ecommerce TO admin;

-- 3. TABLES
-- Customers
CREATE TABLE IF NOT EXISTS ecommerce.customers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(50),
    tier VARCHAR(50) DEFAULT 'bronze' CHECK (tier IN ('bronze', 'silver', 'gold', 'platinum')),
    total_spent NUMERIC(10, 2) DEFAULT 0.00,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Products
CREATE TABLE IF NOT EXISTS ecommerce.products (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sku VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    cost NUMERIC(10, 2) NOT NULL CHECK (cost >= 0),
    is_available BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Orders
CREATE TABLE IF NOT EXISTS ecommerce.orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_number VARCHAR(50) UNIQUE NOT NULL,
    customer_id UUID NOT NULL REFERENCES ecommerce.customers(id),
    status VARCHAR(50) DEFAULT 'pending',
    total NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Inventory
CREATE TABLE IF NOT EXISTS ecommerce.inventory (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    product_id UUID NOT NULL REFERENCES ecommerce.products(id),
    quantity INTEGER NOT NULL DEFAULT 0,
    available INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- CDC Utils
CREATE TABLE IF NOT EXISTS ecommerce.cdc_heartbeat (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4. LOGICAL REPLICATION SETUP (สำคัญมาก!)
-- Debezium ต้องการ REPLICA IDENTITY FULL เพื่ออ่านค่า Before/After image
ALTER TABLE ecommerce.customers REPLICA IDENTITY FULL;
ALTER TABLE ecommerce.products  REPLICA IDENTITY FULL;
ALTER TABLE ecommerce.orders    REPLICA IDENTITY FULL;
ALTER TABLE ecommerce.inventory REPLICA IDENTITY FULL;

-- สร้าง Publication (ลบของเก่าถ้ามีเพื่อป้องกัน Conflict)
DROP PUBLICATION IF EXISTS cdc_publication;
CREATE PUBLICATION cdc_publication FOR TABLE 
    ecommerce.customers, 
    ecommerce.products, 
    ecommerce.orders, 
    ecommerce.inventory,
    ecommerce.cdc_heartbeat;

-- 5. FUNCTIONS & TRIGGERS
CREATE OR REPLACE FUNCTION ecommerce.update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_customers_ts BEFORE UPDATE ON ecommerce.customers FOR EACH ROW EXECUTE FUNCTION ecommerce.update_timestamp();
CREATE TRIGGER trg_update_products_ts  BEFORE UPDATE ON ecommerce.products  FOR EACH ROW EXECUTE FUNCTION ecommerce.update_timestamp();
CREATE TRIGGER trg_update_orders_ts    BEFORE UPDATE ON ecommerce.orders    FOR EACH ROW EXECUTE FUNCTION ecommerce.update_timestamp();
CREATE TRIGGER trg_update_inventory_ts BEFORE UPDATE ON ecommerce.inventory FOR EACH ROW EXECUTE FUNCTION ecommerce.update_timestamp();

-- 6. SEED DATA
INSERT INTO ecommerce.products (sku, name, category, price, cost) VALUES
('LAP-M3', 'MacBook M3', 'Electronics', 1500.00, 1000.00),
('MOU-WL', 'Wireless Mouse', 'Accessories', 50.00, 20.00);

INSERT INTO ecommerce.customers (first_name, last_name, email, tier) VALUES
('John', 'Doe', 'john@example.com', 'gold');

INSERT INTO ecommerce.cdc_heartbeat (id, ts) VALUES (1, NOW());

SELECT 'Schema ready for CDC' as status;