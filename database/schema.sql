
CREATE TABLE creators (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    storage_limit_gb INTEGER DEFAULT 512,
    storage_used_gb INTEGER DEFAULT 0,
    commission_rate INTEGER DEFAULT 20,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE videos (
    id SERIAL PRIMARY KEY,
    creator_id INTEGER REFERENCES creators(id),
    location TEXT,
    recorded_at TIMESTAMP,
    video_url TEXT,
    thumbnail_url TEXT,
    file_size BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE batches (
    id SERIAL PRIMARY KEY,
    creator_id INTEGER REFERENCES creators(id),
    total_size BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    buyer_email TEXT,
    total_price NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE downloads (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id),
    token TEXT,
    expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE creator_click_stats (
    id SERIAL PRIMARY KEY,
    creator_id INTEGER REFERENCES creators(id),
    clicks_today INTEGER DEFAULT 0,
    clicks_week INTEGER DEFAULT 0,
    clicks_month INTEGER DEFAULT 0,
    clicks_lifetime INTEGER DEFAULT 0
);
