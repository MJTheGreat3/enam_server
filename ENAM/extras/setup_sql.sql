-- ======================================================
-- DROP all 9 tables if they exist
-- ======================================================

DROP TABLE IF EXISTS announcements;
DROP TABLE IF EXISTS block_deals;
DROP TABLE IF EXISTS bulk_deals;
DROP TABLE IF EXISTS corp_actions;
DROP TABLE IF EXISTS deliv_deviation;
DROP TABLE IF EXISTS insider_trading;
DROP TABLE IF EXISTS master;
DROP TABLE IF EXISTS vol_deviation;
DROP TABLE IF EXISTS tagging;
DROP TABLE IF EXISTS last_updated;
DROP TABLE IF EXISTS news;
DROP TABLE IF EXISTS symbols;
DROP SEQUENCE IF EXISTS symbols_tag_id_seq;
DROP FUNCTION IF EXISTS set_symbols_tag_id();
DROP TRIGGER IF EXISTS symbols_tag_id_trigger ON symbols;

-- ======================================================
-- CREATE TABLES
-- ======================================================

CREATE SEQUENCE symbols_tag_id_seq START 1;

CREATE TABLE symbols (
    Tag_ID TEXT PRIMARY KEY,
    ISIN TEXT UNIQUE,
    Symbol TEXT,
    Name TEXT,
    Alias TEXT,
    Status BOOLEAN DEFAULT FALSE,
    last_scraped TIMESTAMP,
    CONSTRAINT unique_symbol UNIQUE (symbol)
);

CREATE OR REPLACE FUNCTION set_symbols_tag_id()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.Tag_ID IS NULL THEN
        NEW.Tag_ID := 'T' || LPAD(nextval('symbols_tag_id_seq')::TEXT, 4, '0');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER symbols_tag_id_trigger
BEFORE INSERT ON symbols
FOR EACH ROW
EXECUTE FUNCTION set_symbols_tag_id();

-- 1️⃣ announcements (as before)
CREATE TABLE announcements (
    id SERIAL PRIMARY KEY,
    Stock TEXT,
    Subject TEXT,
    Announcement TEXT,
    Attachment TEXT,
    Time TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock) REFERENCES symbols(symbol),
    CONSTRAINT unique_announcement UNIQUE (stock, subject, time)
);

-- 2️⃣ block_deals (all TEXT)
CREATE TABLE block_deals (
    Source TEXT,
    Deal_Date TEXT,
    Security_Name TEXT,
    Client_Name TEXT,
    Deal_Type TEXT,
    Quantity TEXT,
    Trade_Price TEXT,
    PRIMARY KEY (Source, Deal_Date, Security_Name, Client_Name, Deal_Type, Quantity, Trade_Price)
);

-- 3️⃣ bulk_deals (all TEXT)
CREATE TABLE bulk_deals (
    Source TEXT,
    Deal_Date TEXT,
    Security_Name TEXT,
    Client_Name TEXT,
    Deal_Type TEXT,
    Quantity TEXT,
    Price TEXT,
    PRIMARY KEY (Source, Deal_Date, Security_Name, Client_Name, Deal_Type, Quantity, Price)
);

-- 4️⃣ corp_actions (as before)
CREATE TABLE corp_actions (
    Security_Code TEXT,
    Security_Name TEXT,
    Company_Name TEXT,
    Ex_Date DATE,
    Purpose TEXT,
    Record_Date DATE,
    BC_Start_Date DATE,
    BC_End_Date DATE,
    ND_Start_Date DATE,
    ND_End_Date DATE,
    Actual_Payment_Date DATE,
    BC_End_Date_2 DATE,
    PRIMARY KEY (Security_Code, Security_Name, Company_Name, Ex_Date, Record_Date, Purpose)
);

-- 5️⃣ deliv_deviation (as before)
CREATE TABLE deliv_deviation (
    SYMBOL TEXT PRIMARY KEY,
    AVG_DELIV_QTY NUMERIC,
    NEW_DELIV_QTY NUMERIC,
    PCT_DEVIATION NUMERIC
);

-- 6️⃣ insider_trading (all TEXT)
CREATE TABLE insider_trading (
    id SERIAL PRIMARY KEY,
    Stock VARCHAR(20) NOT NULL,
    Clause TEXT,
    Name TEXT,
    Type TEXT,
    Amount TEXT,
    Value TEXT,
    Transaction TEXT,
    Attachment TEXT,
    Time TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock) REFERENCES symbols(symbol),
    CONSTRAINT unique_insider_trade UNIQUE (stock, name, transaction, time)
);

-- 7️⃣ master (as before)
CREATE TABLE master (
    SYMBOL TEXT PRIMARY KEY,
    AVG_TTL_TRD_QNTY NUMERIC,
    AVG_DELIV_QTY NUMERIC
);

-- 8️⃣ symbols (as before)

-- 9️⃣ vol_deviation (as before)
CREATE TABLE vol_deviation (
    SYMBOL TEXT PRIMARY KEY,
    AVG_TTL_TRD_QNTY NUMERIC,
    NEW_TTL_TRD_QNTY NUMERIC,
    PCT_DEVIATION NUMERIC
);

CREATE TABLE news (
    id SERIAL PRIMARY KEY,
    source TEXT,
    headline TEXT,
    link TEXT UNIQUE,
    category TEXT,
    time TEXT,
    tag_status BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE tagging (
    id SERIAL PRIMARY KEY,
    news_id INTEGER NOT NULL REFERENCES news(id) ON DELETE CASCADE,
    tag_id TEXT NOT NULL REFERENCES symbols(tag_id) ON DELETE CASCADE,
    UNIQUE (news_id, tag_id)
);

CREATE TABLE IF NOT EXISTS last_updated (
    key VARCHAR(50) PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL
);

INSERT INTO last_updated (key, timestamp) VALUES
('data', NOW()),
('news', NOW());