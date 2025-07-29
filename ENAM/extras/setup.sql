-- ============================================
-- 1. Drop existing objects
-- ============================================


-- ============================================
-- 2. Create sequence for Tag_ID
-- ============================================


-- ============================================
-- 3. Create symbols table with Status column
-- ============================================


-- ============================================
-- 4. Trigger function for Tag_ID generation
-- ============================================
CREATE OR REPLACE FUNCTION set_symbols_tag_id()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.Tag_ID IS NULL THEN
        NEW.Tag_ID := 'T' || LPAD(nextval('symbols_tag_id_seq')::TEXT, 4, '0');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- 5. Create trigger
-- ============================================
