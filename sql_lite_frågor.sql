-- SQLite



SELECT DISTINCT p.instrument_id,i.ticker_symbol, i.instrument_name,i.instrument_type
FROM prices p, instruments i
WHERE p.instrument_id = i.id AND p.volume = 0 AND i.instrument_type = "STOCK";