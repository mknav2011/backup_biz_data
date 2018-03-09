select distinct to_date(effectiveto) as effectiveto from listing_dim
WHERE to_date(effectiveto) <> "9999-12-31"