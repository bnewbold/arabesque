
# Crawl QA Report

This crawl report is auto-generated from a sqlite database file, which should be available/included.

### Seedlist Stats

```sql
SELECT COUNT(DISTINCT identifier) as identifiers, COUNT(DISTINCT initial_url) as uris, COUNT(DISTINCT initial_domain) AS domains FROM crawl_result;
```

FTP seed URLs

```sql
SELECT COUNT(*) as ftp_urls FROM crawl_result WHERE initial_url LIKE 'ftp://%';
```

### Successful Hits

```sql
SELECT COUNT(DISTINCT identifier) as identifiers, COUNT(DISTINCT initial_url) as uris, COUNT(DISTINCT final_sha1) as unique_sha1 FROM crawl_result WHERE hit=1;
```

De-duplication percentage (aka, fraction of hits where content had been crawled and identified previously):

```sql
# AVG() hack!
SELECT 100. * AVG(final_was_dedupe) as percent FROM crawl_result WHERE hit=1;
```

Top mimetypes for successful hits (these are usually filtered to a fixed list in post-processing):

```sql
SELECT final_mimetype, COUNT(*) FROM crawl_result WHERE hit=1 GROUP BY final_mimetype ORDER BY COUNT(*) DESC LIMIT 10;
```

Most popular breadcrumbs (a measure of how hard the crawler had to work):

```sql
SELECT breadcrumbs, COUNT(*) FROM crawl_result WHERE hit=1 GROUP BY breadcrumbs ORDER BY COUNT(*) DESC LIMIT 10;
```

FTP vs. HTTP hits (200 is HTTP, 226 is FTP):

```sql
SELECT final_status_code, COUNT(*) FROM crawl_result WHERE hit=1 GROUP BY final_status_code LIMIT 10;
```

### Domain Summary

Top *initial* domains:

```sql
SELECT initial_domain, COUNT(*), 100. * COUNT(*) / (SELECT COUNT(*) FROM crawl_result) as percent FROM crawl_result GROUP BY initial_domain ORDER BY count(*) DESC LIMIT 20;
```

Top *successful, final* domains, where hits were found:

```sql

SELECT initial_domain, COUNT(*), 100. * COUNT(*) / (SELECT COUNT(*) FROM crawl_result WHERE hit=1) AS percent  FROM crawl_result WHERE hit=1 GROUP BY initial_domain ORDER BY COUNT(*) DESC LIMIT 20;
```

Top *non-successful, final* domains where crawl paths terminated before a successful hit (but crawl did run):

```sql
SELECT final_domain, COUNT(*) FROM crawl_result WHERE hit=0 AND final_status_code IS NOT NULL GROUP BY final_domain ORDER BY count(*) DESC LIMIT 20;
```

Top *uncrawled, initial* domains, where the crawl didn't even attempt to run:

```sql
SELECT initial_domain, COUNT(*) FROM crawl_result WHERE hit=0 AND final_status_code IS NULL GROUP BY initial_domain ORDER BY count(*) DESC LIMIT 20;
```

Top *blocked, final* domains:

```sql
SELECT final_domain, COUNT(*) FROM crawl_result WHERE hit=0 AND (final_status_code='-61' OR final_status_code='-2') GROUP BY final_domain ORDER BY count(*) DESC LIMIT 20;
```

Top *rate-limited, final* domains:

```sql
SELECT final_domain, COUNT(*) FROM crawl_result WHERE hit=0 AND final_status_code='429' GROUP BY final_domain ORDER BY count(*) DESC LIMIT 20;
```

### Status Summary

Top failure status codes:

```sql
    SELECT final_status_code, COUNT(*) FROM crawl_result WHERE hit=0 GROUP BY final_status_code ORDER BY count(*) DESC LIMIT 10;
```

### Example Results

A handful of random success lines:

```sql
    SELECT identifier, initial_url, breadcrumbs, final_url, final_sha1, final_mimetype FROM crawl_result WHERE hit=1 ORDER BY random() LIMIT 10;
```

Handful of random non-success lines:

```sql
    SELECT identifier, initial_url, breadcrumbs, final_url, final_status_code, final_mimetype FROM crawl_result WHERE hit=0 ORDER BY random() LIMIT 25;
```
