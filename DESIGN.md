
Going to look something like:

    zcat DOI-LANDING-CRAWL-2018-06-full_crawl_logs/DOI-LANDING-CRAWL-2018-06.$SHARD.us.archive.org.crawl.log.gz | tr -cd '[[:print:]]\n\r\t' | rg '//doi.org/' | /fast/scratch/unpaywall/make_doi_list.py > doi_list.$SHARD.txt

    zcat /fast/unpaywall-munging/DOI-LANDING-CRAWL-2018-06/DOI-LANDING-CRAWL-2018-06-full_crawl_logs/DOI-LANDING-CRAWL-2018-06.$SHARD.us.archive.org.crawl.log.gz | pv | /fast/scratch/unpaywall/make_map.py redirectmap.$SHARD.db

    cat /fast/unpaywall-munging/DOI-LANDING-CRAWL-2018-06/doi_list.$SHARD.txt | pv | /fast/scratch/unpaywall/make_output.py redirectmap.$SHARD.db > doi_index.$SHARD.tsv

Let's start with:

    mkdir UNPAYWALL-PDF-CRAWL-2018-07
    ia download UNPAYWALL-PDF-CRAWL-2018-07-full_crawl_logs

export SHARD=wbgrp-svc279 # running
export SHARD=wbgrp-svc280 # running
export SHARD=wbgrp-svc281 # running
export SHARD=wbgrp-svc282 # running
zcat UNPAYWALL-PDF-CRAWL-2018-07-full_crawl_logs/UNPAYWALL-PDF-CRAWL-2018-07.$SHARD.us.archive.org.crawl.log.gz | pv | /fast/scratch/unpaywall/make_map.py redirectmap.$SHARD.db
zcat UNPAYWALL-PDF-CRAWL-2018-07-full_crawl_logs/UNPAYWALL-PDF-CRAWL-2018-07-PATCH.$SHARD.us.archive.org.crawl.log.gz | pv | /fast/scratch/unpaywall/make_map.py redirectmap.$SHARD-PATCH.db

### Design

If possible, we'd like something that will work with as many crawls as
possible. Want to work with shards, then merge outputs.

Output: JSON and/or sqlite rows with:

- identifier (optional?)
- initial-uri (indexed)
- breadcrumbs
- final-uri
- final-http-status
- final-sha1
- final-mimetype-normalized
- final-was-dedupe (boolean)
- final-cdx (string, if would be extracted)

This will allow filtering on various fields, checking success stats, etc.

Components:

- {identifier, initial-uri} input (basically, seedlist)
- full crawl logs
- raw CDX, indexed by final-uri
- referer map

Process:

- use full crawl logs to generate a referer map; this is a dict with keys as
  URI, and value as {referer URI, status, breadcrumb, was-dedupe, mimetype};
  the referer may be null. database can be whatever.
- iterate through CDX, filtering by HTTP status and mimetype (including
  revists). for each potential, lookup in referer map. if mimetype is
  confirmed, then iterate through full referer chain, and print a final line
  which is all-but-identifier
- iterate through identifier/URI list, inserting identifier columns

Complications:

- non-PDF terminals: error codes, or HTML only (failed to find PDF)
- multiple terminals per seed; eg, multiple PDFs, or PDF+postscript+HTML or
  whatever

Process #2:

- use full crawl logs to generate a bi-directional referer map: sqlite3 table
  with uri, referer-uri both indexed. also {status, breadcrumb, was-dedupe,
  mimetype} rows
- iterate through CDX, selecting successful "terminal" lines (mime, status).
  use referer map to iterate back to an initial URI, and generate a row. lookup
  output table by initial-uri; if an entry already exists, behavior is
  flag-dependent: overwrite if "better", or add a second line
- in a second pass, update rows with identifier based on URI. if rows not
  found/updated, then do a "forwards" lookup to a terminal condition, and write
  that status. note that these rows won't have CDX.

More Complications:

- handling revisits correctly... raw CDX probably not actually helpful for PDF
  case, only landing/HTML case
- given above, should probably just (or as a mode) iterate over only crawl logs
  in "backwards" stage
- fan-out of "forward" redirect map, in the case of embeds and PDF link
  extraction
- could pull out first and final URI domains for easier SQL stats/reporting
- should include final datetime (for wayback lookups)

NOTE/TODO: journal-level dumps of fatcat metadata would be cool... could
roll-up release dumps as an alternative to hitting elasticsearch? or just hit
elasticsearch and both dump to sqlite and enrich elastic doc? should probably
have an indexed "last updated" timestamp in all elastic docs

### Crawl Log Notes

Fields:

    0   timestamp (ISO8601) of log line
    1   status code (HTTP or negative)
    2   size in bytes (content only)
    3   URI of this download
    4   discovery breadcrumbs
    5   "referer" URI
    6   mimetype (as reported?)
    7   worker thread
    8   full timestamp (start of network fetch; this is dt?)
    9   SHA1
    10  source tag
    11  annotations
    12  partial CDX JSON

### External Prep for, Eg, Unpaywall Crawl

    export LC_ALL=C
    sort -S 8G -u seedlist.shard > seedlist.shard.sorted

    zcat unpaywall_20180621.pdf_meta.tsv.gz | awk '{print $2 "\t" $1}' | sort -S 8G -u > unpaywall_20180621.seed_id.tsv

    join -t $'\t' unpaywall_20180621.seed_id.tsv unpaywall_crawl_patch_seedlist.split_3.schedule.sorted > seed_id.shard.tsv

TODO: why don't these sum/match correctly?

    bnewbold@orithena$ wc -l seed_id.shard.tsv unpaywall_crawl_patch_seedlist.split_3.schedule.sorted
    880737 seed_id.shard.tsv
    929459 unpaywall_crawl_patch_seedlist.split_3.schedule.sorted

    why is:
    http://00ec89c.netsolhost.com/brochures/200605_JAWMA_Hg_Paper_Lee_Hastings.pdf
    in unpaywall_crawl_patch_seedlist, but not unpaywall_20180621.pdf_meta?

    # Can't even filter on HTTP 200, because revisits are '-'
    #zcat UNPAYWALL-PDF-CRAWL-2018-07.cdx.gz | rg 'wbgrp-svc282' | rg ' 200 ' | rg '(pdf)|(revisit)' > UNPAYWALL-PDF-CRAWL-2018-07.svc282.filtered.cdx

    zcat UNPAYWALL-PDF-CRAWL-2018-07.cdx.gz | rg 'UNPAYWALL-PDF-CRAWL-2018-07-PATCH' | rg 'wbgrp-svc282' | rg '(pdf)|( warc/revisit )|(postscript)|( unk )' > UNPAYWALL-PDF-CRAWL-2018-07-PATCH.svc282.filtered.cdx

TODO: spaces in URLs, like 'https://www.termedia.pl/Journal/-7/pdf-27330-10?filename=A case.pdf'

### Revisit Notes

Neither CDX nor crawl logs seem to have revisits actually point to final
content, they just point to the revisit record in the (crawl-local) WARC.

### sqlite3 stats

    select count(*) from crawl_result;

    select count(*) from crawl_result where identifier is null;

    select breadcrumbs, count(*) from crawl_result group by breadcrumbs;

    select final_was_dedupe, count(*) from crawl_result group by final_was_dedupe;

    select final_http_status, count(*) from crawl_result group by final_http_status;

    select final_mimetype, count(*) from crawl_result group by final_mimetype;

    select * from crawl_result where final_mimetype = 'text/html' and final_http_status = '200' order by random() limit 5;

    select count(*) from crawl_result where final_uri like 'https://academic.oup.com/Govern%';

    select count(distinct identifier) from crawl_result where final_sha1 is not null;

### testing shard notes

880737  `seed_id` lines
21776   breadcrumbs are null (no crawl logs line); mostly normalized URLs?
24985   "first" URIs with no identifier; mostly normalized URLs?

backward: Counter({'skip-cdx-scope': 807248, 'inserted': 370309, 'skip-map-scope': 2913})
forward (dirty): Counter({'inserted': 509242, 'existing-id-updated': 347218, 'map-uri-missing': 15556, 'existing-complete': 8721, '_normalized-seed-uri': 5520})

874131 identifier is not null
881551 breadcrumbs is not null
376057 final_mimetype is application/pdf
370309 final_sha1 is not null
332931 application/pdf in UNPAYWALL-PDF-CRAWL-2018-07-PATCH.svc282.filtered.cdx

summary:
    370309/874131 42% got a PDF
    264331/874131 30% some domain dead-end
        196747/874131 23% onlinelibrary.wiley.com
        33879/874131   4% www.nature.com
        11074/874131   1% www.tandfonline.com
    125883/874131 14% blocked, 404, other crawl failures
            select count(*) from crawl_result where final_http_status >= '400' or final_http_status < '200';
    121028/874131 14% HTTP 200, but not pdf
        105317/874131 12% academic.oup.com; all rate-limited or cookie fail
    15596/874131  1.7% didn't even try crawling (null final status)

TODO:
- add "success" flag (instead of "final_sha1 is null")
- 

    http://oriental-world.org.ua/sites/default/files/Archive/2017/3/4.pdf   10.15407/orientw2017.03.021 -       http://oriental-world.org.ua/sites/default/files/Archive/2017/3/4.pdf   403     ¤       application/pdf 0       ¤

Iterated:

./arabesque.py backward UNPAYWALL-PDF-CRAWL-2018-07-PATCH.svc282.filtered.cdx map.sqlite out.sqlite
Counter({'skip-cdx-scope': 813760, 'inserted': 370435, 'skip-map-scope': 4620, 'skip-tiny-octetstream-': 30})

./arabesque.py forward unpaywall_20180621.seed_id.shard.tsv map.sqlite out.sqlite
Counter({'inserted': 523594, 'existing-id-updated': 350009, '_normalized-seed-uri': 21371, 'existing-complete': 6638, 'map-uri-missing': 496})

894029 breadcrumbs is not null
874102 identifier is not null
20423 identifier is null
496 breadcrumbs is null
370435 final_sha1 is not null

### URL/seed non-match issues!

Easily fixable:
- capitalization of domains
- empty port number, like `http://genesis.mi.ras.ru:/~razborov/hadamard.ps`

Encodable:
- URL encoding
    http://accounting.rutgers.edu/docs/seminars/Fall11/Clawbacks_9-27-11[1].pdf
    http://accounting.rutgers.edu/docs/seminars/Fall11/Clawbacks_9-27-11%5B1%5D.pdf
- whitespace in URL (should be url-encoded)
    https://www.termedia.pl/Journal/-7/pdf-27330-10?filename=A case.pdf
    https://www.termedia.pl/Journal/-7/pdf-27330-10?filename=A%EF%BF%BD%EF%BF%BDcase.pdf
- tricky hidden unicode
    http://goldhorde.ru/wp-content/uploads/2017/03/ЗО-1-2017-206-212.pdf
    http://goldhorde.ru/wp-content/uploads/2017/03/%EF%BF%BD%EF%BF%BD%EF%BF%BD%EF%BF%BD-1-2017-206-212.pdf

Harder/Custom?
- paths including "/../" or "/./" are collapsed
- port number 80, like `http://fermet.misis.ru:80/jour/article/download/724/700`
- aos2.uniba.it:8080papers

- fragments stripped by crawler: 'https://www.termedia.pl/Journal/-85/pdf-27083-10?filename=BTA#415-06-str307-316.pdf'

### Debugging "redirect terminal" issue

Some are redirect loops; fine.

Some are from 'cookieSet=1' redirects, like 'http://journals.sagepub.com/doi/pdf/10.1177/105971230601400206?cookieSet=1'. This comes through like:

    sqlite> select * from crawl_result where initial_uri = 'http://adb.sagepub.com/cgi/reprint/14/2/147.pdf';
    initial_uri     identifier      breadcrumbs     final_uri       final_http_status       final_sha1      final_mimetype  final_was_dedupe        final_cdx
    http://adb.sagepub.com/cgi/reprint/14/2/147.pdf 10.1177/105971230601400206 R       http://journals.sagepub.com/doi/pdf/10.1177/105971230601400206  302     ¤       text/html       0       ¤

Using 'http' (note: this is not an OA article):

    http://adb.sagepub.com/cgi/reprint/14/2/147.pdf
    https://journals.sagepub.com/doi/pdf/10.1177/105971230601400206
    https://journals.sagepub.com/doi/pdf/10.1177/105971230601400206?cookieSet=1
    http://journals.sagepub.com/action/cookieAbsent

Is heritrix refusing to do that second redirect? In some cases it will do at
leat the first, like:
    
    http://pubs.rsna.org/doi/pdf/10.1148/radiographics.11.1.1996385
    http://pubs.rsna.org/doi/pdf/10.1148/radiographics.11.1.1996385?cookieSet=1
    http://pubs.rsna.org/action/cookieAbsent

I think the vast majority of redirect terminals are when we redirect to a page
that has already been crawled. This is a bummer because we can't find the
redirect target in the logs.

Eg, academic.oup.com sometimes redirects to cookieSet, then cookieAbsent; other
times it redirects to Governer. It's important to distinguish between these.

### Scratch

What are actual advantages/use-cases of CDX mode?
=> easier CDX-to-WARC output mode
=> sending CDX along with WARCs as an index

Interested in scale-up behavior: full unpaywall PDF crawls, and/or full DOI landing crawls
=> eatmydata
dentifier is not null


    zcat UNPAYWALL-PDF-CRAWL-2018-07-PATCH* | time /fast/scratch/unpaywall/arabesque.py referrer - UNPAYWALL-PDF-CRAWL-2018-07-PATCH.map.sqlite
    [snip]
    ... referrer 5542000
    Referrer map complete.
    317.87user 274.57system 21:20.22elapsed 46%CPU (0avgtext+0avgdata 22992maxresident)k
    24inputs+155168464outputs (0major+802114minor)pagefaults 0swaps

    bnewbold@ia601101$ ls -lathr
    -rw-r--r-- 1 bnewbold bnewbold 1.7G Dec 12 12:33 UNPAYWALL-PDF-CRAWL-2018-07-PATCH.map.sqlite

Scaling!

    16,736,800 UNPAYWALL-PDF-CRAWL-2018-07.wbgrp-svc282.us.archive.org.crawl.log
    17,215,895 unpaywall_20180621.seed_id.tsv

Oops; need to shard the seed_id file.

Ugh, this one is a little derp because I didn't sort correctly. Let's say close enough though...

    4318674 unpaywall_crawl_seedlist.svc282.tsv
    3901403 UNPAYWALL-PDF-CRAWL-2018-07.wbgrp-svc282.seed_id.tsv


/fast/scratch/unpaywall/arabesque.py everything CORE-UPSTREAM-CRAWL-2018-11.combined.log core_2018-03-01_metadata.seed_id.tsv CORE-UPSTREAM-CRAWL-2018-11.out.sqlite

    Counter({'inserted': 3226191, 'skip-log-scope': 2811395, 'skip-log-prereq': 108932, 'skip-tiny-octetstream-': 855, 'skip-map-scope': 2})
    Counter({'existing-id-updated': 3221984, 'inserted': 809994, 'existing-complete': 228909, '_normalized-seed-uri': 17287, 'map-uri-missing': 2511, '_redirect-recursion-limit': 221, 'skip-bad-seed-uri': 17})

time /fast/scratch/unpaywall/arabesque.py everything UNPAYWALL-PDF-CRAWL-2018-07.wbgrp-svc282.us.archive.org.crawl.log UNPAYWALL-PDF-CRAWL-2018-07.wbgrp-svc282.seed_id.tsv UNPAYWALL-PDF-CRAWL-2018-07.out.sqlite

    Everything complete!
    Counter({'skip-log-scope': 13476816, 'inserted': 2536452, 'skip-log-prereq': 682460, 'skip-tiny-octetstream-': 41067})
    Counter({'existing-id-updated': 1652463, 'map-uri-missing': 1245789, 'inserted': 608802, 'existing-complete': 394349, '_normalized-seed-uri': 22573, '_redirect-recursion-limit': 157})

    real    63m42.124s
    user    53m31.007s
    sys     6m50.535s

### Performance

Before tweaks:

    real    2m55.975s
    user    2m6.772s
    sys     0m12.684s

After:

    real    1m51.500s
    user    1m44.600s
    sys     0m3.496s

