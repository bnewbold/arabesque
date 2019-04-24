#!/usr/bin/env python3

"""
This is a multi-function script for generating a particular type of crawl
report output: a table of identifiers, seed URLs, and crawl results, taking in
to account long redirect chains.

Commands/modes:
- referrer <input.log> <output-map.sqlite>
- backward_cdx <input.cdx> <input-map.sqlite> <output.sqlite>
- backward <input.log> <input-map.sqlite> <output.sqlite>
- forward <input.seed_identifiers> <output.sqlite>
- everything <input.log> <input.cdx> <input.seed_identifiers> <output.sqlite>
- postprocess <sha1_status.tsv> <output.sqlite>
- dump_json <output.sqlite>

Design docs in DESIGN.md

This script was written by Bryan Newbold <bnewbold@archive.org> and is Free
Software under the GPLv3 license (a copy of which should be included with this
file).

TODO:
- pass SHA-1 and timestamp in forward mode (?)
- include final_size (if possible from crawl log)
- open map in read-only when appropriate
- should referrer map be UNIQ?
- forward outputs get generated multiple times?
- try: https://pypi.org/project/urlcanon/
- https://www.talisman.org/~erlkonig/misc/lunatech%5Ewhat-every-webdev-must-know-about-url-encoding/

BUG:
BAD LOG LINE: 2018-07-27T12:26:24.783Z   200         24 http://www.phywe-es.com/robots.txt 15+LREELLLLLRLELLRLLLRLLLLRLELRRLLLLLLLLLLLLLLLLLLLLP http://www.phywe-es.com/index.php/fuseaction/download/lrn_file/versuchsanleitungen/P2522015/tr/P2522015.pdf text/html #296 20180727122622741+438 sha1:YR6M6GSJYJGMLBBEGCVHLRZO6SISSJAS - unsatisfiableCharsetInHeader:ISO 8859-1 {"contentSize":254,"warcFilename":"UNPAYWALL-PDF-CRAWL-2018-07-20180727122113315-14533-11460~wbgrp-svc282.us.archive.org~8443.warc.gz","warcFileOffset":126308355}
"""

import sys
import json
import time
import urllib
import urllib3
import sqlite3
import argparse
import collections

CrawlLine = collections.namedtuple('CrawlLine', [
    'log_time',
    'status_code',
    'size_bytes',
    'url',
    'breadcrumbs',
    'referrer_url', 
    'mimetype',
    'worker_thread',
    'timestamp',
    'sha1',
    'source_tag',
    'annotations',
    'cdx_json'])

FullCdxLine = collections.namedtuple('FullCdxLine', [
    'surt',
    'datetime',
    'url',
    'mimetype',
    'status_code', # will be '-' for warc/revist
    'sha1', 
    'unused1',
    'unused2',
    'c_size',
    'offset',
    'warc'])

ReferrerRow = collections.namedtuple('ReferrerRow', [
    'url',
    'referrer_url',
    'status_code',
    'breadcrumbs',
    'mimetype',
    'is_dedupe'])

NORMAL_MIMETYPE = (
    'application/pdf',
    'application/postscript',
    'text/html',
    'text/xml',
    'warc/revisit',
    'application/octet-stream',
)

FULLTEXT_MIMETYPES = (
    "application/pdf",
    "application/postscript",
    "application/octet-stream",
)

def normalize_mimetype(raw):
    raw = raw.lower()
    raw = raw.replace('"', '').replace("'", '').replace(',', '')
    for norm in NORMAL_MIMETYPE:
        if raw.startswith(norm):
            return norm

    # Special cases
    if raw.startswith('application/xml'):
        return 'text/xml'
    if raw.startswith('application/x-pdf'):
        return 'application/pdf'
    if raw in ('unk', 'unknown', 'other'):
        return 'application/octet-stream'
    return raw

def normalize_url(raw):
    """
    This is a surprisingly complex function that cleans up URLs.

    Particular issues it fixes:
    - lower-cases scheme and host/domain
    - removes blank or redundant port numbers
    - removes fragments (anchor tags)
    - URL escapes characters in path, particularly whitespace

    Some of these we maybe shouldn't do, but heritrix does by default

    TODO: heritrix removes fragments, but maybe we shouldn't?
    """
    try:
        u = urllib3.util.parse_url(raw.strip())
    except:
        return None

    if u.path is None:
        return None
    port = u.port
    if (port == 80 and u.scheme == 'http') or (port == 443 and u.scheme == 'https'):
        port = None

    # "Dot Segment Removal" per RFC 3986
    # via https://stackoverflow.com/a/40536710/4682349
    segments = u.path.split('/')
    segments = [segment + '/' for segment in segments[:-1]] + [segments[-1]]
    resolved = []
    for segment in segments:
        if segment in ('../', '..'):
            if resolved[1:]:
                resolved.pop()
        elif segment not in ('./', '.'):
            resolved.append(segment)
    path = ''.join(resolved)

    # add any missing slash if there is a query or fragment
    if (u.query or u.fragment) and not path:
        path = '/'

    # URL encode the path segment for HTTP URLs
    if u.scheme in ('http', 'https'):
        path = urllib.parse.quote(path, safe='/%~+:();$!,')
    result = urllib3.util.Url(u.scheme, u.auth, u.host.lower(), port, path, u.query, None)
    return result.url.replace(' ', '%20')

def test_normalize_url():
    
    assert (normalize_url('HTTP://ASDF.com/a/../b') == 'http://asdf.com/b')
    assert (normalize_url('HTTP://ASDF.com/a/./b') == 'http://asdf.com/a/b')
    assert (normalize_url('HTTP://ASDF.com:/') == 'http://asdf.com/')
    assert (normalize_url('HTTP://ASDF.com:80/') == 'http://asdf.com/')
    assert (normalize_url('HTTP://ASDF.com:80papers/123.pdf') == None)
    assert (normalize_url('HTTP://ASDF.com/a.pdf#123') == 'http://asdf.com/a.pdf')
    assert (normalize_url('HTTPs://ASDF.com:443/') == 'https://asdf.com/')
    assert (normalize_url('HTTP://ASDF.com/a/../b') == 'http://asdf.com/b')
    assert (normalize_url('HTTP://ASDF.com/first second') == 'http://asdf.com/first%20second')
    assert (normalize_url('HTTP://ASDF.com/first%20second') == 'http://asdf.com/first%20second')
    assert (normalize_url('Ftp://ASDF.com/a/../b') == 'ftp://asdf.com/b')

    #assert (normalize_url('http://goldhorde.ru/wp-content/uploads/2017/03/ЗО-1-2017-206-212.pdf') ==
    #    'http://goldhorde.ru/wp-content/uploads/2017/03/%EF%BF%BD%EF%BF%BD%EF%BF%BD%EF%BF%BD-1-2017-206-212.pdf')
    assert (normalize_url('http://goldhorde.ru/wp-content/uploads/2017/03/ЗО-1-2017-206-212.pdf') ==
        'http://goldhorde.ru/wp-content/uploads/2017/03/%D0%97%D0%9E-1-2017-206-212.pdf')

    assert (normalize_url('http://accounting.rutgers.edu/docs/seminars/Fall11/Clawbacks_9-27-11[1].pdf') ==
        'http://accounting.rutgers.edu/docs/seminars/Fall11/Clawbacks_9-27-11%5B1%5D.pdf')
    #assert (normalize_url('https://www.termedia.pl/Journal/-7/pdf-27330-10?filename=A case.pdf') ==
    #    'https://www.termedia.pl/Journal/-7/pdf-27330-10?filename=A%EF%BF%BD%EF%BF%BDcase.pdf')
    assert (normalize_url('https://www.termedia.pl/Journal/-7/pdf-27330-10?filename=A case.pdf') ==
        'https://www.termedia.pl/Journal/-7/pdf-27330-10?filename=A%20case.pdf')
    assert (normalize_url('http://mariel.inesc.pt/~lflb/ma98.pdf') ==
        'http://mariel.inesc.pt/~lflb/ma98.pdf')
    assert (normalize_url('http://ijpsr.com?action=download_pdf&postid=9952') ==
        'http://ijpsr.com/?action=download_pdf&postid=9952')
    assert (normalize_url('http://onlinelibrary.wiley.com/doi/10.1002/(SICI)1099-0518(199702)35:3<587::AID-POLA25>3.0.CO;2-J/pdf') ==
        'http://onlinelibrary.wiley.com/doi/10.1002/(SICI)1099-0518(199702)35:3%3C587::AID-POLA25%3E3.0.CO;2-J/pdf')
    assert (normalize_url('http://ntj.tax.org/wwtax/ntjrec.nsf/175d710dffc186a385256a31007cb40f/5e1815e49ceb7d318525796800526cf8/$FILE/A04_Cole.pdf') ==
        'http://ntj.tax.org/wwtax/ntjrec.nsf/175d710dffc186a385256a31007cb40f/5e1815e49ceb7d318525796800526cf8/$FILE/A04_Cole.pdf')
    assert (normalize_url('http://www.nature.com:80/') ==
        'http://www.nature.com/')
    assert (normalize_url('http://www.nature.com:80/polopoly_fs/1.22367!/menu/main/topColumns/topLeftColumn/pdf/547389a.pdf') ==
        'http://www.nature.com/polopoly_fs/1.22367!/menu/main/topColumns/topLeftColumn/pdf/547389a.pdf')
    assert (normalize_url('http://pdfs.journals.lww.com/transplantjournal/2012/11271/Extra_Pulmonary_Nocardiosis_and_Perigraft_Abscess,.1668.pdf?token=method|ExpireAbsolute;source|Journals;ttl|1503135985283;payload|mY8D3u1TCCsNvP5E421JYK6N6XICDamxByyYpaNzk7FKjTaa1Yz22MivkHZqjGP4kdS2v0J76WGAnHACH69s21Csk0OpQi3YbjEMdSoz2UhVybFqQxA7lKwSUlA502zQZr96TQRwhVlocEp/sJ586aVbcBFlltKNKo+tbuMfL73hiPqJliudqs17cHeLcLbV/CqjlP3IO0jGHlHQtJWcICDdAyGJMnpi6RlbEJaRheGeh5z5uvqz3FLHgPKVXJzdGlb2qsojlvlytk14LkMXSI/t5I2LVgySZVyHeaTj/dJdRvauPu3j5lsX4K1l3siV;hash|9tFBJUOSJ1hYPXrgBby2Xg==') ==
        'http://pdfs.journals.lww.com/transplantjournal/2012/11271/Extra_Pulmonary_Nocardiosis_and_Perigraft_Abscess,.1668.pdf?token=method|ExpireAbsolute;source|Journals;ttl|1503135985283;payload|mY8D3u1TCCsNvP5E421JYK6N6XICDamxByyYpaNzk7FKjTaa1Yz22MivkHZqjGP4kdS2v0J76WGAnHACH69s21Csk0OpQi3YbjEMdSoz2UhVybFqQxA7lKwSUlA502zQZr96TQRwhVlocEp/sJ586aVbcBFlltKNKo+tbuMfL73hiPqJliudqs17cHeLcLbV/CqjlP3IO0jGHlHQtJWcICDdAyGJMnpi6RlbEJaRheGeh5z5uvqz3FLHgPKVXJzdGlb2qsojlvlytk14LkMXSI/t5I2LVgySZVyHeaTj/dJdRvauPu3j5lsX4K1l3siV;hash|9tFBJUOSJ1hYPXrgBby2Xg==')

def parse_crawl_line(line):
    # yup, it's just whitespace, and yup, there's a JSON blob at the end that
    # "hopefully" contains no whitespace
    line = line.strip().split()
    if len(line) != 13:
        return None
    # FTP success; need to munge mimetype
    if line[3].startswith('ftp://') and line[1] == "226" and line[6] == "application/octet-stream":
        if line[3].lower().endswith('.pdf'):
            line[6] = "application/pdf"
        elif line[3].lower().endswith('.ps'):
            line[6] = "application/postscript"

    # mimetype
    line[6] = normalize_mimetype(line[6])
    # SHA1
    line[9] = line[9].replace('sha1:', '')
    return CrawlLine(*line)

def parse_full_cdx_line(line):
    line = line.strip().split(' ')
    assert len(line) == 11
    # mimetype
    line[3] = normalize_mimetype(line[3])
    return FullCdxLine(*line)

def lookup_referrer_row(cursor, url):
    #print("Lookup: {}".format(cdx.url))
    raw = list(cursor.execute('SELECT * from referrer WHERE url=? LIMIT 1', [url]))
    if not raw:
        return None
    raw = list(raw[0])
    if not raw[1] or raw[1] == '-':
        raw[1] = None
    return ReferrerRow(*raw)

def lookup_all_referred_rows(cursor, url):
    #print("Lookup: {}".format(cdx.url))
    # TODO: should this SORT BY?
    result = list(cursor.execute('SELECT * from referrer WHERE referrer=?', [url]))
    if not result:
        return None
    for i in range(len(result)):
        raw = list(result[i])
        if not raw[1] or raw[1] == '-':
            raw[1] = None
        result[i] = ReferrerRow(*raw)
    return result

def create_out_table(db):
    # "eat my data" style database, for speed
    # NOTE: don't drop indexes here, because we often reuse DB
    db.executescript("""
        PRAGMA main.page_size = 4096;
        PRAGMA main.cache_size = 20000;
        PRAGMA main.locking_mode = EXCLUSIVE;
        PRAGMA main.synchronous = OFF;
        PRAGMA main.journal_mode = MEMORY;

        CREATE TABLE IF NOT EXISTS crawl_result
            (initial_url text NOT NULL,
             identifier text,
             initial_domain text,
             breadcrumbs text,
             final_url text,
             final_domain text text,
             final_timestamp text,
             final_status_code text,
             final_sha1 text,
             final_mimetype text,
             final_was_dedupe bool,
             hit bool,
             postproc_status text);
    """)

def referrer(log_file, map_db):
    """
    TODO: this would probably be simpler, and much faster, as a simple sqlite3 import from TSV
    """
    print("Mapping referrers from crawl logs")
    # "eat my data" style database, for speed
    map_db.executescript("""
        PRAGMA main.page_size = 4096;
        PRAGMA main.cache_size = 20000;
        PRAGMA main.locking_mode = EXCLUSIVE;
        PRAGMA main.synchronous = OFF;
        PRAGMA main.journal_mode = MEMORY;

        CREATE TABLE IF NOT EXISTS referrer
                 (url text,
                  referrer text,
                  status_code text,
                  breadcrumbs text,
                  mimetype text,
                  is_dedupe bool);
        DROP INDEX IF EXISTS referrer_url;
        DROP INDEX IF EXISTS referrer_referrer;
    """)
    c = map_db.cursor()
    i = 0
    for raw in log_file:
        line = parse_crawl_line(raw)
        if not line:
            print("BAD LOG LINE: {}".format(raw.strip()))
            continue
        if line.url.startswith('dns:') or line.url.startswith('whois:'):
            #print("skipping: {}".format(line.url))
            continue
        is_dedupe = 'duplicate:digest' in line.annotations
        # insert {url, referrer, status_code, breadcrumbs, mimetype, is_dedupe}
        c.execute("INSERT INTO referrer VALUES (?,?,?,?,?,?)",
            (line.url, line.referrer_url, line.status_code, line.breadcrumbs, line.mimetype, is_dedupe))
        i = i+1
        if i % 5000 == 0:
            print("... referrer {}".format(i))
            map_db.commit()

    map_db.commit()
    print("Building indices (this can be slow)...")
    c.executescript("""
        CREATE INDEX IF NOT EXISTS referrer_url on referrer (url);
        CREATE INDEX IF NOT EXISTS referrer_referrer on referrer (referrer);
    """)
    c.close()
    print("Referrer map complete.")

def backward_cdx(cdx_file, map_db, output_db, hit_mimetypes=FULLTEXT_MIMETYPES):
    """
    TODO: Hrm, just realized we don't usually have CDX files on a per-machine
    basis.  Need to call this with a shard string that gets parsed from the
    WARC field of the full CDX for the whole crawl? Oh well. Large file, but
    can be shared, and filter should be fast (can optimize with grep as well).
    """
    print("Mapping backward from CDX 200/226 to initial urls")
    counts = collections.Counter({'inserted': 0})
    m = map_db.cursor()
    create_out_table(output_db)
    c = output_db.cursor()
    i = 0
    for raw_cdx in cdx_file:
        if raw_cdx.startswith('CDX') or raw_cdx.startswith(' '):
            counts['skip-cdx-raw'] += 1
            continue
        cdx = parse_full_cdx_line(raw_cdx)

        if not ((cdx.status_code in ("200", "226") and cdx.mimetype in hit_mimetypes)
                or (cdx.mimetype == "warc/revisit")):
            counts['skip-cdx-scope'] += 1
            continue

        if cdx.mimetype == "application/octet-stream" and line.size_bytes and line.size_bytes != '-' and int(line.size_bytes) < 1000:
            counts['skip-tiny-octetstream-'] += 1
            continue

        #print(time.time())
        final_row = lookup_referrer_row(m, cdx.url)
        #print(time.time())
        if not final_row:
            print("MISSING url: {}".format(raw_cdx.strip()))
            counts['map-url-missing'] += 1
            continue
        if not (final_row.status_code in ("200", "226") and final_row.mimetype in hit_mimetypes):
            counts['skip-map-scope'] += 1
            continue
        row = final_row
        while row and row.referrer_url != None:
            next_row = lookup_referrer_row(m, row.referrer_url)
            if next_row:
                row = next_row
            else:
                break
   
        initial_domain = urllib3.util.parse_url(row.url).host
        final_domain = urllib3.util.parse_url(final_row.url).host
        c.execute("INSERT INTO crawl_result VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (row.url, None, initial_domain, final_row.breadcrumbs, final_row.url, final_domain, cdx.timestamp, final_row.status_code, cdx.sha1, final_row.mimetype, final_row.is_dedupe, True))
        #print(final_row.breadcrumbs)
        i = i+1
        counts['inserted'] += 1
        if i % 2000 == 0:
            print("... backward {}".format(i))
            output_db.commit()

    output_db.commit()
    print("Building indices (this can be slow)...")
    c.executescript("""
        CREATE INDEX IF NOT EXISTS result_initial_url on crawl_result (initial_url);
        CREATE INDEX IF NOT EXISTS result_identifier on crawl_result (identifier);
    """)
    c.close()
    m.close()
    print("Backward map complete.")
    print(counts)
    return counts

def backward(log_file, map_db, output_db, hit_mimetypes=FULLTEXT_MIMETYPES):
    """
    This is a variant of backward_cdx that uses the log files, not CDX file
    """
    print("Mapping backward from log file 200s to initial urls")
    counts = collections.Counter({'inserted': 0})
    m = map_db.cursor()
    create_out_table(output_db)
    c = output_db.cursor()
    i = 0
    for raw in log_file:
        line = parse_crawl_line(raw)
        if not line:
            print("BAD LOG LINE: {}".format(raw.strip()))
            continue
        if line.url.startswith('dns:') or line.url.startswith('whois:'):
            counts['skip-log-prereq'] += 1
            continue
        is_dedupe = 'duplicate:digest' in line.annotations

        if not (line.status_code in ("200", "226") and line.mimetype in hit_mimetypes):
            counts['skip-log-scope'] += 1
            continue

        if line.mimetype == "application/octet-stream" and int(line.size_bytes) < 1000:
            counts['skip-tiny-octetstream'] += 1
            continue

        if int(line.size_bytes) == 0 or line.sha1 == "3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ":
            counts['skip-empty-file'] += 1
            continue

        #print(time.time())
        final_row = lookup_referrer_row(m, line.url)
        #print(time.time())
        if not final_row:
            print("MISSING url: {}".format(raw.strip()))
            counts['map-url-missing'] += 1
            continue
        if not (final_row.status_code in ("200", "226") and final_row.mimetype in hit_mimetypes):
            counts['skip-map-scope'] += 1
            continue
        row = final_row
        loop_stack = []
        while row and row.referrer_url != None:
            next_row = lookup_referrer_row(m, row.referrer_url)
            if next_row:
                row = next_row
            else:
                break
            if row.referrer_url in loop_stack:
                counts['map-url-redirect-loop'] += 1
                break
            loop_stack.append(row.referrer_url)
   
        initial_domain = urllib3.util.parse_url(row.url).host
        final_domain = urllib3.util.parse_url(final_row.url).host
        # convert to IA CDX timestamp format
        #final_timestamp = dateutil.parser.parse(line.timestamp).strftime("%Y%m%d%H%M%S")
        final_timestamp = None
        if len(line.timestamp) >= 12 and line.timestamp[4] != '-':
            final_timestamp = line.timestamp[:12]
        c.execute("INSERT INTO crawl_result VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (row.url, None, initial_domain, final_row.breadcrumbs, final_row.url, final_domain, final_timestamp, final_row.status_code, line.sha1, final_row.mimetype, final_row.is_dedupe, True, None))
        #print(final_row.breadcrumbs)
        i = i+1
        counts['inserted'] += 1
        if i % 2000 == 0:
            print("... backward {}".format(i))
            output_db.commit()

    output_db.commit()
    m.close()
    print("Building indices (this can be slow)...")
    c.executescript("""
        CREATE INDEX IF NOT EXISTS result_initial_url on crawl_result (initial_url);
        CREATE INDEX IF NOT EXISTS result_identifier on crawl_result (identifier);
    """)
    c.close()
    print("Backward map complete.")
    print(counts)
    return counts

def forward(seed_id_file, map_db, output_db):
    print("Mapping forwards from seedlist to terminal urls")
    counts = collections.Counter({'inserted': 0})
    m = map_db.cursor()
    create_out_table(output_db)
    c = output_db.cursor()

    i = 0
    for raw_line in seed_id_file:
        line = raw_line.strip().split('\t')
        if not line:
            counts['skip-raw-line'] += 1
            continue
        if len(line) == 1:
            seed_url, identifier = line[0], None
        elif len(line) == 2:
            seed_url, identifier = line[0:2]
        else:
            print("WEIRD: {}".format(raw_line))
            assert len(line) <= 2
        raw_url = seed_url
        seed_url = normalize_url(seed_url)
        if not seed_url:
            counts['skip-bad-seed-url'] += 1
            continue
        if raw_url != seed_url:
            counts['_normalized-seed-url'] += 1

        # first check if entry already in output table; if so, only upsert with identifier
        existing_row = list(c.execute('SELECT identifier, breadcrumbs from crawl_result WHERE initial_url=? LIMIT 1', [seed_url]))
        if existing_row:
            if not existing_row[0][0]:
                # identifier hasn't been updated
                c.execute('UPDATE crawl_result SET identifier=? WHERE initial_url=?', [identifier, seed_url])
                counts['existing-id-updated'] += 1
                continue
            else:
                counts['existing-complete'] += 1
                continue

        # if not, then do a "forward" lookup for the "best"/"final" terminal crawl line
        # simple for redirect case (no branching); arbitrary for the fan-out case
        first_row = lookup_referrer_row(m, seed_url)
        if not first_row:
            #print("MISSING url: {}".format(raw_line.strip()))
            # need to insert *something* in this case...
            initial_domain = urllib3.util.parse_url(seed_url).host
            c.execute("INSERT INTO crawl_result VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (seed_url, identifier, initial_domain, None, None, None, None, None, None, None, None, False, None))
            counts['map-url-missing'] += 1
            continue
        row = first_row
        # recursively iterate down referal path
        limit = 40
        while True:
            limit = limit - 1
            if limit <= 0:
                counts['_redirect-recursion-limit'] += 1
                break
            next_rows = lookup_all_referred_rows(m, row.url)
            if not next_rows:
                # halt if we hit a dead end
                break

            # there are going to be multiple referrer hits, need to chose among... based on status?
            updated = False
            for potential in next_rows:
                if ('E' in potential.breadcrumbs or 'X' in potential.breadcrumbs or 'I' in potential.breadcrumbs) and not 'pdf' in potential.mimetype:
                    # TODO: still PDF-specific
                    # don't consider simple embeds unless PDF
                    continue
                row = potential
                updated = True
            if not updated:
                break

        final_row = row
        initial_domain = urllib3.util.parse_url(seed_url).host
        final_domain = urllib3.util.parse_url(final_row.url).host
        # TODO: would pass SHA1 here if we had it? but not stored in referrer table
        # XXX: None => timestamp
        c.execute("INSERT INTO crawl_result VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (seed_url, identifier, initial_domain, final_row.breadcrumbs, final_row.url, final_domain, None, final_row.status_code, None, final_row.mimetype, final_row.is_dedupe, False, None))
        #print(final_row.breadcrumbs)
        i = i+1
        counts['inserted'] += 1
        if i % 2000 == 0:
            print("... forward {}".format(i))
            output_db.commit()

    output_db.commit()
    m.close()
    print("Building indices (this can be slow)...")
    c.executescript("""
        CREATE INDEX IF NOT EXISTS result_initial_url on crawl_result (initial_url);
        CREATE INDEX IF NOT EXISTS result_identifier on crawl_result (identifier);
        CREATE INDEX IF NOT EXISTS result_final_sha1 on crawl_result (final_sha1);
    """)
    c.close()
    print("Forward map complete.")
    print(counts)
    return counts

def everything(log_file, seed_id_file, map_db, output_db, hit_mimetypes=FULLTEXT_MIMETYPES):
    referrer(open(log_file, 'r'), map_db)
    bcounts = backward(open(log_file, 'r'), map_db, output_db, hit_mimetypes=hit_mimetypes)
    fcounts = forward(seed_id_file, map_db, output_db)
    print()
    print("Everything complete!")
    print(bcounts)
    print(fcounts)

def postprocess(sha1_status_file, output_db):
    print("Updating database with post-processing status")
    print("""If script fails (on old databases) you may need to manually:
        ALTER TABLE crawl_result ADD COLUMN postproc_status text;""")
    counts = collections.Counter({'lines-parsed': 0})
    c = output_db.cursor()

    i = 0
    for raw_line in sha1_status_file:
        line = raw_line.strip().split('\t')
        if not line or len(line) == 1:
            counts['skip-raw-line'] += 1
            continue
        if len(line) == 2:
            sha1, status = line[0:2]
        else:
            print("WEIRD: {}".format(raw_line))
            assert len(line) <= 2

        # parse/validate SHA-1
        if sha1.startswith("sha1:"):
            sha1 = sha1[5:]
        if not len(sha1) == 32:
            counts['skip-bad-sha1'] += 1
            continue
        status = status.strip()

        res = c.execute('UPDATE crawl_result SET postproc_status=? WHERE final_sha1=?', [status, sha1])
        if res.rowcount == 0:
            counts['sha1-not-found'] += 1
        else:
            counts['rows-updated'] += res.rowcount

        i = i+1
        if i % 2000 == 0:
            print("... postprocess {}".format(i))
            output_db.commit()

    output_db.commit()

    c.close()
    print("Forward map complete.")
    print(counts)
    return counts

def dump_json(read_db, only_identifier_hits=False, max_per_identifier=None):

    read_db.row_factory = sqlite3.Row
    if only_identifier_hits:
        sys.stderr.write("Only dumping hits with identifiers\n\r")
        cur = read_db.execute("SELECT * FROM crawl_result WHERE hit = 1 AND identifier IS NOT NULL ORDER BY identifier;")
    else:
        sys.stderr.write("Dumping all rows\n\r")
        cur = read_db.execute("SELECT * FROM crawl_result ORDER BY identifier;")

    last_ident = None
    ident_count = 0
    for row in cur:
        if last_ident and row[1] == last_ident:
            ident_count += 1
            if max_per_identifier and ident_count > max_per_identifier:
                sys.stderr.write("SKIPPING identifier maxed out: {}\n\r".format(last_ident))
                continue
        else:
            ident_count = 0
        last_ident = row[1]
        print(json.dumps(dict(row)))

def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    sub_referrer = subparsers.add_parser('referrer')
    sub_referrer.set_defaults(func=referrer)
    sub_referrer.add_argument("log_file",
        default=sys.stdin, type=argparse.FileType('rt'))
    sub_referrer.add_argument("map_db_file",
        type=str)

    sub_backward_cdx = subparsers.add_parser('backward_cdx')
    sub_backward_cdx.set_defaults(func=backward_cdx)
    sub_backward_cdx.add_argument("cdx_file",
        default=sys.stdin, type=argparse.FileType('rt'))
    sub_backward_cdx.add_argument("map_db_file",
        type=str)
    sub_backward_cdx.add_argument("output_db_file",
        type=str)

    sub_backward = subparsers.add_parser('backward')
    sub_backward.set_defaults(func=backward)
    sub_backward.add_argument("log_file",
        default=sys.stdin, type=argparse.FileType('rt'))
    sub_backward.add_argument("map_db_file",
        type=str)
    sub_backward.add_argument("output_db_file",
        type=str)

    sub_forward = subparsers.add_parser('forward')
    sub_forward.set_defaults(func=forward)
    sub_forward.add_argument("seed_id_file",
        default=sys.stdin, type=argparse.FileType('rt'))
    sub_forward.add_argument("map_db_file",
        type=str)
    sub_forward.add_argument("output_db_file",
        type=str)

    sub_everything = subparsers.add_parser('everything')
    sub_everything.set_defaults(func=everything)
    sub_everything.add_argument("log_file",
        type=str)
    sub_everything.add_argument("seed_id_file",
        default=sys.stdin, type=argparse.FileType('rt'))
    sub_everything.add_argument("output_db_file",
        type=str)
    sub_everything.add_argument("--map_db_file",
        default=":memory:", type=str)

    sub_postprocess = subparsers.add_parser('postprocess')
    sub_postprocess.set_defaults(func=postprocess)
    sub_postprocess.add_argument("sha1_status_file",
        default=sys.stdin, type=argparse.FileType('rt'))
    sub_postprocess.add_argument("db_file",
        type=str)

    sub_dump_json = subparsers.add_parser('dump_json')
    sub_dump_json.set_defaults(func=dump_json)
    sub_dump_json.add_argument("db_file",
        type=str)
    sub_dump_json.add_argument("--only-identifier-hits",
        action="store_true",
        help="only dump rows where hit=true and identifier is non-null")
    sub_dump_json.add_argument("--max-per-identifier",
        default=False, type=int,
        help="don't dump more than this many rows per unique identifier")

    parser.add_argument("--html-hit",
        action="store_true",
        help="run in mode that considers only terminal HTML success")

    args = parser.parse_args()
    if not args.__dict__.get("func"):
        print("tell me what to do! (try --help)")
        sys.exit(-1)

    if args.html_hit:
        hit_mimetypes = (
            "text/html",
        )
    else:
        hit_mimetypes = FULLTEXT_MIMETYPES

    if args.func is referrer:
        referrer(args.log_file,
                 sqlite3.connect(args.map_db_file, isolation_level='EXCLUSIVE'))
    elif args.func is backward_cdx:
        backward_cdx(args.cdx_file,
                 sqlite3.connect(args.map_db_file, isolation_level='EXCLUSIVE'),
                 sqlite3.connect(args.output_db_file, isolation_level='EXCLUSIVE'),
                 hit_mimetypes=hit_mimetypes)
    elif args.func is backward:
        backward(args.log_file,
                 sqlite3.connect(args.map_db_file, isolation_level='EXCLUSIVE'),
                 sqlite3.connect(args.output_db_file, isolation_level='EXCLUSIVE'),
                 hit_mimetypes=hit_mimetypes)
    elif args.func is forward:
        forward(args.seed_id_file,
                sqlite3.connect(args.map_db_file, isolation_level='EXCLUSIVE'),
                sqlite3.connect(args.output_db_file, isolation_level='EXCLUSIVE'))
    elif args.func is everything:
        everything(args.log_file,
                 args.seed_id_file,
                 sqlite3.connect(args.map_db_file),
                 sqlite3.connect(args.output_db_file, isolation_level='EXCLUSIVE'),
                 hit_mimetypes=hit_mimetypes)
    elif args.func is postprocess:
        postprocess(args.sha1_status_file,
                 sqlite3.connect(args.db_file, isolation_level='EXCLUSIVE'))
    elif args.func is dump_json:
        dump_json(sqlite3.connect(args.db_file, isolation_level='EXCLUSIVE'),
            only_identifier_hits=args.only_identifier_hits,
            max_per_identifier=args.max_per_identifier)
    else:
        raise NotImplementedError

if __name__ == '__main__':
    main()

