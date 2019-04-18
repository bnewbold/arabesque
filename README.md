

                          _                            
                         | |                           
         __,   ,_    __, | |   _   ,   __,          _  
        /  |  /  |  /  | |/ \_|/  / \_/  |  |   |  |/  
        \_/|_/   |_/\_/|_/\_/ |__/ \/ \_/|_/ \_/|_/|__/
                                         |\            
                                         |/            


A simple python3 script to summarize Heritrix3 web crawl logs for a particular
style of crawl: fetching large numbers of files associated with a persistent
identifier. For example, crawling tens of millions of Open Access PDFs (via
direct link or landing page URL) associated with a DOI.

Output is a (large) sqlite3 database file. Combine with
[`sqlite-notebook`](https://github.com/bnewbold/sqlite-notebook) to generate
HTML reports:

    https://github.com/bnewbold/sqlite-notebook

The simplest usage is to specify a seed-url/identifier mapping, a crawl log,
and an output database file name:

    ./arabesque.py everything examples/crawl.log examples/seed_doi.tsv output.sqlite3
    ./arabesque.py postprocess examples/grobid_status_codes.tsv output.sqlite3

Then generate an HTML report:

    sqlite-notebook.py examples/report_template.md output.sqlite3 > report.html

The core feature of this script to is resolve HTTP redirect chains. In the
"backward" mode, all terminal responses (HTTP 200) that are in-scope (by
mimetype) are resolved back to their original seed URL. There may be multiple
in-scope terminal responses per seed (eg, via embeds or other URL extraction
beans). In the "forward" mode, redirects are resolved to a single terminal
response (if there is one), which may be 4xx, 5xx, or other failure response
code.

The result is a single summary table with the following SQL schema:

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
         hit bool);

There aren't many tests, but what there is can be run with:

    pytest-3 arabesque.py
