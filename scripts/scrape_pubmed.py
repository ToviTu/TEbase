from Bio import Entrez
import os
import json
import re
import tqdm
import time

Entrez.email = "jianhong.t@wustl.edu"  # Replace with your real email
checkpoint_file = "checkpoint.json"
output_dir = f"{os.environ['MY_HOME']}/datasets/TEbase/pubmed"
MAX = 100000

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

import urllib.error


def retry(max_retries=5, backoff_factor=1.0):
    def wrapper(func):
        def inner(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except urllib.error.HTTPError as e:
                    if e.code == 400:
                        wait = backoff_factor * (2**attempt)
                        print(
                            f"HTTP 400 Error, retrying in {wait:.1f} sec... (attempt {attempt+1})"
                        )
                        time.sleep(wait)
                    else:
                        raise  # re-raise non-400 errors
            raise RuntimeError("Max retries exceeded.")

        return inner

    return wrapper


def search_pubmed(query):
    handle = Entrez.esearch(db="pubmed", term=query, usehistory="y", retmax=0)
    record = Entrez.read(handle)
    handle.close()
    return {
        "WebEnv": record["WebEnv"],
        "QueryKey": record["QueryKey"],
        "Count": int(record["Count"]),
        "RetStart": 0,
    }


@retry(max_retries=3, backoff_factor=1.0)
def fetch_details(webenv, query_key, retstart=0, retmax=100):
    handle = Entrez.efetch(
        db="pubmed",
        WebEnv=webenv,
        query_key=query_key,
        retstart=retstart,
        retmax=retmax,
        rettype="abstract",
        retmode="xml",
    )
    records = Entrez.read(handle)
    handle.close()
    return records


def get_publication_year(pub_date_info):
    if "Year" in pub_date_info:
        return pub_date_info["Year"]
    elif "MedlineDate" in pub_date_info:
        # Handle range like "2023 Jan-Feb"
        return pub_date_info["MedlineDate"].split()[0]
    else:
        return "UnknownYear"


def sanitize_filename(s):
    # Remove special characters and replace spaces with underscores
    return re.sub(r"[^A-Za-z0-9_]+", "_", s).strip("_")


def parse_records(records):
    papers = []
    for article in records["PubmedArticle"]:
        article_info = article["MedlineCitation"]["Article"]
        pub_date_info = (
            article_info.get("Journal", {}).get("JournalIssue", {}).get("PubDate", {})
        )

        title = article_info.get("ArticleTitle", "")
        abstract = ""
        if "Abstract" in article_info:
            abstract = " ".join(
                [item for item in article_info["Abstract"]["AbstractText"]]
            )

        authors = []
        for author in article_info.get("AuthorList", []):
            name = author.get("LastName", "") + " " + author.get("Initials", "")
            authors.append(name)

        journal = article_info.get("Journal", {}).get("Title", "")
        doi = [
            str(field)
            for field in article_info.get("ELocationID", "")
            if field.attributes.get("EIdType") == "doi"
        ]
        doi = doi[0] if doi else ""
        year = get_publication_year(pub_date_info)
        first_author = authors[0] if authors else "Unknown"

        papers.append(
            {
                "title": title,
                "abstract": abstract,
                "authors": authors,
                "journal": journal,
                "doi": doi,
                "year": year,
                "first_author": first_author,
            }
        )
    return papers


def save_checkpoint(cp):
    with open(checkpoint_file, "w") as f:
        json.dump(cp, f)


def load_checkpoint():
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file) as f:
            return json.load(f)
    return None


if __name__ == "__main__":
    os.makedirs(output_dir, exist_ok=True)

    query = (
        "transposable elements[Title/Abstract] OR "
        "mobile genetic elements[Title/Abstract] OR "
        "jumping genes[Title/Abstract] OR "
        "transposition mechanisms[Title/Abstract] OR "
        "TE class[Title/Abstract] OR "
        "TE family[Title/Abstract] OR "
        "TE subfamily[Title/Abstract] OR "
        "mobile element[Title/Abstract] OR "
        "repeating element[Title/Abstract] OR "
        "transposon[Title/Abstract] OR "
        "RepeatMasker[Title/Abstract] OR "
        "Repbase[Title/Abstract] OR "
        "Dfam[Title/Abstract]"
    )
    chunk_size = 1000

    checkpoint = load_checkpoint()
    if checkpoint is None:
        checkpoint = search_pubmed(query)
        save_checkpoint(checkpoint)

    webenv = checkpoint["WebEnv"]
    query_key = checkpoint["QueryKey"]
    total = checkpoint["Count"]
    retstart = checkpoint["RetStart"]

    for i, start in enumerate(tqdm.tqdm(range(retstart, total, chunk_size))):
        records = fetch_details(webenv, query_key, start, chunk_size)
        papers = parse_records(records)

        with open(f"{output_dir}/papers_chunk_{start}.json", "w") as f:
            json.dump(papers, f, indent=2)

        checkpoint["RetStart"] = start + chunk_size
        save_checkpoint(checkpoint)
        time.sleep(0.4)
