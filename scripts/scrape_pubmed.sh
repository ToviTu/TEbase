#!/bin/bash

# ========= CONFIGURATION =========
QUERY="transposable elements[Title/Abstract] OR mobile genetic elements[Title/Abstract] OR jumping genes[Title/Abstract] OR transposition mechanisms[Title/Abstract] OR TE class[Title/Abstract] OR TE family[Title/Abstract] OR TE subfamily[Title/Abstract] OR mobile element[Title/Abstract] OR repeating element[Title/Abstract] OR transposon[Title/Abstract] OR RepeatMasker[Title/Abstract] OR Repbase[Title/Abstract] OR Dfam[Title/Abstract]"
CHUNK_SIZE=200
OUT_DIR="$MY_HOME/datasets/TEbase/pubmed"
PMID_FILE="$OUT_DIR/pmids.txt"
mkdir -p "$OUT_DIR"

# ========= STEP 1: Get all PMIDs =========
echo "Searching PubMed..."
esearch -db pubmed -query "$QUERY" | efetch -format uid > "$PMID_FILE"

TOTAL=$(wc -l < "$PMID_FILE")
echo "Found $TOTAL PMIDs"

# ========= STEP 2: Batch fetch =========
split -l $CHUNK_SIZE "$PMID_FILE" "$OUT_DIR/pmid_chunk_"

# ========= STEP 3: Extract metadata with Python =========
echo "Extracting metadata..."
python3 <<EOF
import os, re, json
from xml.etree import ElementTree as ET
from xml.etree.ElementTree import ParseError
import html
import time

def get_text(elem, tag):
    node = elem.find(tag)
    return node.text if node is not None else ""

def parse_article(article):
    article_info = article.find("MedlineCitation/Article")
    title = get_text(article_info, "ArticleTitle")

    abstract_texts = article_info.find("Abstract")
    abstract = " ".join(elem.text for elem in (abstract_texts or []) if elem.text) if abstract_texts else ""

    authors = []
    for author in article_info.find("AuthorList") or []:
        lastname = get_text(author, "LastName")
        initials = get_text(author, "Initials")
        if lastname or initials:
            authors.append(f"{lastname} {initials}".strip())

    journal = get_text(article_info.find("Journal"), "Title") if article_info.find("Journal") else ""
    doi = ""
    for eid in article_info.findall("ELocationID"):
        if eid.attrib.get("EIdType") == "doi":
            doi = eid.text
            break

    pub_date = article_info.find("Journal/JournalIssue/PubDate")
    year = get_text(pub_date, "Year") or get_text(pub_date, "MedlineDate").split(" ")[0] if pub_date is not None else "Unknown"
    first_author = authors[0] if authors else "Unknown"

    return {
        "title": title,
        "abstract": abstract,
        "authors": authors,
        "journal": journal,
        "doi": doi,
        "year": year,
        "first_author": first_author
    }

OUT_DIR = os.path.expanduser(f"{os.environ['MY_HOME']}/datasets/TEbase/pubmed")
for file in sorted(os.listdir(OUT_DIR)):
    if file.startswith("pmid_chunk_"):
        base = file.replace("pmid_chunk_", "papers_chunk_")
        xml_file = os.path.join(OUT_DIR, f"{base}.xml")
        json_file = os.path.join(OUT_DIR, f"{base}.json")

        with open(os.path.join(OUT_DIR, file)) as f:
            ids = f.read().strip().replace("\n", ",")

        os.system(f"efetch -db pubmed -id \"{ids}\" -format xml > \"{xml_file}\"")

        try:
            with open(xml_file, encoding="utf-8") as xf:
                root = ET.parse(xf)
                articles = root.findall("PubmedArticle")
                data = [parse_article(a) for a in articles]

            with open(json_file, "w") as f:
                json.dump(data, f, indent=2)
        except ParseError as e:
            print(f"Skipping {xml_file} due to parse error: {e}")
        
    time.sleep(0.2)
EOF

echo "All done. JSON outputs saved to: $OUT_DIR"
