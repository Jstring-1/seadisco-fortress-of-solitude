"""
Parse the OCR'd Blues Words text into structured entries.

Approach: citations are the most reliably-detectable feature
(em-dash + Author, "Title," YYYY). Walk the line stream, anchor on
citation lines, and reconstruct entries around them:

  - Walk backward from each citation through the quote lines, blank
    lines, to find the headword.
  - Walk forward through the definition prose until the next quote
    block (secondary citation within same entry) or next headword
    (new entry).

Headword recognition: a short line (<=55 chars) that is
  - mostly lowercase,
  - not a citation,
  - not lyric prose (heuristics: doesn't end with mid-sentence
    punctuation like "—" or quoted dialect, doesn't start with a
    quote/dash, doesn't contain typical lyric verbs like "got" or
    pronouns that signal a lyric line),
  - has at least one short token (headwords are typically 1-5 words).

Output: scripts/blueswords-a-c.json — list of:
  { headword, definition, citations: [{quote, artist, song, year, position}],
    source_volume, source_pages }
"""
import re, json, os

OCR = r"C:\Users\KJ-NoJesteringStudio\GitHub\discogs-mcp-server\scripts\blueswords-ocr\all.txt"
OUT = r"C:\Users\KJ-NoJesteringStudio\GitHub\discogs-mcp-server\scripts\blueswords-a-c.json"

# -Artist, "Song Title," YYYY  (em/en/hyphen-dash + curly/straight quotes)
CIT_RE = re.compile(
    r'[—–\-]{1,3}\s*'                        # leading dash
    r'([A-Z][A-Za-z0-9\.\'\"’ \-\&]+?)'      # artist
    r',\s*[\"“‘\']'                          # opening quote
    r'(.+?)'                                  # song title
    r'[\"”’\']\s*[,\.]?\s*'                  # closing quote
    r'(\d{4})'                                # year
)

PAGE_RE = re.compile(r"^===PAGE (\d+)===$")

# Running-head footer in all caps, e.g. "ALL RIGHT WITH ONE, TO BE 3"
RUNNING_HEAD_RE = re.compile(r"^[A-Z][A-Z\s,\-\'\.\(\)]{3,}\s*\d*\s*$")


# Common sentence-starter words that appear in body prose but are
# almost never the first word of a dictionary headword.
PROSE_STARTERS = {
    "the", "a", "an", "this", "that", "these", "those", "his", "her",
    "she", "he", "they", "it", "we", "you", "i",
    "and", "but", "or", "so", "yet", "for", "nor",
    "also", "thus", "while", "although", "though", "whereas",
    "as", "if", "when", "where", "what", "which", "who", "whom", "whose",
    "in", "on", "at", "by", "with", "from", "to", "of", "into", "onto",
    "during", "before", "after", "since", "until", "between", "among",
    "however", "moreover", "indeed", "perhaps", "rather",
    "according", "based", "found", "noted", "originally",
    "no", "not", "yes", "now", "then",
    "see", "cf",
}

# Word patterns that indicate OCR garbage (mojibake, fused tokens)
def has_junk_chars(s):
    return any(ord(c) < 32 or c in '{}|' or c == chr(0xfffd) for c in s) or bool(__import__('re').search(r'[a-z]{14,}', s))


def is_headword_candidate(line):
    """A short, mostly-lowercase line that doesn't look like a lyric or prose."""
    s = line.strip()
    if not s or len(s) > 55:
        return False
    # Skip running heads
    if RUNNING_HEAD_RE.match(s):
        return False
    # Skip lines that contain citations
    if CIT_RE.search(s):
        return False
    # Skip lyric/dialogue starts and OCR garbage
    if s[0] in '"“‘\'—-({[':
        return False
    if re.match(r"^[A-Z]", s):
        return False
    if has_junk_chars(s):
        return False
    # Sentence-ending punctuation rules out body prose
    if s.endswith(('.', '!', '?', ':', ';')) and not s.endswith(('(a.)', '(n.)', '(v.)')):
        return False
    # Reject if no vowels (OCR noise)
    if not re.search(r"[aeiou]", s):
        return False
    if len(s.split()) > 8:
        return False
    # Must have at least one alphabetic char in first 3 positions
    if not re.search(r"[a-z]", s[:3]):
        return False
    # First word must not be a common prose-starter
    first = re.split(r"[\s\-,]+", s, maxsplit=1)[0].lower().strip("'’")
    if first in PROSE_STARTERS:
        return False
    # Require the line to be primarily lowercase letters & spaces — at
    # least 60% of chars should be a-z or space (eliminates noise like
    # "{got a crow to pick with you.�")
    letters = sum(1 for c in s if c.isalpha() or c == ' ' or c in "-',")
    if letters / max(1, len(s)) < 0.85:
        return False
    return True


def parse_citation(line):
    m = CIT_RE.search(line)
    if not m:
        return None
    artist = m.group(1).strip().rstrip(",.")
    # Trim leading "Mr." / artifacts? Keep as-is for now.
    song = m.group(2).strip().rstrip(",.")
    year = int(m.group(3))
    return { "artist": artist, "song": song, "year": year }


def main():
    with open(OCR, "r", encoding="utf-8") as f:
        raw = f.read()

    # Normalize OCR mojibake: U+FFFD (replacement char) usually replaces a
    # curly apostrophe in this book.
    raw = raw.replace("�", "'")

    # Tag each line with current page number
    tagged = []
    cur_page = 0
    for L in raw.splitlines():
        m = PAGE_RE.match(L.strip())
        if m:
            cur_page = int(m.group(1))
            continue
        tagged.append((cur_page, L))

    # First pass: find citation line indices
    citation_idxs = []
    for i, (_, L) in enumerate(tagged):
        if CIT_RE.search(L):
            citation_idxs.append(i)

    # Group consecutive citations? In this book each entry can have
    # multiple citations (primary + secondary) separated by definition
    # prose. We'll build entries by walking through citations in order
    # and detecting headword boundaries between them.

    entries = []
    cur = None

    def commit(e):
        if not e:
            return
        defn = " ".join(e["_defn"]).strip()
        defn = re.sub(r"\s+", " ", defn)
        defn = re.sub(r"[—\-\.\,\s]{4,}$", "", defn).strip()
        entries.append({
            "headword": e["headword"].lower(),
            "definition": defn,
            "citations": e["citations"],
            "source_volume": "A-C",
            "source_pages": sorted(set(e["pages"])),
        })

    # Walk lines. Maintain a sliding buffer of "uncommitted" content.
    # When a citation appears, look back to find the most recent
    # headword candidate that has quote lines between it and the
    # citation. That becomes a new entry (if no current entry) or a
    # secondary citation (if the headword candidate isn't strong).

    i = 0
    N = len(tagged)
    last_headword_idx = -1  # index of most-recent headword candidate

    while i < N:
        page, line = tagged[i]
        s = line.strip()

        if not s:
            i += 1
            continue

        # Skip running heads
        if RUNNING_HEAD_RE.match(s):
            i += 1
            continue

        cit = parse_citation(s)

        if cit:
            # Determine whether this citation belongs to a new entry
            # or to the current one. Heuristic: if there's a strong
            # headword candidate between the previous citation and now,
            # this citation starts a new entry. Otherwise it's a
            # secondary citation under the current entry.
            new_entry_head = None
            new_entry_pages = []
            new_entry_quote = []
            if last_headword_idx > -1:
                # Pull the headword candidate
                hp, hl = tagged[last_headword_idx]
                # Gather lines between headword and citation as quote
                between = []
                for j in range(last_headword_idx+1, i):
                    bp, bl = tagged[j]
                    bs = bl.strip()
                    if not bs:
                        continue
                    if RUNNING_HEAD_RE.match(bs):
                        continue
                    between.append(bs)
                    new_entry_pages.append(bp)
                new_entry_head = hl.strip()
                new_entry_pages.append(hp)
                new_entry_quote = between

            if new_entry_head:
                # Start a new entry
                commit(cur)
                cur = {
                    "headword": new_entry_head,
                    "_defn": [],
                    "citations": [{
                        "quote": "\n".join(new_entry_quote).strip(),
                        "artist": cit["artist"],
                        "song": cit["song"],
                        "year": cit["year"],
                        "position": 1,
                    }],
                    "pages": new_entry_pages + [page],
                }
                last_headword_idx = -1
            else:
                # Secondary citation under current entry
                if cur is None:
                    # Drift case: a citation appeared but we never had
                    # a headword. Skip — this is OCR noise.
                    pass
                else:
                    # The "quote" for the secondary citation is the
                    # current accumulated definition tail — actually,
                    # better: the quote lines that came AFTER the last
                    # citation but before this one. We don't have an
                    # easy way to separate, so accumulate into _defn
                    # and let the user review.
                    cur["citations"].append({
                        "quote": "",  # secondary quotes blur into defn for now
                        "artist": cit["artist"],
                        "song": cit["song"],
                        "year": cit["year"],
                        "position": len(cur["citations"]) + 1,
                    })
                    cur["pages"].append(page)
            i += 1
            continue

        # Not a citation: either a headword candidate, a quote line, or
        # definition prose. Track most-recent headword candidate.
        if is_headword_candidate(s):
            last_headword_idx = i
        elif cur is not None:
            # Prose under current entry
            cur["_defn"].append(s)
            cur["pages"].append(page)

        i += 1

    commit(cur)

    # Deduplicate by headword: if OCR produced two entries with the
    # same headword string, merge them (rare).
    by_head = {}
    for e in entries:
        h = e["headword"]
        if h in by_head:
            prev = by_head[h]
            prev["citations"].extend(e["citations"])
            prev["definition"] += " " + e["definition"]
            prev["source_pages"] = sorted(set(prev["source_pages"] + e["source_pages"]))
        else:
            by_head[h] = e
    final = list(by_head.values())

    # Trim definitions of obvious noise: stray uppercase fragments
    for e in final:
        d = e["definition"]
        # collapse spaces again post-merge
        d = re.sub(r"\s+", " ", d).strip()
        e["definition"] = d

    # Drop entries with no citation AND no definition
    final = [e for e in final if e["citations"] or e["definition"]]

    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(final, f, indent=2, ensure_ascii=False)

    print(f"parsed {len(final)} entries -> {OUT}")
    cit_counts = [len(e["citations"]) for e in final]
    print(f"total citations: {sum(cit_counts)}  avg/entry: {sum(cit_counts)/max(1,len(final)):.2f}")
    print(f"first 10 headwords:")
    for e in final[:10]:
        print(f"  {e['headword']!r:40s} cits={len(e['citations'])} pages={e['source_pages']}")
    print(f"last 10 headwords:")
    for e in final[-10:]:
        print(f"  {e['headword']!r:40s} cits={len(e['citations'])} pages={e['source_pages']}")


if __name__ == "__main__":
    main()
