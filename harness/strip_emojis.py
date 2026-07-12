import os
import re

HARNESS_DIR = os.path.dirname(os.path.abspath(__file__))

NON_ASCII_PATTERN = re.compile(r"[^\x09\x0A\x0D\x20-\x7E]+")

DASH_CHARS = "\u2010\u2011\u2012\u2013\u2014\u2015\u2500\u2501\u2E3A\u2E3B"
ARROW_MAP = {"\u2192": "->", "\u2190": "<-", "\u2194": "<->"}

def normalize(content):
    for ch in DASH_CHARS:
        content = content.replace(ch, "-")
    for arrow, repl in ARROW_MAP.items():
        content = content.replace(arrow, repl)
    content = content.replace("\u2026", "...")
    return content

def clean_file(path):
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        original = f.read()

    content = normalize(original)
    cleaned = NON_ASCII_PATTERN.sub("", content)

    if cleaned != original:
        with open(path, "w", encoding="utf-8") as f:
            f.write(cleaned)
        print(f"Cleaned: {path}")
    else:
        print(f"No emojis found: {path}")

def main():
    for fname in os.listdir(HARNESS_DIR):
        if fname.endswith(".py") and fname != "strip_emojis.py":
            clean_file(os.path.join(HARNESS_DIR, fname))

if __name__ == "__main__":
    main()
