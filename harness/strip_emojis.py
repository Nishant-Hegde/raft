import os
import re

HARNESS_DIR = os.path.dirname(os.path.abspath(__file__))

# Matches most emoji ranges (safe for this codebase — only touches print/comment text)
EMOJI_PATTERN = re.compile(
    "["
    "\U0001F300-\U0001FAFF"  # symbols, pictographs, emoticons, transport, etc.
    "\U00002600-\U000027BF"  # misc symbols, dingbats (includes ✅❌⏱️📄🗓️)
    "\U0001F1E6-\U0001F1FF"  # flags
    "\U00002700-\U000027BF"
    "\U0001F900-\U0001F9FF"
    "]+",
    flags=re.UNICODE
)

def clean_file(path):
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    cleaned = EMOJI_PATTERN.sub("", content)
    if cleaned != content:
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