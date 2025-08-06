from tavily import TavilyClient
import os
import re
import csv
from urllib.parse import urlparse

api_key = os.getenv("TAVILY_API_KEY")
tavily = TavilyClient(api_key)

# Function to extract first email from text
def extract_email(text):
    if not text:
        return None
    match = re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
    return match.group(0) if match else None

# Load previously seen domains from CSV
exclude_domains = set()
if os.path.exists("results.csv"):
    with open("results.csv", mode="r", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            url = row.get("URL")
            if url:
                parsed = urlparse(url)
                if parsed.netloc:
                    exclude_domains.add(parsed.netloc)

# Perform the search
search_response = tavily.search(
    "breast cancer new york city contact email",
    max_results=3,
    include_raw_content=True,
    exclude_domains=list(exclude_domains)
)

# Save results to CSV
with open("results.csv", mode="a", newline="", encoding="utf-8") as csv_file:
    writer = csv.writer(csv_file)
    if csv_file.tell() == 0:
        writer.writerow(["URL", "Email"])  # Write header if file is empty

    for result in search_response.get("results", []):
        url = result.get("url")
        raw_content = result.get("raw_content")
        email = extract_email(raw_content)
        writer.writerow([url, email if email else "No email found"])
        print(f"{url}, {email if email else 'No email found'}")
