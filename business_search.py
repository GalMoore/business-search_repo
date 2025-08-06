from tavily import TavilyClient
import os
import re
import csv
from urllib.parse import urlparse
import time

# Init Tavily
api_key = os.getenv("TAVILY_API_KEY")
tavily = TavilyClient(api_key)

# Folder for CSV files
output_folder = "breast_cancer_stakeholders"
os.makedirs(output_folder, exist_ok=True)

# Expanded breast cancer stakeholder search terms (NYC-focused)
base_terms = [
    # Medical & clinical
    "breast cancer clinics New York City",
    "oncology nurses breast cancer NYC",
    "oncology departments hospitals NYC",
    "plastic surgeons breast reconstruction NYC",
    "oncologists breast cancer NYC",
    
    # Products
    "wig shops for cancer patients NYC",
    "prosthetic breast shops NYC",
    "mastectomy bra shops NYC",
    "compression garments for breast cancer NYC",
    "scar care products after mastectomy NYC",
    
    # Support & wellness
    "breast cancer support groups NYC",
    "breast cancer survivor community NYC",
    "mental health therapists cancer NYC",
    "nutritionists for cancer patients NYC",
    "yoga for breast cancer survivors NYC",
    "massage therapy for cancer patients NYC",
    "rehabilitation services breast cancer NYC",
    
    # Non-profits & charities
    "breast cancer non-profits NYC",
    "breast cancer charities NYC",
    "free resources for breast cancer patients NYC",
    "community organizations for breast cancer NYC"
]

# Add "contact email" to each term
search_terms = [term + " contact email" for term in base_terms]

# Extract first email found in a string
def extract_email(text):
    if not text:
        return None
    match = re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
    return match.group(0) if match else None

# Utility to clean a filename from a search term
def sanitize_filename(term):
    return re.sub(r'[^\w\s-]', '', term).replace(' ', '_').lower()

# Main loop over search terms
for term in search_terms:
    print(f"\n🔍 Running search for: {term}")

    filename = os.path.join(output_folder, f"{sanitize_filename(term)}.csv")
    seen_domains = set()

    # Load existing URLs to avoid duplicates
    if os.path.exists(filename):
        with open(filename, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get("URL")
                if url:
                    parsed = urlparse(url)
                    if parsed.netloc:
                        seen_domains.add(parsed.netloc)

    for i in range(2):  # Adjust the number of search iterations if needed
        print(f"  ▶ Run {i + 1}/2")

        # Perform the Tavily search
        search_response = tavily.search(
            term,
            max_results=20,
            include_raw_content=True,
            exclude_domains=list(seen_domains)
        )

        # Write results to CSV
        with open(filename, mode="a", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            if csv_file.tell() == 0:
                writer.writerow(["URL", "Email"])  # Header

            for result in search_response.get("results", []):
                url = result.get("url")
                raw_content = result.get("raw_content")
                email = extract_email(raw_content)

                if url:
                    parsed = urlparse(url)
                    if parsed.netloc:
                        seen_domains.add(parsed.netloc)

                writer.writerow([url, email if email else "No email found"])
                print(f"    ✔ {url}, {email if email else 'No email found'}")

        # Optional delay to avoid rate limiting
        # time.sleep(2)

print("\n✅ All search terms completed.")
