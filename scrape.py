import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from supabase import create_client
from datetime import datetime

url = "https://books.toscrape.com/"
response = requests.get(url, timeout=20)
response.encoding = "utf-8"                            # ensure £ signs display correctly
soup = BeautifulSoup(response.text, "html.parser")

rows = []

scrape_time = datetime.now().isoformat()

for book in soup.select(".product_pod"):

    title = book.select_one("h3 a")["title"]
    price_text = book.select_one(".price_color").get_text(strip=True)
    availability = book.select_one(".availability").get_text(" ", strip=True)  # new field

    # The rating is stored as a CSS class, e.g. class="star-rating Three"
    # We grab the <p> element, then read its class list to find the rating word
    rating_paragraph = book.select_one("p.star-rating")  # new field
    rating = None
    if rating_paragraph:
        classes = rating_paragraph.get("class", [])           # e.g. ["star-rating", "Three"]
        # next() picks the first item from a filtered list
        # here: loop through classes, skip "star-rating", keep the rating word ("Three")
        # if nothing is found, return None instead of crashing
        rating = next((c for c in classes if c != "star-rating"), None)

    rows.append({
        "title": title,
        "price_text": price_text,
        "availability": availability,  # new field
        "rating": rating,
        "scraped_at": scrape_time,
    })

df = pd.DataFrame(rows)








# Remove currency symbols and convert to a decimal number
# Note: you may see "Â£51.77" instead of "£51.77" — this is a
# common encoding issue. The regex below strips both variants.
df["price"] = (
    df["price_text"]
      .str.replace(r"[^0-9.]", "", regex=True)  # keep only digits and dots
      .astype(float)
)
# Fill missing ratings with "Unknown" so they still appear in charts
df["rating"] = df["rating"].fillna("Unknown")

df = df.rename(columns={
    "price_text": "price_raw",     # original scraped string
    "price": "price_eur",          # cleaned numeric price
    "rating": "star_rating"        # clarify what kind of rating
})

# Add a price tier for dashboard grouping
# .apply() runs a function on every value in the column
# The lambda checks: is the price below 20? → "Budget"
#                     below 40? → "Mid-range"
#                     otherwise → "Premium"
df["price_tier"] = df["price_eur"].apply(
    lambda p: "Budget" if p < 20
    else ("Mid-range" if p < 40 else "Premium")
)

# Create a lookup dictionary: word → number
rating_map = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}

# .map() replaces each value using the dictionary
# "Three" becomes 3, "Five" becomes 5, etc.
df["rating_numeric"] = df["star_rating"].map(rating_map)

# .str.contains() checks if the text includes "In stock"
# Returns True or False for each row
# na=False means: if the value is missing, return False instead of NaN
df["in_stock"] = df["availability"].str.contains("In stock", na=False)


# Example: drop the raw price string (you already have price_eur)
# errors="ignore" means: if the column does not exist, do nothing
df = df.drop(columns=["price_raw"], errors="ignore")











SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

scrape_time = datetime.now().isoformat()

rows = []
for _, row in df.iterrows():
    rows.append({
        "title":        str(row.get("title", "")),
        "price_eur":    float(row.get("price_eur", 0)),
        "star_rating":  str(row.get("star_rating", "")),
        "availability": str(row.get("availability", "")),
        "scraped_at":   scrape_time,
        "price_tier": str(row.get("price_tier", "")),
        "rating_numeric": int(row.get("rating_numeric", "")),
        "in_stock": bool(row.get("in_stock", ""))
    })

result = supabase.table("books").insert(rows).execute()


try:
    response = supabase.table("books").insert(rows).execute()
    print("Successfully inserted data!")
    print(f"Inserted {len(rows)} rows.")
except Exception as e:
    print("--- SUPABASE ERROR ---")
    print(e)
    # This line forces GitHub Actions to show a Red X if it fails
    exit(1)
