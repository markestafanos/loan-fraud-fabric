"""
reference_data.py
-----------------
Builds the two "reference" files the project needs BEFORE streaming starts:

  data/customers.csv      -> the pool of existing bank customers the producer picks from
  data/credit_bureau.csv  -> one credit-bureau record per customer (Darshil's CIBIL file,
                             re-made for Australia: Equifax / Experian / illion)

Run once:   python producer/reference_data.py --customers 20000
Same seed = same customers every time, so the stream and the bureau file always match
(this fixes the CUST_1 vs CUST1 ID mismatch in the original course file).
"""
import argparse                       # reads options typed after the command, e.g. --customers 20000
import csv                            # writes .csv files
import random                         # random numbers and random choices
from datetime import date, timedelta  # date = a calendar day, timedelta = a length of time (e.g. 30 days)
from pathlib import Path              # builds file paths that work on Mac and Windows

from faker import Faker               # makes realistic fake names, IP addresses, etc.

# Path to the project's data/ folder:
# __file__ = this file -> .parent = producer/ -> .parent = project folder -> / "data"
DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# A dictionary (key -> value lookup) of Australian states.
# For each state: "weight" = how common customers are there, and its cities.
# For each city: (list of postcodes, (latitude, longitude) of the city centre)
STATES = {
    "NSW": {"weight": 32, "cities": {"Sydney": (["2000", "2150", "2170", "2765"], (-33.87, 151.21)),
                                      "Newcastle": (["2300", "2289"], (-32.93, 151.78)),
                                      "Wollongong": (["2500"], (-34.42, 150.89))}},
    "VIC": {"weight": 26, "cities": {"Melbourne": (["3000", "3121", "3168"], (-37.81, 144.96)),
                                      "Geelong": (["3220"], (-38.15, 144.36)),
                                      "Ballarat": (["3350"], (-37.56, 143.85))}},
    "QLD": {"weight": 20, "cities": {"Brisbane": (["4000", "4101"], (-27.47, 153.03)),
                                      "Gold Coast": (["4217"], (-28.02, 153.40)),
                                      "Townsville": (["4810"], (-19.26, 146.82))}},
    "WA":  {"weight": 10, "cities": {"Perth": (["6000", "6050"], (-31.95, 115.86)),
                                      "Bunbury": (["6230"], (-33.33, 115.64))}},
    "SA":  {"weight": 7,  "cities": {"Adelaide": (["5000", "5067"], (-34.93, 138.60))}},
    "TAS": {"weight": 2,  "cities": {"Hobart": (["7000"], (-42.88, 147.33)),
                                      "Launceston": (["7250"], (-41.44, 147.14))}},
    "ACT": {"weight": 2,  "cities": {"Canberra": (["2600", "2612"], (-35.28, 149.13))}},
    "NT":  {"weight": 1,  "cities": {"Darwin": (["0800", "0810"], (-12.46, 130.84))}},
}

# The column names of customers.csv, in order (a list of text values)
CUSTOMER_FIELDS = ["customer_id", "full_name", "date_of_birth", "gender", "city", "state",
                   "postcode", "employment_type", "annual_income", "account_open_date",
                   "kyc_status", "customer_status", "home_device_id", "home_device_type", "home_ip"]


def pick_location(rng):
    # rng = our random-number generator. choices() picks using weights, so NSW comes up most often.
    state = rng.choices(list(STATES), weights=[s["weight"] for s in STATES.values()])[0]
    city = rng.choice(list(STATES[state]["cities"]))       # random city in that state
    postcodes, centre = STATES[state]["cities"][city]      # unpack the (postcodes, centre) pair
    return state, city, rng.choice(postcodes), centre      # return four values at once


def make_customer(i, rng, fake):
    # i = customer number, rng = random generator, fake = Faker. Returns one customer as a dictionary.
    state, city, postcode, _ = pick_location(rng)          # _ means "I don't need this value"
    age = rng.randint(21, 75)                              # whole number from 21 to 75
    dob = date.today() - timedelta(days=age * 365 + rng.randint(0, 364))  # today minus their age
    return {
        "customer_id": f"CUST{i:05d}",                     # f-string: CUST + i padded to 5 digits -> CUST00042
        "full_name": fake.name(),
        "date_of_birth": dob.isoformat(),                  # date as text: 1990-08-12
        "gender": rng.choice(["M", "F"]),
        "city": city,
        "state": state,
        "postcode": postcode,
        "employment_type": rng.choices(["SALARIED", "SELF_EMPLOYED", "CONTRACT"], [70, 20, 10])[0],
        "annual_income": rng.randrange(45000, 220001, 500),  # $45,000 to $220,000 in steps of $500
        "account_open_date": (date(2005, 1, 1) + timedelta(days=rng.randint(0, 7300))).isoformat(),
        "kyc_status": rng.choices(["VERIFIED", "PENDING"], [92, 8])[0],     # 92% have verified ID
        "customer_status": rng.choices(["ACTIVE", "INACTIVE"], [90, 10])[0],
        "home_device_id": f"DEV{rng.getrandbits(40):010x}",  # random ID written in hexadecimal
        "home_device_type": rng.choice(["ANDROID", "IOS", "WEB"]),
        "home_ip": fake.ipv4_public(),
    }


def make_bureau_row(cust, rng, report_date):
    # One credit report for one customer.
    score = int(min(900, max(350, rng.gauss(700, 90))))   # bell curve around 700, kept between 350 and 900
    return {
        "CUSTOMER_ID": cust["customer_id"],                # same ID as the customer -> joins will match
        "CREDIT_SCORE": score,
        "TOTAL_ACCOUNTS": rng.randint(1, 10),
        "ACTIVE_LOANS": rng.randint(0, 5),
        "PAST_DEFAULTS": rng.choices([0, 1, 2, 3], [80, 12, 6, 2])[0],   # most people: 0 defaults
        "ENQUIRIES_LAST_6M": rng.randint(0, 9),
        "BUREAU_SOURCE": rng.choice(["EQUIFAX", "EXPERIAN", "ILLION"]),
        "REPORT_DATE": report_date.isoformat(),
    }


def main():
    parser = argparse.ArgumentParser()                     # set up the options this script accepts
    parser.add_argument("--customers", type=int, default=20000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()                             # read what was typed: args.customers, args.seed

    rng = random.Random(args.seed)                         # same seed -> same "random" results every run
    fake = Faker("en_AU")                                  # Australian-style names
    Faker.seed(args.seed)

    DATA_DIR.mkdir(exist_ok=True)                          # create data/ if it isn't there
    # list comprehension: build one customer for every number from 1 to args.customers
    customers = [make_customer(i, rng, fake) for i in range(1, args.customers + 1)]

    with open(DATA_DIR / "customers.csv", "w", newline="") as f:   # "w" = write; closes the file automatically
        w = csv.DictWriter(f, fieldnames=CUSTOMER_FIELDS)          # writer that takes dictionaries
        w.writeheader()                                            # first row = column names
        w.writerows(customers)                                     # one row per customer

    today = date.today()
    with open(DATA_DIR / "credit_bureau.csv", "w", newline="") as f:
        rows = [make_bureau_row(c, rng, today) for c in customers]  # one report per customer
        w = csv.DictWriter(f, fieldnames=list(rows[0]))             # column names = keys of the first row
        w.writeheader()
        w.writerows(rows)

    print(f"Wrote {len(customers):,} customers and {len(rows):,} bureau rows to {DATA_DIR}")


if __name__ == "__main__":                                 # only true when you run this file directly
    main()