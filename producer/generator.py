"""
generator.py
------------
Creates loan-application events (same JSON shape as Darshil's script: customer /
application / device blocks) and PLANTS known fraud so we can measure detection.
Every planted fraud application is written to data/planted_fraud.csv (the "answer key").
The label is NOT put inside the event - the detection rules never see the answer.
Planted fraud scenarios (about 6% of applications):
  INCOME_MISMATCH   loan asked for is 12-30x the customer's yearly income
  OFFSHORE_DEVICE   application comes from a device located outside Australia
  SYNTHETIC_ID      brand-new identity: account opened days ago, KYC pending, big loan, online
  UNDERAGE          applicant is under 18
  DEVICE_FARM       one device submits applications for 4 different customers in ~2 minutes
  APPLICATION_BURST one customer fires 4-5 applications in ~2 minutes
  STEALTH           fraud that looks normal - no rule should catch it (keeps the test honest)
"""

import csv
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from faker import Faker

# reuse things already written in reference_data.py (same folder)
from reference_data import STATES, DATA_DIR, make_customer

# product: (possible purposes, min amount, max amount, max multiple of income for a normal applicant)
PRODUCTS = {
    "PERSONAL":  (["Medical", "Debt consolidation", "Travel"], 5000, 50000, 1.0),
    "CAR":       (["New car", "Used car"], 10000, 90000, 1.5),
    "EDUCATION": (["Undergraduate", "Postgraduate", "Diploma"], 5000, 60000, 1.0),
    "BUSINESS":  (["Equipment", "Working capital", "Expansion"], 20000, 500000, 4.0),
    "HOME":      (["Purchase", "Refinance", "Renovation"], 200000, 1500000, 7.0),
}

PRODUCT_WEIGHTS = [30, 25, 10, 10, 25]          # how common each product is (same order as above)

# fraud type -> how often it's chosen when we plant fraud
SCENARIOS = {
    "INCOME_MISMATCH": 25, "OFFSHORE_DEVICE": 20, "SYNTHETIC_ID": 20, "UNDERAGE": 5,
    "DEVICE_FARM": 15, "APPLICATION_BURST": 10, "STEALTH": 5,
}

# (latitude, longitude) of cities outside Australia: Lagos, Moscow, Jakarta, Bucharest, Manila, Sao Paulo, Dubai
OFFSHORE = [(6.52, 3.38), (55.75, 37.62), (-6.21, 106.85), (44.43, 26.10),
            (14.60, 120.98), (-23.55, -46.63), (25.20, 55.27)]


def iso(ts):
    # date-time -> text like 2026-11-26T01:15:03.123456Z  (Z means UTC time)
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class EventGenerator:
    # A class = a blueprint. producer.py creates ONE generator from it and calls gen.tick() over and over.
    # 'self' = this generator object; self.something is data it keeps between calls.

    def __init__(self, fraud_rate=0.06, seed=7, truth_path=None):
        # __init__ runs once, when the generator is created.
        self.rng = random.Random(seed)                     # its own random-number generator
        self.fake = Faker("en_AU")
        Faker.seed(seed)
        self.fraud_rate = fraud_rate                       # 0.06 = 6% of sends are planted fraud
        with open(DATA_DIR / "customers.csv") as f:        # load the customer pool into memory
            self.customers = list(csv.DictReader(f))       # a list of dictionaries, one per customer
        self.app_seq = 0                                   # counter used to make application IDs unique
        self.new_cust_seq = 900000                         # IDs for identities NOT in the bank's pool
        self.pending = []                                  # waiting list: (due_time, event, scenario)
        self.truth_path = Path(truth_path or DATA_DIR / "planted_fraud.csv")   # the answer key file
        if not self.truth_path.exists():                   # first time: create it with a header row
            with open(self.truth_path, "w", newline="") as f:
                csv.writer(f).writerow(["application_id", "scenario", "event_ts"])

    # ---------- building blocks ----------
    def _next_app_id(self):
        self.app_seq += 1                                  # add 1 to the counter
        # APP + month/day/hour/minute + 6-digit counter, e.g. APP11260915000123
        return f"APP{datetime.now(timezone.utc):%m%d%H%M}{self.app_seq:06d}"

    def _jitter(self, lat, lon, km=15):
        # move a point randomly up to ~km kilometres (1 degree is about 111 km)
        d = km / 111
        return round(lat + self.rng.uniform(-d, d), 6), round(lon + self.rng.uniform(-d, d), 6)

    def _city_centre(self, cust):
        # look up the (lat, lon) centre of this customer's city in STATES
        return STATES[cust["state"]]["cities"][cust["city"]][1]

    def _normal_amount(self, product, income):
        purposes, lo, hi, mult = PRODUCTS[product]         # unpack the product's settings
        amount = self.rng.randint(lo, hi)                  # random amount in the product's range
        cap = int(income * mult)                           # the most a normal person would ask for
        if amount > cap:                                   # too big for their income? bring it down
            amount = max(int(cap * self.rng.uniform(0.6, 1.0)), 1000)
        return round(amount, -2)                           # round to the nearest $100

    def _event(self, now, cust, product=None, amount=None, channel=None, device=None):
        # Build ONE application event. Arguments with =None are optional:
        # leave them out for a normal application; fraud recipes pass them in to change things.
        # 'x or y' = use x if it was given, otherwise y. So: the product passed in, or a random one.
        product = product or self.rng.choices(list(PRODUCTS), PRODUCT_WEIGHTS)[0]
        income = int(cust["annual_income"])               # income as a whole number
        lat, lon = self._jitter(*self._city_centre(cust))  # a spot near the customer's city
        device = device or {                               # default device = the customer's own phone
            "device_id": cust["home_device_id"], "device_type": cust["home_device_type"],
            "ip_address": cust["home_ip"], "latitude": lat, "longitude": lon,
        }
        channel = channel or self.rng.choices(["MOBILE", "WEB", "BRANCH"], [50, 35, 15])[0]
        return {                                           # the event, as a dictionary (becomes JSON)
            "event_id": f"EVT{self.rng.getrandbits(48):012x}",
            "event_ts": iso(now),
            # copy these fields from the customer record, then add income as a number ( | merges dicts)
            "customer": {k: cust[k] for k in ["customer_id", "full_name", "date_of_birth", "gender",
                                              "city", "state", "postcode", "employment_type",
                                              "account_open_date", "kyc_status", "customer_status"]}
                        | {"annual_income": income},
            "application": {
                "application_id": self._next_app_id(),
                "channel": channel,
                "loan_product": product,
                "purpose": self.rng.choice(PRODUCTS[product][0]),
                # use the amount passed in, otherwise a normal one
                "requested_amount": amount if amount is not None else self._normal_amount(product, income),
                # home loans run 20-30 years; everything else 1-5 years
                "tenure_months": self.rng.choice([12, 24, 36, 60] if product != "HOME" else [240, 300, 360]),
                "interest_rate": round(self.rng.uniform(5.5, 15.5), 2),
                "branch_cd": self.rng.choice(["001", "034", "098", "076", "089"]),
                "application_status": "SUBMITTED",
            },
            "device": {
                "device_id": device["device_id"],
                "device_type": device["device_type"],
                "os_version": self.rng.choice(["15", "16", "17", "18"]),
                "browser": self.rng.choice(["CHROME", "EDGE", "SAFARI", "FIREFOX"]),
                "ip_address": device["ip_address"],
                "latitude": device["latitude"],
                "longitude": device["longitude"],
            },
        }

    def _new_identity(self, now, age=None, opened_days_ago=None):
        # A person the bank has never seen (not in customers.csv).
        self.new_cust_seq += 1
        c = make_customer(self.new_cust_seq, self.rng, self.fake)   # reuse reference_data's recipe
        if age is not None:                                # force a young age (underage fraud)
            c["date_of_birth"] = (now.date() - timedelta(days=age * 365 + self.rng.randint(10, 300))).isoformat()
        if opened_days_ago is not None:                    # brand-new account + unverified ID (synthetic)
            c["account_open_date"] = (now.date() - timedelta(days=opened_days_ago)).isoformat()
            c["kyc_status"] = "PENDING"
        return c

    # ---------- planted fraud ----------
    def _plant(self, now, scenario):
        """Returns a list of (due_time, event) for one planted scenario."""
        cust = self.rng.choice(self.customers)             # a random existing customer
        income = int(cust["annual_income"])
        if scenario == "INCOME_MISMATCH":                  # ask for 12-30x their income
            return [(now, self._event(now, cust, "PERSONAL" if self.rng.random() < .5 else "BUSINESS",
                                      amount=round(income * self.rng.uniform(12, 30), -2)))]
        if scenario == "OFFSHORE_DEVICE":                  # a phone located overseas
            lat, lon = self._jitter(*self.rng.choice(OFFSHORE), km=30)
            dev = {"device_id": f"DEV{self.rng.getrandbits(40):010x}", "device_type": "WEB",
                   "ip_address": self.fake.ipv4_public(), "latitude": lat, "longitude": lon}
            return [(now, self._event(now, cust, channel="WEB", device=dev))]
        if scenario == "SYNTHETIC_ID":                     # made-up person, new account, big loan online
            ident = self._new_identity(now, opened_days_ago=self.rng.randint(1, 20))
            return [(now, self._event(now, ident, "PERSONAL", amount=self.rng.randrange(60000, 250000, 500),
                                      channel=self.rng.choice(["WEB", "MOBILE"])))]
        if scenario == "UNDERAGE":                         # applicant aged 15-17
            ident = self._new_identity(now, age=self.rng.randint(15, 17))
            return [(now, self._event(now, ident, "CAR", channel="MOBILE"))]
        if scenario == "DEVICE_FARM":                      # ONE phone, FOUR different victims
            lat, lon = self._jitter(*self._city_centre(cust))
            dev = {"device_id": f"DEV{self.rng.getrandbits(40):010x}", "device_type": "ANDROID",
                   "ip_address": self.fake.ipv4_public(), "latitude": lat, "longitude": lon}
            victims = self.rng.sample(self.customers, 4)   # 4 different customers
            out, t = [], now                               # out = list to fill, t = due time
            for v in victims:                              # one application per victim...
                out.append((t, self._event(t, v, "PERSONAL", channel="MOBILE", device=dev)))
                t += timedelta(seconds=self.rng.randint(15, 40))   # ...15-40 seconds apart
            return out
        if scenario == "APPLICATION_BURST":                # ONE customer, 4-5 applications quickly
            out, t = [], now
            for _ in range(self.rng.randint(4, 5)):
                out.append((t, self._event(t, cust, "PERSONAL", channel="WEB")))
                t += timedelta(seconds=self.rng.randint(10, 30))
            return out
        # STEALTH: plausible amount, home device, nothing unusual
        return [(now, self._event(now, cust, "PERSONAL", amount=round(income * 0.9, -2)))]

    # ---------- public API ----------
    def tick(self, now=None):
        """Call once per send slot. Returns a list of (event, scenario_or_None) due now."""
        now = now or datetime.now(timezone.utc)            # use the real time unless one is given
        # random() gives a number from 0.0 to 1.0, so this test is true 6% of the time
        if self.rng.random() < self.fraud_rate:
            scenario = self.rng.choices(list(SCENARIOS), list(SCENARIOS.values()))[0]
            for due, ev in self._plant(now, scenario):     # add each planted event to the waiting list
                self.pending.append((due, ev, scenario))
        else:                                              # otherwise one normal application
            self.pending.append((now, self._event(now, self.rng.choice(self.customers)), None))
        due_now = [p for p in self.pending if p[0] <= now]     # everything whose time has come
        self.pending = [p for p in self.pending if p[0] > now] # keep the rest waiting
        out = []
        for _, ev, scenario in due_now:
            ev["event_ts"] = iso(now)                      # stamp with the real send time
            out.append((ev, scenario))
            if scenario:                                   # planted? write it to the answer key
                with open(self.truth_path, "a", newline="") as f:   # "a" = append to the end
                    csv.writer(f).writerow([ev["application"]["application_id"], scenario, ev["event_ts"]])
        return out