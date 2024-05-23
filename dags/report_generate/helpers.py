import requests
import pandas as pd
import numpy as np
from datetime import datetime


def get_metadata():
    return {
    "Crude Oil Futures": "CL",
    "Copper": "HG",
    "NY Harbor ULSD Futures": "HO",
    "KC HRW Wheat": "KE",
    "Henry Hub Natural Gas Futures": "NG",
    "RBOB Gasoline Futures": "RB",
    "1,000-oz. Silver Futures": "SIL",
    "Corn Futures": "ZC",
    "Soybean Oil Futures": "ZL",
    "Soybean Meal Futures": "ZM",
    "Soybean Futures": "ZS",
    "Chicago SRW Wheat Futures": "ZW",
    "Palladium Futures": "PA",
    "Platinum Futures": "PL",
    "Silver": "SI",
    "Lean Hog Futures": "HE",
    "Feeder Cattle Futures": "GF",
    "Live Cattle Futures": "LE",
    "10-Year T-Note Futures": "ZN",
    "U.S. Treasury Bond Futures": "ZB",
    "30 Day Fed Funds Futures": "ZQ",
    "One-Month SOFR Futures": "SR1",
    "NYCC Cotton No. 2 Futures": "CT",
    "NYCC Coffee C Futures": "KC",
    "ULSD-ARA LS Gas Oil Futures": "G",
    "Brent Crude": "BRN",
    "NYCC Cocoa Futures": "CC",
    "ICEU Robusta Coffee Future": "RC",
    "ICEU London Cocoa Future": "C",
    "ICEU Three Month SONIA": "SO3",
    "ICEU Three Month Euro(Euribor) Future": "I",
    "Sugar No. 11": "SB",
    "E-MINI S&P 500": "ES",
    "EURO STOXX 50 FUT": "FESX",
    "Three-Month SOFR Futures": "SR3",
    "FTSE 100 INDEX": "Z",
    "Gold Futures": "GC",
    "E-mini NASDAQ-100": "NQ",
    "DAX Futures": "FDAX",
    "E-Mini Natural Gas Futures": "QG"
    }


def get_data_from_api():

    metadata = get_metadata()

    # List to store the concatenated values
    contract_list = []

    # Loop through dictionary values and concatenate with "c1"
    for abbreviation in metadata.values():
        contract_list.append(abbreviation + "c1")
    
    contracts = ','.join(contract_list)

    url = "http://192.168.0.172:8080/api/v1/data/ohlcv"
    from_date = "2024-01-01"
    to_date = datetime.now()

    resp = requests.get(
                url,
                {
                    'symbols': contracts,
                    'from': from_date,
                    'to': to_date
                })

    data = resp.json()
    df = pd.DataFrame(data)
    return df