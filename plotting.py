# plotting.py
import os
from datetime import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def plot_yield_curve(csv_path: str, plot_target: str) -> str:
    # Ensure the directory for the target file exists
    out_dir = os.path.dirname(plot_target) or "."
    os.makedirs(out_dir, exist_ok=True)

    # Read CSV (your scraper saved a clean CSV; if headers exist this still works)
    df = pd.read_csv(csv_path, header=None, names=["maturity", "col1", "rate", "ref_date"])

    # Clean rate
    df["rate"] = (
        df["rate"].astype(str)
        .str.replace(",", ".", regex=False)
        .str.replace("%", "", regex=False)
        .str.replace("\u00A0", "", regex=False)
        .str.strip()
    )
    df = df[df["rate"].str.match(r"^\d+(\.\d+)?$")]
    df["rate"] = df["rate"].astype(float)

    # Dates & time-to-maturity
    df["maturity"] = pd.to_datetime(df["maturity"], dayfirst=True, errors="coerce")
    ref_date = pd.to_datetime(df["ref_date"].iloc[0], dayfirst=True, errors="coerce")
    df["ttm_years"] = (df["maturity"] - ref_date).dt.days / 365
    df = df.dropna(subset=["ttm_years"]).sort_values("ttm_years")

    # Tenors (years) and linear interpolation
    tenors = {
        "13 semaines": 13/52,
        "26 semaines": 26/52,
        "52 semaines": 1,
        "2 ans": 2,
        "5 ans": 5,
        "10 ans": 10,
        "15 ans": 15,
        "20 ans": 20,
        "30 ans": 30,
    }
    tenor_years = np.array(list(tenors.values()), dtype=float)
    interp_rates = np.interp(tenor_years, df["ttm_years"], df["rate"])

    # Plot
    plt.figure(figsize=(9, 5))
    plt.plot(list(tenors.keys()), interp_rates, marker="o", label=ref_date.strftime("%d/%m/%Y"))
    plt.ylabel("% Rendement")
    plt.title("Courbe des taux souverains (Marché secondaire)")
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.legend()
    plt.tight_layout()

    # Save to the exact path the caller provided
    plt.savefig(plot_target, dpi=150)
    plt.close()
    return plot_target
