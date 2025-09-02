import os
from datetime import datetime
from scraper import update_reference_data
from excel import export_yield_curve_to_excel  

CSV_DIR = "data/csv"
PLOT_DIR = "data/plots"

os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(PLOT_DIR, exist_ok=True)

def main():
    csv_path = update_reference_data(dedupe=False)
    if not csv_path:
        print("[INFO] No new data today.")
        return

    today = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_target = os.path.join(CSV_DIR, f"rates_{today}.csv")
    os.replace(csv_path, csv_target)
    print(f"[INFO] Saved CSV -> {csv_target}")

    xlsx_target = os.path.join(PLOT_DIR, f"curve_{today}.xlsx")
    out_path = export_yield_curve_to_excel(
        csv_target,
        xlsx_target=os.path.join(PLOT_DIR, f"curve_{today}.xlsx"),
        include_tenors=True,
        y_as_percent=False,
    )
    print(f"[INFO] Saved Excel -> {out_path}")

if __name__ == "__main__":
    main()
