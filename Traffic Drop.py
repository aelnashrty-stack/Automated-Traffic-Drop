import azure.functions as func
import pandas as pd
import numpy as np
import io
import json
import base64

# =========================
# Your Original Logic (Kept Intact)
# =========================
def process_sheet(df, join_key, traffic_cols, availability_col, drop_threshold, min_traffic=5):
    df["Period start time"] = pd.to_datetime(df["Period start time"])
    last_hour = df["Period start time"].max()
    
    df_today = df[df["Period start time"] == last_hour].copy()
    df_yesterday = df.copy()
    df_today["yesterday_time"] = df_today["Period start time"] - pd.Timedelta(days=1)

    merged = df_today.merge(
        df_yesterday,
        left_on=[join_key, "yesterday_time"],
        right_on=[join_key, "Period start time"],
        suffixes=("_today", "_yesterday")
    )

    merged = merged[merged[f"{availability_col}_today"] == 100]
    
    traffic_mask = pd.Series(True, index=merged.index)
    for col in traffic_cols:
        traffic_mask &= merged[f"{col}_yesterday"] >= min_traffic
    merged = merged[traffic_mask]

    drop_flag = pd.Series(False, index=merged.index)
    for col in traffic_cols:
        today = merged[f"{col}_today"].astype(float)
        yesterday = merged[f"{col}_yesterday"].astype(float)
        merged[f"{col}_drop_ratio"] = np.where(yesterday > 0, (yesterday - today) / yesterday, np.nan)
        drop_flag |= merged[f"{col}_drop_ratio"] >= drop_threshold

    violations = merged[drop_flag]
    return violations

# =========================
# Azure Function Entry Point
# =========================
app = func.FunctionApp()

@app.route(route="detect_traffic_drops", auth_level=func.AuthLevel.ANONYMOUS)
def detect_traffic_drops(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # 1. Parse Input from Power Automate
        req_body = req.get_json()
        file_content_base64 = req_body.get('fileContent')
        
        # Power Automate sends content as Base64; we must decode it
        decoded_file = base64.b64decode(file_content_base64)
        excel_file = io.BytesIO(decoded_file)

        # 2. Config (Same as your script)
        sheet_config = {
            "2G performance": {"join_key": "BCF name", "traffic_cols": ["TCH traffic sum in time"], "availability_col": "Cell avail accuracy 1s cellL"},
            "3G performance": {"join_key": "WBTS name", "traffic_cols": ["CS traffic - Erl", "All_Data_Traffic_MB"], "availability_col": "Cell Availability, excluding blocked by user state (BLU)"},
            "4G performance": {"join_key": "LNBTS name", "traffic_cols": ["Total LTE data volume, DL + UL"], "availability_col": "Cell Avail excl BLU"}
        }

        # 3. Process Data
        final_results = {}
        summary_text = "Traffic Drop Report:\n"

        for sheet, cfg in sheet_config.items():
            try:
                df = pd.read_excel(excel_file, sheet_name=sheet)
                violations = process_sheet(df, cfg["join_key"], cfg["traffic_cols"], cfg["availability_col"], 0.80)
                
                if not violations.empty:
                    # Convert only the critical columns to a dictionary for the email
                    final_results[sheet] = violations.to_dict(orient='records')
                    summary_text += f"- {sheet}: Found {len(violations)} drops.\n"
                else:
                    summary_text += f"- {sheet}: No drops detected.\n"
            except Exception:
                continue # Skip sheets that don't exist in the export

        # 4. Return JSON to Power Automate
        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "summary": summary_text,
                "data": final_results
            }),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500)