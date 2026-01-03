import requests
import zipfile
import io
import pandas as pd
from pathlib import Path
import pdfplumber
import sys

# --- CONFIG -----------------
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
RAW_XYZ_DIR = DATA_DIR / "raw_xyz"
DATA_DIR.mkdir(exist_ok=True)
RAW_XYZ_DIR.mkdir(exist_ok=True)

BASE_URL = "https://ehydroprod.blob.core.usgovcloudapi.net/ehydro-surveys/"
DISTRICTS_L = ['CEMVM/', 'CEMVK/', 'CEMVS/','CEMVK/CEMVK_DIS_', 'CEMVS/CEMVS_DIS_', 'CEMVM/CEMVM_DIS_']
DISTRICTS_U = ['CEMVP/', 'CEMVP/CEMVP_DIS_', 'CEMVR', 'CEMVR/CEMVR_DIS_', 'CEMVS/', 'CEMVS/CEMVS_DIS_']

NEW_IDS_LM = DATA_DIR / "new_lm_ids.csv"
NEW_IDS_UM = DATA_DIR / "new_um_ids.csv"
METADATA_FILE = DATA_DIR / "survey_metadata.csv"

# ---------------------------
# FUNCTION FOR XYZ FILE DATUM 
def get_datum_from_xyz(text: str):
    for line in text.splitlines():
        if "datum" in line.lower():
            text_line = line.strip()
            if "NAVD88" in text_line.upper():
                return "NAVD88"
            elif "2014 Low Water Reference Plane" in text_line:
                return "LWRP2014"
            elif "2007 Low Water Reference Plane" in text_line:
                return "LWRP2007"
            elif "Dredging Reference Plane" in text_line:
                return "DredgingRef"
            else:
                return f"Unknown (found: {text_line})"
    return "Unknown"
    
# FUNCTION FOR PDF FILE DATUM 
def get_datum_from_pdf(fobj):
    try:
        with pdfplumber.open(fobj) as pdf:
            page = pdf.pages[0]
            width, height = page.width, page.height
            crop_box = (width * 0.7, height * 0.7, width, height)
            cropped = page.crop(crop_box)
            text = cropped.extract_text() or ""
            text = text.lower()
        if "navd88" in text:
            return "NAVD88"
        elif "dredging reference plane" in text:
            return "DredgingRef"
        elif "2014 low water reference plane" in text:
            return "LWRP2014"
        elif "2007 low water reference plane" in text:
            return "LWRP2007"
        else:
            return "Unknown"
    except Exception as e:
        print(f"PDF reading error: {e}")
        return "Unknown"

# --------GET READY TO READ IN NEW SURVEYS -----------
if not NEW_IDS_LM.exists() and not NEW_IDS_UM.exists():
    print(f"No new IDs file found. Exiting.")
    sys.exit(0)
    
lm_ids_df = pd.read_csv(NEW_IDS_LM)
um_ids_df = pd.read_csv(NEW_IDS_UM)

if lm_ids_df.empty and um_ids_df.empty:
    print("No new surveys found. Exiting.")
    sys.exit(0)

new_ids_lm = lm_ids_df['ID'].tolist()
new_ids_um = um_ids_df['ID'].tolist()

metadata_rows = []

# ------ READ IN LOWER and UPPER MSPI FILES 
for num in range(2):
    if num == 0:
        # lower first 
        new_ids = new_ids_lm 
        DISTRICTS = DISTRICTS_L
    if num == 1:
        # upper next
        new_ids = new_ids_um 
        DISTRICTS = DISTRICTS_U
    for survey_id in new_ids:
        datum_xyz = "Unknown"
        datum_pdf = "Unknown"
        datum = '?'
        zip_content = None
        for dist in DISTRICTS:
            url = ''
            try:
                test_url = f"{BASE_URL}{dist}{survey_id}.ZIP"
                response = requests.get(test_url, timeout=60)
                if response.status_code != 200:
                    continue
                url = test_url
                zip_content = response.content
                print(f"Downloaded: {url}")
                break
            except Exception as e:
                print(f"Error downloading {test_url}: {e}")
                continue
        
        if zip_content is None:
            print(f"Could not find ZIP for {survey_id}")
            continue
                
        # ------- now open zipfiles if the url was read -----
        with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
            # ----- Check XYZ
            xyz_files = [n for n in z.namelist() if n.lower().endswith(".xyz")]
            if xyz_files:
                with z.open(xyz_files[0]) as f:
                    text = f.read().decode(errors="ignore")
                    datum_xyz = get_datum_from_xyz(text)
            # ------ Check PDF
            pdf_files = [n for n in z.namelist() if n.lower().endswith(".pdf")]
            if pdf_files:
                with z.open(pdf_files[0]) as f:
                    datum_pdf = get_datum_from_pdf(f)
        
        # ---------- Decide final datum ------------------
        if datum_xyz == "Unknown" and datum_pdf == "Unknown":
            datum_final = "Unknown"
            datum_source = ""
        elif datum_xyz == "Unknown":
            datum_final = datum_pdf
            datum_source = "PDF"
        elif datum_pdf == "Unknown":
            datum_final = datum_xyz
            datum_source = "XYZ"
        elif datum_xyz != datum_pdf:
            datum_final = f"Mismatch: {datum_xyz}/{datum_pdf}"
            datum_source = "Both"
            print(f"WARNING: Datum mismatch for {survey_id}")
        else:
            datum_final = datum_xyz
            datum_source = "Both"
    
        # ------- Append metadata
        metadata_rows.append({
            "survey_id": survey_id,
            "datum": datum_final,
            "datum_source": datum_source,
            "url": url})

metadata_df = pd.DataFrame(metadata_rows)
metadata_df.to_csv(METADATA_FILE, index=False)
print(f"Metadata saved to {METADATA_FILE}")
