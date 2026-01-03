
# PREPARE URLS 
base_url = "https://ehydroprod.blob.core.usgovcloudapi.net/ehydro-surveys/"
districts_l = ['CEMVM/','CEMVK/','CEMVS/','CEMVK/CEMVK_DIS_','CEMVS/CEMVS_DIS_','CEMVM/CEMVM_DIS_']
districts_u ['CEMVP/','CEMVP/CEMVP_DIS_','CEMVR','CEMVR/CEMVR_DIS_','CEMVS/','CEMVS/CEMVS_DIS_]

# FUNCTION FOR XYZ FILE DATUM 
def get_datum_from_xyz(xyz_path):
    try:
        with open(xyz_path, "r", errors="ignore") as f:
            for line in f:
                if "datum" in line.lower():
                    text = line.strip()
                    if "NAVD88" in text.upper():
                        return "NAVD88"
                    elif "2014 Low Water Reference Plane" in text:
                        return "LWRP2014"
                    elif "2007 Low Water Reference Plane" in text:
                        return "LWRP2007"
                    elif "Dredging Reference Plane" in text:
                        return "DredgingRef"
                    else:
                        return f"Unknown (found: {text})"
        # If no line contains 'datum'
        return "Unknown"
    except Exception as e:
        print(f"Could not read {xyz_path}: {e}")
        return "Unknown"
        
# FUNCTION FOR PDF FILE DATUM 
def get_datum_from_pdf(pdf_path):
    # checking for datum keywords 
    try:
        with pdfplumber.open(pdf_path) as pdf:
            page = pdf.pages[0]  # first page
            width = page.width
            height = page.height
            # (x0, top, x1, bottom)
            crop_box = (width * 0.7, height * 0.7, width, height)
            cropped = page.crop(crop_box)
            text = page.crop(crop_box).extract_text() or ""
            text = text.lower()
        if "navd88" in text:
            return "NAVD88"
        if "dredging reference plane" in text:
            return "DredgingRef"
        elif "2014 low water reference plane" in text:
            return "LWRP2014"
        elif "2007 low water reference plane" in text:
            return "LWRP2007"
        else:
            return "Unknown"
    except Exception as e:
        print(f"Could not read {pdf_path}: {e}")
        return "Unknown"

# READ IN LOWER MSPI FILES 
if new_lm_ids:
  for lm_id in new_lm_ids:
    datum1 = "Unknown"
    datum2 = "Unknown"
    datum = '?'
    for dist in districts_l:
        try:
            url = f"{base_url}{dist}{lm_id}.ZIP"
            response = requests.get(url, timeout=60)
            if response.status_code != 200:
                print(f'tried {url} and didnt work')
                continue  # try next district
            print(f'got it! {url}')
        except Exception as e:
            print(f"Error with {url}: {e}")
            continue
            
    # now open zipfiles if the url was read 
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:

        # first check for an xyz file
        xyz_files = [n for n in z.namelist() if n.lower().endswith(".xyz")]
        if xyz_files:  
          datum2 = get_datum_from_xyz(xyz_files[0])  #i think there should only be one xyz file 
            
        # now check for a pdf file 
        pdf_files = [n for n in z.namelist() if n.lower().endswith(".pdf")]
        if pdf_files:
          datum1 = get_datum_from_pdf(pdf_files[0])
    
    # now decide which datum to use 
    if (datum1 == 'Unknown') and (datum2 == 'Unknown'): 
        datum = 'Unknown'
    elif (datum1 == 'Unknown') or (datum2 == 'Unknown'): 
        if datum1 != 'Unknown': 
            datum = datum1 
        if datum2 != 'Unknown': 
            datum = datum2 
    elif datum1 != datum2: 
        datum = f'mismatched: {datum1} {datum2}'
        print('found mismatch') 
    elif datum1 == datum2: 
        datum = datum1 
    else: 
        print('last else idk what happened')
        datum = 'Unknown'
    if datum == "Unknown":
        if "actualdepth" in base.lower():
            print('found actual depth') 
            datum = "ActualDepth"
        else:
            print('DATUM IS UNKNOWN') 
