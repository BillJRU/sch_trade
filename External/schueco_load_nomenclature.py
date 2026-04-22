#!/usr/bin/env python3
"""
Schüco PL51 Nomenclature Loader via OData
Loads articles from 2026-02-Ukraine-PL51.xlsx into 1C BAS UT via OData REST API.
"""

import openpyxl
import requests
import json
import sys
import time
import base64

# === CONFIGURATION ===
BASE_URL = "http://10.1.5.109/ut_demo/odata/standard.odata"
# Cyrillic username needs UTF-8 encoding for Basic auth (latin-1 fails)
_auth_str = base64.b64encode("Адміненко:".encode("utf-8")).decode("ascii")
AUTH = None  # We'll use custom header instead
HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Basic {_auth_str}",
}

EXCEL_PATH = None  # Pass via --excel argument

# Row range (1-based, row 1 = header)
FIRST_ROW = 2
LAST_ROW = 51  # 0 = all rows; set to small number for testing
UPDATE_MODE = False  # --update flag: if True, update existing items; if False, skip them

# === HELPERS ===

def odata_get(entity, params=""):
    """GET from OData endpoint."""
    url = f"{BASE_URL}/{entity}"
    if params:
        url += f"?{params}"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def odata_post(entity, payload):
    """POST to OData endpoint. Returns created object or raises."""
    url = f"{BASE_URL}/{entity}"
    resp = requests.post(url, headers=HEADERS, json=payload, timeout=30)
    if resp.status_code >= 400:
        try:
            err = resp.json()
            msg = err.get("odata.error", {}).get("message", {}).get("value", resp.text)
        except Exception:
            msg = resp.text
        raise RuntimeError(f"OData POST {entity} failed ({resp.status_code}): {msg}")
    return resp.json()


def odata_patch(entity, ref_key, payload):
    """PATCH existing object by Ref_Key. Returns updated object or raises."""
    url = f"{BASE_URL}/{entity}(guid'{ref_key}')"
    resp = requests.patch(url, headers=HEADERS, json=payload, timeout=30)
    if resp.status_code >= 400:
        try:
            err = resp.json()
            msg = err.get("odata.error", {}).get("message", {}).get("value", resp.text)
        except Exception:
            msg = resp.text
        raise RuntimeError(f"OData PATCH {entity} failed ({resp.status_code}): {msg}")
    return resp.json()


def safe_float(val):
    """Convert cell value to float, return 0 on failure."""
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def safe_str(val):
    """Convert cell value to stripped string."""
    if val is None:
        return ""
    return str(val).strip()


# === PHASE 1: FETCH REFERENCE DATA ===

def fetch_reference_data():
    """Pre-fetch all reference GUIDs needed for Номенклатура creation."""
    refs = {}

    # ВидыНоменклатуры (elements only, not folders)
    print("Fetching ВидыНоменклатуры...")
    data = odata_get("Catalog_ВидыНоменклатуры",
                     "$select=Ref_Key,Description,IsFolder,ТипНоменклатуры")
    refs["виды"] = {
        item["Description"]: item["Ref_Key"]
        for item in data["value"]
        if not item["IsFolder"]
    }
    print(f"  Found {len(refs['виды'])} element types")
    for name, key in sorted(refs["виды"].items()):
        print(f"    {name}: {key[:16]}...")

    # УпаковкиЕдиницыИзмерения
    print("Fetching УпаковкиЕдиницыИзмерения...")
    data = odata_get("Catalog_УпаковкиЕдиницыИзмерения",
                     "$select=Ref_Key,Description")
    uom_map = {}
    for item in data["value"]:
        name = item["Description"].strip().lower()
        uom_map[name] = item["Ref_Key"]
    # Map PL51 codes to 1C UOM keys
    refs["uom"] = {}
    uom_aliases = {
        "M": ["м", "м.", "пог.м"],
        "ST": ["шт", "шт."],
        "PAK": ["паков", "упак", "упак.", "пак"],
        "PAA": ["пар", "пар."],
        "KG": ["кг", "кг."],
        "M2": ["м2", "м²", "кв.м"],
    }
    for pl51_code, aliases in uom_aliases.items():
        for alias in aliases:
            if alias in uom_map:
                refs["uom"][pl51_code] = uom_map[alias]
                break
    print(f"  UOM mapping: {', '.join(f'{k}→{v[:8]}' for k, v in refs['uom'].items())}")

    # ЦеновыеГруппы
    print("Fetching ЦеновыеГруппы...")
    data = odata_get("Catalog_ЦеновыеГруппы", "$select=Ref_Key,Description")
    refs["price_groups"] = {
        item["Description"].strip(): item["Ref_Key"]
        for item in data["value"]
    }
    print(f"  Found {len(refs['price_groups'])} existing price groups")

    # Производители
    print("Fetching Производители...")
    data = odata_get("Catalog_Производители", "$select=Ref_Key,Description")
    refs["producers"] = {
        item["Description"].strip(): item["Ref_Key"]
        for item in data["value"]
    }
    print(f"  Found {len(refs['producers'])} producers")

    # ДополнительныеРеквизитыИСведения
    print("Fetching ДополнительныеРеквизитыИСведения...")
    data = odata_get("ChartOfCharacteristicTypes_ДополнительныеРеквизитыИСведения",
                     "$select=Ref_Key,Description,Имя")
    refs["properties_by_name"] = {
        item["Имя"].strip(): item["Ref_Key"]
        for item in data["value"]
        if item.get("Имя")
    }
    print(f"  Found {len(refs['properties_by_name'])} additional properties (by Имя)")

    # НаборыУпаковок — predefined "ИндивидуальныйДляНоменклатуры"
    print("Fetching НаборыУпаковок (individual)...")
    try:
        data = odata_get("Catalog_НаборыУпаковок",
                         "$filter=PredefinedDataName eq 'ИндивидуальныйДляНоменклатуры'&$select=Ref_Key&$top=1")
        if data.get("value"):
            refs["individual_pack_set_key"] = data["value"][0]["Ref_Key"]
            print(f"  Found: {refs['individual_pack_set_key'][:16]}...")
        else:
            print("  Not found — packaging will use default set")
    except Exception:
        print("  Warning: could not fetch НаборыУпаковок")

    return refs


# === PHASE 2: CREATE MISSING REFERENCE DATA ===

def ensure_price_group(name, refs):
    """Find or create a ЦеноваяГруппа by name."""
    if name in refs["price_groups"]:
        return refs["price_groups"][name]
    result = odata_post("Catalog_ЦеновыеГруппы", {"Description": name})
    key = result["Ref_Key"]
    refs["price_groups"][name] = key
    print(f"  Created ЦеноваяГруппа: {name} → {key[:16]}")
    return key


def ensure_producer(name, refs):
    """Find or create a Производитель by name."""
    if name in refs["producers"]:
        return refs["producers"][name]
    result = odata_post("Catalog_Производители", {
        "Description": name,
        "IsFolder": False
    })
    key = result["Ref_Key"]
    refs["producers"][name] = key
    print(f"  Created Производитель: {name} → {key[:16]}")
    return key


# === PHASE 3: LOAD NOMENCLATURE ===

def load_nomenclature(excel_path, refs, vid_key, vid_key_no_paint, producer_key, parent_key=None,
                      first_row=2, last_row=0, update_mode=False,
                      create_characteristic=False, painting_prop_key=None):
    """Read PL51 Excel and create/update Номенклатура items via OData."""

    print(f"\nReading Excel: {excel_path}")
    print(f"  Mode: {'UPDATE existing + create new' if update_mode else 'CREATE new only (skip existing)'}")
    wb = openpyxl.load_workbook(excel_path, read_only=True, data_only=True)
    ws = wb.active

    rows = list(ws.iter_rows(min_row=first_row,
                             max_row=last_row if last_row > 0 else None,
                             values_only=True))
    print(f"  Rows to process: {len(rows)} (from row {first_row} to {last_row or 'end'})")

    stats = {"processed": 0, "created": 0, "updated": 0, "skipped": 0, "errors": 0}
    uom_map = refs["uom"]

    # Create shared "Без фарбування" characteristic once for ВидНоменклатуры (not per item)
    if create_characteristic and painting_prop_key:
        try:
            existing = odata_get("Catalog_ХарактеристикиНоменклатуры",
                f"$filter=Owner_Key eq guid'{vid_key}' and Description eq 'Без фарбування'&$top=1")
            if not existing.get("value"):
                odata_post("Catalog_ХарактеристикиНоменклатуры", {
                    "Description": "Без фарбування",
                    "Owner_Key": vid_key,
                    "ДополнительныеРеквизиты": [{
                        "Свойство_Key": painting_prop_key,
                        "Значение": False,
                    }]
                })
                print("  Created shared characteristic 'Без фарбування' for ВидНоменклатури")
            else:
                print("  Shared characteristic 'Без фарбування' already exists")
        except Exception as e:
            print(f"  Warning: Could not create shared characteristic: {e}")

    for i, row in enumerate(rows):
        # Pad row to 23 columns (A-W)
        row = list(row) + [None] * (23 - len(row)) if len(row) < 23 else list(row)

        material_no = safe_str(row[0])   # A
        name_de = safe_str(row[1])       # B
        name_en = safe_str(row[2])       # C
        uom_code = safe_str(row[3])      # D
        ms = safe_str(row[4])            # E - Marktsegment
        vs = safe_str(row[5])            # F - Vertriebsschiene
        ws = safe_str(row[6])            # G - Warengruppe
        weight = safe_float(row[7])      # H
        qty_pcs = safe_float(row[10])    # K - Quantity in pcs (for PAK items)
        qty_pair = safe_float(row[11])   # L - Quantity in pair (for PAA items)
        length_m = safe_float(row[9])    # J
        polish_area = safe_float(row[12])  # M
        circumfer = safe_float(row[13])  # N
        prog_ind = safe_str(row[14])     # O - Program indicator
        discount_grp = safe_str(row[15]) # P
        tariff_code = safe_str(row[20])  # U
        sales_unit = safe_str(row[21])   # V - sales unit (ST or empty)
        name_uk = safe_str(row[22])      # W - Ukrainian name

        # UOM is always column D (base unit)
        # sales_unit (V) is only used to detect штанга case (D=M, V=ST)

        if not material_no:
            continue

        stats["processed"] += 1

        # Check if exists by Артикул
        existing_key = None
        try:
            existing = odata_get("Catalog_Номенклатура",
                                 f"$filter=Артикул eq '{material_no}'&$select=Ref_Key&$top=1")
            if existing.get("value"):
                existing_key = existing["value"][0]["Ref_Key"]
                if not update_mode:
                    stats["skipped"] += 1
                    if stats["processed"] % 100 == 0:
                        print(f"  [{stats['processed']}] Skip existing: {material_no}")
                    continue
        except Exception as e:
            print(f"  [{stats['processed']}] Lookup error for {material_no}: {e}")

        # Build payload
        payload = {
            "Description": name_de[:100],
            "НаименованиеПолное": name_uk if name_uk else name_de,
            "Артикул": material_no,
            "ВидНоменклатуры_Key": vid_key_no_paint if (material_no[:1] == "2" and vid_key_no_paint) else vid_key,
            "IsFolder": False,
        }

        if producer_key:
            payload["Производитель_Key"] = producer_key

        if parent_key:
            payload["Parent_Key"] = parent_key

        # UOM — always use base unit (column D)
        uom_key = uom_map.get(uom_code.upper())
        if uom_key:
            payload["ЕдиницаИзмерения_Key"] = uom_key

        # Packaging — for PAK, PAA (count from K), or штанга (D=M, V=ST, J>0)
        needs_packaging = (uom_code.upper() in ("PAK", "PAA") and qty_pcs > 0) or \
                          (uom_code.upper() == "M" and sales_unit.upper() == "ST" and length_m > 0)
        if needs_packaging:
            payload["ИспользоватьУпаковки"] = True
            ind_set_key = refs.get("individual_pack_set_key")
            if ind_set_key:
                payload["НаборУпаковок_Key"] = ind_set_key

        # Measurement fields — will be set via separate PATCH after create,
        # because ПередЗаписью on POST calls ЗаполнитьРеквизитыПоВидуНоменклатуры
        # which overwrites measurement values
        measure_payload = {}

        # Weight (col H)
        if weight > 0:
            measure_payload["ВесИспользовать"] = True
            measure_payload["ВесЧислитель"] = weight
            measure_payload["ВесЗнаменатель"] = 1
            kg_key = uom_map.get("KG")
            if kg_key:
                measure_payload["ВесЕдиницаИзмерения_Key"] = kg_key

        # Length (col J) — only for profiles (base UOM=M)
        # Числитель = length value, Знаменатель = piece count
        if length_m > 0:
            measure_payload["ДлинаИспользовать"] = True
            measure_payload["ДлинаЧислитель"] = length_m
            measure_payload["ДлинаЗнаменатель"] = 1
            measure_payload["ДлинаМожноУказыватьВДокументах"] = True
            m_key = uom_map.get("M")
            if m_key:
                measure_payload["ДлинаЕдиницаИзмерения_Key"] = m_key

        # Circumfer area (col N) → native Площадь
        # Value in ULF/m (1000 ULF = 1 m²): 341 ULF/m = 0.341 m²/m
        if circumfer > 0:
            measure_payload["ПлощадьИспользовать"] = True
            measure_payload["ПлощадьЧислитель"] = circumfer / 1000
            measure_payload["ПлощадьЗнаменатель"] = 1
            m2_key = uom_map.get("M2")
            if m2_key:
                measure_payload["ПлощадьЕдиницаИзмерения_Key"] = m2_key

        # Discount group → ЦеноваяГруппа (col P)
        if discount_grp:
            try:
                pg_key = ensure_price_group(discount_grp, refs)
                payload["ЦеноваяГруппа_Key"] = pg_key
            except Exception as e:
                print(f"  Warning: Could not create price group '{discount_grp}': {e}")

        # ДополнительныеРеквизиты (inline tabular section)
        prop_keys = refs.get("prop_keys", {})
        dop_rows = []

        def add_prop(prop_id, value):
            if not value:
                return
            prop_key = prop_keys.get(prop_id)
            if not prop_key:
                return
            dop_rows.append({
                "Свойство_Key": prop_key,
                "Значение": value,
            })

        add_prop("name_en", name_en)
        add_prop("ms", ms)
        add_prop("vs", vs)
        add_prop("ws", ws)
        add_prop("prog", prog_ind)
        if polish_area > 0:
            add_prop("polish", polish_area / 1000)

        if dop_rows:
            payload["ДополнительныеРеквизиты"] = dop_rows

        # POST (new) or PATCH (update existing)
        try:
            if existing_key:
                # Don't change ВидНоменклатуры on update — triggers complex validation
                # Measurements safe to include — ВидНоменклатуры unchanged, no re-fill
                patch_payload = {k: v for k, v in payload.items()
                                 if k not in ("ВидНоменклатуры_Key", "IsFolder")}
                patch_payload.update(measure_payload)
                result = odata_patch("Catalog_Номенклатура", existing_key, patch_payload)
                ref_key = existing_key
                stats["updated"] += 1
                if stats["updated"] % 10 == 0 or stats["updated"] <= 5:
                    print(f"  [{stats['processed']}] Updated: {material_no} '{name_de[:40]}'")
            else:
                result = odata_post("Catalog_Номенклатура", payload)
                stats["created"] += 1
                ref_key = result["Ref_Key"]
                if stats["created"] % 10 == 0 or stats["created"] <= 5:
                    print(f"  [{stats['processed']}] Created: {material_no} '{name_de[:40]}' → {result.get('Code', '')}")

                # Step 2: PATCH measurement fields (after ПередЗаписью filled defaults)
                if measure_payload:
                    odata_patch("Catalog_Номенклатура", ref_key, measure_payload)

            # Post-create OR post-update: НоменклатураГТД (find or create+update)
            if tariff_code:
                try:
                    # Convert "76042990" → "7604 29 90" for classifier lookup
                    code_spaced = tariff_code[:4]
                    if len(tariff_code) > 4:
                        code_spaced += " " + tariff_code[4:6]
                    if len(tariff_code) > 6:
                        code_spaced += " " + tariff_code[6:8]
                    # Find in КлассификаторУКТВЭД
                    ukt_data = odata_get("Catalog_КлассификаторУКТВЭД",
                        f"$filter=startswith(Code,'{code_spaced}')&$select=Ref_Key&$top=1")
                    if ukt_data.get("value"):
                        ukt_key = ukt_data["value"][0]["Ref_Key"]
                        # Find single existing GTD for this owner (always one record per item)
                        existing_gtd = odata_get("Catalog_НоменклатураГТД",
                            f"$filter=Owner_Key eq guid'{ref_key}'&$select=Ref_Key&$top=1")
                        if existing_gtd.get("value"):
                            gtd_key = existing_gtd["value"][0]["Ref_Key"]
                            odata_patch("Catalog_НоменклатураГТД", gtd_key, {
                                "КодУКТВЭД_Key": ukt_key,
                            })
                        else:
                            gtd = odata_post("Catalog_НоменклатураГТД", {
                                "Owner_Key": ref_key,
                                "КодУКТВЭД_Key": ukt_key,
                            })
                            gtd_key = gtd.get("Ref_Key")
                        # Link back to nomenclature
                        if gtd_key:
                            odata_patch("Catalog_Номенклатура", ref_key, {
                                "НоменклатураГТД_Key": gtd_key,
                            })
                    else:
                        print(f"  Warning: УКТЗЕД '{tariff_code}' ({code_spaced}) not found for {material_no}")
                except Exception as e:
                    print(f"  Warning: GTD for {material_no}: {e}")

            # Post-create OR post-update: Упаковка for PAK/PAA (розупаковка) or штанга (кінцева)
            create_pack = False
            pack_uom_key = None
            pack_num = 0
            pack_den = 0
            pack_type = None
            pack_name = None
            if qty_pcs > 0:
                if uom_code.upper() == "PAK":
                    create_pack = True
                    pack_uom_key = uom_map.get("ST")
                    pack_num = 1
                    pack_den = qty_pcs
                    pack_type = "Разупаковка"
                    pack_name = f"шт (1/{int(qty_pcs)} паков)"
                elif uom_code.upper() == "PAA":
                    create_pack = True
                    pack_uom_key = uom_map.get("ST")
                    pack_num = 1
                    pack_den = qty_pcs
                    pack_type = "Разупаковка"
                    pack_name = f"шт (1/{int(qty_pcs)} пар)"
            # Штанга: D=M, V=ST, J>0 → кінцева: 1 st = J м
            if uom_code.upper() == "M" and sales_unit.upper() == "ST" and length_m > 0:
                shtanga_key = refs.get("shtanga_key")
                if shtanga_key:
                    create_pack = True
                    pack_uom_key = shtanga_key
                    pack_num = length_m
                    pack_den = 1
                    pack_type = "Конечная"
                    pack_name = f"st (1/{int(length_m)} м)"
            if create_pack and pack_uom_key:
                try:
                    # Delete ALL existing packagings for this nomenclature owner
                    existing_packs = odata_get("Catalog_УпаковкиЕдиницыИзмерения",
                        f"$filter=Owner eq cast(guid'{ref_key}', 'Catalog_Номенклатура')&$select=Ref_Key")
                    for old in existing_packs.get("value", []):
                        url = f"{BASE_URL}/Catalog_УпаковкиЕдиницыИзмерения(guid'{old['Ref_Key']}')"
                        requests.delete(url, headers=HEADERS, timeout=30)
                    # Create fresh
                    odata_post("Catalog_УпаковкиЕдиницыИзмерения", {
                        "Description": pack_name,
                        "Owner_Key": ref_key,
                        "ЕдиницаИзмерения_Key": pack_uom_key,
                        "Числитель": pack_num,
                        "Знаменатель": pack_den,
                        "ТипИзмеряемойВеличины": "Упаковка",
                        "ТипУпаковки": pack_type,
                        "Безразмерная": True,
                    })
                except Exception as e:
                    print(f"  Warning: Packaging for {material_no}: {e}")

        except Exception as e:
            stats["errors"] += 1
            print(f"  [{stats['processed']}] ERROR {material_no}: {e}")

        # Rate limiting
        time.sleep(0.05)

    wb.close()
    return stats


# === DELETE ===

def delete_nomenclature(producer_key, dry_run=False):
    """Delete all Номенклатура items by Производитель via OData."""
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Deleting items with Производитель_Key={producer_key[:16]}...")

    stats = {"found": 0, "deleted": 0, "errors": 0}

    # Fetch all items for this producer
    skip = 0
    while True:
        data = odata_get("Catalog_Номенклатура",
                         f"$filter=Производитель_Key eq guid'{producer_key}' and IsFolder eq false"
                         f"&$select=Ref_Key,Description,Артикул&$top=100&$skip={skip}")
        items = data.get("value", [])
        if not items:
            break

        for item in items:
            ref_key = item["Ref_Key"]
            art = item.get("Артикул", "")
            stats["found"] += 1

            if dry_run:
                if stats["found"] <= 10:
                    print(f"  Would delete: {art} '{item['Description'][:50]}'")
                elif stats["found"] == 11:
                    print(f"  ...")
                continue

            # Delete subordinate НоменклатураГТД first
            try:
                gtd_data = odata_get("Catalog_НоменклатураГТД",
                                     f"$filter=Owner_Key eq guid'{ref_key}'&$select=Ref_Key")
                for gtd in gtd_data.get("value", []):
                    url = f"{BASE_URL}/Catalog_НоменклатураГТД(guid'{gtd['Ref_Key']}')"
                    requests.delete(url, headers=HEADERS, timeout=30)
            except Exception:
                pass

            # Delete the item
            try:
                url = f"{BASE_URL}/Catalog_Номенклатура(guid'{ref_key}')"
                resp = requests.delete(url, headers=HEADERS, timeout=30)
                if resp.status_code < 400:
                    stats["deleted"] += 1
                    if stats["deleted"] % 10 == 0 or stats["deleted"] <= 5:
                        print(f"  Deleted: {art} '{item['Description'][:40]}'")
                else:
                    stats["errors"] += 1
                    print(f"  ERROR deleting {art}: {resp.status_code}")
            except Exception as e:
                stats["errors"] += 1
                print(f"  ERROR deleting {art}: {e}")

            time.sleep(0.02)

        if dry_run:
            skip += 100
        else:
            # Don't increment skip — items shift after deletion
            pass

        if len(items) < 100:
            break

    if dry_run:
        print(f"\n[DRY RUN] Found {stats['found']} items that would be deleted")
    else:
        print(f"\nDelete done. Deleted: {stats['deleted']}, Errors: {stats['errors']}")
    return stats


# === MAIN ===

def parse_args():
    """Parse command-line arguments."""
    import argparse
    parser = argparse.ArgumentParser(description="Schüco PL51 Nomenclature Loader via OData")
    parser.add_argument("--update", action="store_true",
                        help="Update existing items (default: skip existing)")
    parser.add_argument("--first-row", type=int, default=FIRST_ROW,
                        help=f"First Excel row to process (default: {FIRST_ROW})")
    parser.add_argument("--last-row", type=int, default=LAST_ROW,
                        help=f"Last Excel row (0=all, default: {LAST_ROW})")
    parser.add_argument("--excel", type=str, default=EXCEL_PATH,
                        help="Path to PL51 Excel file (required for loading)")
    parser.add_argument("--vid", type=int, default=None,
                        help="ВидНоменклатуры index for painting items (skips interactive prompt)")
    parser.add_argument("--vid-no-paint", type=int, default=None,
                        help="ВидНоменклатуры index for articles starting with 2 (no painting)")
    parser.add_argument("--uom-shtanga", type=str, default=None,
                        help="Description of 'штанга' UOM in УпаковкиЕдиницыИзмерения (for M+ST profiles)")
    parser.add_argument("--delete", action="store_true",
                        help="Delete items by Производитель instead of loading")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without making changes")
    parser.add_argument("--producer", type=str, default="Schüco International KG",
                        help="Producer name (default: 'Schüco International KG')")
    # Additional property Имя identifiers (for ДополнительныеРеквизиты lookup by developer name)
    parser.add_argument("--prop-name-en", type=str, default="НаименованиеАнглийское",
                        help="Имя for EN name property")
    parser.add_argument("--prop-ms", type=str, default="Marktsegment",
                        help="Имя for Marktsegment property")
    parser.add_argument("--prop-vs", type=str, default="Vertriebsschiene",
                        help="Имя for Vertriebsschiene property")
    parser.add_argument("--prop-ws", type=str, default="Warengruppe",
                        help="Имя for Warengruppe property")
    parser.add_argument("--prop-polish", type=str, default="ПлощадьПолировки",
                        help="Имя for polishing area property")
    parser.add_argument("--prop-prog", type=str, default="ИндикаторПрограммы",
                        help="Имя for program indicator property")
    parser.add_argument("--create-characteristic", action="store_true",
                        help="Create default 'Без фарбування' characteristic for each new item")
    parser.add_argument("--prop-painting", type=str, default="ЕстьПокраска",
                        help="Имя for painting flag property on characteristics")
    return parser.parse_args()


def main():
    args = parse_args()

    mode = "DELETE" if args.delete else ("UPDATE" if args.update else "CREATE only")
    if args.dry_run:
        mode = f"DRY RUN ({mode})"
    print("=" * 60)
    print("Schüco PL51 Nomenclature Loader via OData")
    print(f"  Mode: {mode}")
    print(f"  Producer: {args.producer}")
    if not args.delete:
        print(f"  Rows: {args.first_row}–{args.last_row or 'end'}")
    print("=" * 60)

    # Fetch references
    refs = fetch_reference_data()

    # Resolve ДопРеквізити by Имя → Ref_Key
    props_by_name = refs.get("properties_by_name", {})
    prop_mapping = {
        "name_en": args.prop_name_en,
        "ms":      args.prop_ms,
        "vs":      args.prop_vs,
        "ws":      args.prop_ws,
        "polish":  args.prop_polish,
        "prog":    args.prop_prog,
    }
    refs["prop_keys"] = {}
    for prop_id, ima_value in prop_mapping.items():
        key = props_by_name.get(ima_value)
        if key:
            refs["prop_keys"][prop_id] = key
            print(f"    ✓ {prop_id} → {ima_value}")
        else:
            print(f"    ✗ {prop_id} → {ima_value} (not found — will be skipped)")

    # Resolve painting property for characteristics
    painting_prop_key = None
    if args.create_characteristic:
        painting_prop_key = props_by_name.get(args.prop_painting)
        if painting_prop_key:
            print(f"    ✓ painting → {args.prop_painting}")
        else:
            print(f"    ✗ painting → {args.prop_painting} (not found — characteristics will be skipped)")
            args.create_characteristic = False

    # Ensure producer exists (only create if not dry-run)
    if args.dry_run:
        producer_key = refs["producers"].get(args.producer)
        if not producer_key:
            print(f"  Producer '{args.producer}' not found (dry run — won't create)")
        else:
            print(f"  Producer: {args.producer} ({producer_key[:16]}...)")
    else:
        producer_key = ensure_producer(args.producer, refs)
        print(f"  Producer: {args.producer} ({producer_key[:16]}...)")

    if args.delete:
        if not producer_key:
            print("ERROR: Producer not found, nothing to delete")
            return
        delete_nomenclature(producer_key, dry_run=args.dry_run)
        return

    # Validate --excel is provided for load mode
    if not args.excel:
        print("ERROR: --excel path is required. Usage: --excel /path/to/2026-02-Ukraine-PL51.xlsx")
        sys.exit(1)

    # Select ВидНоменклатуры (must be an element, NOT a folder!)
    vid_items = sorted(refs["виды"].items())

    if args.vid is not None:
        try:
            vid_name, vid_key = vid_items[args.vid]
        except IndexError:
            print(f"Invalid --vid {args.vid}. Max index: {len(vid_items)-1}")
            sys.exit(1)
    else:
        print("\nAvailable ВидНоменклатуры (elements only):")
        for i, (name, key) in enumerate(vid_items):
            print(f"  [{i}] {name}")
        vid_choice = input(f"\nSelect ВідНоменклатуры [0-{len(vid_items)-1}]: ").strip()
        try:
            vid_name, vid_key = vid_items[int(vid_choice)]
        except (ValueError, IndexError):
            print("Invalid choice. Using 'Товари (без особливостей)'")
            vid_key = refs["виды"].get("Товари (без особливостей)")
            if not vid_key:
                vid_key = list(refs["виды"].values())[0]
            vid_name = [k for k, v in refs["виды"].items() if v == vid_key][0]
    print(f"  Selected: {vid_name} ({vid_key[:16]}...)")

    # Select ВидНоменклатуры for articles starting with "2" (no painting)
    vid_key_no_paint = None
    if args.vid_no_paint is not None:
        try:
            vid_name_np, vid_key_no_paint = vid_items[args.vid_no_paint]
            print(f"  No-paint Вид: {vid_name_np} ({vid_key_no_paint[:16]}...)")
        except IndexError:
            print(f"  Warning: invalid --vid-no-paint {args.vid_no_paint}, articles 2xx will use main Вид")

    # Lookup штанга UOM
    if args.uom_shtanga:
        shtanga_data = [v for k, v in refs.get("properties_by_name", {}).items()]  # unused, look in UOMs
        # Search by description in base UOMs
        for name_lower, key in {k: v for k, v in zip(
            [item["Description"].strip().lower() for item in odata_get("Catalog_УпаковкиЕдиницыИзмерения",
                f"$filter=Owner_Key eq guid'{refs.get('individual_pack_set_key', '')}'&$select=Ref_Key,Description&$top=100").get("value", [])],
            [item["Ref_Key"] for item in odata_get("Catalog_УпаковкиЕдиницыИзмерения",
                f"$filter=Owner_Key eq guid'{refs.get('individual_pack_set_key', '')}'&$select=Ref_Key,Description&$top=100").get("value", [])]
        )}.items():
            pass  # Complex approach, simplify
        # Simpler: search by description directly
        try:
            sht_data = odata_get("Catalog_УпаковкиЕдиницыИзмерения",
                f"$filter=Description eq '{args.uom_shtanga}'&$select=Ref_Key&$top=1")
            if sht_data.get("value"):
                refs["shtanga_key"] = sht_data["value"][0]["Ref_Key"]
                print(f"  Штанга UOM: {args.uom_shtanga} ({refs['shtanga_key'][:16]}...)")
            else:
                print(f"  Warning: UOM '{args.uom_shtanga}' not found — штанга Упаковки won't be created")
        except Exception:
            print(f"  Warning: could not lookup штанга UOM")

    # Load
    stats = load_nomenclature(
        args.excel, refs,
        vid_key=vid_key,
        vid_key_no_paint=vid_key_no_paint,
        producer_key=producer_key,
        parent_key=None,
        first_row=args.first_row,
        last_row=args.last_row,
        update_mode=args.update,
        create_characteristic=args.create_characteristic,
        painting_prop_key=painting_prop_key,
    )

    print(f"\n{'=' * 60}")
    print(f"DONE. Processed: {stats['processed']}, "
          f"Created: {stats['created']}, "
          f"Updated: {stats['updated']}, "
          f"Skipped: {stats['skipped']}, "
          f"Errors: {stats['errors']}")


if __name__ == "__main__":
    main()
