import os
import argparse
import csv
import shutil
import pydicom
from datetime import datetime, timedelta
from pydicom.valuerep import DA, DT
# from pydicom.datadict import keyword_for_tag



# Load lookup tables
def load_lookup_tables(image_map_path, personal_map_path):
    image_map = {}
    with open(image_map_path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            pid = row["PatientID"].strip()
            acc = row["AccessionNumber"].strip()
            trial_acc = row["image_occurence_id"].strip()
            image_map[(pid, acc)] = trial_acc

    personal_map = {}
    with open(personal_map_path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            pid = row["PatientID"].strip()
            personal_map[pid] = {
                "person_id": row["person_id"].strip(),
                "Days_Shifted": int(row["Days_Shifted"])
            }

    return image_map, personal_map


def shift_da(da_value, days):
    return DA((datetime.strptime(str(da_value), "%Y%m%d") + timedelta(days=days)).date())


def shift_dt(dt_value, days): 
    dt_str = str(dt_value)
    shifted_date = (datetime.strptime(dt_str[:8], "%Y%m%d") + timedelta(days=days)).strftime("%Y%m%d")
    return DT(shifted_date + dt_str[8:])


# Main processor function
def process_dicom_file(dcm_path, output_root, unprocessed_root, image_map, personal_map, input_dicom_root_dir):

    ds = pydicom.dcmread(dcm_path) 
    
    original_pid = getattr(ds, "PatientID", "").strip()
    original_acc = getattr(ds, "AccessionNumber", "").strip()

    matched_patient = original_pid in personal_map
    matched_accession = (original_pid, original_acc) in image_map

    if not (matched_patient and matched_accession):
        relative_path = os.path.relpath(dcm_path, start=input_dicom_root_dir)
        target_path = os.path.join(unprocessed_root, relative_path)

        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        shutil.copy2(dcm_path, target_path)
        print(f"Unmatched PatientID or AccessionNumber: {dcm_path}")
        return False  # unprocessed

    # De-identify
    ds.PatientID = personal_map[original_pid]["person_id"]
    ds.AccessionNumber = image_map[(original_pid, original_acc)]
    days_shifted = personal_map[original_pid]["Days_Shifted"]

    date_shift_tags = [
        ("0008", "0012"), ("0008", "0020"), ("0008", "0021"), ("0008", "0022"),
        ("0008", "0023"), ("0008", "002a"), ("0010", "0030"), ("0018", "1012"),
        ("0018", "1078"), ("0018", "1079"), ("0018", "1200"), ("0018", "700c"),
        ("0032", "1000"), ("0032", "1010"), ("0032", "1040"), ("0032", "1050"),
        ("0038", "0020"), ("0038", "0030"), ("3006", "0008")
    ]

    for group, element in date_shift_tags:
        
        tag = (int(group, 16), int(element, 16))
        # print(f"({group},{element}): {keyword_for_tag(tag)}")
        
        if tag in ds:
            value = ds[tag].value
            if value:  # Check that value is not None or empty
                vr = ds[tag].VR
                if vr == 'DA':
                    ds[tag].value = shift_da(value, days_shifted)
                elif vr == 'DT':
                    ds[tag].value = shift_dt(value, days_shifted)
                else:
                    relative_path = os.path.relpath(dcm_path, start=input_dicom_root_dir)
                    target_path = os.path.join(unprocessed_root, relative_path)
                    os.makedirs(os.path.dirname(target_path), exist_ok=True)
                    shutil.copy2(dcm_path, target_path)
                    print(f"Unhandled VR {vr} for tag {tag}: {dcm_path}")
                    return False

    relative_path = os.path.relpath(dcm_path, start=input_dicom_root_dir)
    output_path = os.path.join(output_root, relative_path)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    ds.save_as(output_path)
    return True  # processed

def find_dicom_files(root_dir):
    for root, _, files in os.walk(root_dir):
        for fname in files:
            if fname.lower().endswith(".dcm"):
                yield os.path.join(root, fname)



if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="De-identify DICOM metadata (PatientID, AccessionNumber and selected date tags) using lookup tables.")
    parser.add_argument("--map_table_dir", default="./lookup_table", help="Directory containing lookup tables")
    parser.add_argument("--input_root_dir", default="../test_sample/dicom_original", help="Root directory of input DICOM files")
    parser.add_argument("--output_root_dir", default="../test_sample/output/pydicom", help="Root directory for output files")
    args = parser.parse_args()

    map_table_dir = args.map_table_dir
    input_root_dir = args.input_root_dir
    output_root_dir = args.output_root_dir
    
    processed_dicom_root_dir = os.path.join(output_root_dir, "dicom_processed")
    unprocessed_dicom_root_dir = os.path.join(output_root_dir, "dicom_unprocessed")
 
    os.makedirs(processed_dicom_root_dir, exist_ok=True)
    os.makedirs(unprocessed_dicom_root_dir, exist_ok=True)
    
    image_map_path = f"{map_table_dir}/Image_map.csv"
    personal_map_path = f"{map_table_dir}/Personal_map.csv"

    image_map, personal_map = load_lookup_tables(image_map_path, personal_map_path)
    
    for idx, subfolder in enumerate(os.listdir(input_root_dir), start=1):
        
        subfolder_path = os.path.join(input_root_dir, subfolder)
        print(f"[{idx}] Processing subfolder: {subfolder}")
        
        processed_count = 0
        unprocessed_count = 0
        Total_count = 0

        for dcm_path in find_dicom_files(subfolder_path):
            Total_count += 1
            success = process_dicom_file(
                dcm_path,
                processed_dicom_root_dir,
                unprocessed_dicom_root_dir,
                image_map,
                personal_map,
                input_root_dir
            )
            if success:
                processed_count += 1
            else:
                unprocessed_count += 1

        print(f"Processed: {processed_count} | Unprocessed: {unprocessed_count} | Total: {Total_count}\n")
        
    print("De-identification of PatientID, AccessionNumber, and selected date tags has been completed.\n")
    
# python pydicom_deid.py --map_table_dir ./lookup_table --input_root_dir ../test_sample/dicom_original --output_root_dir ../test_sample/output/pydicom 2>&1 | tee logs.txt

