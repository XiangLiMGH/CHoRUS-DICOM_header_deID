import pydicom
import pandas as pd
import argparse
import os

def compare_dicom_tags(file1_path, file2_path, output_csv='dicom_tag_differences.csv'):
    # Load DICOM files
    ds1 = pydicom.dcmread(file1_path, force=True)
    ds2 = pydicom.dcmread(file2_path, force=True)

    # Extract all tags
    tags1 = {elem.tag: elem for elem in ds1.iterall()}
    tags2 = {elem.tag: elem for elem in ds2.iterall()}
    all_tags = set(tags1).union(tags2)

    differences = []

    for tag in sorted(all_tags):
        tag_hex = f"({tag.group:04X},{tag.element:04X})"
        tag_name = pydicom.datadict.keyword_for_tag(tag) or tag_hex

        val1_elem = tags1.get(tag)
        val2_elem = tags2.get(tag)

        val1_value = str(val1_elem.value) if val1_elem else "MISSING TAG"
        val2_value = str(val2_elem.value) if val2_elem else "MISSING TAG"

        if val1_value != val2_value:
            differences.append({
                'Tag': tag_hex,
                'Tag Name': tag_name,
                'File1 Value': val1_value,
                'File2 Value': val2_value
            })

    if differences:
        df_diff = pd.DataFrame(differences)
        df_diff = df_diff.astype(str)
        df_diff.to_excel(output_csv, index=False)
        print(df_diff)
        print(f"\nDifferences saved to: {os.path.abspath(output_csv)}")
    else:
        print("✅ All tags and values match between the two DICOM files.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare DICOM metadata between two files.")
    parser.add_argument(
        "--file1",
        default="./dicom_original/10000032/50414267/02aa804e-bde0afdd-112c0b34-7bc16630-4e384014.dcm",
        help="Path to the first DICOM file."
    )
    parser.add_argument(
        "--file2",
        default="./output/pydicom/dicom_processed/10000032/50414267/02aa804e-bde0afdd-112c0b34-7bc16630-4e384014.dcm",
        help="Path to the second DICOM file."
    )
    parser.add_argument(
        "--output", "-o",
        default="dicom_tag_differences.xlsx",
        help="Optional output CSV filename (default: dicom_tag_differences.xlsx)"
    )

    args = parser.parse_args()
    compare_dicom_tags(args.file1, args.file2, args.output)
