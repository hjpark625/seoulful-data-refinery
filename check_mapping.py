import pandas as pd
import chardet
from pathlib import Path
import sys

# Add root to sys.path to import enums
sys.path.append(str(Path.cwd()))

from enums.category import CategorySeq, CategoryLabel
from enums.gu import GuSeq, GuLabel
from utils.enum_mapping import get_enum_seq


def detect_encoding(file_path):
    with open(file_path, "rb") as f:
        return chardet.detect(f.read())["encoding"]


file_path = Path("./서울시 문화행사 정보(1.31).csv")
try:
    encoding = detect_encoding(file_path)
    # latin-1 fallback for mojibake repair logic simulation if needed,
    # but for inspection let's try direct load or smart load logic.
    # We'll use a simplified version of the smart load from main.py
    try:
        df = pd.read_csv(file_path, encoding="utf-8")
    except:
        df = pd.read_csv(file_path, encoding="cp949")

    # Mojibake check (simplified)
    valid_labels = set(label.value for label in CategoryLabel)
    if "category_seq" in df.columns:
        match_rate = df["category_seq"].isin(valid_labels).mean()
        if match_rate < 0.1:
            print("Mojibake suspected, repairing...")

            def fix_text(text):
                if not isinstance(text, str):
                    return text
                try:
                    return text.encode("latin-1").decode("cp949")
                except:
                    return text

            for col in df.select_dtypes(include=["object"]):
                df[col] = df[col].apply(fix_text)

    # Check mappings
    print("--- Category Mapping Check ---")
    category_label_to_seq = {
        label.value: seq.value for label, seq in zip(CategoryLabel, CategorySeq)
    }
    unique_cats = df["category_seq"].unique()
    for cat in unique_cats:
        mapped = get_enum_seq(cat, category_label_to_seq, CategorySeq.OTHER.value)
        is_mapped = mapped != CategorySeq.OTHER.value
        # Assuming OTHER is a specific fallback value.
        # Need to know what OTHER value is. usually it's distinguishable.
        # But `get_enum_seq` returns `CategorySeq.OTHER.value` on failure.
        print(
            f"'{cat}': {mapped} ("
            + ("Mapped" if cat in category_label_to_seq else "MISSING")
            + ")"
        )

    print("\n--- Gu Mapping Check ---")
    gu_label_to_seq = {label.value: seq.value for label, seq in zip(GuLabel, GuSeq)}
    unique_gus = df["gu_seq"].unique()
    for gu in unique_gus:
        mapped = get_enum_seq(gu, gu_label_to_seq, GuSeq.OTHER.value)
        print(
            f"'{gu}': {mapped} ("
            + ("Mapped" if gu in gu_label_to_seq else "MISSING")
            + ")"
        )

except Exception as e:
    print(f"Error: {e}")
