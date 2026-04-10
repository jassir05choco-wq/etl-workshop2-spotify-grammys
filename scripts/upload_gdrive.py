import shutil
import os

def store_csv():
    src  = "data/merged_dataset.csv"
    dest = "data/merged_dataset_gdrive_ready.csv"
    shutil.copy(src, dest)
    print(f"✅ File ready: {dest}")
    print(f"   Size: {os.path.getsize(dest) / 1024:.1f} KB")

if __name__ == "__main__":
    store_csv()