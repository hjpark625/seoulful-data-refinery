import chardet

file_path = "./data_name.csv"

with open(file_path, "rb") as f:
    result = chardet.detect(f.read())
    print(result)
