import chardet

file_path = "./서울시 문화행사 정보(6.9 기준).csv"

with open(file_path, "rb") as f:
    result = chardet.detect(f.read())
    print(result)
