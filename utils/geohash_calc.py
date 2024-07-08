import geohash2


def calculate_geohash(row: dict[str, str]):
    if row["latitude"] != "NULL" and row["longitude"] != "NULL":
        return geohash2.encode(
            latitude=float(row["latitude"]),
            longitude=float(row["longitude"]),
            precision=8,
        )
    else:
        return "NULL"
