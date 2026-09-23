"""Pre-flight for dbt-duckdb: Delta with deletion vectors, and S3 via credential chain."""

import duckdb

from s2warehouse.silver import SILVER
from s2warehouse.storage import BUCKET


def main() -> None:
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta; INSTALL httpfs; LOAD httpfs;")
    con.execute("CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION 'us-west-2')")
    q = f"SELECT count(*), count(DISTINCT scene_id) FROM delta_scan('{SILVER}')"
    print("silver delta:", con.execute(q).fetchone())
    for key in ("grid/tile=17TPJ/cells.parquet", "bronze/manifest/manifest.parquet"):
        q = f"SELECT count(*) FROM read_parquet('s3://{BUCKET}/{key}')"
        print(f"{key}:", con.execute(q).fetchone())


if __name__ == "__main__":
    main()