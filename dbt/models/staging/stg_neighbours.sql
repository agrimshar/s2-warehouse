select
    cast(cell_idx as integer)      as cell_idx,
    cast(neighbour_idx as integer) as neighbour_idx
from read_parquet('s3://{{ var("s3_bucket") }}/grid/tile=17TPJ/neighbours.parquet')