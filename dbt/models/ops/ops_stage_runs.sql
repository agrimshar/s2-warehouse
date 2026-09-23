select *
from read_json_auto('{{ var("metrics_path") }}', format = 'newline_delimited', sample_size = -1)
