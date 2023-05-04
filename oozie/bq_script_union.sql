CREATE OR REPLACE PROCEDURE ${target_dataset_name}.target_table_name_2(select_column string, final_master_data string, master_union_data string, master_history_data string, partition_master_data_rank string,valid_latest_data string, key_column string, master_dataset_name string, trans_dt_i string,no_of_days_i INT64)

BEGIN


CREATE or REPLACE VIEW ${master_dataset_name}.${master_history_data}(select_column) AS (
select select_column
FROM ${master_dataset_name}.${valid_latest_data}
WHERE trans_dt BETWEEN trans_dt_i AND to_date(trans_dt_i,'yy-mon-mm')+no_of_days_i
                                                                                      )

CREATE or REPLACE VIEW ${master_dataset_name}.${master_union_data}(select_column) as (
select * from ${master_dataset_name}.${valid_latest_data}
union
select * from ${master_dataset_name}.${master_history_data}
            )

WITH ${partition_master_data_rank} as (
select * ,dense_rank() over
partition by key_column
order by trans_dt desc as rank
from ${master_dataset_name}.${master_union_data}
                       )

CREATE or REPLACE VIEW ${master_dataset_name}.${final_master_data}(select_column) as (
select select_column from ${partition_master_data_rank}
)

end