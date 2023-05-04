CREATE OR REPLACE PROCEDURE ${target_dataset_name}.{radio_decoder_table_name}( timestamp_i date, src_i string,event_ingress_ts_i  date, input_dir_i  string,
 array_i  string,host_i string, final_master_table_view string,master_dataset_name_view string, source_dataset_name string, source_master_table string,master_tgt_project_id string,
 master_tgt_dataset_name string,master_target_tbl_name string)

BEGIN
with radio_temp as (

    select cast ( timestamp_i AS long) as timestamp,
         cast ( host_i AS string) as host,
         cast ( src_i AS string) as src,
         cast ( event_ingress_ts_i AS long) as _event_ingress_ts,
         cast ( input_dir_i AS string) as _event_origin ,
         cast ( array_i AS array) as _event_tags,
         band_capable, configuration,frequency_capable , model,product_code,site_type,vendor,
    case  when rx4_capable == 'Y' then "true" when rx4_capable == 'N' then "false"  else NULL end as rx4_capable,
    case when tx4_capable == 'Y' then "true" when tx4_capable == 'N' then "false" else NULL end as tx4_capable ,
    case when nr_capable == 'Y' then "true" when nr_capable == 'N'then "false"  else NULL end as nr_capable,
    case when dual_band_capable == 'Y' then "true" when dual_band_capable == 'N' then "false" else NULL end as dual_band_capable
    from (
        select Bands supported as band_capable, MIMO as configuration, DL Freq (MHz) as frequency_capable , Name - temp field as model, productCode as product_code, 4Rx Capable as rx4_capable , 4Tx Capable? as tx4_capable, Site Type as site_type, Vendor as vendor , Dual-Band? as dual_band_capable, NR Capable as nr_capable
        from ${source_dataset_name}.${source_master_table}
        )
        );
-----
-- valid data query
CREATE or REPLACE VIEW ${master_dataset_name_view}.&{final_master_table_view} as(
select  host,src,timestamp,_event_ingress_ts,_event_origin, _event_tags, band_capable, configuration,frequency_capable , model,product_code,rx4_capable ,tx4_capable,site_type,vendor ,dual_band_capable,nr_capable,
 from radio_temp
                                    where  host is not NULL or host != '' or  (host is not NULL and cast(host as string) ='') or host !='null'
                                    and src is not NULL or src !='' or  (src is not NULL and cast(src as string) ='') or src !='null'
                                    and timestamp is NULL or timestamp ='' or (timestamp is NULL and cast(timestamp as string) !='') or timestamp ='null'
                                    and _event_ingress_ts is not NULL or _event_ingress_ts !='' or (_event_ingress_ts is not NULL and cast(_event_ingress_ts as string) ='') or _event_ingress_ts !='null'
                                    and _event_origin is not NULL or _event_origin != '' or (_event_origin is not NULL and cast(_event_origin as string) ='') or _event_origin !='null'
                                    and _event_tags is not NULL or _event_tags != ''
                                    and band_capable is not NULL or band_capable != '' or (band_capable is not NULL and cast(band_capable as string) ='') or band_capable !='null'
                                    and configuration is not NULL or configuration != '' or (configuration is not NULL and cast(configuration as string) ='') or configuration !='null'
                                    and frequency_capable is not NULL or frequency_capable != '' or (frequency_capable is not NULL and cast(frequency_capable as string) ='') or frequency_capable !='null'
                                    and model is not NULL or model != '' or (model is not NULL and cast(model as string) ='') or model !='null'
                                    and product_code is not NULL or product_code != '' or (product_code is not NULL and cast(product_code as string) ='') or product_code !='null'
                                    and rx4_capable is not NULL or rx4_capable != '' or (rx4_capable is not NULL and cast(rx4_capable as string) ='') or rx4_capable !='null'
                                    and tx4_capable is not NULL or tx4_capable != '' or (tx4_capable is not NULL and cast(tx4_capable as string) ='') or tx4_capable !='null'
                                    and site_type is not NULL or site_type != '' or (site_type is not NULL and cast(site_type as string) ='') or site_type !='null'
                                    and vendor is not NULL or vendor != '' or (vendor is not NULL and cast(vendor as string) ='') or vendor !='null'
                                    and dual_band_capable is not NULL or dual_band_capable != '' or (dual_band_capable is not NULL and cast(dual_band_capable as string) ='') or dual_band_capable !='null'
                                    and nr_capable is not NULL or nr_capable != '' or (nr_capable is not NULL and cast(nr_capable as string) ='') or nr_capable !='null'
                                    )
INSERT INTO `${master_tgt_project_id}.${master_tgt_dataset_name}.${master_target_tbl_name}`
(host,src,timestamp,_event_ingress_ts,_event_origin, _event_tags, band_capable, configuration,frequency_capable , model,product_code,rx4_capable ,
tx4_capable,site_type,vendor ,dual_band_capable,nr_capable
)

select  host,src,timestamp,_event_ingress_ts,_event_origin, _event_tags, band_capable, configuration,frequency_capable , model,product_code,
rx4_capable ,tx4_capable,site_type,vendor ,dual_band_capable,nr_capable from ${master_dataset_name_view}.&{final_master_table_view}

end;

