
CREATE OR REPLACE PROCEDURE ${target_dataset_name}.${target_table_name}(timestamp_i date, src_i string,event_ingress_ts_i  date, input_dir_i  string,
 array_i  string, sector_i INT64, carrier_i INT64, device_uid_i string, enodeb_id_i string, centerline_i float, band_enabled_i string,
 trans_dt_i string,select_column string,master_union_data string,master_history_data string, final_master_data string, valid_latest_data string, master_target_tblname string,
  master_tgt_dataset_name string,partition_master_data_rank string,valid_latest_data string,key_column string, master_dataset_name string)
BEGIN


with oc as (
select gnb_enb_id,sector_number,carrier_number,antenna_model,antenna_manufacturer,number_of_transmission_antenna_ports,ret_antenna_model,
antenna_type,band_info,antenna_frequency_mhz,centerline_ft,number_of_reception_antenna_ports,radio_access_technology,
azimuth_deg,gnb_du_id,trans_dt from `vz-it-pr-i37v-ndldo-0.vzn_ndl_vzatoll_core_tbls_v.lte_topology_raw_v4` where gnb_enb_vendor IN ('Nokia', 'Samsung','Ericsson')
 and site_version='0000' and gnb_enb_id!='' and sector_number!='' and carrier_number!='' and (radio_access_technology='LTE' OR
 radio_access_technology='5GNR' and trans_dt= trans_dt_i) ),

data_collect as (
        select distinct azimuth_deg as azimuth, band_info as band_enabled, carrier_number as carrier,carrier_type, centerline_ft as centerline , md5(antenna_uid) as device_uid,
        gnb_enb_id as enodeb_id ,antenna_frequency_mhz as frequency_enabled , antenna_model as model,number_of_reception_antenna_ports as rx_port_count, sector_number as sector,
        number_of_transmission_antenna_ports as tx_port_count, antenna_type as type, antenna_manufacturer as vendor, gnb_du_id as gnb_du_id ,
         cast ( timestamp_i AS long) as timestamp,cast ( host AS string) as host,
         cast ( src_i AS string) as src,
         cast ( event_ingress_ts_i AS long) as _event_ingress_ts,
         cast ( input_dir_i AS string) as _event_origin ,
         cast ( array_i AS array) as _event_tags from data_collect from
            (
                select gnb_enb_id,sector_number,carrier_number,antenna_model,antenna_manufacturer,number_of_transmission_antenna_ports,ret_antenna_model,
                antenna_type,band_info,antenna_frequency_mhz,centerline_ft,number_of_reception_antenna_ports,radio_access_technology,
                azimuth_deg,gnb_du_id,band_info_1,band_info_2, band_earfcn, carrier_type,antenna_uid from
                    (
                        select gnb_enb_id,sector_number,carrier_number,antenna_model,antenna_manufacturer,number_of_transmission_antenna_ports,ret_antenna_model,
                        antenna_type,band_info,antenna_frequency_mhz,centerline_ft,number_of_reception_antenna_ports,radio_access_technology,
                        azimuth_deg,gnb_du_id,band_info_1,band_info_2, band_earfcn, carrier_type , case when number_of_reception_antenna_ports = '2' and carrier_type = 'LB' Then (gnb_enb_id || '_' || sector_number || '_' || band_earfcn || '_' || antenna_model || '_' || azimuth_deg || '_' || centerline_ft)
                        when number_of_reception_antenna_ports = '4' and carrier_type = 'LB' Then ''
                        when number_of_reception_antenna_ports = '2' and carrier_type = 'MB' Then (gnb_enb_id || '_' || sector_number || '_' || band_earfcn || '_' || antenna_model || '_' || azimuth_deg || '_' || centerline_ft)
                        when number_of_reception_antenna_ports = '4' and carrier_type = 'MB' Then (gnb_enb_id || '_' || sector_number || '_' || band_earfcn || '_' || antenna_model || '_' || azimuth_deg)
                        when carrier_type = 'LAA' Then (gnb_enb_id || '_' || sector_number || '_' || carrier_type || '_' || antenna_model || '_' || azimuth_deg)
                        when carrier_type = 'CBRS' Then (gnb_enb_id || '_' || sector_number || '_' || carrier_type || '_' || antenna_model || '_' || azimuth_deg)
                        when carrier_type = 'HB' Then (gnb_enb_id || '_' || sector_number || '_' || carrier_type || '_' || antenna_model || '_' || azimuth_deg)
                        when number_of_reception_antenna_ports = '1' or number_of_reception_antenna_ports = '16' or number_of_reception_antenna_ports= '8' or number_of_reception_antenna_ports = '6' Then (gnb_enb_id || '_' || sector_number || '_' || band_earfcn || '_' || antenna_model || '_' || azimuth_deg)
                        end as antenna_uid from

                           (

                            select  gnb_enb_id,sector_number,carrier_number,antenna_model,antenna_manufacturer,number_of_transmission_antenna_ports,ret_antenna_model,
                            antenna_type,band_info,antenna_frequency_mhz,centerline_ft,number_of_reception_antenna_ports,radio_access_technology,
                            azimuth_deg,gnb_du_id,band_info_1,band_info_2, band_earfcn, case when band_info_1 = 'B1' Then 'MB'
                                                    when band_info_1 = 'B13' Then 'LB'
                                                    when band_info_1 = 'B2' Then 'MB'
                                                    when band_info_1 = 'B2' Then 'MB'
                                                    when band_info_1 = 'B252' Then 'LAA'
                                                    when band_info_1 = 'B4' Then 'MB'
                                                    when band_info_1 = 'B46' Then 'LAA'
                                                    when band_info_1 = 'B48' Then 'CBRS'
                                                    when band_info_1 = 'B5' Then 'LB'
                                                    when band_info_1 = 'B66' Then 'MB'
                                                    when band_info_1 = 'Bn2' Then 'MB'
                                                    when band_info_1 = 'Bn260' Then 'HB'
                                                    when band_info_1 = 'Bn261' Then 'HB'
                                                    when band_info_1 = 'Bn5' Then 'LB'
                                                    when band_info_1 = 'Bn66' Then 'MB'
                                                    when band_info_1 = 'BnLS6' Then 'MB' else '' end as carrier_type from
                                  (
                                    SELECT gnb_enb_id,sector_number,carrier_number,antenna_model,antenna_manufacturer,number_of_transmission_antenna_ports,ret_antenna_model,
                                    antenna_type,band_info,antenna_frequency_mhz,centerline_ft,number_of_reception_antenna_ports,radio_access_technology,
                                    azimuth_deg,gnb_du_id,
                                     SPLIT(band_info, '_')[offset(0)] as band_info_1,
                                     SPLIT(band_info, '_')[offset(1)] as band_info_2,
                                     SPLIT(band_info, '_')[offset(2)] as band_earfcn,
                                     from oc
                                  )

                           )
                    ) tb1 where tb1.antenna_uid != ''
            )
            ),
quarantine_data as (
select host,src,timestamp,_event_ingress_ts,_event_origin,_event_tags,azimuth,band_enabled,carrier,carrier_type,centerline,device_uid,enodeb_id,
frequency_enabled,model,rx_port_count,sector,tx_port_count,type,vendor,trans_dt,
        case when  host is  NULL or cast(host as string) = '' then struct('host' as col_name,host as col_value,'column can not be null or blank' as quarantine_reason)
        when host is NULL or cast(host as string) = '' then struct('host' as col_name,host as col_value,'could not cast to string' as quarantine_reason ) else NULL end as quarantine_information,
        case when  src is  NULL or cast(src as string) = '' then struct('src' as col_name,src as col_value,'column can not be null or blank' as quarantine_reason)
        when src is NULL or cast(src as string) = '' then struct('src' as col_name,src as col_value,'could not cast to string' as quarantine_reason ) else NULL end as quarantine_information,
        case when timestamp is NULL or cast(timestamp as string) = '' then struct('timestamp' as col_name,timestamp as col_value,'could not cast to long' as quarantine_reason ) else NULL end as quarantine_information,
        case when _event_ingress_ts is NULL or cast(_event_ingress_ts as string) = '' then struct('_event_ingress_ts' as col_name,_event_ingress_ts as col_value,'could not cast to long' as quarantine_reason ) else NULL end as quarantine_information,
        case when  _event_origin is  NULL or cast(_event_origin as string) = '' then struct('_event_origin' as col_name,_event_origin as col_value,'column can not be null or blank' as quarantine_reason)
        when _event_origin is NULL or cast(_event_origin as string) = '' then struct('_event_origin' as col_name,_event_origin as col_value,'could not cast to string' as quarantine_reason ) else NULL end as quarantine_information,
        case when  _event_tags is  NULL or cast(_event_tags as string) = '' then struct('_event_tags' as col_name,_event_tags as col_value,'column can not be null or blank' as quarantine_reason),
        case when azimuth is NULL or cast(azimuth as string) = '' then struct('azimuth' as col_name,azimuth as col_value,'could not cast to double' as quarantine_reason ) else NULL end as quarantine_information,
        case when band_enabled is NULL or cast(band_enabled as string) = '' then struct('band_enabled' as col_name,band_enabled as col_value,'could not cast to string' as quarantine_reason ) else NULL end as quarantine_information,
        case when  carrier is  NULL or cast(carrier as string) = '' then struct('carrier' as col_name,carrier as col_value,'column can not be null or blank' as quarantine_reason)
        when carrier is NULL or cast(carrier as string) = '' then struct('carrier' as col_name,carrier as col_value,'could not cast to INT64' as quarantine_reason ) else NULL end as quarantine_information,
        case when carrier_type is NULL or cast(carrier_type as string) = '' then struct('carrier_type' as col_name,carrier_type as col_value,'could not cast to string' as quarantine_reason ) else NULL end as quarantine_information,
        case when centerline is NULL or cast(centerline as string) = '' then struct('centerline' as col_name,centerline as col_value,'could not cast to double' as quarantine_reason ) else NULL end as quarantine_information,
        case when  device_uid is  NULL or cast(device_uid as string) = '' then struct('device_uid' as col_name,device_uid as col_value,'column can not be null or blank' as quarantine_reason)
        when device_uid is NULL or cast(device_uid as string) = '' then struct('device_uid' as col_name,device_uid as col_value,'could not cast to string' as quarantine_reason ) else NULL end as quarantine_information,
        case when  enodeb_id is  NULL or cast(enodeb_id as string) = '' then struct('enodeb_id' as col_name,enodeb_id as col_value,'column can not be null or blank' as quarantine_reason)
        when enodeb_id is NULL or cast(enodeb_id as string) = '' then struct('enodeb_id' as col_name,enodeb_id as col_value,'could not cast to string' as quarantine_reason ) else NULL end as quarantine_information,
        case when frequency_enabled is NULL or cast(frequency_enabled as string) = '' then struct('frequency_enabled' as col_name,frequency_enabled as col_value,'could not cast to INT64' as quarantine_reason ) else NULL end as quarantine_information,
        case when model is NULL or cast(model as string) = '' then struct('model' as col_name,model as col_value,'could not cast to string' as quarantine_reason ) else NULL end as quarantine_information,
        case when rx_port_count is NULL or cast(rx_port_count as string) = '' then struct('rx_port_count' as col_name,rx_port_count as col_value,'could not cast to INT64' as quarantine_reason ) else NULL end as quarantine_information,
        case when  sector is  NULL or cast(sector as string) = '' then struct('sector' as col_name,sector as col_value,'column can not be null or blank' as quarantine_reason)
        when sector is NULL or cast(sector as string) = '' then struct('sector' as col_name,sector as col_value,'could not cast to INT64' as quarantine_reason ) else NULL end as quarantine_information,
        case when tx_port_count is NULL or cast(tx_port_count as string) = '' then struct('tx_port_count' as col_name,tx_port_count as col_value,'could not cast to INT64' as quarantine_reason ) else NULL end as quarantine_information,
        case when type is NULL or cast(type as string) = '' then struct('type' as col_name,type as col_value,'could not cast to string' as quarantine_reason ) else NULL end as quarantine_information,
        case when vendor is NULL or cast(vendor as string) = '' then struct('vendor' as col_name,vendor as col_value,'could not cast to string' as quarantine_reason ) else NULL end as quarantine_information,
        from data_collect
)

-- valid data query
CREATE or REPLACE VIEW ${target_dataset_name}.&{valid_latest_data} as(
select host,src,timestamp,_event_ingress_ts,_event_origin,_event_tags,azimuth,band_enabled,carrier,carrier_type,centerline,device_uid,enodeb_id,
frequency_enabled,model,rx_port_count,sector,tx_port_count,type,vendor,trans_dt from data_collect
                                    where  host is not NULL or host != '' or  (host is not NULL and cast(host as string) ='') or host !='null'
                                    and src is not NULL or src !='' or  (src is not NULL and cast(src as string) ='') or src !='null'
                                    and timestamp is NULL or timestamp ='' or (timestamp is NULL and cast(timestamp as string) !='') or timestamp ='null'
                                    and _event_ingress_ts is not NULL or _event_ingress_ts !='' or (_event_ingress_ts is not NULL and cast(_event_ingress_ts as string) ='') or _event_ingress_ts !='null'
                                    and _event_origin is not NULL or _event_origin != '' or (_event_origin is not NULL and cast(_event_origin as string) ='') or _event_origin !='null'
                                    and _event_tags is not NULL or _event_tags != ''
                                    and azimuth is not NULL or azimuth != '' or (azimuth is not NULL and cast(azimuth as string) ='') or azimuth !='null'
                                    and band_enabled is not NULL or band_enabled != '' or (band_enabled is not NULL and cast(band_enabled as string) ='') or band_enabled !='null'
                                    and carrier is not NULL or carrier != '' or (carrier is not NULL and cast(carrier as string) ='') or carrier !='null'
                                    and carrier_type is not NULL or carrier_type != '' or (carrier_type is not NULL and cast(carrier_type as string) ='') or carrier_type !='null'
                                    and centerline is not NULL or centerline != '' or (centerline is not NULL and cast(centerline as string) ='') or centerline !='null'
                                    and device_uid is not NULL or device_uid != '' or (device_uid is not NULL and cast(device_uid as string) ='') or device_uid !='null'
                                    and enodeb_id is not NULL or enodeb_id != '' or (enodeb_id is not NULL and cast(enodeb_id as string) ='') or enodeb_id !='null'
                                    and frequency_enabled is not NULL or frequency_enabled != '' or (frequency_enabled is not NULL and cast(frequency_enabled as string) ='') or frequency_enabled !='null'
                                    and model is not NULL or model != '' or (model is not NULL and cast(model as string) ='') or model !='null'
                                    and rx_port_count is not NULL or rx_port_count != '' or (rx_port_count is not NULL and cast(rx_port_count as string) ='') or rx_port_count !='null'
                                    and sector is not NULL or sector != '' or (sector is not NULL and cast(sector as string) ='') or sector !='null'
                                    and tx_port_count is not NULL or tx_port_count != '' or (tx_port_count is not NULL and cast(tx_port_count as string) ='') or tx_port_count !='null'
                                    and type is not NULL or type != '' or (type is not NULL and cast(type as string) ='') or type !='null'
                                    and vendor is not NULL or vendor != '' or (vendor is not NULL and cast(vendor as string) ='') or vendor !='null'

);

--master_history_data as(
--=========================
--SELECT *
--FROM ${master_dataset_name}.${master_table_name}
--WHERE trans_dt BETWEEN trans_dt_i AND to_date(trans_dt_i,'yy-mon-mm')+no_of_days_i
--to_date(jdate,'dd-mon-yy')+2
--                       )
--
--union_data as (
--select * from valid_latest_data
--union
--select * from master_history_data
--            )
--
--partition_master_data as (
--select * ,dense_rank() over
--partition by sector, carrier, device_uid,enodeb_id,centerline,band_enabled
--order by trans_dt desc as rank
--from union_data
--                        )
-- ==================
call ${target_dataset_name}.target_table_name_2(select_column,final_master_data , master_union_data ,
master_history_data , partition_master_data_rank ,valid_latest_data , key_column ,
master_dataset_name , trans_dt_i ,no_of_days_i );

BEGIN


-- funal master data query for arrange the column and insert it
INSERT INTO `${master_tgt_project_id}.${master_tgt_dataset_name}.${master_target_tblname}`
(host,src,timestamp,_event_ingress_ts,_event_origin,_event_tags,azimuth,band_enabled,carrier,carrier_type,centerline,device_uid,enodeb_id,
frequency_enabled,model,rx_port_count,sector,tx_port_count,type,vendor,trans_dt
)

select host,src,timestamp,_event_ingress_ts,_event_origin,_event_tags,azimuth,band_enabled,carrier,carrier_type,centerline,device_uid,enodeb_id,
frequency_enabled,model,rx_port_count,sector,tx_port_count,type,vendor,trans_dt from ${master_dataset_name}.${final_master_data}



--quarantine data query
select host,src,timestamp,_event_ingress_ts,_event_origin,_event_tags,azimuth,band_enabled,carrier,carrier_type,centerline,device_uid,enodeb_id,
frequency_enabled,model,rx_port_count,sector,tx_port_count,type,vendor,trans_dt,quarantine_information.col_name, quarantine_information.col_value,quarantine_information.quarantine_reason
 from quarantine_data where quarantine_information.col_name is not null and quarantine_information.col_value is not null and quarantine_information.quarantine_reason is not null



END


