
"host",
"src",
"timestamp",
"_event_ingress_ts",
"_event_origin",
"_event_tags",
"azimuth",
"band_enabled",
"carrier",
"carrier_type",
"centerline",
"device_uid",
 "enodeb_id",
"frequency_enabled",
 "model",
 "rx_port_count",
"sector",
"tx_port_count",
"type",
"vendor",
trans_dt

with c as
(
SELECT column_name
FROM dushyant-373205.gopal.INFORMATION_SCHEMA.COLUMNS
WHERE  table_name='mohan')

select m.name,c.column_name from dushyant-373205.gopal.mohan m join c on m.name!=c.column_name

"host",
"src",
"timestamp",
"_event_ingress_ts",
"_event_origin",
"_event_tags",
"azimuth",
"band_enabled",
"carrier",
"carrier_type",
"centerline",
"device_uid",
 "enodeb_id",
"frequency_enabled",
 "model",
 "rx_port_count",
"sector",
"tx_port_count",
"type",
"vendor",
