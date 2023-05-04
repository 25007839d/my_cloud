# big query nested schema

[
    {"name":"id","type":"INTEGER","mode":"REQUIRED"},
    {"name":"name","type":"STRING","mode":"REQUIRED"},
    {"name":"ADD","type":"STRING","mode":"REQUIRED","fields":
     [
         {"name":"city","type":"STRING","mode":"REQUIRED"},
         {"name":"state","type":"STRING","mode":"REQUIRED"}
     ]}
]