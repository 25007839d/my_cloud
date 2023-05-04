import xml.etree.ElementTree as ET

tree = ET.parse(r"C:\Users\Dell\PycharmProjects\cloud\oozie\worlflow.xml")

root = tree.getroot()

tag = root.tag

print("tag",tag)
print("root",root)