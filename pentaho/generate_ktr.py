import xml.etree.ElementTree as ET
from xml.dom import minidom
import os

def create_pentaho_transform():
    """Generate a valid Pentaho .ktr file for Silver to Gold aggregation."""
    
    root = ET.Element("transformation")
    
    # Info section
    info = ET.SubElement(root, "info")
    ET.SubElement(info, "name").text = "silver_to_gold"
    ET.SubElement(info, "description").text = "Aggregate Silver comments to Gold daily video stats"
    ET.SubElement(info, "extended_description").text = "Reads silver_comments, groups by video and date, writes to daily_video_stats"
    ET.SubElement(info, "trans_version").text = "1.0"
    
    # Database connection
    connection = ET.SubElement(root, "connection")
    ET.SubElement(connection, "name").text = "youtube_dw"
    ET.SubElement(connection, "server").text = "postgres"
    ET.SubElement(connection, "type").text = "POSTGRESQL"
    ET.SubElement(connection, "access").text = "Native"
    ET.SubElement(connection, "database").text = "youtube_dw"
    ET.SubElement(connection, "port").text = "5432"
    ET.SubElement(connection, "username").text = "de_user"
    ET.SubElement(connection, "password").text = "de_pass"
    ET.SubElement(connection, "servername")
    ET.SubElement(connection, "data_tablespace")
    ET.SubElement(connection, "index_tablespace")
    ET.SubElement(connection, "attributes")
    
    # Order section (defines step execution order)
    order = ET.SubElement(root, "order")
    
    # Step 1: Table Input - Read Silver Comments
    step1 = ET.SubElement(root, "step")
    ET.SubElement(step1, "name").text = "Read Silver Comments"
    ET.SubElement(step1, "type").text = "TableInput"
    ET.SubElement(step1, "description")
    ET.SubElement(step1, "distribute").text = "Y"
    ET.SubElement(step1, "custom_distribution")
    ET.SubElement(step1, "copies").text = "1"
    ET.SubElement(step1, "partitioning").text = "Method: none\nSchema_name: \n"
    ET.SubElement(step1, "connection").text = "youtube_dw"
    ET.SubElement(step1, "sql").text = """SELECT 
    video_id,
    DATE(published_at) as comment_date,
    COUNT(*) as total_comments,
    AVG(like_count) as avg_likes,
    AVG(text_length) as avg_length,
    SUM(CASE WHEN has_url THEN 1 ELSE 0 END) as url_count,
    SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) as positive_count,
    SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END) as negative_count,
    SUM(CASE WHEN sentiment_label = 'neutral' THEN 1 ELSE 0 END) as neutral_count
FROM silver_comments
GROUP BY video_id, DATE(published_at)"""
    ET.SubElement(step1, "limit").text = "0"
    ET.SubElement(step1, "lookup").text = "youtube_dw"
    ET.SubElement(step1, "execute_each_row").text = "N"
    ET.SubElement(step1, "variables_active").text = "N"
    ET.SubElement(step1, "lazy_conversion_active").text = "N"
    ET.SubElement(step1, "cached_row_meta_active").text = "N"
    ET.SubElement(step1, "row-meta")
    ET.SubElement(step1, "attributes")
    
    # Step 2: Table Output - Write to Gold
    step2 = ET.SubElement(root, "step")
    ET.SubElement(step2, "name").text = "Write Daily Stats"
    ET.SubElement(step2, "type").text = "TableOutput"
    ET.SubElement(step2, "description")
    ET.SubElement(step2, "distribute").text = "Y"
    ET.SubElement(step2, "custom_distribution")
    ET.SubElement(step2, "copies").text = "1"
    ET.SubElement(step2, "partitioning").text = "Method: none\nSchema_name: \n"
    ET.SubElement(step2, "connection").text = "youtube_dw"
    ET.SubElement(step2, "schema").text = "public"
    ET.SubElement(step2, "table").text = "daily_video_stats"
    ET.SubElement(step2, "commit").text = "1000"
    ET.SubElement(step2, "truncate").text = "N"
    ET.SubElement(step2, "ignore_errors").text = "N"
    ET.SubElement(step2, "use_batch").text = "Y"
    ET.SubElement(step2, "specify_fields").text = "N"
    ET.SubElement(step2, "partitioning_enabled").text = "N"
    ET.SubElement(step2, "partitioning_field")
    ET.SubElement(step2, "partitioning_daily").text = "N"
    ET.SubElement(step2, "partitioning_monthly").text = "Y"
    ET.SubElement(step2, "tablename_in_field").text = "N"
    ET.SubElement(step2, "tablename_field")
    ET.SubElement(step2, "tablename_in_table").text = "N"
    ET.SubElement(step2, "return_keys").text = "N"
    ET.SubElement(step2, "return_field")
    ET.SubElement(step2, "fields")
    ET.SubElement(step2, "attributes")
    
    # Hop (connection between steps)
    hop = ET.SubElement(root, "hop")
    ET.SubElement(hop, "from").text = "Read Silver Comments"
    ET.SubElement(hop, "to").text = "Write Daily Stats"
    ET.SubElement(hop, "enabled").text = "Y"
    ET.SubElement(hop, "unconditional").text = "N"
    ET.SubElement(hop, "error").text = "N"
    ET.SubElement(hop, "evaluation").text = "Y"
    ET.SubElement(hop, "source_step").text = "Read Silver Comments"
    ET.SubElement(hop, "target_step").text = "Write Daily Stats"
    
    # Add steps to order
    hop_order = ET.SubElement(order, "hop")
    ET.SubElement(hop_order, "from").text = "Read Silver Comments"
    ET.SubElement(hop_order, "to").text = "Write Daily Stats"
    ET.SubElement(hop_order, "enabled").text = "Y"
    ET.SubElement(hop_order, "unconditional").text = "N"
    ET.SubElement(hop_order, "error").text = "N"
    ET.SubElement(hop_order, "evaluation").text = "Y"
    ET.SubElement(hop_order, "source_step").text = "Read Silver Comments"
    ET.SubElement(hop_order, "target_step").text = "Write Daily Stats"
    
    # Notepads (empty)
    notepads = ET.SubElement(root, "notepads")
    
    # Attributes
    attributes = ET.SubElement(root, "attributes")
    
    # Pretty print XML
    xml_str = ET.tostring(root, encoding="unicode")
    reparsed = minidom.parseString(xml_str.encode("utf-8"))
    pretty_xml = reparsed.toprettyxml(indent="  ")
    
    # Remove empty lines
    lines = [line for line in pretty_xml.split("\n") if line.strip()]
    return "\n".join(lines)

if __name__ == "__main__":
    # Ensure directory exists
    os.makedirs("pentaho", exist_ok=True)
    
    ktr_content = create_pentaho_transform()
    
    filepath = "pentaho/silver_to_gold.ktr"
    with open(filepath, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write(ktr_content)
    
    print(f"✅ Generated: {filepath}")
    print(f"Size: {len(ktr_content)} characters")
    print("\nNext steps:")
    print("1. Create target table in PostgreSQL")
    print("2. Run: docker exec -it pentaho /opt/pentaho/data-integration/pan.sh -file=/pentaho-jobs/silver_to_gold.ktr")