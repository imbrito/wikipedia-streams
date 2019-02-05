#!/usr/bin/env python
#-*- coding: utf-8 -*
import logging, os, re
from google.cloud import bigquery

def schema():
    return [
        bigquery.schema.SchemaField(name="id",field_type="INTEGER",mode="NULLABLE"),
        bigquery.schema.SchemaField(name="user",field_type="STRING",mode="NULLABLE"),
        bigquery.schema.SchemaField(name="title",field_type="STRING",mode="NULLABLE"),
        bigquery.schema.SchemaField(name="comment",field_type="STRING",mode="NULLABLE"),
        bigquery.schema.SchemaField(name="parsedcomment",field_type="STRING",mode="NULLABLE"),
        bigquery.schema.SchemaField(name="minor",field_type="BOOLEAN",mode="NULLABLE"),
        bigquery.schema.SchemaField(name="patrolled",field_type="BOOLEAN",mode="NULLABLE"),
        bigquery.schema.SchemaField(name="namespace",field_type="INTEGER",mode="NULLABLE"),
        bigquery.schema.SchemaField(name="type",field_type="STRING",mode="NULLABLE"),
        bigquery.schema.SchemaField(name="bot",field_type="BOOLEAN",mode="NULLABLE"),
        bigquery.schema.SchemaField(name="server_script_path",field_type="STRING",mode="NULLABLE"),
        bigquery.schema.SchemaField(name="server_name",field_type="STRING",mode="NULLABLE"),
        bigquery.schema.SchemaField(name="server_url",field_type="STRING",mode="NULLABLE"),
        bigquery.schema.SchemaField(name="length",field_type="RECORD",mode="NULLABLE",fields=(
            bigquery.schema.SchemaField(name="old",field_type="INTEGER",mode="NULLABLE"),
            bigquery.schema.SchemaField(name="new",field_type="INTEGER",mode="NULLABLE")
        )),
        bigquery.schema.SchemaField(name="revision",field_type="RECORD",mode="NULLABLE",fields=(
            bigquery.schema.SchemaField(name="old",field_type="INTEGER",mode="NULLABLE"),
            bigquery.schema.SchemaField(name="new",field_type="INTEGER",mode="NULLABLE")
        )),
        bigquery.schema.SchemaField(name="meta",field_type="RECORD",mode="NULLABLE",fields=(
            bigquery.schema.SchemaField(name="request_id",field_type="STRING",mode="NULLABLE"),
            bigquery.schema.SchemaField(name="offset",field_type="INTEGER",mode="NULLABLE"),
            bigquery.schema.SchemaField(name="dt",field_type="TIMESTAMP",mode="NULLABLE"),
            bigquery.schema.SchemaField(name="schema_uri",field_type="STRING",mode="NULLABLE"),
            bigquery.schema.SchemaField(name="id",field_type="STRING",mode="NULLABLE"),
            bigquery.schema.SchemaField(name="uri",field_type="STRING",mode="NULLABLE"),
            bigquery.schema.SchemaField(name="partition",field_type="INTEGER",mode="NULLABLE"),
            bigquery.schema.SchemaField(name="domain",field_type="STRING",mode="NULLABLE"),
            bigquery.schema.SchemaField(name="topic",field_type="STRING",mode="NULLABLE")
        )),
        bigquery.schema.SchemaField(name="wiki",field_type="STRING",mode="NULLABLE"),
        bigquery.schema.SchemaField(name="timestamp",field_type="TIMESTAMP",mode="NULLABLE")
    ]

def partitioning():
    return bigquery.table.TimePartitioning(
        type_=bigquery.table.TimePartitioningType.DAY,
        field="timestamp",
        require_partition_filter=True
    )

def clustering():
    return ["wiki"]

def job_config():
    return bigquery.LoadJobConfig(
        autodetect=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        source_format="NEWLINE_DELIMITED_JSON",
        clustering_fields=clustering(),
        schema=schema(),
        time_partitioning=partitioning(),
        ignore_unknown_values=True
    )

def gcs2bigquery(data, context):
    print("Event ID: {}".format(context.event_id))
    print("Event type: {}".format(context.event_type))
    print("Bucket: {}".format(data["bucket"]))
    print("File: {}".format(data["name"]))
    print("Metageneration: {}".format(data["metageneration"]))
    print("Created: {}".format(data["timeCreated"]))

    client = bigquery.Client()
    dataset_ref = client.dataset(os.getenv("DATASET_ID"))
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        logging.warn("Creating dataset: {project}.{dataset}".format(
            project=os.getenv("PROJECT_ID"),
            dataset=os.getenv("DATASET_ID"))
        )
        client.create_dataset(dataset_ref)

    # wildcard="".join(re.findall("\d+", data["name"].split("T")[0]))
    table_id = "{project}.{dataset}.{table}".format(
        project=os.getenv("PROJECT_ID"),
        dataset=os.getenv("DATASET_ID"),
        table=os.getenv("TABLE_ID")
    )
    try:
        load_job = client.load_table_from_uri(
            "gs://{bucket}/{file}".format(bucket=data["bucket"],file=data["name"]),
            table_id,
            job_config=job_config(),
        )
        print("Load job: {id} [{table}]".format(id=load_job.job_id, table=table_id))
    except Exception as e:
        logging.error("Failed to create load job: {}".format(e))
    