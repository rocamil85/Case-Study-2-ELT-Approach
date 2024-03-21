#------------------------------   Este es un código demostrativo a partir de la implementación real   ---------------------------------

import warnings
import apache_beam as beam
import requests
from datetime import datetime
from dateutil import parser
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, WorkerOptions
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
import logging
import json

warnings.filterwarnings("ignore", category=DeprecationWarning)


def parse_message(pubsub_message):
    from datetime import datetime
    from dateutil import parser
    import requests

    def parse_and_format_date(date_str):
        if date_str:
            try:
                date_object = parser.parse(date_str)
                return date_object.strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                return None
        else:
            return None

    def parse_and_format_date_only(date_str):
        if date_str:
            try:
                date_object = parser.parse(date_str)
                return date_object.strftime('%Y-%m-%d')  # Solo fecha, sin hora
            except ValueError:
                return None
        else:
            return None

    try:
        message = pubsub_message.data
        message_string = message.decode('utf-8')
        logging.info("message_string: %s", message_string)

        orden = json.loads(message_string)

        delivery_order_id = orden.get('delivery_order_id', '')
        code = orden.get('code', '')
        recycling = orden.get('recycling', False) if orden.get(
            'recycling') not in [None, ""] else False
        assigned_courier = orden.get('assigned_courier', None) if orden.get(
            'assigned_courier') not in [None, ""] else None
        size_box = "".join(char for char in orden.get('size_box', '') if not char.isdigit(
        )) if orden.get('size_box') not in [None, ""] else None
        cross_docking_location_code = orden.get('cross_docking_location_code', None) if orden.get(
            'cross_docking_location_code') not in [None, ""] else None
        delivery_attemps = orden.get('delivery_attemps', None) if orden.get(
            'delivery_attemps') not in [None, ""] else None

        destination_geo_coding = orden.get('destination', {}).get('geo_coding', None) if orden.get(
            'destination', {}).get('geo_coding') not in [None, ""] else None
        destination_geo_location_lon = orden.get('destination', {}).get('geo_location', {}).get(
            'lon', None) if orden.get('destination', {}).get('geo_location', {}).get('lon') not in [None, ""] else None
        destination_geo_location_lat = orden.get('destination', {}).get('geo_location', {}).get(
            'lat', None) if orden.get('destination', {}).get('geo_location', {}).get('lat') not in [None, ""] else None
        destination_local = orden.get('destination', {}).get('local', None) if orden.get(
            'destination', {}).get('local') not in [None, ""] else None
        destination_not_located = orden.get('destination', {}).get('not_located', None) if orden.get(
            'destination', {}).get('not_located') not in [None, ""] else None
        destination_number = orden.get('destination', {}).get('number', None) if orden.get(
            'destination', {}).get('number') not in [None, ""] else None
        destination_street = orden.get('destination', {}).get('street', None) if orden.get(
            'destination', {}).get('street') not in [None, ""] else None
        destination_structure_id = orden.get('destination', {}).get('structure_id', None) if orden.get(
            'destination', {}).get('structure_id') not in [None, ""] else None
        destination_polygon = orden.get('destination', {}).get('polygon', None) if 'polygon' in orden.get(
            'destination', {}) and orden.get('destination', {}).get('polygon') not in [None, ""] else None

        
        destination_polygon_lab = None
        if destination_structure_id in [13123, 13120, 13101]:
            params = {"lat": destination_geo_location_lat,
                      "lon": destination_geo_location_lon, "structure_id": destination_structure_id}
            response = requests.post(
                'https://url/sistema/externo', json=params)
            if response.status_code == 200:
                destination_polygon_lab = json.loads(response.content)
                destination_polygon_lab = destination_polygon_lab if 'contained' in destination_polygon_lab and destination_polygon_lab[
                    'contained'] else None
                if destination_polygon_lab is not None:
                    destination_polygon_lab = destination_polygon_lab.get(
                        'segmentation')

      #aqui van más transformaciones...

        

        datos_orden = {
            "delivery_order_id": delivery_order_id,
            "code": code,
            "recycling": recycling,
            "assigned_courier": assigned_courier,
            "size_box": size_box,
            "cross_docking_location_code": cross_docking_location_code,
            "delivery_attemps": delivery_attemps,
            "destination_geo_coding": destination_geo_coding,
            "destination_geo_location_lon": destination_geo_location_lon,
            "destination_geo_location_lat": destination_geo_location_lat,
            "destination_local": destination_local,
            "destination_not_located": destination_not_located,
            "destination_number": destination_number,
            "destination_street": destination_street,
            "destination_structure_id": destination_structure_id,
            "destination_polygon": destination_polygon,
            "destination_polygon_lab": destination_polygon_lab,
            #aquí van mas campos...
        }

        return datos_orden

    except Exception as e:
        logging.error("Unexpected error: %s", str(e))
        logging.error("Message causing error: %s", message.decode('utf-8'))
        return None


def filter_none(element):
    if element is None:
        logging.warning("Element is None and will be filtered out.")
        return False
    return True


def log_elements(element):
    logging.info("Element to be inserted in BigQuery: %s", str(element))
    return element


def run_pipeline():
    runner_type = 'DataflowRunner'  # Cambiar a 'DataflowRunner' para ejecución en GCP

    pipeline_options = PipelineOptions()

    standard_options = pipeline_options.view_as(StandardOptions)
    standard_options.runner = runner_type
    standard_options.streaming = True

    if runner_type == 'DataflowRunner':
        google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
        google_cloud_options.project = 'project_id'
        google_cloud_options.job_name = 'job-streaming-order-ceo'
        google_cloud_options.staging_location = 'gs://nombre_bucket/temp'
        google_cloud_options.temp_location = 'gs://nombre_bucket/temp'
        google_cloud_options.region = 'us-west1'        

        # Worker Options
        worker_options = pipeline_options.view_as(WorkerOptions)
        worker_options.machine_type = 'n1-standard-2'
        worker_options.max_num_workers = 5
        worker_options.autoscaling_algorithm = 'THROUGHPUT_BASED'

    schema = {
        'fields': [
            {
                "mode": "NULLABLE",
                "name": "delivery_order_id",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "recycling",
                        "type": "BOOLEAN"
            },
            {
                "mode": "NULLABLE",
                "name": "assigned_courier",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "size_box",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "code",
                        "type": "STRING"
            },
            #se ponen todos los campos necesarios...
         
        ]
    }

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | 'Read from PubSub' >> ReadFromPubSub(subscription='projects/project_id/subscriptions/sub_delivery_order_ceo_pull', with_attributes=True)
            | 'Parse and Process Message' >> beam.Map(parse_message)
            | 'Filter and Log None Messages' >> beam.Filter(filter_none)
            | 'Log Elements Before Insertion' >> beam.Map(log_elements)
            | 'Write to BigQuery' >> WriteToBigQuery(
                table='project_id.nombre_dataset.delivery_order',
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
