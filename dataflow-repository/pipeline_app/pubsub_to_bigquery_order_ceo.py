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

        # Transformación para lograr destination_polygon_lab
        destination_polygon_lab = None
        if destination_structure_id in [13123, 13120, 13101]:
            params = {"lat": destination_geo_location_lat,
                      "lon": destination_geo_location_lon, "structure_id": destination_structure_id}
            response = requests.post(
                'https://pickup.alasxpress.com/api/_polygon-finder-lab', json=params)
            if response.status_code == 200:
                destination_polygon_lab = json.loads(response.content)
                destination_polygon_lab = destination_polygon_lab if 'contained' in destination_polygon_lab and destination_polygon_lab[
                    'contained'] else None
                if destination_polygon_lab is not None:
                    destination_polygon_lab = destination_polygon_lab.get(
                        'segmentation')

        dispatcher_employee_code = orden.get('dispatcher_employee_code', None) if orden.get(
            'dispatcher_employee_code') not in [None, ""] else None
        equipment_serial = orden.get('equipment_serial', None) if orden.get(
            'equipment_serial') not in [None, ""] else None
        equipment_serial_2nd = orden.get('equipment_serial_2nd', None) if orden.get(
            'equipment_serial_2nd') not in [None, ""] else None

        extended_info_documents = json.dumps(orden.get('extended_info', {}).get('documents', None)) if orden.get(
            'extended_info', {}).get('documents') not in [None, ""] else None
        extended_info_integration = orden.get('extended_info', {}).get('integration', None) if orden.get(
            'extended_info', {}).get('integration') not in [None, ""] else None
        extended_info_same_day_service = True if orden.get(
            'extended_info', {}).get('same_day_service') else False
        extended_info_am_pm_service = True if orden.get(
            'extended_info', {}).get('am_pm_service') else False
        external_id = orden.get('external_id', None)

        lob = orden.get('lob', 1) if orden.get('lob') not in [None, ""] else 1
        branch = orden.get('branch', lob) if orden.get(
            'branch') not in [None, ""] else lob
        planning_status = orden.get('planning_status', None) if orden.get(
            'planning_status') not in [None, ""] else None
        portability_code = orden.get('portability_code', None) if orden.get(
            'portability_code') not in [None, ""] else None
        priority = orden.get('priority', None) if orden.get(
            'priority') not in [None, ""] else None
        receiver_b2c_code = orden.get('receiver', {}).get('b2c_code', None) if orden.get(
            'receiver', {}).get('b2c_code') not in [None, ""] else None
        receiver_first_name = orden.get('receiver', {}).get('first_name', None) if orden.get(
            'receiver', {}).get('first_name') not in [None, ""] else None
        receiver_last_name = orden.get('receiver', {}).get('last_name', None) if orden.get(
            'receiver', {}).get('last_name') not in [None, ""] else None
        receiver_mobile_phone = orden.get('receiver', {}).get('mobile_phone', None) if orden.get(
            'receiver', {}).get('mobile_phone') not in [None, ""] else None
        receiver_email = orden.get('receiver', {}).get('email', None) if orden.get(
            'receiver', {}).get('email') not in [None, ""] else None
        route_tag = orden.get('route_tag', None) if orden.get(
            'route_tag') not in [None, ""] else None
        sender_code = orden.get('sender_code', None) if orden.get(
            'sender_code') not in [None, ""] else None
        sender_name = orden.get('sender_name', None) if orden.get(
            'sender_name') not in [None, ""] else None
        sim_serial = orden.get('sim_serial', None) if orden.get(
            'sim_serial') not in [None, ""] else None
        status = orden.get('status', None)
        statuses = json.dumps(orden.get('statuses')) if orden.get(
            'statuses') is not None else None

        time_info_b2b_delivery_actual = parse_and_format_date(
            orden.get('time_info', {}).get('b2b_delivery_actual'))
        time_info_b2b_delivery_expected = parse_and_format_date(
            orden.get('time_info', {}).get('b2b_delivery_expected'))
        time_info_b2b_reception_actual = parse_and_format_date(
            orden.get('time_info', {}).get('b2b_reception_actual'))
        time_info_b2b_reception_expected = parse_and_format_date(
            orden.get('time_info', {}).get('b2b_reception_expected'))
        time_info_b2c_delivery_actual = parse_and_format_date(
            orden.get('time_info', {}).get('b2c_delivery_actual'))
        time_info_b2c_delivery_expected = parse_and_format_date(
            orden.get('time_info', {}).get('b2c_delivery_expected'))

        date_type8 = parse_and_format_date_only(
            time_info_b2c_delivery_expected)

        time_info_carrier_delivery_actual = parse_and_format_date(
            orden.get('time_info', {}).get('carrier_delivery_actual'))
        time_info_carrier_delivery_expected = parse_and_format_date(
            orden.get('time_info', {}).get('carrier_delivery_expected'))
        time_info_carrier_dispatch_actual = parse_and_format_date(
            orden.get('time_info', {}).get('carrier_dispatch_actual'))
        time_info_carrier_dispatch_expected = parse_and_format_date(
            orden.get('time_info', {}).get('carrier_dispatch_expected'))
        time_info_courier_reception_actual = parse_and_format_date(
            orden.get('time_info', {}).get('courier_reception_actual'))
        time_info_courier_reception_expected = parse_and_format_date(
            orden.get('time_info', {}).get('courier_reception_expected'))
        time_info_dc_reception_actual = parse_and_format_date(
            orden.get('time_info', {}).get('dc_reception_actual'))
        time_info_pickup_dc_reception_actual = parse_and_format_date(
            orden.get('time_info', {}).get('pickup_dc_reception_actual'))
        time_info_dc_reception_expected = parse_and_format_date(
            orden.get('time_info', {}).get('dc_reception_expected'))
        time_info_pickup_dc_reception_expected = parse_and_format_date(
            orden.get('time_info', {}).get('pickup_dc_reception_expected'))
        time_info_ser_reception_actual = parse_and_format_date(
            orden.get('time_info', {}).get('ser_reception_actual'))
        time_info_pickup_ser_reception_actual = parse_and_format_date(
            orden.get('time_info', {}).get('pickup_ser_reception_actual'))
        time_info_ser_reception_expected = parse_and_format_date(
            orden.get('time_info', {}).get('ser_reception_expected'))
        time_info_pickup_ser_reception_expected = parse_and_format_date(
            orden.get('time_info', {}).get('pickup_ser_reception_expected'))
        time_info_tl_reception_actual = parse_and_format_date(
            orden.get('time_info', {}).get('tl_reception_actual'))
        time_info_tl_reception_expected = parse_and_format_date(
            orden.get('time_info', {}).get('tl_reception_expected'))
        timestamp = parse_and_format_date(orden.get('timestamp'))
        date_type9 = parse_and_format_date_only(timestamp)

        # campos json se convierten en string
        events_info = orden.get('events_info')
        events_info_json = json.dumps(
            events_info) if events_info not in (None, []) else None

        schedule_events_info = orden.get('schedule_events_info')
        schedule_events_info_json = json.dumps(
            schedule_events_info) if schedule_events_info not in (None, []) else None

        reschedule_events_info = orden.get('reschedule_events_info')
        reschedule_events_info_json = json.dumps(
            reschedule_events_info) if reschedule_events_info not in (None, []) else None

        changes_info = orden.get('changes_info')
        changes_info_json = json.dumps(
            changes_info) if changes_info not in (None, []) else None

        packages = orden.get('packages')
        packages_json = json.dumps(
            packages) if packages not in (None, []) else None

        items = orden.get('items')
        items_json = json.dumps(items) if items not in (None, []) else None

        debt = orden.get('debt', None) if orden.get(
            'debt') not in [None, "", 0] else None
        add_maquila = orden.get('add_maquila', None) if orden.get(
            'add_maquila') not in [None, "", 0] else None
        pallet_code = orden.get('pallet_code', None) if orden.get(
            'pallet_code') not in [None, ""] else None

        self_management = orden.get('self_management', None) if orden.get(
            'self_management') not in [None, ""] else None

        schedule_status = orden.get('schedule_status', None) if orden.get(
            'schedule_status') not in [None, ""] else None   

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
            "dispatcher_employee_code": dispatcher_employee_code,
            "equipment_serial": equipment_serial,
            "equipment_serial_2nd": equipment_serial_2nd,
            "extended_info_documents": extended_info_documents,
            "extended_info_integration": extended_info_integration,
            "extended_info_same_day_service": extended_info_same_day_service,
            "extended_info_am_pm_service": extended_info_am_pm_service,
            "external_id": external_id,
            "lob": lob,
            "branch": branch,
            "planning_status": planning_status,
            "portability_code": portability_code,
            "priority": priority,
            "receiver_b2c_code": receiver_b2c_code,
            "receiver_first_name": receiver_first_name,
            "receiver_last_name": receiver_last_name,
            "receiver_mobile_phone": receiver_mobile_phone,
            "receiver_email": receiver_email,
            "route_tag": route_tag,
            "sender_code": sender_code,
            "sender_name": sender_name,
            "sim_serial": sim_serial,
            "status": status,
            "statuses": statuses,
            "time_info_b2b_delivery_actual": time_info_b2b_delivery_actual,
            "time_info_b2b_delivery_expected": time_info_b2b_delivery_expected,
            "time_info_b2b_reception_actual": time_info_b2b_reception_actual,
            "time_info_b2b_reception_expected": time_info_b2b_reception_expected,
            "time_info_b2c_delivery_actual": time_info_b2c_delivery_actual,
            "time_info_b2c_delivery_expected": time_info_b2c_delivery_expected,
            "date_type8": date_type8,
            "time_info_carrier_delivery_actual": time_info_carrier_delivery_actual,
            "time_info_carrier_delivery_expected": time_info_carrier_delivery_expected,
            "time_info_carrier_dispatch_actual": time_info_carrier_dispatch_actual,
            "time_info_carrier_dispatch_expected": time_info_carrier_dispatch_expected,
            "time_info_courier_reception_actual": time_info_courier_reception_actual,
            "time_info_courier_reception_expected": time_info_courier_reception_expected,
            "time_info_dc_reception_actual": time_info_dc_reception_actual,
            "time_info_pickup_dc_reception_actual": time_info_pickup_dc_reception_actual,
            "time_info_dc_reception_expected": time_info_dc_reception_expected,
            "time_info_pickup_dc_reception_expected": time_info_pickup_dc_reception_expected,
            "time_info_ser_reception_actual": time_info_ser_reception_actual,
            "time_info_pickup_ser_reception_actual": time_info_pickup_ser_reception_actual,
            "time_info_ser_reception_expected": time_info_ser_reception_expected,
            "time_info_pickup_ser_reception_expected": time_info_pickup_ser_reception_expected,
            "time_info_tl_reception_actual": time_info_tl_reception_actual,
            "time_info_tl_reception_expected": time_info_tl_reception_expected,
            "timestamp": timestamp,
            "date_type9": date_type9,
            "events_info_json": events_info_json,
            "schedule_events_info_json": schedule_events_info_json,
            "reschedule_events_info_json": reschedule_events_info_json,
            "changes_info_json": changes_info_json,
            "packages_json": packages_json,
            "items_json": items_json,
            "debt": debt,
            "add_maquila": add_maquila,
            "pallet_code": pallet_code,
            "self_management": self_management,
            "schedule_status": schedule_status
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
        google_cloud_options.project = 'warehousev2-ce0-alas'
        google_cloud_options.job_name = 'job-streaming-order-ceo'
        google_cloud_options.staging_location = 'gs://bucket-dataflow-order_ceo/job-pubsub-to-bigquery-order_ceo/temp'
        google_cloud_options.temp_location = 'gs://bucket-dataflow-order_ceo/job-pubsub-to-bigquery-order_ceo/temp'
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
            {
                "mode": "NULLABLE",
                "name": "cross_docking_location_code",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "delivery_attemps",
                        "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "destination_geo_coding",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "destination_geo_location_lon",
                        "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "destination_geo_location_lat",
                        "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "destination_local",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "destination_not_located",
                        "type": "BOOLEAN"
            },
            {
                "mode": "NULLABLE",
                "name": "destination_number",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "destination_street",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "destination_structure_id",
                        "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "destination_polygon",
                        "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "dispatcher_employee_code",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "equipment_serial",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "equipment_serial_2nd",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "extended_info_documents",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "extended_info_integration",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "extended_info_same_day_service",
                        "type": "BOOLEAN"
            },
            {
                "mode": "NULLABLE",
                "name": "extended_info_am_pm_service",
                        "type": "BOOLEAN"
            },
            {
                "mode": "NULLABLE",
                "name": "external_id",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "lob",
                        "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "branch",
                        "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "planning_status",
                        "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "portability_code",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "priority",
                        "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "receiver_b2c_code",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "receiver_first_name",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "receiver_last_name",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "receiver_mobile_phone",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "receiver_email",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "route_tag",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "sender_code",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "sender_name",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "sim_serial",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "status",
                        "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "statuses",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "time_info_b2b_delivery_actual",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "time_info_b2b_delivery_expected",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "time_info_b2b_reception_actual",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "time_info_b2b_reception_expected",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "time_info_b2c_delivery_actual",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "time_info_b2c_delivery_expected",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "date_type8",
                        "type": "DATE"
            },
            {
                "mode": "NULLABLE",
                "name": "time_info_carrier_delivery_actual",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "time_info_carrier_delivery_expected",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "time_info_carrier_dispatch_actual",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "time_info_carrier_dispatch_expected",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "time_info_courier_reception_actual",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "time_info_courier_reception_expected",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "time_info_dc_reception_actual",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "time_info_dc_reception_expected",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "time_info_pickup_dc_reception_actual",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "time_info_pickup_dc_reception_expected",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "time_info_ser_reception_actual",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "time_info_ser_reception_expected",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "time_info_pickup_ser_reception_actual",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "time_info_pickup_ser_reception_expected",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "time_info_tl_reception_actual",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "time_info_tl_reception_expected",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "timestamp",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "date_type9",
                        "type": "DATE"
            },
            {
                "mode": "NULLABLE",
                "name": "events_info_json",
                        "type": "JSON"
            },
            {
                "mode": "NULLABLE",
                "name": "schedule_events_info_json",
                        "type": "JSON"
            },
            {
                "mode": "NULLABLE",
                "name": "reschedule_events_info_json",
                        "type": "JSON"
            },
            {
                "mode": "NULLABLE",
                "name": "changes_info_json",
                        "type": "JSON"
            },
            {
                "mode": "NULLABLE",
                "name": "packages_json",
                        "type": "JSON"
            },
            {
                "mode": "NULLABLE",
                "name": "items_json",
                        "type": "JSON"
            },
            {
                "mode": "NULLABLE",
                "name": "destination_polygon_lab",
                        "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "debt",
                        "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "add_maquila",
                        "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "pallet_code",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "created_at",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "updated_at",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "b2c_delivery_attempt_1",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "b2c_delivery_attempt_1_user_name",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "b2c_delivery_attempt_1_status",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "b2c_delivery_attempt_2",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "b2c_delivery_attempt_2_user_name",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "b2c_delivery_attempt_2_status",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "b2c_delivery_attempt_3",
                        "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "b2c_delivery_attempt_3_user_name",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "b2c_delivery_attempt_3_status",
                        "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "self_management",
                        "type": "BOOLEAN"
            },
            {
                "mode": "NULLABLE",
                "name": "schedule_status",
                        "type": "INTEGER"
            }
        ]
    }

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | 'Read from PubSub' >> ReadFromPubSub(subscription='projects/warehousev2-ce0-alas/subscriptions/sub_delivery_order_ceo_pull', with_attributes=True)
            | 'Parse and Process Message' >> beam.Map(parse_message)
            | 'Filter and Log None Messages' >> beam.Filter(filter_none)
            | 'Log Elements Before Insertion' >> beam.Map(log_elements)
            | 'Write to BigQuery' >> WriteToBigQuery(
                table='warehousev2-ce0-alas.alas_dataset.delivery_order',
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
