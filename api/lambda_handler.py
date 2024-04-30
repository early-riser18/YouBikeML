import json
from api.forecast_service import get_fill_rate_forecast
import api.get_bike_station_status

def lambda_handler(event: dict, context):
    """Wrapper function to handle calls to api via AWS Lambda API"""
    # print("EVENT RECEIVED: ", event)
    event_body = json.loads(event["body"])
    try:
        event_body["service"]
    except:
        return "Value Error: Parameter 'service' missing."

    match event_body["service"]:
        case 'bike_station_status':
            
            res = api.get_bike_station_status.get_bike_station_status(bool(event_body["extended"]))
        case 'fill_rate_forecast':
            res = get_fill_rate_forecast(event_body["station_id"])

        case _:
            return "Error: the service requested in unavailable."

    return res.to_json()
