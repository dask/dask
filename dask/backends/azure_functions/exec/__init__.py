from ..serialization_utils import serialize, deserialize
import azure.functions as func
import logging


def generate_response(body: bytes, status_code: int) -> func.HttpResponse:
    """Generate an HTTP response from the given body and status code.
    Converts the body into a JSON string.
    """
    response = func.HttpResponse(
        body=body,
        mimetype="application/octet-stream",
        status_code=status_code,
    )
    return response


def main(req: func.HttpRequest) -> func.HttpResponse:
    """Executes the provided function with the provided arguments.
    Returns the result in the HTTP response.
    """
    # Get the HTTP body and deserialize it
    body_serialized = req.get_body().decode()
    body = deserialize(body_serialized)

    # Check if all required fields are present in the body
    if "func" not in body:
        status_code = 400
        response_payload = {
            "status": "fail",
            "reason": "missing param: func"
        }
        response_serialized = serialize(response_payload)
        return generate_response(response_serialized, status_code)
    elif "args" not in body:
        status_code = 400
        response_payload = {
            "status": "fail",
            "reason": "missing param: args"
        }
        response_serialized = serialize(response_payload)
        return generate_response(response_serialized, status_code)

    # Deserialize inputs to execute the code
    func = body["func"]
    args = body["args"]

    # Execute code
    result = func(*args)
    logging.info(f"Execution result: {result}")

    # Serialize the response to send back
    response_payload = {
        "status": "success",
        "result": result
    }
    response_serialized = serialize(response_payload)

    # Generate response
    status_code = 200
    return generate_response(response_serialized, status_code)
