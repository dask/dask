"""
Client to schedule tasks on to an Azure Functions backend.
"""
from .backends.azure_functions import serialize, deserialize
from .type_checks import ishashable, istask
import requests


class AzureFunctionsClient():
    def __init__(self,
        hostname='localhost',
        port=7071,
        use_https=False
    ):
        scheme = "https" if use_https else "http"
        endpoint = "exec"
        self.func_url = f"{scheme}://{hostname}:{port}/api/{endpoint}"

    def exec(self, arg, cache):
        if isinstance(arg, list):
            return [self.exec(a, cache) for a in arg]
        elif istask(arg):
            # Extract the function to execute and its arguments
            func, args = arg[0], arg[1:]
            func_args = [self.exec(a, cache) for a in args]

            # Create the HTTP request for execution
            body = {
                "func": func,
                "args": func_args
            }

            # Serialize the request body
            print(f'Serializing: {body}')
            body_serialized = serialize(body)

            # Send the request and get the response
            func_url = "http://localhost:7071/api/exec"
            with requests.Session() as session:
                response = session.post(func_url, data=body_serialized)

            # Verify if the request succeeded
            if response.status_code != 200:
                raise Exception(f"Execution failed: {response}")

            # Deserialize the response content
            response_payload = deserialize(response.content)

            # Check if the execution itself succeeded
            status = response_payload["status"]
            if status != "success":
                raise Exception(f"Execution failed with status: {status}")

            # Return the execution result
            result = response_payload["result"]
            return result
        elif not ishashable(arg):
            return arg
        elif arg in cache:
            return cache[arg]
        else:
            return arg

