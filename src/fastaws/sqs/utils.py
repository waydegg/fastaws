def get_endpoint_from_url(queue_url: str):
    endpoint = "/".join(queue_url.split("/")[-2:])
    endpoint = "/" + endpoint

    return endpoint
