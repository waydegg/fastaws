import re


def get_endpoint_from_url(queue_url: str):
    endpoint_pattern = r"\/[0-9]+\/[a-z]*"
    endpoint_regex = re.search(endpoint_pattern, queue_url)
    assert endpoint_regex is not None
    endpoint = endpoint_regex.group(0)

    return endpoint
