BASE_URL = 'https://api.apexvs.com/'


def get_header(token, custom_args: dict = None) -> dict:
    header = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json'
    }

    if custom_args is not None:
        header.update(custom_args)
    return header
