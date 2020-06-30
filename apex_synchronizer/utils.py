import re


BASE_URL = 'https://api.apexvs.com/'
CAMEL_REG = re.compile(r'(?<!^)(?=[A-Z])')


def get_header(token, custom_args: dict = None) -> dict:
    header = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json'
    }

    if custom_args is not None:
        header.update(custom_args)
    return header


def snake_to_camel(var: str) -> str:
    """Converts snake_case to CamelCase."""
    return ''.join(ch.capitalize() or '_' for ch in var.split('_'))


def camel_to_snake(var: str) -> str:
    """Converts CamelCase to snake_case"""
    return re.sub(CAMEL_REG, '_', var).lower()
