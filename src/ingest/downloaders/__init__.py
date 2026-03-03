from .registry import register_downloader, get_downloader, list_downloaders

# import side effects: regisztrálják magukat
from . import haldepo  # noqa: F401