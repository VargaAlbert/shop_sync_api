from .module import HaldepoSupplier
from src.core.registry import register_supplier

register_supplier(HaldepoSupplier())