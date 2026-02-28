"""
normalize.suppliers

Ennek a package-nek az a célja, hogy importálja a beszállító specifikus
normalizáló modulokat, amik regisztrálják magukat a registry-be.

Használat:
    import src.normalize.suppliers  # ez betölti és regisztrálja az összes normalizálót
"""

from . import natura  # noqa: F401