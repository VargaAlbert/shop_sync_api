from decimal import Decimal, ROUND_UP


VAT_RATE = Decimal("1.27")
ROUND_STEP = Decimal("5")


def net_to_gross_rounded_5(net: float | int | Decimal) -> float:
    """
    Nettó -> bruttó (27% ÁFA) -> 5 Ft-ra felfelé kerekítve.
    """
    net = Decimal(str(net))
    gross = net * VAT_RATE

    rounded = (
        (gross / ROUND_STEP)
        .to_integral_value(rounding=ROUND_UP)
        * ROUND_STEP
    )

    return float(rounded)