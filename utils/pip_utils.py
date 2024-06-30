def get_price_by_pip(pip):
    return (2.00 + (3.0 + (5.0 + (15.0 + (75.0 + (100 + (200 if pip > 1300 else (pip - 1100) * 0.5)) if pip > 1100 else (pip - 900) * 0.50) if pip > 900 else (pip - 600) * 0.25) if pip > 600 else (pip - 450) * 0.1) if pip > 450 else (pip - 350) * 0.05) if pip > 350 else (pip - 200) * 0.02) if pip > 200 else pip * 0.01

def get_buy_pip(price, side):
    pip = 0
    if price <= 2.00:
        pip = price / 0.01
    elif price <= 5.00:
        pip = (price - 2) / 0.02
    elif price <= 10.00:
        pip = (price - 5) / 0.05
    elif price <= 25.00:
        pip = (price - 10) / 0.1
    elif price <= 100.0:
        pip = (price - 25) / 0.25
    elif price <= 200.0:
        pip = (price - 100) / 0.50
    elif price <= 400.0:
        pip = (price - 200) / 1.0
    elif price > 400.0:
        pip = (price - 400) / 2.0
    return int(round(pip) + 1) if side == "S" else int(round(pip))
