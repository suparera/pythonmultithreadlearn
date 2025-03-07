from decimal import Decimal

def get_buy_pip(price, side):
    try:
        price = float(price)
        if price != price:  # Check for NaN
            return 0

        pip = 0.0
        if price <= 2.00:
            pip = price / 0.01
        elif price <= 5.00:
            pip = 200 + (price - 2) / 0.02
        elif price <= 10.00:
            pip = 350 + (price - 5) / 0.05
        elif price <= 25.00:
            pip = 450 + (price - 10) / 0.1
        elif price <= 100.0:
            pip = 600 + (price - 25) / 0.25
        elif price <= 200.0:
            pip = 900 + (price - 100) / 0.50
        elif price <= 400.0:
            pip = 1100 + (price - 200) / 1.0
        elif price > 400.0:
            pip = 1300 + (price - 400) / 2.0
        return int(round(pip) + 1) if side == "S" else int(round(pip))
    except Exception as e:
        raise ValueError(f"Invalid price value: {price}, price type is {type(price)}")

def get_price_by_pip(pip):
    try:
        pip = Decimal(pip)
        if pip <= 200:
            price = pip * Decimal('0.01')
        elif pip <= 350:
            price = 2 + (pip - 200) * Decimal('0.02')
        elif pip <= 450:
            price = 5 + (pip - 350) * Decimal('0.05')
        elif pip <= 600:
            price = 10 + (pip - 450) * Decimal('0.1')
        elif pip <= 900:
            price = 25 + (pip - 600) * Decimal('0.25')
        elif pip <= 1100:
            price = 100 + (pip - 900) * Decimal('0.50')
        elif pip <= 1300:
            price = 200 + (pip - 1100) * Decimal('1.0')
        else:
            price = 400 + (pip - 1300) * Decimal('2.0')
        return float(price)
    except Exception as e:
        print(f"get_price_by_pip Error: {e}, pip: {pip}, type(pip): {type(pip)}")
        # print stack trace
        import traceback
        traceback.print_exc()
        return 0.0
