from decimal import Decimal

def get_price_by_pip(pip):
    pip = Decimal(pip)
    return (Decimal('2.00') + (Decimal('3.0') + (Decimal('5.0') + (Decimal('15.0') + (Decimal('75.0') + (Decimal('100') + (Decimal('200') if pip > 1300 else (pip - Decimal('1100')) * Decimal('0.5'))) if pip > 1100 else (pip - Decimal('900')) * Decimal('0.50')) if pip > 900 else (pip - Decimal('600')) * Decimal('0.25')) if pip > 600 else (pip - Decimal('450')) * Decimal('0.1')) if pip > 450 else (pip - Decimal('350')) * Decimal('0.05')) if pip > 350 else (pip - Decimal('200')) * Decimal('0.02')) if pip > 200 else pip * Decimal('0.01')

def get_buy_pip(price, side):
    price = Decimal(price)
    pip = Decimal('0')
    if price <= Decimal('2.00'):
        pip = price / Decimal('0.01')
    elif price <= Decimal('5.00'): # max of this step = 350 = 200 (previous step) + (5 - 2) / 0.02 = 200 + 3/0.02 = 200 + 150
        pip = 200 + (price - Decimal('2')) / Decimal('0.02')
    elif price <= Decimal('10.00'): # max of this step = 450 = 350 (previous step) + (10 - 5) / 0.05 = 350 + 5/0.05 = 350 + 100
        pip = 350 + (price - Decimal('5')) / Decimal('0.05')
    elif price <= Decimal('25.00'): # max of this step = 600 = 450 (previous step) + (25 - 10) / 0.1 = 450 + 15/0.1 = 450 + 150
        pip = 450 + (price - Decimal('10')) / Decimal('0.1')
    elif price <= Decimal('100.0'): # max of this step = 900 = 600 (previous step) + (100 - 25) / 0.25 = 600 + 75/0.25 = 600 + 300
        pip = 600 + (price - Decimal('25')) / Decimal('0.25')
    elif price <= Decimal('200.0'): # max of this step = 1100 = 900 (previous step) + (200 - 100) / 0.5 = 900 + 100/0.5 = 900 + 200
        pip = 900 + (price - Decimal('100')) / Decimal('0.50')
    elif price <= Decimal('400.0'): # max of this step = 1300 = 1100 (previous step) + (400 - 200) / 1.0 = 1100 + 200/1.0 = 1100 + 200
        pip = 1100 + (price - Decimal('200')) / Decimal('1.0')
    elif price > Decimal('400.0'): # no max of this step, just calculate 1100 + (price - 400) / 2.0
        pip = 1300 + (price - Decimal('400')) / Decimal('2.0')
    return int(round(pip) + 1) if side == "S" else int(round(pip))