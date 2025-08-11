from django import template

register = template.Library()

@register.filter
def humanize_number(value):
    try:
        num = float(value)
        if num.is_integer():
            return str(int(num))
        return ('{:.2f}'.format(num)).rstrip('0').rstrip('.')
    except Exception:
        return value
