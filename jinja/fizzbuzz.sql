{% for num in range(6) %}
  {% if num % 2 == 0 %}
    {% set value = 'fizz' %}
  {% else %}
    {% set value = 'buzz' %}
  {% endif %}
  select {{ num }} as number,  '{{ value }}' as output {% if not loop.last %} union all {% endif %}
{% endfor %}