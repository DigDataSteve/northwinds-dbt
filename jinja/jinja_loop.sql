{% for i in range(3) %}
  select {{ i }} as number 
  {% if not loop.last %} union all {% endif %}
{% endfor %}