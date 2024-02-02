{% docs order_status %}
The last known status of the order. Can be any of the following:

| Status         | Description                                    |
|----------------|------------------------------------------------|
| placed         | Order has been successfully created.           |
| completed      | Order has been successfully completed.         |
| shipped        | Order has been successfully shipped.           |
| return_pending | Order is currently being processed for return. |
| returned       | Order is has been returned.                    |
{% enddocs %}