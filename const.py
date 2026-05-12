RABBITMQ_ADDR = '32.195.37.234'
RABBITMQ_URL  = f'amqp://myuser:abc123@{RABBITMQ_ADDR}:5672/my_vhost'

EXCHANGE_NAME = 'loja'

QUEUE_PEDIDOS_NOVOS     = 'pedidos.novos'
QUEUE_PAGAMENTOS        = 'pagamentos.pendentes'
QUEUE_PEDIDOS_APROVADOS = 'pedidos.aprovados'
QUEUE_PEDIDOS_ENVIADOS  = 'pedidos.enviados'
