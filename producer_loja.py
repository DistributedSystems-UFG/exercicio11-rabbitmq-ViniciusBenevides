# ---------------------------------------------------------------------------
# producer_loja.py  –  (1) Loja virtual (produtor de pedidos)
#
# Simula clientes realizando compras na loja online. Gera pedidos aleatórios
# com produto, quantidade, cliente e valor, publicando-os na fila
# QUEUE_PEDIDOS_NOVOS para que o consumer_estoque.py os processe.
#
# Uso:
#   python3 producer_loja.py
# ---------------------------------------------------------------------------

import rabbitpy
import json
import time
import random
import uuid
from datetime import datetime, timezone
from const import RABBITMQ_URL, EXCHANGE_NAME, QUEUE_PEDIDOS_NOVOS

PRODUTOS = {
    'NOTEBOOK-PRO': {'preco': 3499.90, 'peso_kg': 1.8},
    'SMARTPHONE-X': {'preco': 1899.00, 'peso_kg': 0.2},
    'TABLET-PLUS':  {'preco': 1299.00, 'peso_kg': 0.5},
    'FONE-BT':      {'preco':  299.90, 'peso_kg': 0.3},
    'CAMERA-4K':    {'preco': 2199.00, 'peso_kg': 0.6},
}

CLIENTES = [
    {'id': 'CLI-001', 'nome': 'Ana Silva',    'email': 'ana@email.com'},
    {'id': 'CLI-002', 'nome': 'Bruno Costa',  'email': 'bruno@email.com'},
    {'id': 'CLI-003', 'nome': 'Carla Mendes', 'email': 'carla@email.com'},
    {'id': 'CLI-004', 'nome': 'Diego Lima',   'email': 'diego@email.com'},
    {'id': 'CLI-005', 'nome': 'Eva Rocha',    'email': 'eva@email.com'},
]


def gerar_pedido():
    produto_id = random.choice(list(PRODUTOS.keys()))
    produto    = PRODUTOS[produto_id]
    quantidade = random.randint(1, 3)
    cliente    = random.choice(CLIENTES)
    return {
        'pedido_id':    str(uuid.uuid4())[:8].upper(),
        'cliente':      cliente,
        'produto_id':   produto_id,
        'quantidade':   quantidade,
        'valor_total':  round(produto['preco'] * quantidade, 2),
        'peso_total_kg': round(produto['peso_kg'] * quantidade, 2),
        'timestamp':    datetime.now(timezone.utc).isoformat(),
    }


def main():
    with rabbitpy.Connection(RABBITMQ_URL) as conn:
        with conn.channel() as channel:
            exchange = rabbitpy.Exchange(channel, EXCHANGE_NAME, exchange_type='direct')
            exchange.declare()

            queue = rabbitpy.Queue(channel, QUEUE_PEDIDOS_NOVOS, durable=True, auto_delete=False)
            queue.declare()
            queue.bind(exchange, QUEUE_PEDIDOS_NOVOS)

            print(f'[LOJA] Publicando pedidos na fila "{QUEUE_PEDIDOS_NOVOS}"')
            print(f'[LOJA] Catálogo: {list(PRODUTOS.keys())}\n')

            while True:
                pedido = gerar_pedido()
                body   = json.dumps(pedido, ensure_ascii=False)
                msg    = rabbitpy.Message(
                    channel, body,
                    properties={'content_type': 'application/json'},
                )
                msg.publish(exchange, QUEUE_PEDIDOS_NOVOS)

                print(
                    f'[LOJA] Pedido {pedido["pedido_id"]} enviado: '
                    f'{pedido["quantidade"]}x {pedido["produto_id"]} '
                    f'(R$ {pedido["valor_total"]:.2f}) '
                    f'– Cliente: {pedido["cliente"]["nome"]}'
                )

                time.sleep(random.uniform(2.0, 5.0))


if __name__ == '__main__':
    main()
