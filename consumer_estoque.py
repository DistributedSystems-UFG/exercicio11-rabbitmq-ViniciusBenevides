# ---------------------------------------------------------------------------
# consumer_estoque.py  –  (3) Gerenciador de estoque
#
# Consome pedidos novos de QUEUE_PEDIDOS_NOVOS. Para cada pedido:
#   • Verifica se o produto tem estoque suficiente (inventário em memória).
#   • Se sim: reserva as unidades e encaminha o pedido para
#             QUEUE_PEDIDOS_APROVADOS (consumido por consumer_logistica.py).
#   • Se não: registra a recusa por falta de estoque (sem reenvio).
#
# Este componente é simultaneamente consumidor e produtor, formando um
# pipeline de mensagens típico de sistemas de integração.
#
# Uso:
#   python3 consumer_estoque.py
# ---------------------------------------------------------------------------

import rabbitpy
import json
import time
import random
from const import (
    RABBITMQ_URL, EXCHANGE_NAME,
    QUEUE_PEDIDOS_NOVOS, QUEUE_PEDIDOS_APROVADOS,
)

ESTOQUE = {
    'NOTEBOOK-PRO': 15,
    'SMARTPHONE-X': 42,
    'TABLET-PLUS':   8,
    'FONE-BT':      67,
    'CAMERA-4K':     3,
}


def processar_pedido(channel, exchange, pedido):
    produto_id = pedido['produto_id']
    quantidade = pedido['quantidade']

    print(f'\n[ESTOQUE] Pedido {pedido["pedido_id"]}: '
          f'{quantidade}x {produto_id} – cliente {pedido["cliente"]["nome"]}')

    time.sleep(random.uniform(0.5, 1.5))   # simula consulta ao banco de dados

    disponivel = ESTOQUE.get(produto_id, 0)

    if disponivel >= quantidade:
        ESTOQUE[produto_id] -= quantidade
        pedido['status_estoque'] = 'RESERVADO'
        pedido['estoque_restante'] = ESTOQUE[produto_id]

        fila_dest = rabbitpy.Queue(
            channel, QUEUE_PEDIDOS_APROVADOS, durable=True, auto_delete=False,
        )
        fila_dest.declare()
        fila_dest.bind(exchange, QUEUE_PEDIDOS_APROVADOS)

        body = json.dumps(pedido, ensure_ascii=False)
        msg  = rabbitpy.Message(
            channel, body,
            properties={'content_type': 'application/json'},
        )
        msg.publish(exchange, QUEUE_PEDIDOS_APROVADOS)

        print(f'[ESTOQUE] ✓ Reserva OK – restam {ESTOQUE[produto_id]} unidades de {produto_id}')
        print(f'[ESTOQUE] → Pedido {pedido["pedido_id"]} encaminhado para logística')
    else:
        print(f'[ESTOQUE] ✗ Sem estoque: solicitado={quantidade}, '
              f'disponível={disponivel} ({produto_id}) – pedido recusado')


def main():
    with rabbitpy.Connection(RABBITMQ_URL) as conn:
        with conn.channel() as channel:
            exchange = rabbitpy.Exchange(channel, EXCHANGE_NAME, exchange_type='direct')
            exchange.declare()

            queue = rabbitpy.Queue(
                channel, QUEUE_PEDIDOS_NOVOS, durable=True, auto_delete=False,
            )
            queue.declare()
            queue.bind(exchange, QUEUE_PEDIDOS_NOVOS)

            print(f'[ESTOQUE] Aguardando pedidos em "{QUEUE_PEDIDOS_NOVOS}"')
            print(f'[ESTOQUE] Inventário inicial: {ESTOQUE}\n')

            for message in queue:
                try:
                    pedido = json.loads(message.body.decode('utf-8'))
                    processar_pedido(channel, exchange, pedido)
                    message.ack()
                except Exception as exc:
                    print(f'[ESTOQUE] ERRO: {exc}')
                    message.nack()


if __name__ == '__main__':
    main()
