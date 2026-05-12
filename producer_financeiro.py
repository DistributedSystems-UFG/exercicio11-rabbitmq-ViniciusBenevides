# ---------------------------------------------------------------------------
# producer_financeiro.py  –  (2) Gateway de pagamento (produtor de confirmações)
#
# Simula o retorno assíncrono de um gateway de pagamento externo. Gera
# confirmações (aprovadas ou rejeitadas) com método de pagamento e valor,
# publicando-as em QUEUE_PAGAMENTOS para que consumer_pagamento.py processe.
#
# Em produção, este componente seria um webhook acionado pelo provedor de
# pagamento (ex.: Stripe, PagSeguro) ao concluir a autorização.
#
# Uso:
#   python3 producer_financeiro.py
# ---------------------------------------------------------------------------

import rabbitpy
import json
import time
import random
import uuid
from datetime import datetime, timezone
from const import RABBITMQ_URL, EXCHANGE_NAME, QUEUE_PAGAMENTOS

METODOS = ['CARTAO_CREDITO', 'CARTAO_DEBITO', 'PIX', 'BOLETO']

MOTIVOS_REJEICAO = [
    'SALDO_INSUFICIENTE',
    'CARTAO_EXPIRADO',
    'LIMITE_EXCEDIDO',
    'SUSPEITA_FRAUDE',
]


def gerar_confirmacao():
    aprovado = random.random() > 0.15   # 85 % de aprovação
    return {
        'pagamento_id':    str(uuid.uuid4())[:8].upper(),
        'pedido_ref':      str(uuid.uuid4())[:8].upper(),   # referência ao pedido
        'metodo':          random.choice(METODOS),
        'valor':           round(random.uniform(100.0, 5000.0), 2),
        'aprovado':        aprovado,
        'motivo_rejeicao': None if aprovado else random.choice(MOTIVOS_REJEICAO),
        'timestamp':       datetime.now(timezone.utc).isoformat(),
    }


def main():
    with rabbitpy.Connection(RABBITMQ_URL) as conn:
        with conn.channel() as channel:
            exchange = rabbitpy.Exchange(channel, EXCHANGE_NAME, exchange_type='direct')
            exchange.declare()

            queue = rabbitpy.Queue(channel, QUEUE_PAGAMENTOS, durable=True, auto_delete=False)
            queue.declare()
            queue.bind(exchange, QUEUE_PAGAMENTOS)

            print(f'[FINANCEIRO] Publicando confirmações na fila "{QUEUE_PAGAMENTOS}"\n')

            while True:
                conf = gerar_confirmacao()
                body = json.dumps(conf, ensure_ascii=False)
                msg  = rabbitpy.Message(
                    channel, body,
                    properties={'content_type': 'application/json'},
                )
                msg.publish(exchange, QUEUE_PAGAMENTOS)

                status = ('APROVADO'
                          if conf['aprovado']
                          else f'REJEITADO ({conf["motivo_rejeicao"]})')
                print(
                    f'[FINANCEIRO] Pagamento {conf["pagamento_id"]}: '
                    f'{conf["metodo"]} R$ {conf["valor"]:.2f} → {status}'
                )

                time.sleep(random.uniform(3.0, 7.0))


if __name__ == '__main__':
    main()
