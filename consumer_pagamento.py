# ---------------------------------------------------------------------------
# consumer_pagamento.py  –  (4) Processador financeiro
#
# Consome confirmações de QUEUE_PAGAMENTOS enviadas pelo gateway de pagamento
# (producer_financeiro.py). Para cada transação:
#   • Aprovada : calcula taxa da operadora, registra o valor líquido e simula
#                o crédito na conta da loja.
#   • Rejeitada: registra o motivo e simula notificação ao cliente para
#                nova tentativa.
#
# Mantém um registro em memória de todas as transações processadas e exibe
# um resumo financeiro acumulado a cada mensagem recebida.
#
# Uso:
#   python3 consumer_pagamento.py
# ---------------------------------------------------------------------------

import rabbitpy
import json
import time
import random
from const import RABBITMQ_URL, EXCHANGE_NAME, QUEUE_PAGAMENTOS

TAXA_POR_METODO = {
    'CARTAO_CREDITO': 0.030,
    'CARTAO_DEBITO':  0.015,
    'PIX':            0.000,
    'BOLETO':         0.020,
}

registros      = {}
total_aprovado = 0.0
total_rejeitado = 0


def processar_pagamento(pagamento):
    global total_aprovado, total_rejeitado

    pid    = pagamento['pagamento_id']
    metodo = pagamento['metodo']
    valor  = pagamento['valor']

    print(f'\n[PAGAMENTO] Transação {pid}: {metodo} R$ {valor:.2f}')
    time.sleep(random.uniform(0.3, 1.2))   # simula chamada à API do banco

    if pagamento['aprovado']:
        taxa   = round(valor * TAXA_POR_METODO.get(metodo, 0.025), 2)
        liquido = round(valor - taxa, 2)
        total_aprovado += liquido

        registros[pid] = {
            'status':        'CREDITADO',
            'valor_bruto':   valor,
            'taxa':          taxa,
            'valor_liquido': liquido,
        }
        print(f'[PAGAMENTO] ✓ Creditado: bruto=R$ {valor:.2f}  '
              f'taxa=R$ {taxa:.2f}  líquido=R$ {liquido:.2f}')
    else:
        total_rejeitado += 1
        registros[pid] = {
            'status': 'REJEITADO',
            'motivo': pagamento['motivo_rejeicao'],
        }
        print(f'[PAGAMENTO] ✗ Rejeitado: {pagamento["motivo_rejeicao"]} '
              f'– cliente será notificado para nova tentativa')

    print(f'[PAGAMENTO] Resumo acumulado: '
          f'recebido=R$ {total_aprovado:.2f}  rejeitados={total_rejeitado}  '
          f'transações={len(registros)}')


def main():
    with rabbitpy.Connection(RABBITMQ_URL) as conn:
        with conn.channel() as channel:
            exchange = rabbitpy.Exchange(channel, EXCHANGE_NAME, exchange_type='direct')
            exchange.declare()

            queue = rabbitpy.Queue(
                channel, QUEUE_PAGAMENTOS, durable=True, auto_delete=False,
            )
            queue.declare()
            queue.bind(exchange, QUEUE_PAGAMENTOS)

            print(f'[PAGAMENTO] Aguardando transações em "{QUEUE_PAGAMENTOS}"\n')

            for message in queue:
                try:
                    pagamento = json.loads(message.body.decode('utf-8'))
                    processar_pagamento(pagamento)
                    message.ack()
                except Exception as exc:
                    print(f'[PAGAMENTO] ERRO: {exc}')
                    message.nack()


if __name__ == '__main__':
    main()
