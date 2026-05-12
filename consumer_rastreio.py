# ---------------------------------------------------------------------------
# consumer_rastreio.py  –  (6) Monitoramento de rastreio e entrega
#
# Consome pedidos despachados de QUEUE_PEDIDOS_ENVIADOS (publicados por
# consumer_logistica.py). Para cada envio:
#   • Registra o código de rastreio e o cliente no histórico em memória.
#   • Simula o próximo evento de status na sequência de entrega.
#   • Exibe o estado atual do ciclo de vida do pacote e o tempo estimado
#     até o próximo evento.
#   • Imprime um painel de status com todos os envios ativos ao final de
#     cada processamento.
#
# Uso:
#   python3 consumer_rastreio.py
# ---------------------------------------------------------------------------

import rabbitpy
import json
import random
import time
from datetime import datetime, timezone
from const import RABBITMQ_URL, EXCHANGE_NAME, QUEUE_PEDIDOS_ENVIADOS

STATUS_CICLO = [
    'COLETADO',
    'EM_TRANSITO',
    'CHEGOU_CENTRO_DISTRIBUICAO',
    'SAIU_PARA_ENTREGA',
    'ENTREGUE',
]

historico = {}   # codigo_rastreio → dict com status e dados do pedido


def proximo_status(atual):
    try:
        idx = STATUS_CICLO.index(atual)
        return STATUS_CICLO[idx + 1] if idx + 1 < len(STATUS_CICLO) else None
    except ValueError:
        return STATUS_CICLO[1]


def processar_rastreio(pedido):
    pedido_id = pedido['pedido_id']
    envio     = pedido.get('envio', {})
    rastreio  = envio.get('codigo_rastreio', 'N/A')
    transp    = envio.get('transportadora', 'N/A')
    cliente   = pedido.get('cliente', {})
    status    = envio.get('status', 'COLETADO')

    print(f'\n[RASTREIO] Pedido {pedido_id} – {cliente.get("nome", "?")} '
          f'<{cliente.get("email", "?")}>',
          )
    print(f'[RASTREIO]   Código: {rastreio}  |  Transportadora: {transp}  '
          f'|  Prazo: {envio.get("prazo_dias", "?")} dias')

    time.sleep(random.uniform(0.5, 1.0))   # simula polling da API da transportadora

    proximo = proximo_status(status)

    if proximo:
        h_estimado = random.randint(1, 48)
        print(f'[RASTREIO] ✓ Status atual   : {status}')
        print(f'[RASTREIO]   Próximo evento  : {proximo} (estimado em ~{h_estimado} h)')
    else:
        print(f'[RASTREIO] ✓ Pedido {pedido_id} ENTREGUE com sucesso ao cliente!')

    historico[rastreio] = {
        'pedido_id':     pedido_id,
        'cliente':       cliente.get('nome'),
        'email':         cliente.get('email'),
        'transportadora': transp,
        'status':        status,
        'proximo':       proximo,
        'atualizado_em': datetime.now(timezone.utc).isoformat(),
    }

    print(f'\n[RASTREIO] === Painel de envios ativos ({len(historico)}) ===')
    for cod, info in historico.items():
        indicador = '✓' if info['status'] == 'ENTREGUE' else '→'
        print(f'[RASTREIO]   {indicador} {cod} | {info["transportadora"]:15s} | '
              f'{info["status"]:30s} | {info["cliente"]}')


def main():
    with rabbitpy.Connection(RABBITMQ_URL) as conn:
        with conn.channel() as channel:
            exchange = rabbitpy.Exchange(channel, EXCHANGE_NAME, exchange_type='direct')
            exchange.declare()

            queue = rabbitpy.Queue(
                channel, QUEUE_PEDIDOS_ENVIADOS, durable=True, auto_delete=False,
            )
            queue.declare()
            queue.bind(exchange, QUEUE_PEDIDOS_ENVIADOS)

            print(f'[RASTREIO] Aguardando pedidos despachados em "{QUEUE_PEDIDOS_ENVIADOS}"\n')

            for message in queue:
                try:
                    pedido = json.loads(message.body.decode('utf-8'))
                    processar_rastreio(pedido)
                    message.ack()
                except Exception as exc:
                    print(f'[RASTREIO] ERRO: {exc}')
                    message.nack()


if __name__ == '__main__':
    main()
