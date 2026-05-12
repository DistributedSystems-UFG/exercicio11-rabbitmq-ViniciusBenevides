# ---------------------------------------------------------------------------
# consumer_logistica.py  –  (5) Central de logística
#
# Consome pedidos aprovados de QUEUE_PEDIDOS_APROVADOS (publicados por
# consumer_estoque.py). Para cada pedido:
#   • Seleciona a transportadora com base no peso e calcula o frete.
#   • Gera um código de rastreio no padrão da transportadora escolhida.
#   • Simula a emissão da nota fiscal e o despacho do pacote.
#   • Publica o pedido atualizado em QUEUE_PEDIDOS_ENVIADOS para que
#     consumer_rastreio.py registre e monitore a entrega.
#
# Uso:
#   python3 consumer_logistica.py
# ---------------------------------------------------------------------------

import rabbitpy
import json
import random
import string
import time
from datetime import datetime, timezone
from const import (
    RABBITMQ_URL, EXCHANGE_NAME,
    QUEUE_PEDIDOS_APROVADOS, QUEUE_PEDIDOS_ENVIADOS,
)

TRANSPORTADORAS = {
    'CORREIOS':      {'base': 8.00,  'por_kg': 2.50, 'prazo_min': 5,  'prazo_max': 15},
    'JADLOG':        {'base': 12.00, 'por_kg': 3.00, 'prazo_min': 3,  'prazo_max': 8},
    'SEQUOIA':       {'base': 15.00, 'por_kg': 3.50, 'prazo_min': 2,  'prazo_max': 6},
    'TOTAL-EXPRESS': {'base': 10.00, 'por_kg': 2.80, 'prazo_min': 4,  'prazo_max': 10},
}

nf_contador = 1000   # número sequencial de nota fiscal


def escolher_transportadora(peso_kg):
    # Pedidos pesados evitam Sequoia; acima de 2 kg preferem Jadlog ou Correios
    if peso_kg > 2.0:
        return random.choice(['CORREIOS', 'JADLOG', 'TOTAL-EXPRESS'])
    return random.choice(list(TRANSPORTADORAS.keys()))


def gerar_rastreio(nome):
    letras  = ''.join(random.choices(string.ascii_uppercase, k=2))
    numeros = ''.join(random.choices(string.digits, k=9))
    if nome == 'CORREIOS':
        return f'{letras}{numeros}BR'
    prefixo = nome[:3]
    return f'{prefixo}-{numeros}'


def calcular_frete(nome, peso_kg):
    cfg = TRANSPORTADORAS[nome]
    return round(cfg['base'] + cfg['por_kg'] * peso_kg, 2)


def emitir_nota_fiscal():
    global nf_contador
    nf_contador += 1
    return f'NF-{nf_contador:06d}'


def processar_logistica(channel, exchange, pedido):
    pedido_id = pedido['pedido_id']
    peso_kg   = pedido.get('peso_total_kg', 1.0)

    print(f'\n[LOGÍSTICA] Pedido {pedido_id}: {pedido["produto_id"]} '
          f'({peso_kg} kg) – {pedido["cliente"]["nome"]}')

    time.sleep(random.uniform(1.0, 2.5))   # simula separação e embalagem

    transportadora = escolher_transportadora(peso_kg)
    cfg            = TRANSPORTADORAS[transportadora]
    rastreio       = gerar_rastreio(transportadora)
    frete          = calcular_frete(transportadora, peso_kg)
    prazo          = random.randint(cfg['prazo_min'], cfg['prazo_max'])
    nf             = emitir_nota_fiscal()

    pedido['envio'] = {
        'transportadora':  transportadora,
        'codigo_rastreio': rastreio,
        'frete_r$':        frete,
        'prazo_dias':      prazo,
        'nota_fiscal':     nf,
        'status':          'COLETADO',
        'data_despacho':   datetime.now(timezone.utc).isoformat(),
    }

    fila_dest = rabbitpy.Queue(
        channel, QUEUE_PEDIDOS_ENVIADOS, durable=True, auto_delete=False,
    )
    fila_dest.declare()
    fila_dest.bind(exchange, QUEUE_PEDIDOS_ENVIADOS)

    body = json.dumps(pedido, ensure_ascii=False)
    msg  = rabbitpy.Message(
        channel, body,
        properties={'content_type': 'application/json'},
    )
    msg.publish(exchange, QUEUE_PEDIDOS_ENVIADOS)

    print(f'[LOGÍSTICA] ✓ Despachado via {transportadora}')
    print(f'[LOGÍSTICA]   Rastreio: {rastreio}  |  '
          f'Frete: R$ {frete:.2f}  |  Prazo: {prazo} dias  |  NF: {nf}')
    print(f'[LOGÍSTICA] → Pedido {pedido_id} encaminhado para monitoramento de rastreio')


def main():
    with rabbitpy.Connection(RABBITMQ_URL) as conn:
        with conn.channel() as channel:
            exchange = rabbitpy.Exchange(channel, EXCHANGE_NAME, exchange_type='direct')
            exchange.declare()

            queue = rabbitpy.Queue(
                channel, QUEUE_PEDIDOS_APROVADOS, durable=True, auto_delete=False,
            )
            queue.declare()
            queue.bind(exchange, QUEUE_PEDIDOS_APROVADOS)

            print(f'[LOGÍSTICA] Aguardando pedidos aprovados em "{QUEUE_PEDIDOS_APROVADOS}"\n')

            for message in queue:
                try:
                    pedido = json.loads(message.body.decode('utf-8'))
                    processar_logistica(channel, exchange, pedido)
                    message.ack()
                except Exception as exc:
                    print(f'[LOGÍSTICA] ERRO: {exc}')
                    message.nack()


if __name__ == '__main__':
    main()
