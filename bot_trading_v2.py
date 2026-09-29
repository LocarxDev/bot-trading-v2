import pandas as pd
import time
import logging
import requests
import threading
import json
import os
import sys
from flask import Flask, jsonify
from flask_cors import CORS
from datetime import datetime, time as dtime, timedelta

# ─────────────────────────────────────────────
# LOGGING — compatível com Railway/cloud
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIGURACOES
# ─────────────────────────────────────────────
API_KEY    = 'JwuT5Kj9rtYzrfKkNL7MMIjhwYBTg1hX5mIKa8B91lERO5QZnJnZbWXy2PIKifmD'
API_SECRET = 'qN1qwMR3YbHkKAmjfRxzbx6Hpa8JRUL0CdvTvQgcvcXpds5UKaJ8FC5BeIcIm34L'

TELEGRAM_TOKEN = '8408182756:AAFcuwOazK89UAjQZERaqFiUQKjPKV1z0nQ'
TELEGRAM_CHATS = ['1998336872']  # Apenas Silas

# Pares monitorados — v2
PARES = [
    'BTC/USDT', 'ETH/USDT', 'XRP/USDT', 'ADA/USDT',
    'SOL/USDT', 'AVAX/USDT', 'DOGE/USDT', 'LINK/USDT',
]
# BNB removido — nao disponivel no Kraken (fonte de dados atual)
# Substituido por AVAX/USDT

TIMEFRAME              = '1m'
CAPITAL_BASE           = 10.0       # minimo por ordem
RISCO_POR_TRADE        = 0.12       # 12% do capital por trade (~$12 de $100)
MAX_POSICOES           = 6          # maximo de posicoes simultaneas
STOP_LOSS              = 0.015      # 1.5%
TRAILING_STOP          = 0.008      # 0.8% padrao
LUCRO_MINIMO_SAIDA     = 0.008      # 0.8% minimo pra sair com lucro
LUCRO_MINIMO_VENDA     = 0.008      # 0.8% pra venda por sinal MA
LIMITE_PERDA_DIARIA    = 0.10       # 10% do capital
QUEDA_RESERVA          = 0.10       # 10% de queda pra usar reserva
STOPS_CONSECUTIVOS_MAX = 3          # pausa 1h apos 3 stops seguidos
RSI_PERIODO            = 14
RSI_SOBRECOMPRADO      = 75
RSI_SOBREVENDIDO       = 25
SCORE_MINIMO           = 3
SANDBOX_MODE           = True        # True = simulacao, sem ordens reais
CAPITAL_SIMULADO       = 100.0       # $100 simulados
RESERVA_PCT            = 0.20        # 20% do capital em reserva

TAXA_OPERACAO = 0.002   # 0.2% por operacao (compra + venda = 0.4% round trip)

# Usa /data se existir (Railway Volume) para sobreviver redeployments
import pathlib
_DATA_DIR = '/data' if pathlib.Path('/data').exists() else '.'
ARQUIVO_ESTADO    = f'{_DATA_DIR}/estado_bot_v2.json'
ARQUIVO_HISTORICO = f'{_DATA_DIR}/historico_trades_v2.json'
ARQUIVO_RELATORIO = f'{_DATA_DIR}/relatorio_ia.json'

# ─────────────────────────────────────────────
# KRAKEN REST API PUBLICA — sem autenticacao, sem bloqueio de datacenter
# Binance e Bybit bloqueiam IPs de cloud (Railway/AWS/GCP).
# Kraken tem API publica totalmente aberta para qualquer IP.
# Apenas dados de mercado (OHLCV + ticker). Ordens 100% simuladas.
# ─────────────────────────────────────────────
KRAKEN_BASE = 'https://api.kraken.com'

# Kraken usa "XBT" para Bitcoin internamente
_KRAKEN_PAIR = {
    'BTC/USDT':  'XBTUSDT',
    'ETH/USDT':  'ETHUSDT',
    'XRP/USDT':  'XRPUSDT',
    'ADA/USDT':  'ADAUSDT',
    'SOL/USDT':  'SOLUSDT',
    'AVAX/USDT': 'AVAXUSDT',
    'DOGE/USDT': 'DOGEUSDT',
    'LINK/USDT': 'LINKUSDT',
}

# Intervalos em minutos
_INTERVAL_MAP = {
    '1m': 1, '5m': 5, '15m': 15, '30m': 30,
    '1h': 60, '4h': 240, '1d': 1440,
}

def _kraken_pair(par: str) -> str:
    return _KRAKEN_PAIR.get(par, par.replace('/', ''))

def kraken_get(path: str, params: dict = None, retries: int = 3):
    url = KRAKEN_BASE + path
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()
            if data.get('error'):
                raise ValueError(f"Kraken erro: {data['error']}")
            return data['result']
        except Exception as e:
            if i == retries - 1:
                raise
            time.sleep(1)

def fetch_ohlcv(par: str, interval: str = '1m', limit: int = 288) -> pd.DataFrame:
    """Busca candles OHLCV do Kraken (endpoint publico, sem bloqueio)."""
    iv = _INTERVAL_MAP.get(interval, 1)
    pair = _kraken_pair(par)
    result = kraken_get('/0/public/OHLC', {'pair': pair, 'interval': iv})
    data_key = [k for k in result.keys() if k != 'last'][0]
    rows = result[data_key]
    # Kraken: [time, open, high, low, close, vwap, volume, count]
    rows = rows[-limit:]
    df = pd.DataFrame(rows, columns=['time','open','high','low','close','vwap','volume','count'])
    df = df[['time','open','high','low','close','volume']].copy()
    for col in ['open','high','low','close','volume']:
        df[col] = df[col].astype(float)
    df['time'] = pd.to_datetime(df['time'].astype('int64'), unit='s')
    return df

def fetch_ticker(par: str) -> dict:
    """Busca preco atual do par (endpoint publico Kraken)."""
    pair = _kraken_pair(par)
    result = kraken_get('/0/public/Ticker', {'pair': pair})
    data_key = list(result.keys())[0]
    last_price = float(result[data_key]['c'][0])
    return {'last': last_price}

# ─────────────────────────────────────────────
# ESTADO GLOBAL
# ─────────────────────────────────────────────
estado = {
    'posicoes': {},
    'capital_inicial':    None,
    'capital_atual':      CAPITAL_SIMULADO,
    'saldo_usdt':         0.0,
    'saldo_total':        0.0,
    'capital_reserva':    CAPITAL_SIMULADO * RESERVA_PCT,
    'reserva_usada':      False,
    'perdas_dia':         0.0,
    'lucros_dia':         0.0,
    'transacoes_dia':     0,
    'wins_dia':           0,
    'losses_dia':         0,
    'stops_consecutivos': 0,
    'pausado_ate':        None,
    'stats_pares':        {},
    'stats_horarios':     {},
    'scores':             {},
    'pares_detalhes':     {},
    'sinal_atual':        'NEUTRO',
    'rsi_atual':          0.0,
    'preco_atual':        0.0,
    'ultimo_reset':       datetime.now().date().isoformat(),
    'relatorio_enviado':  False,
    'status':             'iniciando',
    'ultimo_update':      datetime.now().isoformat(),
    'versao':             'v2',
    # IA / Auto-aprendizado
    'erros_capturados':   [],
    'relatorio_ia':       None,
    'aprendizados':       [],
}

historico    = []
feed_eventos = []

def add_evento(tipo, msg):
    feed_eventos.append({
        'hora': datetime.now().strftime('%H:%M:%S'),
        'tipo': tipo,
        'msg':  msg,
    })
    if len(feed_eventos) > 200:
        feed_eventos.pop(0)

# ─────────────────────────────────────────────
# APRENDIZADO — Score dinamico por par
# ─────────────────────────────────────────────
def atualizar_stats(par, resultado, hora):
    if par not in estado['stats_pares']:
        estado['stats_pares'][par] = {'wins': 0, 'losses': 0, 'peso': 1.0, 'pnl_total': 0.0}

    if resultado == 'win':
        estado['stats_pares'][par]['wins'] += 1
    else:
        estado['stats_pares'][par]['losses'] += 1

    wins   = estado['stats_pares'][par]['wins']
    losses = estado['stats_pares'][par]['losses']
    total  = wins + losses
    if total >= 5:
        winrate = wins / total
        estado['stats_pares'][par]['peso'] = round(0.5 + winrate, 2)

    hora_str = hora[:2]
    if par not in estado['stats_horarios']:
        estado['stats_horarios'][par] = {}
    if hora_str not in estado['stats_horarios'][par]:
        estado['stats_horarios'][par][hora_str] = {'wins': 0, 'losses': 0}

    if resultado == 'win':
        estado['stats_horarios'][par][hora_str]['wins'] += 1
    else:
        estado['stats_horarios'][par][hora_str]['losses'] += 1

def get_peso_par(par):
    if par not in estado['stats_pares']:
        return 1.0
    total = estado['stats_pares'][par]['wins'] + estado['stats_pares'][par]['losses']
    if total < 5:
        return 1.0
    return estado['stats_pares'][par]['peso']

def horario_favoravel(par):
    hora_str = datetime.now().strftime('%H')
    if par not in estado['stats_horarios']:
        return True, 'sem dados'
    if hora_str not in estado['stats_horarios'][par]:
        return True, 'hora nova'
    h     = estado['stats_horarios'][par][hora_str]
    total = h['wins'] + h['losses']
    if total < 3:
        return True, 'poucos dados'
    winrate = h['wins'] / total
    if winrate < 0.35:
        return False, f"Win rate {winrate*100:.0f}% nessa hora"
    return True, f"Win rate {winrate*100:.0f}% nessa hora"

def registrar_aprendizado(tipo, descricao, par=None):
    """Registra um aprendizado automatico do bot"""
    aprendizado = {
        'data': datetime.now().strftime('%d/%m %H:%M'),
        'tipo': tipo,
        'par': par or 'GERAL',
        'descricao': descricao,
    }
    estado['aprendizados'].append(aprendizado)
    if len(estado['aprendizados']) > 50:
        estado['aprendizados'].pop(0)
    log.info(f"[APRENDIZADO] {tipo} | {par or 'GERAL'} | {descricao}")

# ─────────────────────────────────────────────
# AGENTE IA — Analise diaria e relatorio
# ─────────────────────────────────────────────
def gerar_relatorio_ia():
    """Gera relatorio de performance com analise inteligente"""
    try:
        total  = estado['wins_dia'] + estado['losses_dia']
        wr     = (estado['wins_dia'] / total * 100) if total > 0 else 0
        result = estado['lucros_dia'] - abs(estado['perdas_dia'])

        # Analisa o historico dos ultimos 50 trades
        ultimos = historico[-50:] if len(historico) >= 50 else historico
        stats_por_par = {}
        for t in ultimos:
            p = t.get('par', '?')
            if p not in stats_por_par:
                stats_por_par[p] = {'wins': 0, 'losses': 0, 'pnl': 0.0}
            if t.get('resultado') == 'win':
                stats_por_par[p]['wins'] += 1
            else:
                stats_por_par[p]['losses'] += 1
            stats_por_par[p]['pnl'] += t.get('pnl_usd', 0)

        # Melhor e pior par
        melhor_par = max(stats_por_par.items(), key=lambda x: x[1]['pnl'], default=(None, {}))
        pior_par   = min(stats_por_par.items(), key=lambda x: x[1]['pnl'], default=(None, {}))

        # Detecta padroes automaticamente
        padroes = []
        for par, s in stats_por_par.items():
            total_par = s['wins'] + s['losses']
            if total_par >= 3:
                wr_par = s['wins'] / total_par
                if wr_par >= 0.70:
                    padroes.append(f"{par} excelente ({wr_par*100:.0f}% win rate)")
                    registrar_aprendizado('MELHOR_PAR', f"Win rate {wr_par*100:.0f}% nos ultimos {total_par} trades", par)
                elif wr_par <= 0.30:
                    padroes.append(f"{par} fraco ({wr_par*100:.0f}% win rate) — reduzindo peso")
                    registrar_aprendizado('PIOR_PAR', f"Win rate baixo {wr_par*100:.0f}% — peso reduzido automaticamente", par)

        relatorio = {
            'data': datetime.now().strftime('%d/%m/%Y %H:%M'),
            'resultado_dia': round(result, 2),
            'lucros_dia': round(estado['lucros_dia'], 2),
            'perdas_dia': round(abs(estado['perdas_dia']), 2),
            'wins': estado['wins_dia'],
            'losses': estado['losses_dia'],
            'winrate': round(wr, 1),
            'transacoes': estado['transacoes_dia'],
            'capital_atual': round(estado['capital_atual'], 2),
            'capital_inicial': round(estado['capital_inicial'] or CAPITAL_SIMULADO, 2),
            'variacao_pct': round((estado['capital_atual'] / (estado['capital_inicial'] or CAPITAL_SIMULADO) - 1) * 100, 2),
            'stats_por_par': stats_por_par,
            'melhor_par': melhor_par[0],
            'pior_par': pior_par[0],
            'padroes_detectados': padroes,
            'aprendizados': estado['aprendizados'][-10:],
            'avaliacao': avaliar_desempenho(wr, result),
        }

        estado['relatorio_ia'] = relatorio

        try:
            with open(ARQUIVO_RELATORIO, 'w', encoding='utf-8') as f:
                json.dump(relatorio, f, indent=2, ensure_ascii=False)
        except Exception as e:
            log.warning(f"Erro ao salvar relatorio: {e}")

        return relatorio
    except Exception as e:
        log.error(f"Erro ao gerar relatorio IA: {e}")
        return {}

def avaliar_desempenho(winrate, resultado):
    """Avaliacao simples de desempenho"""
    if winrate >= 60 and resultado > 0:
        return "EXCELENTE - Estrategia funcionando bem, manter configuracoes"
    elif winrate >= 50 and resultado >= 0:
        return "BOM - Desempenho positivo, monitorar pares fracos"
    elif winrate >= 40:
        return "REGULAR - Revisar horarios e pares com mais losses"
    else:
        return "FRACO - Considerar pausar e revisar estrategia"

def enviar_relatorio_ia():
    """Envia relatorio IA pelo Telegram as 23:55"""
    agora = datetime.now().time()
    if dtime(23, 55) <= agora <= dtime(23, 59) and not estado['relatorio_enviado']:
        rel = gerar_relatorio_ia()
        if not rel:
            return

        padroes_txt = '\n'.join([f"  - {p}" for p in rel.get('padroes_detectados', [])]) or '  Nenhum padrao novo'

        msg = (
            f"RELATORIO IA - {rel.get('data', '')}\n\n"
            f"RESULTADO DO DIA: ${rel['resultado_dia']:+.2f}\n"
            f"Capital: ${rel['capital_atual']:.2f} ({rel['variacao_pct']:+.2f}% desde inicio)\n\n"
            f"Trades: {rel['wins']}W / {rel['losses']}L ({rel['winrate']:.1f}% win rate)\n"
            f"Lucros: +${rel['lucros_dia']:.2f} | Perdas: -${rel['perdas_dia']:.2f}\n\n"
            f"AVALIACAO: {rel['avaliacao']}\n\n"
            f"PADROES DETECTADOS:\n{padroes_txt}\n\n"
            f"Melhor par: {rel.get('melhor_par', '-')}\n"
            f"Pior par: {rel.get('pior_par', '-')}"
        )
        telegram(msg)
        estado['relatorio_enviado'] = True
        salvar_estado()
        log.info("Relatorio IA enviado via Telegram")

def capturar_erro(contexto, erro, par=None):
    """Captura e registra erros para auto-aprendizado"""
    erro_info = {
        'data': datetime.now().strftime('%d/%m %H:%M'),
        'contexto': contexto,
        'erro': str(erro),
        'par': par or 'GERAL',
    }
    estado['erros_capturados'].append(erro_info)
    if len(estado['erros_capturados']) > 30:
        estado['erros_capturados'].pop(0)

    # Auto-aprendizado: muitos erros no mesmo par = reduz peso
    if par:
        erros_par = [e for e in estado['erros_capturados'] if e['par'] == par]
        if len(erros_par) >= 3:
            if par in estado['stats_pares']:
                peso_atual = estado['stats_pares'][par].get('peso', 1.0)
                novo_peso  = max(0.5, peso_atual - 0.1)
                estado['stats_pares'][par]['peso'] = novo_peso
                registrar_aprendizado('ERRO_RECORRENTE', f"Muitos erros — peso reduzido para {novo_peso}", par)

# ─────────────────────────────────────────────
# PERSISTENCIA
# ─────────────────────────────────────────────
def salvar_estado():
    try:
        salvo = {k: v for k, v in estado.items() if k not in ('pares_detalhes',)}
        with open(ARQUIVO_ESTADO, 'w', encoding='utf-8') as f:
            json.dump(salvo, f, indent=2, default=str)
    except Exception as e:
        log.error(f"Erro ao salvar estado: {e}")

def carregar_estado():
    try:
        if os.path.exists(ARQUIVO_ESTADO):
            with open(ARQUIVO_ESTADO, 'r', encoding='utf-8') as f:
                salvo = json.load(f)
            campos = [
                'posicoes', 'capital_atual', 'capital_inicial', 'capital_reserva',
                'reserva_usada', 'transacoes_dia', 'wins_dia', 'losses_dia',
                'perdas_dia', 'lucros_dia', 'ultimo_reset', 'relatorio_enviado',
                'stats_pares', 'stats_horarios', 'stops_consecutivos', 'pausado_ate',
                'aprendizados', 'erros_capturados',
            ]
            for campo in campos:
                if campo in salvo:
                    estado[campo] = salvo[campo]
            log.info(f"Estado restaurado | {len(estado['posicoes'])} posicoes abertas")
    except Exception as e:
        log.error(f"Erro ao carregar estado: {e}")

def salvar_historico():
    try:
        with open(ARQUIVO_HISTORICO, 'w', encoding='utf-8') as f:
            json.dump(historico, f, indent=2, default=str)
    except Exception as e:
        log.error(f"Erro ao salvar historico: {e}")

def carregar_historico():
    global historico
    try:
        if os.path.exists(ARQUIVO_HISTORICO):
            with open(ARQUIVO_HISTORICO, 'r', encoding='utf-8') as f:
                historico = json.load(f)
            log.info(f"Historico restaurado: {len(historico)} trades")
            for trade in historico:
                par  = trade.get('par', '')
                res  = trade.get('resultado', '')
                hora = trade.get('data', '00:00')[-5:]
                if par and res:
                    atualizar_stats(par, res, hora)
            log.info(f"Stats reconstruidas: {len(estado['stats_pares'])} pares")
    except Exception as e:
        log.error(f"Erro ao carregar historico: {e}")

# ─────────────────────────────────────────────
# FLASK API
# ─────────────────────────────────────────────
app = Flask(__name__)
CORS(app, origins="*")

@app.after_request
def add_headers(response):
    response.headers['ngrok-skip-browser-warning'] = 'true'
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = '*'
    return response

@app.route('/status')
def get_status():
    total   = estado['wins_dia'] + estado['losses_dia']
    winrate = (estado['wins_dia'] / total * 100) if total > 0 else 0

    valor_posicoes = 0.0
    posicoes_info  = {}
    for par, pos in estado['posicoes'].items():
        preco_atual     = estado['pares_detalhes'].get(par, {}).get('preco', pos['preco_compra'])
        valor_investido = pos.get('valor_investido', pos['preco_compra'] * pos['quantidade'])
        receita_atual   = preco_atual * pos['quantidade'] * (1 - TAXA_OPERACAO)
        pnl_usdt        = receita_atual - valor_investido
        pnl_pct         = (pnl_usdt / valor_investido) * 100 if valor_investido else 0
        valor_posicoes += receita_atual
        posicoes_info[par] = {
            'preco_entrada': round(pos.get('preco_entrada', pos['preco_compra']), 6),
            'preco_compra':  round(pos['preco_compra'], 6),
            'preco_atual':   round(preco_atual, 6),
            'preco_pico':    round(pos.get('preco_pico', pos['preco_compra']), 6),
            'stop_loss':     round(pos.get('stop_loss', pos['preco_compra'] * (1-STOP_LOSS)), 6),
            'quantidade':    round(pos['quantidade'], 6),
            'valor_investido': round(valor_investido, 4),
            'pnl_pct':       round(pnl_pct, 2),
            'pnl_usdt':      round(pnl_usdt, 4),
            'abertura':      pos.get('abertura', pos.get('entrada', '')),
            'entrada':       pos.get('entrada', ''),
        }

    capital_disponivel = round(estado['capital_atual'], 2)
    capital_total      = round(estado['capital_atual'] + valor_posicoes, 2)
    capital_inicial    = round(estado['capital_inicial'] or CAPITAL_SIMULADO, 2)
    pnl_total          = round(capital_total - capital_inicial, 2)

    return jsonify({
        'versao':            'v2',
        'status':            estado['status'],
        'sandbox':           True,
        'capital_simulado':  CAPITAL_SIMULADO,
        'posicoes':          posicoes_info,
        'num_posicoes':      len(estado['posicoes']),
        'max_posicoes':      MAX_POSICOES,
        'pnl_total':         pnl_total,
        'pnl_total_usd':     pnl_total,
        'pares_detalhes':    estado['pares_detalhes'],
        'pares_monitorados': PARES,
        'scores':            estado['scores'],
        'capital_inicial':   capital_inicial,
        'capital_atual':     capital_total,        # total = disponivel + posicoes abertas
        'capital_disponivel': capital_disponivel,  # so o USDT em caixa
        'capital_reserva':   round(estado['capital_reserva'], 2),
        'reserva_usada':     int(estado['reserva_usada']),
        'lucros_dia':        round(estado['lucros_dia'], 2),
        'perdas_dia':        round(abs(estado['perdas_dia']), 2),
        'resultado_dia':     round(estado['lucros_dia'] - abs(estado['perdas_dia']), 2),
        'transacoes_dia':    estado['transacoes_dia'],
        'total_trades':      estado['transacoes_dia'],
        'wins_dia':          estado['wins_dia'],
        'losses_dia':        estado['losses_dia'],
        'trades_lucro':      estado['wins_dia'],
        'trades_stop':       estado['losses_dia'],
        'winrate':           round(winrate, 1),
        'sinal_atual':       estado['sinal_atual'],
        'rsi_atual':         round(float(estado['rsi_atual']), 1),
        'preco_atual':       estado['preco_atual'],
        'ultimo_update':     estado['ultimo_update'],
        'stats_pares':       estado['stats_pares'],
        'stops_consecutivos': estado['stops_consecutivos'],
        'pausado_ate':       estado['pausado_ate'],
        'aprendizados':      estado['aprendizados'][-5:],
        'relatorio_ia':      estado.get('relatorio_ia'),
    })

@app.route('/historico')
def get_historico():
    return jsonify(historico[-100:])

@app.route('/feed')
def get_feed():
    # Retorna sinais dos pares no formato que o dashboard espera
    sinais = []
    for par in PARES:
        det   = estado['pares_detalhes'].get(par, {})
        score = estado['scores'].get(par, 0)
        sinal = 'COMPRA' if score >= SCORE_MINIMO else 'NEUTRO'
        sinais.append({
            'par':   par,
            'preco': det.get('preco', 0),
            'rsi':   det.get('rsi', 0),
            'score': score,
            'sinal': sinal,
        })
    sinais.sort(key=lambda x: x['score'], reverse=True)
    return jsonify(sinais)

@app.route('/stats')
def get_stats():
    total_trades = estado['wins_dia'] + estado['losses_dia']
    # P&L por par a partir do historico
    pares_stats = {}
    for t in historico:
        p = t.get('par','')
        if p not in pares_stats:
            pares_stats[p] = {'pnl_total': 0.0, 'total': 0}
        pares_stats[p]['pnl_total'] += t.get('pnl_usdt', t.get('pnl_usd', 0))
        pares_stats[p]['total']     += 1
    return jsonify({
        'total_trades':   total_trades,
        'trades_lucro':   estado['wins_dia'],
        'trades_stop':    estado['losses_dia'],
        'capital_atual':  round(estado['capital_atual'], 2),
        'pnl_total':      round(estado['lucros_dia'] - abs(estado['perdas_dia']), 2),
        'pares':          pares_stats,
        'stats_pares':    estado['stats_pares'],
        'stats_horarios': estado['stats_horarios'],
        'aprendizados':   estado['aprendizados'],
        'erros':          estado['erros_capturados'][-10:],
        'relatorio_ia':   estado.get('relatorio_ia'),
    })

@app.route('/relatorio')
def get_relatorio():
    rel = gerar_relatorio_ia()
    return jsonify(rel)

DASHBOARD_HTML = r"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Trading Bot v2</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
:root{--bg:#0d1117;--surface:#161b22;--surface2:#21262d;--border:#30363d;--fg:#e6edf3;--fg2:#8b949e;--accent:#58a6ff;--green:#3fb950;--green-dim:#1a4a26;--red:#f85149;--red-dim:#4a1a1a;color-scheme:dark}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--fg);font-family:'Inter',system-ui,sans-serif;font-size:14px;line-height:1.5;padding:0 16px 32px;min-height:100vh}
header{display:flex;align-items:center;gap:12px;padding:16px 0 20px;border-bottom:1px solid var(--border);margin-bottom:20px}
.logo{font-size:20px;font-weight:700;letter-spacing:-.5px}.logo span{color:var(--accent)}
.badge{font-size:11px;font-weight:600;padding:2px 8px;border-radius:20px;background:var(--green-dim);color:var(--green);display:flex;align-items:center;gap:5px}
.badge::before{content:'';width:6px;height:6px;border-radius:50%;background:var(--green);display:inline-block;animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.badge.offline{background:var(--red-dim);color:var(--red)}.badge.offline::before{background:var(--red);animation:none}
.refresh-btn{margin-left:auto;background:var(--surface2);border:1px solid var(--border);color:var(--fg2);padding:6px 14px;border-radius:6px;font-size:12px;font-family:inherit;cursor:pointer;display:flex;align-items:center;gap:6px;transition:color .15s,border-color .15s}
.refresh-btn:hover{color:var(--fg);border-color:var(--accent)}
.last-update{font-size:11px;color:var(--fg2)}
.kpi-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:20px}
.kpi{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px 18px}
.kpi-label{font-size:11px;color:var(--fg2);text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px}
.kpi-value{font-size:24px;font-weight:700;font-variant-numeric:tabular-nums;line-height:1.1}
.kpi-sub{font-size:11px;color:var(--fg2);margin-top:4px}
.pos{color:var(--green)}.neg{color:var(--red)}.neu{color:var(--accent)}
.grid-2{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px}
.grid-3{display:grid;grid-template-columns:2fr 1fr;gap:14px;margin-bottom:14px}
@media(max-width:680px){.grid-2,.grid-3{grid-template-columns:1fr}}
.card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px 18px;margin-bottom:0}
.card-title{font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:.07em;color:var(--fg2);margin-bottom:14px}
canvas{display:block;width:100%!important;max-height:180px}
.chart-wrap{position:relative;height:180px;width:100%}
.tbl-wrap{overflow-x:auto}
table{width:100%;border-collapse:collapse}
th{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--fg2);font-weight:600;text-align:left;padding:6px 10px;border-bottom:1px solid var(--border)}
td{padding:8px 10px;border-bottom:1px solid var(--border);font-size:13px;font-variant-numeric:tabular-nums}
tr:last-child td{border-bottom:none}tr:hover td{background:var(--surface2)}
.feed-list{display:flex;flex-direction:column;gap:8px}
.feed-item{display:grid;grid-template-columns:70px 1fr auto auto;gap:8px;align-items:center;background:var(--surface2);border-radius:8px;padding:8px 10px;font-size:12px}
.feed-par{font-weight:700;font-size:12px}.feed-info{color:var(--fg2)}.feed-score{font-weight:600}
.pill{font-size:10px;font-weight:700;padding:2px 7px;border-radius:10px;text-transform:uppercase;letter-spacing:.04em}
.pill-compra{background:var(--green-dim);color:var(--green)}.pill-venda{background:var(--red-dim);color:var(--red)}.pill-neutro{background:#21262d;color:var(--fg2)}
.pair-bar-list{display:flex;flex-direction:column;gap:8px}
.pair-row{display:flex;align-items:center;gap:8px;font-size:12px}
.pair-name{width:72px;font-weight:600;color:var(--fg);flex-shrink:0}
.bar-track{flex:1;height:8px;background:var(--surface2);border-radius:4px;overflow:hidden}
.bar-fill{height:100%;border-radius:4px;transition:width .4s ease}
.pair-val{width:60px;text-align:right;font-variant-numeric:tabular-nums;font-family:'JetBrains Mono',monospace;font-size:11px}
.pos-list{display:flex;flex-direction:column;gap:8px}
.pos-item{border-radius:8px;padding:10px 12px;border:1px solid var(--border);display:grid;grid-template-columns:1fr auto auto;gap:6px 12px;align-items:center}
.pos-par{font-weight:700}.pos-meta{color:var(--fg2);font-size:11px;grid-column:1/-1}
.state-msg{color:var(--fg2);font-size:13px;text-align:center;padding:32px 0}
.mb14{margin-bottom:14px}
</style></head><body>
<header>
  <div class="logo">Trading <span>v2</span></div>
  <div class="badge offline" id="status-badge">Conectando…</div>
  <span class="last-update" id="last-update"></span>
  <button class="refresh-btn" onclick="fetchAll()">&#x21bb; Atualizar</button>
</header>
<div class="kpi-row">
  <div class="kpi"><div class="kpi-label">Capital</div><div class="kpi-value neu" id="kpi-capital">—</div><div class="kpi-sub">Início: USDT 100,00</div></div>
  <div class="kpi"><div class="kpi-label">P&amp;L Total</div><div class="kpi-value" id="kpi-pnl">—</div><div class="kpi-sub" id="kpi-pnl-sub"></div></div>
  <div class="kpi"><div class="kpi-label">Trades</div><div class="kpi-value" id="kpi-trades">—</div><div class="kpi-sub" id="kpi-trades-sub"></div></div>
  <div class="kpi"><div class="kpi-label">Win Rate</div><div class="kpi-value" id="kpi-wr">—</div><div class="kpi-sub" id="kpi-wr-sub"></div></div>
  <div class="kpi"><div class="kpi-label">Posições</div><div class="kpi-value neu" id="kpi-pos">—</div><div class="kpi-sub" id="kpi-pos-max">Máx: —</div></div>
</div>
<div class="grid-3 mb14">
  <div class="card"><div class="card-title">Evolução do Capital (USDT)</div><div class="chart-wrap"><canvas id="chart-capital"></canvas></div><div class="state-msg" id="capital-msg" hidden></div></div>
  <div class="card"><div class="card-title">Sinais Recentes</div><div class="feed-list" id="feed-list"><div class="state-msg">Carregando…</div></div></div>
</div>
<div class="grid-2 mb14">
  <div class="card"><div class="card-title">P&amp;L por Par</div><div class="pair-bar-list" id="pair-bars"><div class="state-msg">Carregando…</div></div></div>
  <div class="card"><div class="card-title">Posições Abertas</div><div class="pos-list" id="pos-list"><div class="state-msg">Nenhuma posição aberta</div></div></div>
</div>
<div class="card">
  <div class="card-title">Últimas Operações</div>
  <div class="tbl-wrap"><table><thead><tr><th>Par</th><th>Tipo</th><th>Entrada</th><th>Saída</th><th>P&amp;L</th><th>P&amp;L %</th><th>Motivo</th><th>Abertura</th></tr></thead>
  <tbody id="hist-body"><tr><td colspan="8" style="text-align:center;color:var(--fg2);padding:24px">Carregando…</td></tr></tbody></table></div>
</div>
<script>
let capChart=null;
function fmt(n,d=2){if(n==null||isNaN(n))return'—';return Number(n).toLocaleString('pt-BR',{minimumFractionDigits:d,maximumFractionDigits:d})}
function fmtPct(n){if(n==null||isNaN(n))return'—';return(n>=0?'+':'')+fmt(n)+'%'}
function cc(n){return n>0?'pos':n<0?'neg':''}
function ts(iso){if(!iso)return'—';try{return new Date(iso).toLocaleString('pt-BR',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'})}catch{return iso}}
function setOnline(ok){const b=document.getElementById('status-badge');if(ok){b.textContent='Rodando';b.className='badge'}else{b.textContent='Offline';b.className='badge offline'}}
function renderKPIs(s,st){
  const cap=s?.capital_atual??st?.capital_atual??100;
  const pnl=s?.pnl_total??st?.pnl_total??0;
  const trades=st?.total_trades??s?.total_trades??0;
  const venced=st?.trades_lucro??0;const perdeu=st?.trades_stop??0;
  const wr=trades>0?(venced/trades*100):null;
  const posA=Object.keys(s?.posicoes??{}).length;
  document.getElementById('kpi-capital').textContent='USDT '+fmt(cap);
  const pe=document.getElementById('kpi-pnl');pe.textContent=(pnl>=0?'+':'')+fmt(pnl)+' USDT';pe.className='kpi-value '+cc(pnl);
  document.getElementById('kpi-pnl-sub').textContent=fmtPct((cap-100));
  document.getElementById('kpi-trades').textContent=trades;
  document.getElementById('kpi-trades-sub').textContent=venced+' lucro · '+perdeu+' stop';
  const we=document.getElementById('kpi-wr');we.textContent=wr!=null?fmt(wr,1)+'%':'—';we.className='kpi-value '+(wr>=55?'pos':wr<45?'neg':'neu');
  document.getElementById('kpi-wr-sub').textContent=wr!=null?venced+' de '+trades+' ganhos':'Sem trades ainda';
  document.getElementById('kpi-pos').textContent=posA;
  document.getElementById('kpi-pos-max').textContent='Máx: '+(s?.max_posicoes??'—');
}
function renderCap(h){
  const ctx=document.getElementById('chart-capital').getContext('2d');
  const closed=(h||[]).filter(x=>x.preco_saida).sort((a,b)=>new Date(a.abertura)-new Date(b.abertura));
  const labels=['Início'];const values=[100];let r=100;
  closed.forEach(t=>{r+=(t.pnl_usdt??0);labels.push(ts(t.fechamento||t.abertura));values.push(parseFloat(r.toFixed(4)))});
  const msg=document.getElementById('capital-msg');
  if(values.length<=1){document.getElementById('chart-capital').hidden=true;msg.textContent='Nenhuma operação fechada ainda';msg.hidden=false;return}
  document.getElementById('chart-capital').hidden=false;msg.hidden=true;
  if(capChart)capChart.destroy();
  const lv=values[values.length-1];const gc=lv>=100?'#3fb950':'#f85149';
  const g=ctx.createLinearGradient(0,0,0,180);g.addColorStop(0,gc+'44');g.addColorStop(1,gc+'00');
  const mn=Math.min(...values),mx=Math.max(...values),pad=Math.max((mx-mn)*0.3,mx*0.005);
  capChart=new Chart(ctx,{type:'line',data:{labels,datasets:[{label:'Capital',data:values,borderColor:gc,borderWidth:2,pointRadius:values.length<30?5:1,pointHoverRadius:7,pointBackgroundColor:gc,pointBorderColor:'#0d1117',pointBorderWidth:2,fill:true,backgroundColor:g,tension:0}]},options:{responsive:true,maintainAspectRatio:false,animation:{duration:400},plugins:{legend:{display:false},tooltip:{backgroundColor:'#21262d',borderColor:'#30363d',borderWidth:1,titleColor:'#8b949e',bodyColor:'#e6edf3',padding:10,callbacks:{label:c=>' USDT '+fmt(c.parsed.y)}}},scales:{x:{ticks:{color:'#8b949e',font:{size:10},maxTicksLimit:6,maxRotation:0},grid:{color:'#30363d22'}},y:{min:mn-pad,max:mx+pad,ticks:{color:'#8b949e',font:{size:10},callback:v=>'U$'+fmt(v,1)},grid:{color:'#30363d55'}}}}});
}
function renderFeed(f){
  const el=document.getElementById('feed-list');const items=(f||[]).slice(0,8);
  if(!items.length){el.innerHTML='<div class="state-msg">Sem sinais ainda</div>';return}
  el.innerHTML=items.map(x=>{const sig=(x.sinal||'NEUTRO').toUpperCase();const cls=sig==='COMPRA'?'pill-compra':sig==='VENDA'?'pill-venda':'pill-neutro';return`<div class="feed-item"><span class="feed-par">${x.par||'—'}</span><span class="feed-info">U$ ${fmt(x.preco,4)} · RSI ${fmt(x.rsi,1)}</span><span class="feed-score" style="color:var(--accent)">Score ${x.score??'?'}</span><span class="pill ${cls}">${sig}</span></div>`}).join('');
}
function renderBars(st){
  const el=document.getElementById('pair-bars');const pares=st?.pares||{};
  const entries=Object.entries(pares).map(([p,d])=>({p,pnl:d.pnl_total??0})).sort((a,b)=>Math.abs(b.pnl)-Math.abs(a.pnl));
  if(!entries.length){el.innerHTML='<div class="state-msg">Sem dados ainda</div>';return}
  const max=Math.max(...entries.map(e=>Math.abs(e.pnl)),.01);
  el.innerHTML=entries.map(e=>{const col=e.pnl>=0?'var(--green)':'var(--red)';const pct=(Math.abs(e.pnl)/max)*100;return`<div class="pair-row"><span class="pair-name">${e.p.replace('/USDT','')}</span><div class="bar-track"><div class="bar-fill" style="width:${pct}%;background:${col}"></div></div><span class="pair-val" style="color:${col}">${e.pnl>=0?'+':''}${fmt(e.pnl)}</span></div>`}).join('');
}
function renderPos(s){
  const el=document.getElementById('pos-list');const pos=s?.posicoes||{};const entries=Object.entries(pos);
  if(!entries.length){el.innerHTML='<div class="state-msg">Nenhuma posição aberta</div>';return}
  el.innerHTML=entries.map(([par,p])=>{const pu=p.pnl_usdt??0;return`<div class="pos-item"><span class="pos-par">${par}</span><span class="${cc(pu)}" style="font-weight:700">${pu>=0?'+':''}${fmt(pu)} USDT</span><span class="${cc(pu)}" style="font-size:12px">${fmtPct(p.pnl_pct??0)}</span><span class="pos-meta">Entrada: U$ ${fmt(p.preco_entrada,4)} · Stop: U$ ${fmt(p.stop_loss,4)} · ${ts(p.abertura)}</span></div>`}).join('');
}
function renderHist(h){
  const tbody=document.getElementById('hist-body');const rows=(h||[]).slice(0,30);
  if(!rows.length){tbody.innerHTML='<tr><td colspan="8" style="text-align:center;color:var(--fg2);padding:24px">Sem histórico ainda</td></tr>';return}
  tbody.innerHTML=rows.map(r=>{const pnl=r.pnl_usdt??0;const cls=cc(pnl);return`<tr><td style="font-weight:600">${r.par}</td><td><span class="pill ${r.tipo==='COMPRA'?'pill-compra':'pill-venda'}">${r.tipo||'—'}</span></td><td style="font-family:'JetBrains Mono',monospace;font-size:12px">U$ ${fmt(r.preco_entrada,4)}</td><td style="font-family:'JetBrains Mono',monospace;font-size:12px">${r.preco_saida?'U$ '+fmt(r.preco_saida,4):'<span style="color:var(--fg2)">Aberta</span>'}</td><td class="${cls}" style="font-weight:600">${pnl>=0?'+':''}${fmt(pnl)}</td><td class="${cls}">${fmtPct(r.pnl_pct??0)}</td><td style="color:var(--fg2);font-size:12px">${r.motivo_saida||'—'}</td><td style="color:var(--fg2);font-size:12px">${ts(r.abertura)}</td></tr>`}).join('');
}
async function fetchAll(){
  document.getElementById('last-update').textContent='Atualizando…';
  try{
    const [s,h,f,st]=await Promise.allSettled([fetch('/status').then(r=>r.json()),fetch('/historico').then(r=>r.json()),fetch('/feed').then(r=>r.json()),fetch('/stats').then(r=>r.json())]);
    const sv=s.status==='fulfilled'?s.value:null;
    const hv=h.status==='fulfilled'?(Array.isArray(h.value)?h.value:h.value?.historico||[]):[];
    const fv=f.status==='fulfilled'?(Array.isArray(f.value)?f.value:f.value?.feed||[]):[];
    const stv=st.status==='fulfilled'?st.value:null;
    setOnline(!!sv);
    if(sv||stv)renderKPIs(sv,stv);
    renderCap(hv);renderFeed(fv);renderBars(stv);renderPos(sv);renderHist(hv);
    document.getElementById('last-update').textContent='Atualizado às '+new Date().toLocaleTimeString('pt-BR',{hour:'2-digit',minute:'2-digit',second:'2-digit'});
  }catch(e){setOnline(false);document.getElementById('last-update').textContent='Erro ao conectar'}
}
fetchAll();setInterval(fetchAll,30000);
</script></body></html>"""

@app.route('/')
def index():
    from flask import Response
    return Response(DASHBOARD_HTML, mimetype='text/html')

@app.route('/health')
def health():
    return jsonify({'ok': True, 'versao': 'v2', 'uptime': estado['ultimo_update']})

def rodar_api():
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

# ─────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────
def telegram(msg: str):
    for chat_id in TELEGRAM_CHATS:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            requests.post(url, json={
                'chat_id':    chat_id,
                'text':       msg,
                'parse_mode': 'HTML'
            }, timeout=10)
        except Exception as e:
            log.warning(f"Erro Telegram ({chat_id}): {e}")

# ─────────────────────────────────────────────
# RESET DIARIO
# ─────────────────────────────────────────────
def checar_reset_diario():
    hoje = datetime.now().date().isoformat()
    if hoje != estado['ultimo_reset']:
        # Gera relatorio antes de resetar
        gerar_relatorio_ia()

        estado['transacoes_dia']     = 0
        estado['perdas_dia']         = 0.0
        estado['lucros_dia']         = 0.0
        estado['wins_dia']           = 0
        estado['losses_dia']         = 0
        estado['reserva_usada']      = False
        estado['relatorio_enviado']  = False
        estado['stops_consecutivos'] = 0
        estado['pausado_ate']        = None
        estado['ultimo_reset']       = hoje
        estado['capital_reserva']    = estado['capital_atual'] * RESERVA_PCT
        salvar_estado()
        log.info("Reset diario realizado")
        telegram(
            f"Novo dia - Bot v2 ativo\n"
            f"Capital: ${estado['capital_atual']:.2f}\n"
            f"Reserva: ${estado['capital_reserva']:.2f}\n"
            f"Pares: {len(PARES)} monitorados"
        )

# ─────────────────────────────────────────────
# INICIALIZAR — Modo simulacao puro (sem fetch_balance)
# ─────────────────────────────────────────────
def sincronizar_posicao():
    try:
        # Modo simulacao: nao consulta saldo real na exchange
        if estado['capital_inicial'] is None:
            estado['capital_inicial'] = CAPITAL_SIMULADO
            estado['capital_atual']   = CAPITAL_SIMULADO
            estado['capital_reserva'] = CAPITAL_SIMULADO * RESERVA_PCT

        estado['saldo_usdt']  = estado['capital_atual']
        estado['saldo_total'] = estado['capital_atual']
        estado['status']      = 'rodando'
        salvar_estado()

        log.info(f"MODO SIMULACAO | Capital: ${estado['capital_atual']:.2f} | Posicoes: {len(estado['posicoes'])}/{MAX_POSICOES}")
        telegram(
            f"Bot v2 iniciado!\n\n"
            f"Modo: SIMULACAO (dados reais Binance, ordens simuladas)\n"
            f"Capital: ${estado['capital_atual']:.2f}\n"
            f"Reserva: ${estado['capital_reserva']:.2f}\n"
            f"Posicoes abertas: {len(estado['posicoes'])}/{MAX_POSICOES}\n"
            f"Pares: {', '.join(PARES)}\n"
            f"Timeframe: {TIMEFRAME}"
        )
    except Exception as e:
        log.error(f"Erro ao inicializar: {e}")
        capturar_erro('sincronizar_posicao', e)

# ─────────────────────────────────────────────
# DADOS E INDICADORES
# ─────────────────────────────────────────────
def pegar_dados(par, timeframe=None, limit=288):
    tf = timeframe or TIMEFRAME
    return fetch_ohlcv(par, interval=tf, limit=limit)

def calcular_rsi(df, periodo=RSI_PERIODO):
    delta = df['close'].diff()
    ganho = delta.clip(lower=0).rolling(periodo).mean()
    perda = (-delta.clip(upper=0)).rolling(periodo).mean()
    rs    = ganho / perda
    return 100 - (100 / (1 + rs))

def calcular_score(par):
    try:
        df = pegar_dados(par)
        df['ma9']    = df['close'].rolling(9).mean()
        df['ma21']   = df['close'].rolling(21).mean()
        df['ma50']   = df['close'].rolling(50).mean()
        df['rsi']    = calcular_rsi(df)
        df['vol_ma'] = df['volume'].rolling(20).mean()

        ma9_atual  = df['ma9'].iloc[-1]
        ma21_atual = df['ma21'].iloc[-1]
        ma9_prev   = df['ma9'].iloc[-2]
        ma21_prev  = df['ma21'].iloc[-2]
        ma50_atual = df['ma50'].iloc[-1]
        rsi_atual  = df['rsi'].iloc[-1]
        vol_atual  = df['volume'].iloc[-1]
        vol_media  = df['vol_ma'].iloc[-1]
        preco      = df['close'].iloc[-1]

        rsi_val = round(float(rsi_atual), 1) if not pd.isna(rsi_atual) else 50.0

        # Filtro de tendencia no 1h
        tendencia_alta_1h = False
        try:
            df_1h = pegar_dados(par, timeframe='1h', limit=50)
            df_1h['ma50_1h'] = df_1h['close'].rolling(50).mean()
            preco_1h         = df_1h['close'].iloc[-1]
            ma50_1h          = df_1h['ma50_1h'].iloc[-1]
            tendencia_alta_1h = bool(preco_1h > ma50_1h)
        except:
            tendencia_alta_1h = True

        score   = 0
        sinal   = 'NEUTRO'
        motivos = []

        # Cruzamento MA9 x MA21
        cruzou_para_cima  = (ma9_prev <= ma21_prev) and (ma9_atual > ma21_atual)
        cruzou_para_baixo = (ma9_prev >= ma21_prev) and (ma9_atual < ma21_atual)

        if cruzou_para_cima:
            score += 3
            sinal  = 'COMPRA'
            motivos.append('MA cruzou alta +3')
        elif cruzou_para_baixo:
            score -= 2
            sinal  = 'VENDA'
            motivos.append('MA cruzou baixa -2')
        else:
            motivos.append('MA sem cruzamento')

        # Preco vs MA50
        if preco > ma50_atual:
            score += 2
            motivos.append('Acima MA50 +2')
        else:
            motivos.append('Abaixo MA50')

        # Volume
        if vol_atual >= vol_media * 1.0:
            score += 2
            motivos.append('Volume OK +2')
        else:
            motivos.append('Volume fraco')

        # RSI
        if 35 <= rsi_val <= 65:
            score += 2
            motivos.append('RSI ideal +2')
        elif rsi_val < 35:
            score += 1
            motivos.append('RSI sobrevendido +1')
        elif rsi_val > RSI_SOBRECOMPRADO:
            score -= 3
            sinal  = 'NEUTRO'
            motivos.append('RSI sobrecomprado -3')

        # Tendencia 1h
        if sinal == 'COMPRA' and not tendencia_alta_1h:
            sinal   = 'NEUTRO'
            score  -= 2
            motivos.append('Tendencia 1h baixa -2')
        elif tendencia_alta_1h:
            motivos.append('Tendencia 1h OK')

        # Score dinamico por par
        peso = get_peso_par(par)
        if peso != 1.0:
            bonus = round((peso - 1.0) * 3)
            score += bonus
            motivos.append(f'Historico {bonus:+d}')

        # Horario inteligente
        favoravel, motivo_hora = horario_favoravel(par)
        if not favoravel and sinal == 'COMPRA':
            sinal   = 'NEUTRO'
            score  -= 2
            motivos.append(f'Hora ruim: {motivo_hora}')
        elif favoravel and par in estado['stats_horarios']:
            motivos.append(f'Hora: {motivo_hora}')

        pico_24h  = df['high'].max()
        queda_24h = round((preco - pico_24h) / pico_24h * 100, 2)

        log.info(f"{par} | ${preco:.4f} | Score:{score} | RSI:{rsi_val} | {sinal}")
        add_evento('INFO', f"{par} | ${preco:.4f} | S:{score} | RSI:{rsi_val} | {sinal}")

        return {
            'par':        par,
            'score':      score,
            'sinal':      sinal,
            'rsi':        rsi_val,
            'preco':      round(float(preco), 6),
            'ma9':        round(float(ma9_atual), 6),
            'ma21':       round(float(ma21_atual), 6),
            'ma50':       round(float(ma50_atual), 6),
            'volume_ok':  bool(vol_atual >= vol_media * 1.0),
            'motivos':    motivos,
            'df':         df,
            'queda_24h':  queda_24h,
            'pico_24h':   round(float(pico_24h), 6),
        }
    except Exception as e:
        capturar_erro('calcular_score', e, par)
        log.error(f"Erro score {par}: {e}")
        return {
            'par': par, 'score': -1, 'sinal': 'ERRO', 'rsi': 50.0,
            'preco': 0, 'ma9': 0, 'ma21': 0, 'ma50': 0,
            'volume_ok': False, 'motivos': [f'Erro: {e}'], 'df': None,
            'queda_24h': 0, 'pico_24h': 0,
        }

# ─────────────────────────────────────────────
# COMPRAR — SIMULADO (sem enviar ordem real)
# ─────────────────────────────────────────────
def comprar(par, rsi):
    try:
        if par in estado['posicoes']:
            return
        if len(estado['posicoes']) >= MAX_POSICOES:
            return

        ticker = fetch_ticker(par)
        preco  = ticker['last']

        capital_total_calc = estado['capital_atual'] + sum(
            p.get('valor_investido', p['preco_compra'] * p['quantidade']) for p in estado['posicoes'].values()
        )
        valor_op = max(capital_total_calc * RISCO_POR_TRADE, CAPITAL_BASE)
        capital_livre = estado['capital_atual'] - estado['capital_reserva']
        if valor_op > capital_livre:
            valor_op = capital_livre * 0.90

        if valor_op < CAPITAL_BASE:
            log.warning(f"Capital insuficiente para comprar {par}: ${valor_op:.2f}")
            return

        # SIMULADO — registra posicao sem enviar ordem para exchange
        preco_executado = preco
        taxa_compra     = valor_op * TAXA_OPERACAO
        valor_liquido   = valor_op - taxa_compra
        quantidade_sim  = valor_liquido / preco_executado

        estado['capital_atual'] -= valor_op          # desconta USDT gasto
        stop_price = preco_executado * (1 - STOP_LOSS)

        estado['posicoes'][par] = {
            'preco_compra':    preco_executado,
            'preco_entrada':   preco_executado,
            'quantidade':      quantidade_sim,
            'valor_investido': valor_liquido,
            'preco_pico':      preco_executado,
            'stop_loss':       round(stop_price, 6),
            'entrada':         datetime.now().strftime('%d/%m %H:%M'),
            'abertura':        datetime.now().isoformat(),
        }
        estado['transacoes_dia'] += 1
        salvar_estado()

        log.info(f"[SIM] COMPROU {par} | ${preco_executado:.4f} | ${valor_op:.2f} | Taxa:${taxa_compra:.3f} | Qtd:{quantidade_sim:.4f} | RSI:{rsi:.1f}")
        add_evento('COMPRA', f"[SIM] {par} | ${preco_executado:.4f} | ${valor_op:.2f} | RSI:{rsi:.1f}")
        telegram(
            f"COMPRA SIMULADA\n\n"
            f"Par: {par}\n"
            f"Preco: ${preco_executado:.4f}\n"
            f"Valor: ${valor_op:.2f} (taxa: ${taxa_compra:.3f})\n"
            f"Qtd: {quantidade_sim:.6f}\n"
            f"RSI: {rsi:.1f}\n"
            f"Stop: ${stop_price:.4f}\n"
            f"Posicoes: {len(estado['posicoes'])}/{MAX_POSICOES}\n"
            f"Capital disp: ${estado['capital_atual']:.2f}"
        )
    except Exception as e:
        capturar_erro('comprar', e, par)
        log.error(f"Erro compra {par}: {e}")

# ─────────────────────────────────────────────
# COMPRAR COM RESERVA — SIMULADO
# ─────────────────────────────────────────────
def comprar_reserva(par):
    try:
        if estado['reserva_usada'] or estado['capital_reserva'] <= 0:
            return

        ticker        = fetch_ticker(par)
        preco         = ticker['last']
        valor_reserva = estado['capital_reserva']

        # SIMULADO
        preco_executado = preco
        taxa_reserva    = valor_reserva * TAXA_OPERACAO
        valor_liq_res   = valor_reserva - taxa_reserva
        quantidade_sim  = valor_liq_res / preco_executado

        if par in estado['posicoes']:
            pos          = estado['posicoes'][par]
            qtd_total    = pos['quantidade'] + quantidade_sim
            val_total    = pos.get('valor_investido', pos['preco_compra']*pos['quantidade']) + valor_liq_res
            pm           = ((pos['preco_compra'] * pos['quantidade']) +
                            (preco_executado * quantidade_sim)) / qtd_total
            estado['posicoes'][par]['preco_compra']    = pm
            estado['posicoes'][par]['preco_entrada']   = pm
            estado['posicoes'][par]['quantidade']      = qtd_total
            estado['posicoes'][par]['valor_investido'] = val_total
            estado['posicoes'][par]['stop_loss']       = round(pm * (1 - STOP_LOSS), 6)
        else:
            estado['posicoes'][par] = {
                'preco_compra':    preco_executado,
                'preco_entrada':   preco_executado,
                'quantidade':      quantidade_sim,
                'valor_investido': valor_liq_res,
                'preco_pico':      preco_executado,
                'stop_loss':       round(preco_executado * (1 - STOP_LOSS), 6),
                'entrada':         datetime.now().strftime('%d/%m %H:%M'),
                'abertura':        datetime.now().isoformat(),
            }

        estado['reserva_usada']   = True
        estado['capital_reserva'] = 0.0
        salvar_estado()
        registrar_aprendizado('RESERVA_USADA', f"Queda brusca detectada — reserva aplicada em {par}", par)

        log.info(f"RESERVA USADA {par} | ${preco_executado:.4f} | ${valor_reserva:.2f}")
        add_evento('ALERTA', f"Reserva usada em {par} | ${preco_executado:.4f}")
        telegram(
            f"RESERVA ATIVADA\n"
            f"Par: {par}\n"
            f"Queda de 10% detectada\n"
            f"Valor: ${valor_reserva:.2f}\n"
            f"Preco: ${preco_executado:.4f}"
        )
    except Exception as e:
        capturar_erro('comprar_reserva', e, par)
        log.error(f"Erro reserva {par}: {e}")

def checar_queda_brusca_todos():
    if estado['reserva_usada'] or estado['capital_reserva'] <= 0:
        return
    melhor_par   = None
    melhor_queda = 0.0
    for par, detalhes in estado['pares_detalhes'].items():
        queda = detalhes.get('queda_24h', 0)
        if queda <= -QUEDA_RESERVA * 100 and queda < melhor_queda:
            melhor_queda = queda
            melhor_par   = par
    if melhor_par:
        add_evento('ALERTA', f"Queda 24h: {melhor_par} | {melhor_queda:.2f}%")
        comprar_reserva(melhor_par)

# ─────────────────────────────────────────────
# VENDER — SIMULADO (sem enviar ordem real)
# ─────────────────────────────────────────────
def vender(par, motivo="SINAL"):
    try:
        if par not in estado['posicoes']:
            return

        pos    = estado['posicoes'][par]
        ticker = fetch_ticker(par)
        preco  = ticker['last']

        qtd_sim = pos['quantidade']

        # SIMULADO — usa preco atual de mercado como preco de execucao
        preco_executado = preco

        receita_bruta   = preco_executado * qtd_sim
        taxa_venda      = receita_bruta * TAXA_OPERACAO
        receita_liquida = receita_bruta - taxa_venda
        valor_investido = pos.get('valor_investido', pos['preco_compra'] * qtd_sim)
        lucro_liquido   = receita_liquida - valor_investido
        lucro_pct       = (lucro_liquido / valor_investido) * 100

        now_str = datetime.now().isoformat()
        historico.append({
            'par':          par,
            'tipo':         'COMPRA',
            'motivo_saida': motivo,
            'preco_entrada': round(pos.get('preco_entrada', pos['preco_compra']), 6),
            'preco_saida':  round(preco_executado, 6),
            'quantidade':   round(qtd_sim, 6),
            'valor_investido': round(valor_investido, 4),
            'receita':      round(receita_liquida, 4),
            'taxa_total':   round(taxa_venda + valor_investido * TAXA_OPERACAO, 4),
            'pnl_pct':      round(lucro_pct, 2),
            'pnl_usdt':     round(lucro_liquido, 4),
            'resultado':    'win' if lucro_liquido >= 0 else 'loss',
            'abertura':     pos.get('abertura', now_str),
            'fechamento':   now_str,
            'data':         datetime.now().strftime('%d/%m %H:%M'),
        })
        salvar_historico()

        del estado['posicoes'][par]
        estado['capital_atual'] += receita_liquida   # soma receita completa (nao so lucro)
        nova_reserva = estado['capital_atual'] * RESERVA_PCT
        estado['capital_reserva'] = nova_reserva
        if not estado['posicoes']:
            estado['reserva_usada'] = False

        hora_trade = datetime.now().strftime('%H:%M')
        atualizar_stats(par, 'win' if lucro_liquido >= 0 else 'loss', hora_trade)

        if lucro_liquido >= 0:
            estado['lucros_dia'] += lucro_liquido
            estado['wins_dia']   += 1
            estado['stops_consecutivos'] = 0
            resultado_txt = "LUCRO"
        else:
            estado['perdas_dia'] += lucro_liquido
            estado['losses_dia'] += 1
            resultado_txt = "PERDA"
            if motivo == "STOP LOSS":
                estado['stops_consecutivos'] += 1
                if estado['stops_consecutivos'] >= STOPS_CONSECUTIVOS_MAX:
                    pausado_ate = (datetime.now() + timedelta(hours=1)).isoformat()
                    estado['pausado_ate'] = pausado_ate
                    registrar_aprendizado('PAUSA', f"3 stops consecutivos — pausado por 1h")
                    log.warning(f"3 stops seguidos! Pausando ate {pausado_ate[11:16]}")
                    add_evento('ALERTA', f"3 stops consecutivos — pausando 1h")
                    telegram(
                        f"3 STOPS CONSECUTIVOS\n"
                        f"Bot pausado por 1 hora\n"
                        f"Retoma as {pausado_ate[11:16]}"
                    )

        salvar_estado()

        log.info(f"VENDEU {par} [{motivo}] | ${preco_executado:.4f} | {lucro_pct:+.2f}% | ${lucro_liquido:+.2f}")
        add_evento('VENDA', f"{par} [{motivo}] | ${preco_executado:.4f} | {lucro_pct:+.2f}% | ${lucro_liquido:+.2f}")
        telegram(
            f"{resultado_txt} - VENDA [{motivo}]\n\n"
            f"Par: {par}\n"
            f"Preco: ${preco_executado:.4f}\n"
            f"P&L: {lucro_pct:+.2f}% (${lucro_liquido:+.2f})\n"
            f"Taxa: -${taxa:.2f}\n"
            f"Capital: ${estado['capital_atual']:.2f}\n"
            f"Reserva: ${nova_reserva:.2f}\n"
            f"Wins: {estado['wins_dia']} | Losses: {estado['losses_dia']}\n"
            f"Posicoes: {len(estado['posicoes'])}/{MAX_POSICOES}"
        )
    except Exception as e:
        capturar_erro('vender', e, par)
        log.error(f"Erro venda {par}: {e}")
        telegram(f"Erro ao vender {par}: {e}")

# ─────────────────────────────────────────────
# CHECAR RISCO — trailing stop dinamico
# ─────────────────────────────────────────────
def checar_risco(par):
    if par not in estado['posicoes']:
        return False

    pos    = estado['posicoes'][par]
    ticker = fetch_ticker(par)
    preco  = ticker['last']

    # HIGH do candle atual para capturar picos intracandle
    try:
        candles    = fetch_ohlcv(par, interval=TIMEFRAME, limit=2)
        high_atual = candles[-1][2]
        preco_pico_candle = max(preco, high_atual)
    except:
        preco_pico_candle = preco

    if preco_pico_candle > pos['preco_pico']:
        estado['posicoes'][par]['preco_pico'] = preco_pico_candle

    lucro      = (preco - pos['preco_compra']) / pos['preco_compra']
    lucro_pico = (pos['preco_pico'] - pos['preco_compra']) / pos['preco_compra']
    trailing   = (pos['preco_pico'] - preco) / pos['preco_pico']

    # Stop Loss fixo
    if lucro <= -STOP_LOSS:
        add_evento('ALERTA', f"STOP LOSS {par} | {lucro*100:.2f}%")
        log.warning(f"STOP LOSS {par} | {lucro*100:.2f}%")
        vender(par, motivo="STOP LOSS")
        return True

    # Trailing Stop dinamico — quanto maior o lucro, mais apertado
    if lucro_pico >= 0.005:
        if lucro_pico >= 0.03:
            trailing_din = 0.002    # +3% -> trava 0.2%
        elif lucro_pico >= 0.02:
            trailing_din = 0.003    # +2% -> trava 0.3%
        elif lucro_pico >= 0.01:
            trailing_din = 0.005    # +1% -> trava 0.5%
        else:
            trailing_din = TRAILING_STOP  # padrao 0.8%

        if trailing >= trailing_din:
            if lucro >= LUCRO_MINIMO_SAIDA:
                add_evento('ALERTA', f"TRAILING {par} | Pico:${pos['preco_pico']:.4f} | {lucro*100:.2f}%")
                log.info(f"TRAILING STOP {par} | {lucro*100:.2f}% | Trailing:{trailing_din*100:.1f}%")
                vender(par, motivo="TRAILING STOP")
                return True
            else:
                add_evento('INFO', f"Trailing aguardando {par} | {lucro*100:.2f}% < {LUCRO_MINIMO_SAIDA*100:.1f}%")

    return False

# ─────────────────────────────────────────────
# LIMITE DE PERDA DIARIA
# ─────────────────────────────────────────────
def limite_diario_atingido():
    if estado['capital_inicial'] is None:
        return False
    limite = estado['capital_inicial'] * LIMITE_PERDA_DIARIA
    if abs(estado['perdas_dia']) >= limite:
        log.critical(f"LIMITE DE PERDA DIARIA | ${abs(estado['perdas_dia']):.2f}")
        telegram(f"LIMITE DE PERDA DIARIA\nPerda: ${abs(estado['perdas_dia']):.2f}\nBot encerrado por hoje.")
        return True
    return False

# ─────────────────────────────────────────────
# LOOP PRINCIPAL
# ─────────────────────────────────────────────
def rodar_bot():
    log.info("Bot v2 iniciando...")
    carregar_estado()
    carregar_historico()
    sincronizar_posicao()

    while True:
        try:
            checar_reset_diario()
            enviar_relatorio_ia()

            if limite_diario_atingido():
                for par in list(estado['posicoes'].keys()):
                    vender(par, motivo="LIMITE DIARIO")
                estado['status'] = 'encerrado'
                salvar_estado()
                log.info("Limite diario atingido — bot pausado ate amanha")
                time.sleep(3600)
                continue

            # Checa pausa por stops consecutivos
            if estado.get('pausado_ate'):
                agora = datetime.now().isoformat()
                if agora < estado['pausado_ate']:
                    restante = estado['pausado_ate'][11:16]
                    log.info(f"Bot pausado — retoma as {restante}")
                    add_evento('ALERTA', f"Pausado ate {restante}")
                    time.sleep(60)
                    continue
                else:
                    estado['pausado_ate']        = None
                    estado['stops_consecutivos'] = 0
                    log.info("Pausa encerrada — retomando operacoes")
                    add_evento('INFO', "Pausa encerrada — retomando")
                    telegram("Pausa encerrada — bot retomando operacoes")

            estado['ultimo_update'] = datetime.now().isoformat()

            # Monitora posicoes abertas
            for par in list(estado['posicoes'].keys()):
                try:
                    ticker = fetch_ticker(par)
                    preco  = ticker['last']
                    pos    = estado['posicoes'][par]
                    pnl    = (preco - pos['preco_compra']) / pos['preco_compra'] * 100
                    pnl_u  = (preco - pos['preco_compra']) * pos['quantidade']

                    add_evento('INFO', f"Monit. {par} | ${preco:.4f} | P&L: {pnl:+.2f}% (${pnl_u:+.2f})")

                    ja_vendeu = checar_risco(par)

                    if not ja_vendeu:
                        resultado = calcular_score(par)
                        estado['pares_detalhes'][par] = {
                            'preco':     resultado['preco'],
                            'score':     resultado['score'],
                            'sinal':     resultado['sinal'],
                            'rsi':       resultado['rsi'],
                            'ma9':       resultado['ma9'],
                            'ma21':      resultado['ma21'],
                            'ma50':      resultado['ma50'],
                            'volume_ok': bool(resultado['volume_ok']),
                            'motivos':   resultado['motivos'],
                            'queda_24h': resultado.get('queda_24h', 0),
                            'pico_24h':  resultado.get('pico_24h', 0),
                        }
                        if resultado['sinal'] == 'VENDA':
                            lucro_atual = (preco - pos['preco_compra']) / pos['preco_compra']
                            if lucro_atual >= LUCRO_MINIMO_VENDA:
                                vender(par, motivo="SINAL MA")
                            else:
                                add_evento('INFO', f"Venda aguardando {par} | {lucro_atual*100:.2f}% < {LUCRO_MINIMO_VENDA*100:.1f}%")
                except Exception as e:
                    capturar_erro('loop_monit', e, par)
                    log.error(f"Erro monitorando {par}: {e}")

            # Checa queda brusca em todos os pares
            checar_queda_brusca_todos()

            # Analisa pares sem posicao
            pares_sem_posicao = [p for p in PARES if p not in estado['posicoes']]
            slots = MAX_POSICOES - len(estado['posicoes'])

            if slots > 0 and pares_sem_posicao:
                resultados = []
                for par in pares_sem_posicao:
                    r = calcular_score(par)
                    resultados.append(r)
                    estado['scores'][par] = r['score']
                    estado['pares_detalhes'][par] = {
                        'preco':     r['preco'],
                        'score':     r['score'],
                        'sinal':     r['sinal'],
                        'rsi':       r['rsi'],
                        'ma9':       r['ma9'],
                        'ma21':      r['ma21'],
                        'ma50':      r['ma50'],
                        'volume_ok': bool(r['volume_ok']),
                        'motivos':   r['motivos'],
                        'queda_24h': r.get('queda_24h', 0),
                        'pico_24h':  r.get('pico_24h', 0),
                    }

                candidatos = [r for r in resultados if r['sinal'] == 'COMPRA' and r['score'] >= SCORE_MINIMO]
                candidatos.sort(key=lambda x: x['score'], reverse=True)

                for melhor in candidatos[:slots]:
                    log.info(f"Entrando em {melhor['par']} (Score:{melhor['score']})")
                    estado['sinal_atual'] = f"COMPRA {melhor['par']}"
                    estado['rsi_atual']   = melhor['rsi']
                    estado['preco_atual'] = melhor['preco']
                    comprar(melhor['par'], melhor['rsi'])

                if not candidatos and resultados:
                    m = max(resultados, key=lambda x: x['score'])
                    estado['sinal_atual'] = 'NEUTRO'
                    estado['rsi_atual']   = m['rsi']
                    estado['preco_atual'] = m['preco']

                # Atualiza saldo (simulado)
                estado['saldo_usdt']  = estado['capital_atual']
                estado['saldo_total'] = estado['capital_atual']

            time.sleep(60)

        except ccxt.NetworkError as e:
            log.warning(f"Rede: {e}")
            capturar_erro('rede', e)
            time.sleep(30)
        except ccxt.ExchangeError as e:
            log.error(f"Exchange: {e}")
            capturar_erro('exchange', e)
            time.sleep(60)
        except KeyboardInterrupt:
            log.info("Bot encerrado manualmente")
            telegram("Bot v2 encerrado manualmente")
            for par in list(estado['posicoes'].keys()):
                vender(par, motivo="ENCERRAMENTO MANUAL")
            salvar_estado()
            break
        except Exception as e:
            capturar_erro('loop_principal', e)
            log.error(f"Erro inesperado: {e}")
            telegram(f"Erro inesperado: {e}")
            time.sleep(10)

# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == '__main__':
    t = threading.Thread(target=rodar_api, daemon=True)
    t.start()
    log.info("API Flask iniciada em background")
    rodar_bot()
