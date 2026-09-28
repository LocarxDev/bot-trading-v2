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
    'SOL/USDT', 'BNB/USDT', 'DOGE/USDT', 'LINK/USDT',
]

TIMEFRAME              = '1m'
CAPITAL_BASE           = 10.0       # minimo por ordem
RISCO_POR_TRADE        = 0.12       # 12% do capital por trade (~$12 de $100)
MAX_POSICOES           = 4          # maximo de posicoes simultaneas
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

ARQUIVO_ESTADO    = 'estado_bot_v2.json'
ARQUIVO_HISTORICO = 'historico_trades_v2.json'
ARQUIVO_RELATORIO = 'relatorio_ia.json'

# ─────────────────────────────────────────────
# BINANCE REST API PUBLICA — sem autenticacao, sem ccxt
# Apenas dados de mercado (OHLCV + ticker). Ordens 100% simuladas.
# Evita o bloqueio 451 que ocorre em datacenters com /sapi/* autenticado.
# ─────────────────────────────────────────────
BINANCE_BASE = 'https://api.binance.com'

def _par_para_symbol(par: str) -> str:
    """'BTC/USDT' -> 'BTCUSDT'"""
    return par.replace('/', '')

def binance_get(path: str, params: dict = None, retries: int = 3) -> dict | list:
    url = BINANCE_BASE + path
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if i == retries - 1:
                raise
            time.sleep(1)

def fetch_ohlcv(par: str, interval: str = '1m', limit: int = 288) -> pd.DataFrame:
    """Busca candles OHLCV da Binance (endpoint publico)."""
    data = binance_get('/api/v3/klines', {
        'symbol': _par_para_symbol(par),
        'interval': interval,
        'limit': limit,
    })
    df = pd.DataFrame(data, columns=[
        'time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_vol', 'trades', 'taker_buy_base',
        'taker_buy_quote', 'ignore'
    ])
    df = df[['time', 'open', 'high', 'low', 'close', 'volume']].copy()
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = df[col].astype(float)
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    return df

def fetch_ticker(par: str) -> dict:
    """Busca preco atual do par (endpoint publico)."""
    data = binance_get('/api/v3/ticker/price', {'symbol': _par_para_symbol(par)})
    return {'last': float(data['price'])}

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

    pnl_total_usd = 0.0
    posicoes_info = {}
    for par, pos in estado['posicoes'].items():
        preco_atual = estado['pares_detalhes'].get(par, {}).get('preco', pos['preco_compra'])
        pnl_pct = (preco_atual - pos['preco_compra']) / pos['preco_compra'] * 100
        pnl_usd = (preco_atual - pos['preco_compra']) * pos['quantidade']
        pnl_total_usd += pnl_usd
        posicoes_info[par] = {
            'preco_compra': round(pos['preco_compra'], 6),
            'preco_atual':  round(preco_atual, 6),
            'preco_pico':   round(pos.get('preco_pico', pos['preco_compra']), 6),
            'quantidade':   round(pos['quantidade'], 6),
            'pnl_pct':      round(pnl_pct, 2),
            'pnl_usd':      round(pnl_usd, 2),
            'total_invest': round(pos['preco_compra'] * pos['quantidade'], 2),
            'entrada':      pos.get('entrada', ''),
        }

    return jsonify({
        'versao':            'v2',
        'status':            estado['status'],
        'sandbox':           True,
        'capital_simulado':  CAPITAL_SIMULADO,
        'posicoes':          posicoes_info,
        'num_posicoes':      len(estado['posicoes']),
        'max_posicoes':      MAX_POSICOES,
        'pnl_total_usd':     round(pnl_total_usd, 2),
        'pares_detalhes':    estado['pares_detalhes'],
        'pares_monitorados': PARES,
        'scores':            estado['scores'],
        'capital_inicial':   round(estado['capital_inicial'] or CAPITAL_SIMULADO, 2),
        'capital_atual':     round(estado['capital_atual'], 2),
        'capital_reserva':   round(estado['capital_reserva'], 2),
        'reserva_usada':     int(estado['reserva_usada']),
        'lucros_dia':        round(estado['lucros_dia'], 2),
        'perdas_dia':        round(abs(estado['perdas_dia']), 2),
        'resultado_dia':     round(estado['lucros_dia'] - abs(estado['perdas_dia']), 2),
        'transacoes_dia':    estado['transacoes_dia'],
        'wins_dia':          estado['wins_dia'],
        'losses_dia':        estado['losses_dia'],
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
    return jsonify(list(reversed(feed_eventos[-50:])))

@app.route('/stats')
def get_stats():
    return jsonify({
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

        valor_op = max(estado['capital_atual'] * RISCO_POR_TRADE, CAPITAL_BASE)
        capital_livre = estado['capital_atual'] - estado['capital_reserva']
        if valor_op > capital_livre:
            valor_op = capital_livre * 0.90

        if valor_op < CAPITAL_BASE:
            log.warning(f"Capital insuficiente para comprar {par}: ${valor_op:.2f}")
            return

        # SIMULADO — registra posicao sem enviar ordem para exchange
        preco_executado = preco
        quantidade_sim  = valor_op / preco_executado

        estado['posicoes'][par] = {
            'preco_compra': preco_executado,
            'quantidade':   quantidade_sim,
            'preco_pico':   preco_executado,
            'entrada':      datetime.now().strftime('%d/%m %H:%M'),
        }
        estado['transacoes_dia'] += 1
        salvar_estado()

        log.info(f"[SIM] COMPROU {par} | ${preco_executado:.4f} | ${valor_op:.2f} | Qtd:{quantidade_sim:.4f} | RSI:{rsi:.1f}")
        add_evento('COMPRA', f"[SIM] {par} | ${preco_executado:.4f} | ${valor_op:.2f} | RSI:{rsi:.1f}")
        telegram(
            f"COMPRA SIMULADA\n\n"
            f"Par: {par}\n"
            f"Preco: ${preco_executado:.4f}\n"
            f"Valor: ${valor_op:.2f}\n"
            f"Qtd: {quantidade_sim:.6f}\n"
            f"RSI: {rsi:.1f}\n"
            f"Stop: ${preco_executado*(1-STOP_LOSS):.4f}\n"
            f"Posicoes: {len(estado['posicoes'])}/{MAX_POSICOES}\n"
            f"Capital: ${estado['capital_atual']:.2f}"
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
        quantidade_sim  = valor_reserva / preco_executado

        if par in estado['posicoes']:
            pos       = estado['posicoes'][par]
            qtd_total = pos['quantidade'] + quantidade_sim
            pm        = ((pos['preco_compra'] * pos['quantidade']) +
                         (preco_executado * quantidade_sim)) / qtd_total
            estado['posicoes'][par]['preco_compra'] = pm
            estado['posicoes'][par]['quantidade']   = qtd_total
        else:
            estado['posicoes'][par] = {
                'preco_compra': preco_executado,
                'quantidade':   quantidade_sim,
                'preco_pico':   preco_executado,
                'entrada':      datetime.now().strftime('%d/%m %H:%M'),
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

        lucro_pct     = (preco_executado - pos['preco_compra']) / pos['preco_compra'] * 100
        lucro_usd     = (preco_executado - pos['preco_compra']) * qtd_sim
        taxa          = (pos['preco_compra'] * qtd_sim) * 0.002
        lucro_liquido = lucro_usd - taxa

        historico.append({
            'data':         datetime.now().strftime('%d/%m %H:%M'),
            'par':          par,
            'tipo':         motivo,
            'preco_compra': round(pos['preco_compra'], 6),
            'preco_venda':  round(preco_executado, 6),
            'quantidade':   round(qtd_sim, 6),
            'pnl_pct':      round(lucro_pct, 2),
            'pnl_usd':      round(lucro_liquido, 2),
            'resultado':    'win' if lucro_liquido >= 0 else 'loss',
            'entrada':      pos.get('entrada', ''),
        })
        salvar_historico()

        del estado['posicoes'][par]
        estado['capital_atual'] += lucro_liquido
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
