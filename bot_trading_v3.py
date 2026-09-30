"""
Trading Bot v3 — LONG + SHORT simulado + Machine Learning adaptativo
"""
import os, json, time, threading, logging, pathlib
from datetime import datetime, timedelta
import pandas as pd
import requests
from flask import Flask, jsonify
from flask_cors import CORS

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
log = logging.getLogger('bot_v3')

# ─────────────────────────────────────────────
# PERSISTENCIA
# ─────────────────────────────────────────────
_DATA_DIR = '/data' if pathlib.Path('/data').exists() else '.'
ARQUIVO_ESTADO    = f'{_DATA_DIR}/estado_bot_v3.json'
ARQUIVO_HISTORICO = f'{_DATA_DIR}/historico_trades_v3.json'

# ─────────────────────────────────────────────
# CONFIGURACAO
# ─────────────────────────────────────────────
CAPITAL_SIMULADO   = 100.0
CAPITAL_BASE       = 10.0
RISCO_POR_TRADE    = 0.12       # 12% do capital total
MAX_POSICOES       = 6          # máx posições simultâneas (LONG + SHORT)
STOP_LOSS          = 0.015      # 1.5%
TRAILING_STOP      = 0.008      # 0.8%
LUCRO_MINIMO_SAIDA = 0.008      # 0.8% mínimo para trailing ativar
LUCRO_MINIMO_VENDA = 0.005      # 0.5% para sair por sinal MA
TAXA_OPERACAO      = 0.002      # 0.2%
RESERVA_PCT        = 0.10       # 10% reserva
QUEDA_RESERVA      = 0.10       # -10% aciona reserva
LIMITE_PERDA_DIARIA = 0.05      # -5% do capital inicial
STOPS_CONSECUTIVOS_MAX = 3
TIMEFRAME          = '5m'
RSI_PERIODO        = 14
RSI_SOBRECOMPRADO  = 72
RSI_SOBREVENDIDO   = 28

# ─── ML adaptativo — limites ────────────────
SCORE_MINIMO       = 3          # base, ajustado pelo ML
SCORE_MINIMO_MIN   = 2
SCORE_MINIMO_MAX   = 6
SCORE_SHORT_MIN    = 5          # mínimo para abrir SHORT (mais seletivo)

PARES = [
    'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT',
    'XRP/USDT', 'ADA/USDT', 'DOGE/USDT', 'AVAX/USDT',
    'MATIC/USDT', 'LINK/USDT',
]

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN', '')
TELEGRAM_CHATS = [c.strip() for c in os.environ.get('TELEGRAM_CHATS', '').split(',') if c.strip()]

# ─────────────────────────────────────────────
# ESTADO GLOBAL
# ─────────────────────────────────────────────
estado = {
    'status':            'iniciando',
    'posicoes':          {},           # par -> {tipo: 'LONG'|'SHORT', ...}
    'capital_atual':     CAPITAL_SIMULADO,
    'capital_inicial':   None,
    'capital_reserva':   CAPITAL_SIMULADO * RESERVA_PCT,
    'reserva_usada':     False,
    'transacoes_dia':    0,
    'wins_dia':          0,
    'losses_dia':        0,
    'perdas_dia':        0.0,
    'lucros_dia':        0.0,
    'ultimo_reset':      datetime.now().date().isoformat(),
    'relatorio_enviado': False,
    'stops_consecutivos': 0,
    'pausado_ate':       None,
    'saldo_usdt':        CAPITAL_SIMULADO,
    'saldo_total':       CAPITAL_SIMULADO,
    'sinal_atual':       'AGUARDANDO',
    'rsi_atual':         50.0,
    'preco_atual':       0.0,
    'ultimo_update':     datetime.now().isoformat(),
    'pares_detalhes':    {},
    'scores':            {},
    'stats_pares':       {},
    'stats_horarios':    {},
    'aprendizados':      [],
    'erros_capturados':  [],
    # ML
    'score_minimo_atual': SCORE_MINIMO,
    'ajuste_ml_log':     [],
    'ml_ciclos':         0,
}
historico = []
_eventos  = []

# ─────────────────────────────────────────────
# EXCHANGE (Kraken — dados reais, sem chaves)
# ─────────────────────────────────────────────
def fetch_ohlcv(par, interval='5m', limit=288):
    tf_map = {'1m': 1, '5m': 5, '15m': 15, '30m': 30, '1h': 60, '4h': 240, '1d': 1440}
    minutes = tf_map.get(interval, 5)
    symbol  = par.replace('/', '')
    url     = f'https://api.kraken.com/0/public/OHLC?pair={symbol}&interval={minutes}&count={limit}'
    r = requests.get(url, timeout=15)
    data = r.json()
    if data.get('error'):
        raise Exception(f"Kraken erro: {data['error']}")
    result_key = [k for k in data['result'] if k != 'last'][0]
    rows = data['result'][result_key]
    df = pd.DataFrame(rows, columns=['time','open','high','low','close','vwap','volume','count'])
    for col in ['open','high','low','close','volume']:
        df[col] = df[col].astype(float)
    return df

def fetch_ticker(par):
    symbol = par.replace('/', '')
    url    = f'https://api.kraken.com/0/public/Ticker?pair={symbol}'
    r      = requests.get(url, timeout=10)
    data   = r.json()
    if data.get('error'):
        raise Exception(f"Kraken ticker erro: {data['error']}")
    result_key = list(data['result'].keys())[0]
    tk = data['result'][result_key]
    return {'last': float(tk['c'][0]), 'bid': float(tk['b'][0]), 'ask': float(tk['a'][0])}

# ─────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────
def add_evento(tipo, msg):
    global _eventos
    _eventos.append({'tipo': tipo, 'msg': msg, 'ts': datetime.now().strftime('%H:%M:%S')})
    if len(_eventos) > 200:
        _eventos = _eventos[-200:]

def capturar_erro(ctx, e, par=''):
    estado['erros_capturados'].append({
        'ctx': ctx, 'par': par, 'erro': str(e)[:200],
        'ts': datetime.now().isoformat()
    })
    if len(estado['erros_capturados']) > 50:
        estado['erros_capturados'] = estado['erros_capturados'][-50:]

def registrar_aprendizado(tipo, msg, par=''):
    estado['aprendizados'].append({
        'tipo': tipo, 'msg': msg, 'par': par,
        'ts': datetime.now().strftime('%d/%m %H:%M')
    })
    if len(estado['aprendizados']) > 100:
        estado['aprendizados'] = estado['aprendizados'][-100:]

def atualizar_stats(par, resultado, hora):
    if par not in estado['stats_pares']:
        estado['stats_pares'][par] = {'wins': 0, 'losses': 0}
    if resultado == 'win':
        estado['stats_pares'][par]['wins'] += 1
    else:
        estado['stats_pares'][par]['losses'] += 1

    if hora not in estado['stats_horarios']:
        estado['stats_horarios'][hora] = {'wins': 0, 'losses': 0}
    if resultado == 'win':
        estado['stats_horarios'][hora]['wins'] += 1
    else:
        estado['stats_horarios'][hora]['losses'] += 1

def get_peso_par(par):
    st = estado['stats_pares'].get(par, {})
    w  = st.get('wins', 0)
    l  = st.get('losses', 0)
    total = w + l
    if total < 3:
        return 1.0
    wr = w / total
    if wr >= 0.65:
        return 1.3
    if wr >= 0.55:
        return 1.1
    if wr <= 0.30:
        return 0.7
    return 1.0

def horario_favoravel(par):
    hora   = datetime.now().hour
    minuto = datetime.now().minute
    hora_str = f'{hora:02d}:{minuto:02d}'
    if 0 <= hora < 6:
        return False, 'Madrugada (vol. baixo)'
    if hora in estado['stats_horarios']:
        st  = estado['stats_horarios'][hora_str]
        tot = st.get('wins', 0) + st.get('losses', 0)
        if tot >= 3 and st.get('wins', 0) / tot < 0.35:
            return False, f'Hora historicamente ruim ({int(st["wins"]/tot*100)}% WR)'
    return True, 'OK'

# ─────────────────────────────────────────────
# MACHINE LEARNING ADAPTATIVO
# ─────────────────────────────────────────────
def ajustar_parametros_ml():
    """Ajusta SCORE_MINIMO baseado nos resultados recentes."""
    try:
        total = estado['wins_dia'] + estado['losses_dia']
        if total < 5:
            return  # aguarda dados suficientes

        wr = estado['wins_dia'] / total
        score_atual = estado['score_minimo_atual']

        if wr < 0.35 and score_atual < SCORE_MINIMO_MAX:
            novo = score_atual + 1
            estado['score_minimo_atual'] = novo
            msg = f"ML: WR {wr*100:.0f}% baixo → score mínimo ↑ {score_atual}→{novo}"
            registrar_aprendizado('ML_AJUSTE', msg)
            log.info(msg)
        elif wr > 0.65 and score_atual > SCORE_MINIMO_MIN:
            novo = score_atual - 1
            estado['score_minimo_atual'] = novo
            msg = f"ML: WR {wr*100:.0f}% alto → score mínimo ↓ {score_atual}→{novo}"
            registrar_aprendizado('ML_AJUSTE', msg)
            log.info(msg)

        # Ajuste por par: bloqueia pares com muitas perdas recentes
        for par, st in estado['stats_pares'].items():
            w = st.get('wins', 0)
            l = st.get('losses', 0)
            tot = w + l
            if tot >= 4 and l / tot >= 0.75:
                st['bloqueado'] = True
                registrar_aprendizado('ML_BLOQUEIO', f'{par} bloqueado (perda {int(l/tot*100)}%)', par)
            elif tot >= 6 and w / tot >= 0.60 and st.get('bloqueado'):
                st['bloqueado'] = False
                registrar_aprendizado('ML_DESBLOQUEIO', f'{par} desbloqueado ({int(w/tot*100)}% WR)', par)

        estado['ml_ciclos'] += 1
    except Exception as e:
        log.warning(f"Erro ML: {e}")

# ─────────────────────────────────────────────
# PERSISTENCIA
# ─────────────────────────────────────────────
def salvar_estado():
    try:
        salvar = {k: v for k, v in estado.items() if k != 'pares_detalhes'}
        with open(ARQUIVO_ESTADO, 'w', encoding='utf-8') as f:
            json.dump(salvar, f, indent=2, default=str)
    except Exception as e:
        log.error(f"Erro salvar estado: {e}")

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
                'aprendizados', 'erros_capturados', 'score_minimo_atual', 'ajuste_ml_log', 'ml_ciclos',
            ]
            for campo in campos:
                if campo in salvo:
                    estado[campo] = salvo[campo]
            log.info(f"Estado restaurado | {len(estado['posicoes'])} posicoes")
    except Exception as e:
        log.error(f"Erro carregar estado: {e}")

def salvar_historico():
    try:
        with open(ARQUIVO_HISTORICO, 'w', encoding='utf-8') as f:
            json.dump(historico, f, indent=2, default=str)
    except Exception as e:
        log.error(f"Erro salvar historico: {e}")

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
    except Exception as e:
        log.error(f"Erro carregar historico: {e}")

# ─────────────────────────────────────────────
# INDICADORES
# ─────────────────────────────────────────────
def calcular_rsi(df, periodo=RSI_PERIODO):
    delta = df['close'].diff()
    ganho = delta.clip(lower=0).rolling(periodo).mean()
    perda = (-delta.clip(upper=0)).rolling(periodo).mean()
    rs    = ganho / perda
    return 100 - (100 / (1 + rs))

def calcular_score(par):
    """Retorna score LONG (positivo=comprar) e score SHORT (negativo=vender a descoberto)."""
    try:
        df = fetch_ohlcv(par, interval=TIMEFRAME, limit=288)
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

        cruzou_para_cima  = (ma9_prev <= ma21_prev) and (ma9_atual > ma21_atual)
        cruzou_para_baixo = (ma9_prev >= ma21_prev) and (ma9_atual < ma21_atual)

        # Tendência 1h
        tendencia_alta_1h = True
        try:
            df_1h = fetch_ohlcv(par, timeframe='1h', limit=50)
            df_1h['ma50_1h'] = df_1h['close'].rolling(50).mean()
            tendencia_alta_1h = bool(df_1h['close'].iloc[-1] > df_1h['ma50_1h'].iloc[-1])
        except:
            pass

        # ── SCORE LONG ──────────────────────────
        score_long = 0
        sinal_long = 'NEUTRO'

        if cruzou_para_cima:
            score_long += 3; sinal_long = 'COMPRA'
        elif cruzou_para_baixo:
            score_long -= 2

        if preco > ma50_atual:
            score_long += 2
        if vol_atual >= vol_media:
            score_long += 2

        if 35 <= rsi_val <= 65:
            score_long += 2
        elif rsi_val < 35:
            score_long += 1
        elif rsi_val > RSI_SOBRECOMPRADO:
            score_long -= 3; sinal_long = 'NEUTRO'

        if not tendencia_alta_1h and sinal_long == 'COMPRA':
            sinal_long = 'NEUTRO'; score_long -= 2

        peso = get_peso_par(par)
        bonus = round((peso - 1.0) * 3)
        score_long += bonus

        favoravel, _ = horario_favoravel(par)
        if not favoravel and sinal_long == 'COMPRA':
            sinal_long = 'NEUTRO'; score_long -= 2

        if sinal_long == 'COMPRA' and score_long >= estado['score_minimo_atual']:
            sinal_long = 'COMPRA'
        elif score_long < estado['score_minimo_atual']:
            sinal_long = 'NEUTRO'

        # ── SCORE SHORT ─────────────────────────
        score_short = 0
        sinal_short = 'NEUTRO'

        if cruzou_para_baixo:
            score_short += 3; sinal_short = 'SHORT'
        elif cruzou_para_cima:
            score_short -= 2

        if preco < ma50_atual:
            score_short += 2
        if vol_atual >= vol_media:
            score_short += 1

        if rsi_val > RSI_SOBRECOMPRADO:
            score_short += 3; sinal_short = 'SHORT'
        elif rsi_val > 65:
            score_short += 1
        elif rsi_val < RSI_SOBREVENDIDO:
            score_short -= 3; sinal_short = 'NEUTRO'

        # Filtro: tendência 1h alta cancela SHORT
        if tendencia_alta_1h and sinal_short == 'SHORT':
            score_short -= 3

        # Filtro extra: se BTC está em tendência de alta, bloqueia SHORT em altcoins
        if par != 'BTC/USDT' and sinal_short == 'SHORT':
            try:
                df_btc = fetch_ohlcv('BTC/USDT', interval='1h', limit=50)
                df_btc['ma20'] = df_btc['close'].rolling(20).mean()
                btc_alta = float(df_btc['close'].iloc[-1]) > float(df_btc['ma20'].iloc[-1])
                if btc_alta:
                    score_short -= 2  # mercado em alta geral = SHORT arriscado
            except:
                pass

        # RSI da altcoin precisa estar claramente sobrecomprado para SHORT
        if sinal_short == 'SHORT' and rsi_val < 70:
            score_short -= 2  # sem RSI alto, sinal fraco

        if sinal_short == 'SHORT' and score_short < SCORE_SHORT_MIN:
            sinal_short = 'NEUTRO'

        pico_24h  = float(df['high'].max())
        queda_24h = round((preco - pico_24h) / pico_24h * 100, 2)

        log.info(f"{par} | ${preco:.4f} | LONG:{score_long}({sinal_long}) SHORT:{score_short}({sinal_short}) | RSI:{rsi_val}")

        return {
            'par':        par,
            'score':      score_long,
            'score_long': score_long,
            'score_short': score_short,
            'sinal':      sinal_long,
            'sinal_long': sinal_long,
            'sinal_short': sinal_short,
            'rsi':        rsi_val,
            'preco':      round(float(preco), 6),
            'ma9':        round(float(ma9_atual), 6),
            'ma21':       round(float(ma21_atual), 6),
            'ma50':       round(float(ma50_atual), 6),
            'volume_ok':  bool(vol_atual >= vol_media),
            'queda_24h':  queda_24h,
            'pico_24h':   round(pico_24h, 6),
            'df':         df,
        }
    except Exception as e:
        capturar_erro('calcular_score', e, par)
        log.error(f"Erro score {par}: {e}")
        return {
            'par': par, 'score': -1, 'score_long': -1, 'score_short': -1,
            'sinal': 'ERRO', 'sinal_long': 'NEUTRO', 'sinal_short': 'NEUTRO',
            'rsi': 50.0, 'preco': 0, 'ma9': 0, 'ma21': 0, 'ma50': 0,
            'volume_ok': False, 'queda_24h': 0, 'pico_24h': 0, 'df': None,
        }

# ─────────────────────────────────────────────
# ABRIR POSIÇÃO LONG
# ─────────────────────────────────────────────
def comprar_long(par, rsi):
    try:
        if par in estado['posicoes']:
            return
        if len(estado['posicoes']) >= MAX_POSICOES:
            return
        if estado['stats_pares'].get(par, {}).get('bloqueado'):
            log.info(f"[ML] {par} bloqueado — ignorando LONG")
            return

        ticker = fetch_ticker(par)
        preco  = ticker['last']

        capital_total_calc = estado['capital_atual'] + sum(
            _valor_posicao_usdt(p, preco) for p in estado['posicoes'].values()
        )
        valor_op = max(capital_total_calc * RISCO_POR_TRADE, CAPITAL_BASE)
        capital_livre = estado['capital_atual'] - estado['capital_reserva']
        if valor_op > capital_livre:
            valor_op = capital_livre * 0.90
        if valor_op < CAPITAL_BASE:
            log.warning(f"Capital insuficiente LONG {par}: ${valor_op:.2f}")
            return

        taxa          = valor_op * TAXA_OPERACAO
        valor_liquido = valor_op - taxa
        quantidade    = valor_liquido / preco
        stop_price    = preco * (1 - STOP_LOSS)

        estado['capital_atual'] -= valor_op
        estado['posicoes'][par] = {
            'tipo':          'LONG',
            'preco_entrada': preco,
            'preco_compra':  preco,
            'quantidade':    quantidade,
            'valor_investido': valor_liquido,
            'preco_pico':    preco,
            'preco_vale':    preco,   # usado no short
            'stop_loss':     round(stop_price, 6),
            'entrada':       datetime.now().strftime('%d/%m %H:%M'),
            'abertura':      datetime.now().isoformat(),
        }
        estado['transacoes_dia'] += 1
        salvar_estado()

        log.info(f"[LONG] ABRIU {par} | ${preco:.4f} | ${valor_op:.2f} | RSI:{rsi:.1f}")
        add_evento('COMPRA', f"[LONG] {par} | ${preco:.4f} | ${valor_op:.2f} | RSI:{rsi:.1f}")
        telegram(
            f"🟢 LONG ABERTO\n\nPar: {par}\nPreço: ${preco:.4f}\n"
            f"Valor: ${valor_op:.2f}\nRSI: {rsi:.1f}\n"
            f"Stop: ${stop_price:.4f}\nPosições: {len(estado['posicoes'])}/{MAX_POSICOES}\n"
            f"Capital disp: ${estado['capital_atual']:.2f}"
        )
    except Exception as e:
        capturar_erro('comprar_long', e, par)
        log.error(f"Erro LONG {par}: {e}")

# ─────────────────────────────────────────────
# ABRIR POSIÇÃO SHORT
# ─────────────────────────────────────────────
def abrir_short(par, rsi):
    """SHORT simulado: lucra quando preço cai."""
    try:
        if par in estado['posicoes']:
            return
        if len(estado['posicoes']) >= MAX_POSICOES:
            return
        if estado['stats_pares'].get(par, {}).get('bloqueado'):
            log.info(f"[ML] {par} bloqueado — ignorando SHORT")
            return

        ticker = fetch_ticker(par)
        preco  = ticker['last']

        capital_total_calc = estado['capital_atual'] + sum(
            _valor_posicao_usdt(p, preco) for p in estado['posicoes'].values()
        )
        valor_op = max(capital_total_calc * RISCO_POR_TRADE, CAPITAL_BASE)
        capital_livre = estado['capital_atual'] - estado['capital_reserva']
        if valor_op > capital_livre:
            valor_op = capital_livre * 0.90
        if valor_op < CAPITAL_BASE:
            log.warning(f"Capital insuficiente SHORT {par}: ${valor_op:.2f}")
            return

        taxa          = valor_op * TAXA_OPERACAO
        valor_liquido = valor_op - taxa
        # Simulamos "vender a descoberto" pelo preço atual
        # Qtd = quanto vendemos em criptomoeda
        quantidade = valor_liquido / preco
        stop_price = preco * (1 + STOP_LOSS)  # stop ACIMA da entrada no short

        estado['capital_atual'] -= valor_op
        estado['posicoes'][par] = {
            'tipo':          'SHORT',
            'preco_entrada': preco,
            'preco_compra':  preco,
            'quantidade':    quantidade,
            'valor_investido': valor_liquido,
            'preco_pico':    preco,    # no short, "pico" é o preço mais alto (pior)
            'preco_vale':    preco,    # mínimo atingido (favorável para short)
            'stop_loss':     round(stop_price, 6),
            'entrada':       datetime.now().strftime('%d/%m %H:%M'),
            'abertura':      datetime.now().isoformat(),
        }
        estado['transacoes_dia'] += 1
        salvar_estado()

        log.info(f"[SHORT] ABRIU {par} | ${preco:.4f} | ${valor_op:.2f} | RSI:{rsi:.1f}")
        add_evento('SHORT', f"[SHORT] {par} | ${preco:.4f} | ${valor_op:.2f} | RSI:{rsi:.1f}")
        telegram(
            f"🔴 SHORT ABERTO\n\nPar: {par}\nPreço: ${preco:.4f}\n"
            f"Valor: ${valor_op:.2f}\nRSI: {rsi:.1f}\n"
            f"Stop: ${stop_price:.4f}\nPosições: {len(estado['posicoes'])}/{MAX_POSICOES}\n"
            f"Capital disp: ${estado['capital_atual']:.2f}"
        )
    except Exception as e:
        capturar_erro('abrir_short', e, par)
        log.error(f"Erro SHORT {par}: {e}")

# ─────────────────────────────────────────────
# FECHAR POSIÇÃO (LONG ou SHORT)
# ─────────────────────────────────────────────
def _valor_posicao_usdt(pos, preco_atual=None):
    """Valor atual da posição em USDT."""
    if preco_atual is None:
        return pos.get('valor_investido', 0)
    if pos['tipo'] == 'LONG':
        return preco_atual * pos['quantidade'] * (1 - TAXA_OPERACAO)
    else:  # SHORT
        # P&L SHORT: lucro quando preço cai
        # receita = valor_investido + (entrada - atual) * qtd
        pnl = (pos['preco_entrada'] - preco_atual) * pos['quantidade']
        return pos['valor_investido'] + pnl * (1 - TAXA_OPERACAO)

def fechar_posicao(par, motivo="SINAL"):
    try:
        if par not in estado['posicoes']:
            return

        pos    = estado['posicoes'][par]
        ticker = fetch_ticker(par)
        preco  = ticker['last']
        tipo   = pos['tipo']

        valor_investido = pos['valor_investido']
        taxa_saida      = valor_investido * TAXA_OPERACAO

        if tipo == 'LONG':
            receita_bruta   = preco * pos['quantidade']
            taxa_venda      = receita_bruta * TAXA_OPERACAO
            receita_liquida = receita_bruta - taxa_venda
            lucro_liquido   = receita_liquida - valor_investido
        else:  # SHORT
            # Lucro = (entrada - saída) * qtd - taxa
            pnl_bruto       = (pos['preco_entrada'] - preco) * pos['quantidade']
            receita_liquida = valor_investido + pnl_bruto - taxa_saida
            lucro_liquido   = receita_liquida - valor_investido

        lucro_pct = (lucro_liquido / valor_investido) * 100
        now_str   = datetime.now().isoformat()

        historico.append({
            'par':           par,
            'tipo':          tipo,
            'motivo_saida':  motivo,
            'preco_entrada': round(pos['preco_entrada'], 6),
            'preco_saida':   round(preco, 6),
            'quantidade':    round(pos['quantidade'], 6),
            'valor_investido': round(valor_investido, 4),
            'receita':       round(receita_liquida, 4),
            'pnl_pct':       round(lucro_pct, 2),
            'pnl_usdt':      round(lucro_liquido, 4),
            'resultado':     'win' if lucro_liquido >= 0 else 'loss',
            'abertura':      pos.get('abertura', now_str),
            'fechamento':    now_str,
            'data':          datetime.now().strftime('%d/%m %H:%M'),
        })
        salvar_historico()

        del estado['posicoes'][par]
        estado['capital_atual'] += receita_liquida
        estado['capital_reserva'] = estado['capital_atual'] * RESERVA_PCT
        if not estado['posicoes']:
            estado['reserva_usada'] = False

        hora_trade = datetime.now().strftime('%H:%M')
        atualizar_stats(par, 'win' if lucro_liquido >= 0 else 'loss', hora_trade)

        if lucro_liquido >= 0:
            estado['lucros_dia']  += lucro_liquido
            estado['wins_dia']    += 1
            estado['stops_consecutivos'] = 0
            emoji = '✅'; resultado_txt = 'LUCRO'
        else:
            estado['perdas_dia']  += lucro_liquido
            estado['losses_dia']  += 1
            emoji = '❌'; resultado_txt = 'PERDA'
            if motivo in ("STOP LOSS", "STOP SHORT"):
                estado['stops_consecutivos'] += 1
                if estado['stops_consecutivos'] >= STOPS_CONSECUTIVOS_MAX:
                    pausado_ate = (datetime.now() + timedelta(hours=1)).isoformat()
                    estado['pausado_ate'] = pausado_ate
                    registrar_aprendizado('PAUSA', f"3 stops — pausado 1h")
                    telegram(f"⚠️ 3 STOPS SEGUIDOS — pausado 1h")

        salvar_estado()
        ajustar_parametros_ml()

        log.info(f"FECHOU {tipo} {par} [{motivo}] | ${preco:.4f} | {lucro_pct:+.2f}% | ${lucro_liquido:+.2f}")
        add_evento('VENDA', f"{tipo} {par} [{motivo}] | {lucro_pct:+.2f}% | ${lucro_liquido:+.2f}")
        telegram(
            f"{emoji} {resultado_txt} — {tipo} [{motivo}]\n\nPar: {par}\n"
            f"Preço saída: ${preco:.4f}\nP&L: {lucro_pct:+.2f}% (${lucro_liquido:+.2f})\n"
            f"Capital: ${estado['capital_atual']:.2f}"
        )
    except Exception as e:
        capturar_erro('fechar_posicao', e, par)
        log.error(f"Erro fechar {par}: {e}")

# ─────────────────────────────────────────────
# CHECAR RISCO — trailing stop para LONG e SHORT
# ─────────────────────────────────────────────
def checar_risco(par):
    if par not in estado['posicoes']:
        return False

    pos  = estado['posicoes'][par]
    tipo = pos['tipo']

    try:
        ticker = fetch_ticker(par)
        preco  = ticker['last']
    except:
        return False

    if tipo == 'LONG':
        # Atualiza pico
        if preco > pos['preco_pico']:
            estado['posicoes'][par]['preco_pico'] = preco

        lucro      = (preco - pos['preco_entrada']) / pos['preco_entrada']
        lucro_pico = (pos['preco_pico'] - pos['preco_entrada']) / pos['preco_entrada']
        trailing   = (pos['preco_pico'] - preco) / pos['preco_pico']

        if lucro <= -STOP_LOSS:
            add_evento('ALERTA', f"STOP LOSS LONG {par} | {lucro*100:.2f}%")
            fechar_posicao(par, motivo="STOP LOSS")
            return True

        if lucro_pico >= 0.005:
            if lucro_pico >= 0.03:
                trailing_din = 0.002
            elif lucro_pico >= 0.02:
                trailing_din = 0.003
            elif lucro_pico >= 0.01:
                trailing_din = 0.005
            else:
                trailing_din = TRAILING_STOP

            if trailing >= trailing_din and lucro >= LUCRO_MINIMO_SAIDA:
                fechar_posicao(par, motivo="TRAILING STOP")
                return True

    else:  # SHORT
        # Para short: atualiza vale (mínimo atingido = mais lucrativo)
        if preco < pos['preco_vale']:
            estado['posicoes'][par]['preco_vale'] = preco

        lucro     = (pos['preco_entrada'] - preco) / pos['preco_entrada']  # positivo = queda
        lucro_min = (pos['preco_entrada'] - pos['preco_vale']) / pos['preco_entrada']
        # trailing = o quanto o preço subiu desde o mínimo
        trailing  = (preco - pos['preco_vale']) / pos['preco_entrada'] if pos['preco_vale'] > 0 else 0

        # Stop: preço subiu 1.5% desde entrada (prejuízo)
        if lucro <= -STOP_LOSS:
            add_evento('ALERTA', f"STOP SHORT {par} | {lucro*100:.2f}%")
            fechar_posicao(par, motivo="STOP SHORT")
            return True

        # Trailing: preço subiu desde o mínimo (revertendo ganho)
        if lucro_min >= 0.005:
            if lucro_min >= 0.03:
                trailing_din = 0.002
            elif lucro_min >= 0.02:
                trailing_din = 0.003
            elif lucro_min >= 0.01:
                trailing_din = 0.005
            else:
                trailing_din = TRAILING_STOP

            if trailing >= trailing_din and lucro >= LUCRO_MINIMO_SAIDA:
                fechar_posicao(par, motivo="TRAILING SHORT")
                return True

    return False

# ─────────────────────────────────────────────
# FLASK API
# ─────────────────────────────────────────────
app = Flask(__name__)
CORS(app, origins="*")

@app.after_request
def add_headers(response):
    response.headers['ngrok-skip-browser-warning'] = 'true'
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response

@app.route('/status')
def get_status():
    total   = estado['wins_dia'] + estado['losses_dia']
    winrate = (estado['wins_dia'] / total * 100) if total > 0 else 0

    posicoes_info = {}
    valor_posicoes = 0.0

    for par, pos in estado['posicoes'].items():
        preco_atual = estado['pares_detalhes'].get(par, {}).get('preco', pos['preco_entrada'])
        tipo = pos['tipo']

        valor_investido = pos.get('valor_investido', pos['preco_entrada'] * pos['quantidade'])

        if tipo == 'LONG':
            receita_atual = preco_atual * pos['quantidade'] * (1 - TAXA_OPERACAO)
            pnl_usdt      = receita_atual - valor_investido
        else:  # SHORT
            pnl_usdt    = (pos['preco_entrada'] - preco_atual) * pos['quantidade'] * (1 - TAXA_OPERACAO)
            receita_atual = valor_investido + pnl_usdt

        pnl_pct = (pnl_usdt / valor_investido) * 100 if valor_investido else 0
        valor_posicoes += receita_atual

        posicoes_info[par] = {
            'tipo':          tipo,
            'preco_entrada': round(pos['preco_entrada'], 6),
            'preco_atual':   round(preco_atual, 6),
            'preco_pico':    round(pos.get('preco_pico', pos['preco_entrada']), 6),
            'preco_vale':    round(pos.get('preco_vale', pos['preco_entrada']), 6),
            'stop_loss':     round(pos.get('stop_loss', 0), 6),
            'quantidade':    round(pos['quantidade'], 6),
            'valor_investido': round(valor_investido, 4),
            'pnl_pct':       round(pnl_pct, 2),
            'pnl_usdt':      round(pnl_usdt, 4),
            'abertura':      pos.get('abertura', ''),
            'entrada':       pos.get('entrada', ''),
        }

    capital_disponivel = round(estado['capital_atual'], 2)
    capital_total      = round(estado['capital_atual'] + valor_posicoes, 2)
    capital_inicial    = round(estado['capital_inicial'] or CAPITAL_SIMULADO, 2)
    pnl_total          = round(capital_total - capital_inicial, 2)

    return jsonify({
        'versao':            'v3',
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
        'capital_atual':     capital_total,
        'capital_disponivel': capital_disponivel,
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
        # ML info
        'score_minimo_atual': estado['score_minimo_atual'],
        'ml_ciclos':         estado['ml_ciclos'],
    })

@app.route('/historico')
def get_historico():
    return jsonify(historico[-100:])

@app.route('/feed')
def get_feed():
    sinais = []
    for par in PARES:
        det        = estado['pares_detalhes'].get(par, {})
        score_long = det.get('score_long', det.get('score', 0))
        score_short = det.get('score_short', 0)
        sinal      = 'SHORT' if det.get('sinal_short') == 'SHORT' else ('COMPRA' if det.get('sinal_long') == 'COMPRA' else 'NEUTRO')
        sinais.append({
            'par':         par,
            'preco':       det.get('preco', 0),
            'rsi':         det.get('rsi', 0),
            'score':       score_long,
            'score_long':  score_long,
            'score_short': score_short,
            'sinal':       sinal,
            'sinal_long':  det.get('sinal_long', 'NEUTRO'),
            'sinal_short': det.get('sinal_short', 'NEUTRO'),
        })
    sinais.sort(key=lambda x: max(abs(x['score_long']), abs(x['score_short'])), reverse=True)
    return jsonify(sinais)

@app.route('/stats')
def get_stats():
    total_trades = estado['wins_dia'] + estado['losses_dia']
    pares_stats  = {}
    long_trades  = sum(1 for t in historico if t.get('tipo') == 'LONG')
    short_trades = sum(1 for t in historico if t.get('tipo') == 'SHORT')
    for t in historico:
        p = t.get('par', '')
        if p not in pares_stats:
            pares_stats[p] = {'pnl_total': 0.0, 'total': 0}
        pares_stats[p]['pnl_total'] += t.get('pnl_usdt', 0)
        pares_stats[p]['total']     += 1
    return jsonify({
        'total_trades':   total_trades,
        'trades_lucro':   estado['wins_dia'],
        'trades_stop':    estado['losses_dia'],
        'long_trades':    long_trades,
        'short_trades':   short_trades,
        'capital_atual':  round(estado['capital_atual'], 2),
        'pnl_total':      round(estado['lucros_dia'] - abs(estado['perdas_dia']), 2),
        'pares':          pares_stats,
        'stats_pares':    estado['stats_pares'],
        'aprendizados':   estado['aprendizados'],
        'erros':          estado['erros_capturados'][-10:],
        'score_minimo_atual': estado['score_minimo_atual'],
        'ml_ciclos':      estado['ml_ciclos'],
        'ajuste_ml_log':  estado.get('ajuste_ml_log', [])[-10:],
    })

# ─────────────────────────────────────────────
# DASHBOARD HTML v3
# ─────────────────────────────────────────────
DASHBOARD_HTML = r"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Trading Bot v3</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
:root{
  --bg:#0d1117;--surface:#161b22;--surface2:#21262d;--border:#30363d;
  --fg:#e6edf3;--fg2:#8b949e;--accent:#58a6ff;
  --green:#3fb950;--green-dim:#1a4a26;
  --red:#f85149;--red-dim:#4a1a1a;
  --orange:#f0883e;--purple:#bc8cff;
  color-scheme:dark
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--fg);font-family:'Inter',system-ui,sans-serif;font-size:14px;line-height:1.5;padding:0 16px 32px;min-height:100vh}
header{display:flex;align-items:center;gap:12px;padding:16px 0 20px;border-bottom:1px solid var(--border);margin-bottom:20px;flex-wrap:wrap}
.logo{font-size:20px;font-weight:700;letter-spacing:-.5px}.logo span{color:var(--accent)}
.badge{font-size:11px;font-weight:600;padding:2px 8px;border-radius:20px;background:var(--green-dim);color:var(--green);display:flex;align-items:center;gap:5px}
.badge::before{content:'';width:6px;height:6px;border-radius:50%;background:var(--green);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.badge.offline{background:var(--red-dim);color:var(--red)}.badge.offline::before{background:var(--red);animation:none}
.ml-badge{font-size:11px;padding:2px 10px;border-radius:20px;background:#bc8cff22;border:1px solid #bc8cff44;color:var(--purple)}
.refresh-btn{margin-left:auto;background:var(--surface2);border:1px solid var(--border);color:var(--fg2);padding:6px 14px;border-radius:6px;font-size:12px;font-family:inherit;cursor:pointer;display:flex;align-items:center;gap:6px;transition:color .15s,border-color .15s}
.refresh-btn:hover{color:var(--fg);border-color:var(--accent)}
.last-update{font-size:11px;color:var(--fg2)}
.kpi-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:20px}
.kpi{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px 18px}
.kpi-label{font-size:11px;color:var(--fg2);text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px}
.kpi-value{font-size:22px;font-weight:700;font-variant-numeric:tabular-nums;line-height:1.1}
.kpi-sub{font-size:11px;color:var(--fg2);margin-top:4px}
.pos{color:var(--green)}.neg{color:var(--red)}.neu{color:var(--accent)}.ml{color:var(--purple)}
.grid-2{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px}
.grid-3{display:grid;grid-template-columns:2fr 1fr;gap:14px;margin-bottom:14px}
@media(max-width:680px){.grid-2,.grid-3{grid-template-columns:1fr}}
.card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px 18px}
.card-title{font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:.07em;color:var(--fg2);margin-bottom:14px;display:flex;align-items:center;gap:8px}
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
.pill{font-size:10px;font-weight:700;padding:2px 7px;border-radius:10px;text-transform:uppercase;letter-spacing:.04em;white-space:nowrap}
.pill-long{background:var(--green-dim);color:var(--green)}
.pill-short{background:var(--red-dim);color:var(--red)}
.pill-neutro{background:#21262d;color:var(--fg2)}
.pair-bar-list{display:flex;flex-direction:column;gap:8px}
.pair-row{display:flex;align-items:center;gap:8px;font-size:12px}
.pair-name{width:72px;font-weight:600;color:var(--fg);flex-shrink:0}
.bar-track{flex:1;height:8px;background:var(--surface2);border-radius:4px;overflow:hidden}
.bar-fill{height:100%;border-radius:4px;transition:width .4s ease}
.pair-val{width:60px;text-align:right;font-variant-numeric:tabular-nums;font-family:'JetBrains Mono',monospace;font-size:11px}
.pos-list{display:flex;flex-direction:column;gap:8px}
.pos-item{border-radius:8px;padding:10px 12px;border:1px solid var(--border);display:grid;grid-template-columns:auto 1fr auto auto;gap:6px 10px;align-items:center}
.pos-item.long-pos{border-left:3px solid var(--green)}
.pos-item.short-pos{border-left:3px solid var(--red)}
.pos-par{font-weight:700}.pos-meta{color:var(--fg2);font-size:11px;grid-column:1/-1}
.state-msg{color:var(--fg2);font-size:13px;text-align:center;padding:32px 0}
.ml-section{background:var(--surface2);border-radius:8px;padding:10px 12px;font-size:12px;color:var(--fg2);margin-top:8px}
.ml-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px}
.ml-row:last-child{margin-bottom:0}
.mb14{margin-bottom:14px}
</style></head><body>
<header>
  <div class="logo">Trading <span>v3</span></div>
  <div class="badge offline" id="status-badge">Conectando…</div>
  <div class="ml-badge" id="ml-badge">ML —</div>
  <span class="last-update" id="last-update"></span>
  <button class="refresh-btn" onclick="fetchAll()">&#x21bb; Atualizar</button>
</header>

<div class="kpi-row">
  <div class="kpi"><div class="kpi-label">Capital</div><div class="kpi-value neu" id="kpi-capital">—</div><div class="kpi-sub">Início: USDT 100,00</div></div>
  <div class="kpi"><div class="kpi-label">P&amp;L Total</div><div class="kpi-value" id="kpi-pnl">—</div><div class="kpi-sub" id="kpi-pnl-sub"></div></div>
  <div class="kpi"><div class="kpi-label">Trades</div><div class="kpi-value" id="kpi-trades">—</div><div class="kpi-sub" id="kpi-trades-sub"></div></div>
  <div class="kpi"><div class="kpi-label">Win Rate</div><div class="kpi-value" id="kpi-wr">—</div><div class="kpi-sub" id="kpi-wr-sub"></div></div>
  <div class="kpi"><div class="kpi-label">Posições</div><div class="kpi-value neu" id="kpi-pos">—</div><div class="kpi-sub" id="kpi-pos-max">Máx: —</div></div>
  <div class="kpi"><div class="kpi-label">Long / Short</div><div class="kpi-value" id="kpi-ls">—</div><div class="kpi-sub" id="kpi-ls-sub"></div></div>
</div>

<div class="grid-3 mb14">
  <div class="card">
    <div class="card-title">Evolução do Capital (USDT)</div>
    <div class="chart-wrap"><canvas id="chart-capital"></canvas></div>
    <div class="state-msg" id="capital-msg" hidden></div>
  </div>
  <div class="card">
    <div class="card-title">Sinais Recentes</div>
    <div class="feed-list" id="feed-list"><div class="state-msg">Carregando…</div></div>
  </div>
</div>

<div class="grid-2 mb14">
  <div class="card">
    <div class="card-title">P&amp;L por Par</div>
    <div class="pair-bar-list" id="pair-bars"><div class="state-msg">Carregando…</div></div>
  </div>
  <div class="card">
    <div class="card-title">Posições Abertas</div>
    <div class="pos-list" id="pos-list"><div class="state-msg">Nenhuma posição aberta</div></div>
    <div class="ml-section" id="ml-info" hidden>
      <div class="ml-row"><span>Score mínimo (ML)</span><span id="ml-score" class="ml">—</span></div>
      <div class="ml-row"><span>Ciclos de ajuste</span><span id="ml-ciclos" class="ml">—</span></div>
    </div>
  </div>
</div>

<div class="card">
  <div class="card-title">Últimas Operações</div>
  <div class="tbl-wrap"><table><thead><tr>
    <th>Par</th><th>Tipo</th><th>Direção</th><th>Entrada</th><th>Saída</th><th>P&amp;L</th><th>P&amp;L %</th><th>Motivo</th><th>Abertura</th>
  </tr></thead>
  <tbody id="hist-body"><tr><td colspan="9" style="text-align:center;color:var(--fg2);padding:24px">Carregando…</td></tr></tbody></table></div>
</div>

<script>
let capChart=null;
function fmt(n,d=2){if(n==null||isNaN(n))return'—';return Number(n).toLocaleString('pt-BR',{minimumFractionDigits:d,maximumFractionDigits:d})}
function fmtPct(n){if(n==null||isNaN(n))return'—';return(n>=0?'+':'')+fmt(n)+'%'}
function cc(n){return n>0?'pos':n<0?'neg':''}
function ts(iso){if(!iso)return'—';try{return new Date(iso).toLocaleString('pt-BR',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'})}catch{return iso}}
function setOnline(ok){const b=document.getElementById('status-badge');if(ok){b.textContent='Rodando';b.className='badge'}else{b.textContent='Offline';b.className='badge offline'}}

function renderKPIs(s,st){
  const cap=s?.capital_atual??100;
  const pnl=s?.pnl_total??0;
  const venced=s?.wins_dia??0;const perdeu=s?.losses_dia??0;
  const trades=venced+perdeu;
  const wr=trades>0?(venced/trades*100):null;
  const posA=Object.keys(s?.posicoes??{}).length;
  const longA=Object.values(s?.posicoes??{}).filter(p=>p.tipo==='LONG').length;
  const shortA=Object.values(s?.posicoes??{}).filter(p=>p.tipo==='SHORT').length;
  const longT=st?.long_trades??0;const shortT=st?.short_trades??0;

  document.getElementById('kpi-capital').textContent='USDT '+fmt(cap);
  const pe=document.getElementById('kpi-pnl');pe.textContent=(pnl>=0?'+':'')+fmt(pnl)+' USDT';pe.className='kpi-value '+cc(pnl);
  document.getElementById('kpi-pnl-sub').textContent=fmtPct((cap-100));
  document.getElementById('kpi-trades').textContent=trades;
  document.getElementById('kpi-trades-sub').textContent=venced+' lucro · '+perdeu+' stop';
  const we=document.getElementById('kpi-wr');we.textContent=wr!=null?fmt(wr,1)+'%':'—';we.className='kpi-value '+(wr>=55?'pos':wr<45?'neg':'neu');
  document.getElementById('kpi-wr-sub').textContent=wr!=null?venced+' de '+trades+' ganhos':'Sem trades ainda';
  document.getElementById('kpi-pos').textContent=posA;
  document.getElementById('kpi-pos-max').textContent='Máx: '+(s?.max_posicoes??'—');
  const lse=document.getElementById('kpi-ls');
  lse.innerHTML=`<span style="color:var(--green)">${longA}L</span> / <span style="color:var(--red)">${shortA}S</span>`;
  document.getElementById('kpi-ls-sub').textContent=`Total: ${longT} long · ${shortT} short`;

  // ML badge
  const sm=s?.score_minimo_atual??3;
  document.getElementById('ml-badge').textContent=`ML Score: ${sm}`;
  const mi=document.getElementById('ml-info');mi.hidden=false;
  document.getElementById('ml-score').textContent=sm;
  document.getElementById('ml-ciclos').textContent=s?.ml_ciclos??0;
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
  el.innerHTML=items.map(x=>{
    const sl=x.sinal_long,ss=x.sinal_short;
    let pill='';
    if(sl==='COMPRA')pill=`<span class="pill pill-long">LONG</span>`;
    else if(ss==='SHORT')pill=`<span class="pill pill-short">SHORT</span>`;
    else pill=`<span class="pill pill-neutro">NEUTRO</span>`;
    return`<div class="feed-item"><span class="feed-par">${x.par||'—'}</span><span class="feed-info">U$ ${fmt(x.preco,4)} · RSI ${fmt(x.rsi,1)}</span><span class="feed-score" style="color:var(--accent)">L:${x.score_long??'?'} S:${x.score_short??'?'}</span>${pill}</div>`;
  }).join('');
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
  el.innerHTML=entries.map(([par,p])=>{
    const pu=p.pnl_usdt??0;
    const tipo=p.tipo||'LONG';
    const cls=tipo==='LONG'?'long-pos':'short-pos';
    const pill=tipo==='LONG'?'<span class="pill pill-long">LONG</span>':'<span class="pill pill-short">SHORT</span>';
    return`<div class="pos-item ${cls}">${pill}<span class="pos-par">${par}</span><span class="${cc(pu)}" style="font-weight:700">${pu>=0?'+':''}${fmt(pu)} USDT</span><span class="${cc(pu)}" style="font-size:12px">${fmtPct(p.pnl_pct??0)}</span><span class="pos-meta">Entrada: U$ ${fmt(p.preco_entrada,4)} · Stop: U$ ${fmt(p.stop_loss,4)} · ${ts(p.abertura)}</span></div>`;
  }).join('');
}

function renderHist(h){
  const tbody=document.getElementById('hist-body');const rows=(h||[]).slice(0,30);
  if(!rows.length){tbody.innerHTML='<tr><td colspan="9" style="text-align:center;color:var(--fg2);padding:24px">Sem histórico ainda</td></tr>';return}
  tbody.innerHTML=rows.map(r=>{
    const pnl=r.pnl_usdt??0;const cls=cc(pnl);
    const tipo=r.tipo||'LONG';
    const dirPill=tipo==='LONG'?'<span class="pill pill-long">LONG</span>':'<span class="pill pill-short">SHORT</span>';
    return`<tr>
      <td style="font-weight:600">${r.par}</td>
      <td><span class="pill ${r.tipo==='LONG'?'pill-long':'pill-short'}">${r.tipo||'—'}</span></td>
      <td>${dirPill}</td>
      <td style="font-family:'JetBrains Mono',monospace;font-size:12px">U$ ${fmt(r.preco_entrada,4)}</td>
      <td style="font-family:'JetBrains Mono',monospace;font-size:12px">${r.preco_saida?'U$ '+fmt(r.preco_saida,4):'<span style="color:var(--fg2)">Aberta</span>'}</td>
      <td class="${cls}" style="font-weight:600">${pnl>=0?'+':''}${fmt(pnl)}</td>
      <td class="${cls}">${fmtPct(r.pnl_pct??0)}</td>
      <td style="color:var(--fg2);font-size:12px">${r.motivo_saida||'—'}</td>
      <td style="color:var(--fg2);font-size:12px">${ts(r.abertura)}</td>
    </tr>`;
  }).join('');
}

async function fetchAll(){
  document.getElementById('last-update').textContent='Atualizando…';
  try{
    const [s,h,f,st]=await Promise.allSettled([
      fetch('/status').then(r=>r.json()),
      fetch('/historico').then(r=>r.json()),
      fetch('/feed').then(r=>r.json()),
      fetch('/stats').then(r=>r.json())
    ]);
    const sv=s.status==='fulfilled'?s.value:null;
    const hv=h.status==='fulfilled'?(Array.isArray(h.value)?h.value:[]):[];
    const fv=f.status==='fulfilled'?(Array.isArray(f.value)?f.value:[]):[];
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
    return jsonify({'ok': True, 'versao': 'v3', 'uptime': estado['ultimo_update']})

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
            requests.post(url, json={'chat_id': chat_id, 'text': msg, 'parse_mode': 'HTML'}, timeout=10)
        except Exception as e:
            log.warning(f"Erro Telegram: {e}")

# ─────────────────────────────────────────────
# RESET DIÁRIO
# ─────────────────────────────────────────────
def checar_reset_diario():
    hoje = datetime.now().date().isoformat()
    if hoje != estado['ultimo_reset']:
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
        log.info("Reset diário")
        telegram(f"📅 Novo dia — Bot v3\nCapital: ${estado['capital_atual']:.2f}\nScore ML: {estado['score_minimo_atual']}")

def limite_diario_atingido():
    if estado['capital_inicial'] is None:
        return False
    limite = (estado['capital_inicial'] or CAPITAL_SIMULADO) * LIMITE_PERDA_DIARIA
    return abs(estado['perdas_dia']) >= limite

# ─────────────────────────────────────────────
# INICIALIZAÇÃO
# ─────────────────────────────────────────────
def sincronizar():
    if estado['capital_inicial'] is None:
        estado['capital_inicial'] = CAPITAL_SIMULADO
        estado['capital_atual']   = CAPITAL_SIMULADO
        estado['capital_reserva'] = CAPITAL_SIMULADO * RESERVA_PCT
    estado['status'] = 'rodando'
    salvar_estado()
    log.info(f"Bot v3 | Capital: ${estado['capital_atual']:.2f} | Posições: {len(estado['posicoes'])}/{MAX_POSICOES}")
    telegram(
        f"🤖 Bot v3 iniciado!\n\n"
        f"Modo: LONG + SHORT simulado\n"
        f"ML: Score mínimo {estado['score_minimo_atual']}\n"
        f"Capital: ${estado['capital_atual']:.2f}\n"
        f"Posições abertas: {len(estado['posicoes'])}/{MAX_POSICOES}\n"
        f"Pares: {len(PARES)} monitorados"
    )

# ─────────────────────────────────────────────
# LOOP PRINCIPAL
# ─────────────────────────────────────────────
def rodar_bot():
    log.info("Bot v3 iniciando...")
    carregar_estado()
    carregar_historico()
    sincronizar()

    while True:
        try:
            checar_reset_diario()

            if limite_diario_atingido():
                for par in list(estado['posicoes'].keys()):
                    fechar_posicao(par, motivo="LIMITE DIARIO")
                estado['status'] = 'encerrado'
                salvar_estado()
                log.info("Limite diário — pausado")
                time.sleep(3600)
                continue

            if estado.get('pausado_ate'):
                if datetime.now().isoformat() < estado['pausado_ate']:
                    log.info(f"Pausado até {estado['pausado_ate'][11:16]}")
                    time.sleep(60)
                    continue
                else:
                    estado['pausado_ate'] = None
                    estado['stops_consecutivos'] = 0
                    log.info("Pausa encerrada")
                    telegram("▶️ Pausa encerrada — retomando")

            estado['ultimo_update'] = datetime.now().isoformat()

            # ── Monitora posições abertas ────────────
            for par in list(estado['posicoes'].keys()):
                try:
                    pos  = estado['posicoes'][par]
                    tipo = pos['tipo']
                    try:
                        ticker = fetch_ticker(par)
                        preco  = ticker['last']
                    except:
                        continue

                    if tipo == 'LONG':
                        pnl = (preco - pos['preco_entrada']) / pos['preco_entrada'] * 100
                    else:
                        pnl = (pos['preco_entrada'] - preco) / pos['preco_entrada'] * 100

                    add_evento('INFO', f"[{tipo}] {par} | ${preco:.4f} | P&L: {pnl:+.2f}%")

                    ja_fechou = checar_risco(par)

                    if not ja_fechou:
                        # Verifica sinal contrário
                        res = calcular_score(par)
                        estado['pares_detalhes'][par] = {
                            'preco':       res['preco'],
                            'score':       res['score'],
                            'score_long':  res['score_long'],
                            'score_short': res['score_short'],
                            'sinal':       res['sinal'],
                            'sinal_long':  res['sinal_long'],
                            'sinal_short': res['sinal_short'],
                            'rsi':         res['rsi'],
                            'ma9':         res['ma9'],
                            'ma21':        res['ma21'],
                            'ma50':        res['ma50'],
                            'volume_ok':   bool(res['volume_ok']),
                            'queda_24h':   res.get('queda_24h', 0),
                        }
                        if tipo == 'LONG' and res['sinal_long'] == 'VENDA':
                            lucro_atual = (preco - pos['preco_entrada']) / pos['preco_entrada']
                            if lucro_atual >= LUCRO_MINIMO_VENDA:
                                fechar_posicao(par, motivo="SINAL MA")
                        elif tipo == 'SHORT' and res['sinal_short'] == 'COMPRA':
                            lucro_atual = (pos['preco_entrada'] - preco) / pos['preco_entrada']
                            if lucro_atual >= LUCRO_MINIMO_VENDA:
                                fechar_posicao(par, motivo="SINAL MA SHORT")
                except Exception as e:
                    capturar_erro('loop_monit', e, par)

            # ── Analisa pares sem posição ────────────
            pares_sem_posicao = [p for p in PARES if p not in estado['posicoes']]
            slots = MAX_POSICOES - len(estado['posicoes'])

            if slots > 0 and pares_sem_posicao:
                resultados = []
                for par in pares_sem_posicao:
                    r = calcular_score(par)
                    resultados.append(r)
                    estado['scores'][par] = r['score']
                    estado['pares_detalhes'][par] = {
                        'preco':       r['preco'],
                        'score':       r['score'],
                        'score_long':  r['score_long'],
                        'score_short': r['score_short'],
                        'sinal':       r['sinal'],
                        'sinal_long':  r['sinal_long'],
                        'sinal_short': r['sinal_short'],
                        'rsi':         r['rsi'],
                        'ma9':         r['ma9'],
                        'ma21':        r['ma21'],
                        'ma50':        r['ma50'],
                        'volume_ok':   bool(r['volume_ok']),
                        'queda_24h':   r.get('queda_24h', 0),
                    }

                # LONG candidatos
                long_cand = [r for r in resultados if r['sinal_long'] == 'COMPRA' and r['score_long'] >= estado['score_minimo_atual']]
                long_cand.sort(key=lambda x: x['score_long'], reverse=True)

                # SHORT candidatos
                short_cand = [r for r in resultados if r['sinal_short'] == 'SHORT' and r['score_short'] >= SCORE_SHORT_MIN]
                short_cand.sort(key=lambda x: x['score_short'], reverse=True)

                # Intercala LONG e SHORT para diversificar
                usados = set()
                acoesLong  = iter(long_cand)
                acoesShort = iter(short_cand)
                abertos = 0

                while slots > 0 and abertos < slots:
                    tentou_long  = False
                    tentou_short = False

                    try:
                        c = next(acoesLong)
                        if c['par'] not in usados and c['par'] not in estado['posicoes']:
                            estado['sinal_atual'] = f"LONG {c['par']}"
                            estado['rsi_atual']   = c['rsi']
                            estado['preco_atual'] = c['preco']
                            comprar_long(c['par'], c['rsi'])
                            usados.add(c['par'])
                            slots -= 1
                        tentou_long = True
                    except StopIteration:
                        pass

                    try:
                        c = next(acoesShort)
                        if c['par'] not in usados and c['par'] not in estado['posicoes'] and slots > 0:
                            estado['sinal_atual'] = f"SHORT {c['par']}"
                            estado['rsi_atual']   = c['rsi']
                            estado['preco_atual'] = c['preco']
                            abrir_short(c['par'], c['rsi'])
                            usados.add(c['par'])
                            slots -= 1
                        tentou_short = True
                    except StopIteration:
                        pass

                    if not tentou_long and not tentou_short:
                        break

                    abertos += 1

                if not long_cand and not short_cand and resultados:
                    m = max(resultados, key=lambda x: x['score'])
                    estado['sinal_atual'] = 'NEUTRO'
                    estado['rsi_atual']   = m['rsi']
                    estado['preco_atual'] = m['preco']

            time.sleep(60)

        except requests.exceptions.RequestException as e:
            log.warning(f"Rede: {e}")
            time.sleep(30)
        except KeyboardInterrupt:
            log.info("Bot encerrado manualmente")
            telegram("⏹ Bot v3 encerrado manualmente")
            for par in list(estado['posicoes'].keys()):
                fechar_posicao(par, motivo="ENCERRAMENTO MANUAL")
            salvar_estado()
            break
        except Exception as e:
            capturar_erro('loop_principal', e)
            log.error(f"Erro inesperado: {e}")
            time.sleep(10)

# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == '__main__':
    t = threading.Thread(target=rodar_api, daemon=True)
    t.start()
    log.info("API Flask iniciada")
    rodar_bot()
