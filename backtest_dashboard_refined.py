# backtest_dashboard_refined.py
import sqlite3
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import io
import re
from datetime import datetime
import zipfile, os

if not os.path.exists("banco_test.db") and os.path.exists("banco_test.zip"):
    with zipfile.ZipFile("banco_test.zip", "r") as zip_ref:
        zip_ref.extractall(".")

# --------------------------
# Config / Helpers
# --------------------------
DB_PATH = "banco_test.db"  # arquivo SQLite gerado pelo coletor

st.set_page_config(page_title="Backtest Futebol (Refinado)", layout="wide")
st.title("⚽ Backtest Plataforma — Versão refinada")

# --------------------------
# Utilitários de DB e carga
# --------------------------


@st.cache_data(ttl=3600)
def load_raw_tables(db_path=DB_PATH):
    """Carrega tabelas 'jogos' e 'odds' do banco. Retorna dois DataFrames."""
    try:
        conn = sqlite3.connect(db_path)
    except Exception as e:
        st.error(f"Erro abrindo banco {db_path}: {e}")
        return pd.DataFrame(), pd.DataFrame()

    try:
        df_jogos = pd.read_sql_query(
            "SELECT * FROM jogos", conn, parse_dates=["data"])
    except Exception:
        df_jogos = pd.DataFrame()

    try:
        df_odds = pd.read_sql_query("SELECT * FROM odds", conn)
    except Exception:
        df_odds = pd.DataFrame()

    conn.close()
    return df_jogos, df_odds


def aggregate_odds(df_odds: pd.DataFrame, method="max"):
    """Agrupa odds por id_jogo e mercado."""
    if df_odds.empty:
        return df_odds
    df_odds = df_odds.copy()
    if "odd" in df_odds.columns:
        df_odds["odd"] = pd.to_numeric(df_odds["odd"], errors="coerce")
        df_odds = df_odds.dropna(subset=["odd"])
    else:
        return pd.DataFrame()  # sem odds válidas

    # garantir coluna 'mercado' e 'id_jogo' exista
    if not {"id_jogo", "mercado"}.issubset(df_odds.columns):
        return pd.DataFrame()

    if method == "max":
        agg = df_odds.groupby(["id_jogo", "mercado"],
                              as_index=False)["odd"].max()
    else:
        agg = df_odds.groupby(["id_jogo", "mercado"],
                              as_index=False)["odd"].mean()
    return agg

# --------------------------
# Parsing de mercados (robusto)
# --------------------------


def parse_market_label(mercado_label: str):
    m = mercado_label or ""
    if "Over/Under" in m or "Over" in m and "/" in m:
        match = re.search(r"(Over|Under)\s*([0-9]+(?:\.[0-9]+)?)", m)
        if match:
            return {"type": "over_under", "side": match.group(1), "limit": float(match.group(2))}
    if "Both Teams" in m or "Both Teams to Score" in m or m.upper().startswith("BTTS"):
        match = re.search(r"(Yes|No)", m, re.IGNORECASE)
        side = match.group(1).capitalize() if match else None
        return {"type": "btts", "side": side}
    if "Match Winner" in m or "1X2" in m or "Match" in m:
        if re.search(r"Home", m, re.IGNORECASE):
            return {"type": "match_winner", "side": "Home"}
        if re.search(r"Away", m, re.IGNORECASE):
            return {"type": "match_winner", "side": "Away"}
        if re.search(r"Draw", m, re.IGNORECASE):
            return {"type": "match_winner", "side": "Draw"}
    return {"type": "unknown", "label": m}

# --------------------------
# Simulação (core)
# --------------------------


def simulate_strategy(df: pd.DataFrame, mercado_label: str, stake=100,
                      odd_min=1.01, odd_max=10.0, initial_bank=0.0):
    """Recebe df (já contendo colunas de jogo e 'odd' e 'mercado' correspondente),
       aplica filtros de odds, simula lucro por aposta e retorna df_result + metrics."""
    if df is None or df.empty:
        return pd.DataFrame(), {}

    # copiar e garantir tipos
    df = df.copy()
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], utc=True, errors="coerce")
    # Filtrar por odd disponível
    df["odd"] = pd.to_numeric(df["odd"], errors="coerce")
    df = df.dropna(subset=["odd"])

    # aplicar bound de odds
    df = df[(df["odd"] >= odd_min) & (df["odd"] <= odd_max)].copy()
    if df.empty:
        return pd.DataFrame(), {}

    # ordenar por data (importante para curvas)
    if "data" in df.columns:
        df = df.sort_values("data").reset_index(drop=True)
    else:
        df = df.reset_index(drop=True)

    parsed = parse_market_label(mercado_label)

    lucros = []
    wins = 0
    losses = 0
    total_staked = 0.0
    bets_placed = 0

    for _, row in df.iterrows():
        odd = float(row["odd"])
        gols_home = int(row.get("gols_mandante") or 0)
        gols_away = int(row.get("gols_visitante") or 0)
        gols = gols_home + gols_away

        lucro = 0.0
        bet_placed = False

        if parsed["type"] == "over_under":
            limit = parsed.get("limit", 0.0)
            side = parsed.get("side")
            bet_placed = True
            if side == "Over":
                # Over se gols > limite (ex: Over 2.5 => >=3)
                if gols > limit:
                    lucro = (odd - 1) * stake
                else:
                    lucro = -stake
            else:  # Under
                # Under se gols <= limite
                if gols <= limit:
                    lucro = (odd - 1) * stake
                else:
                    lucro = -stake

        elif parsed["type"] == "btts":
            side = parsed.get("side")
            bet_placed = True
            if side == "Yes":
                if gols_home > 0 and gols_away > 0:
                    lucro = (odd - 1) * stake
                else:
                    lucro = -stake
            else:  # No
                if gols_home == 0 or gols_away == 0:
                    lucro = (odd - 1) * stake
                else:
                    lucro = -stake

        elif parsed["type"] == "match_winner":
            side = parsed.get("side")
            bet_placed = True
            if side == "Home":
                if gols_home > gols_away:
                    lucro = (odd - 1) * stake
                else:
                    lucro = -stake
            elif side == "Away":
                if gols_away > gols_home:
                    lucro = (odd - 1) * stake
                else:
                    lucro = -stake
            else:  # Draw
                if gols_home == gols_away:
                    lucro = (odd - 1) * stake
                else:
                    lucro = -stake

        else:
            bet_placed = False
            lucro = 0.0

        if bet_placed:
            total_staked += stake
            bets_placed += 1
            if lucro > 0:
                wins += 1
            else:
                losses += 1

        lucros.append(lucro)

    df["lucro"] = lucros
    df["banca"] = df["lucro"].cumsum() + initial_bank

    lucro_final = df["lucro"].sum()
    total_apostas = int((df["lucro"] != 0).sum())
    roi = (lucro_final / total_staked) if total_staked > 0 else 0.0
    taxa_acerto = (wins / (wins + losses)) if (wins + losses) > 0 else None
    dd_series = calc_drawdown_series(df["banca"])
    max_drawdown = float(dd_series.min()) if not dd_series.empty else 0.0
    avg_odd = float(df["odd"].mean()) if not df["odd"].empty else None

    metrics = {
        "mercado": mercado_label,
        "registros": int(len(df)),
        "apostas_realizadas": int(total_apostas),
        "lucro_final": float(lucro_final),
        "total_staked": float(total_staked),
        "roi": float(roi),
        "taxa_acerto": float(taxa_acerto) if taxa_acerto is not None else None,
        "max_drawdown": float(max_drawdown),
        "avg_odd": float(avg_odd) if avg_odd is not None else None
    }
    return df, metrics


def calc_drawdown_series(banca_series: pd.Series):
    if banca_series.empty:
        return pd.Series(dtype=float)
    running_max = banca_series.cummax()
    drawdown = banca_series - running_max
    return drawdown


# --------------------------
# UI / Interação
# --------------------------
df_jogos, df_odds = load_raw_tables()

if df_jogos.empty or df_odds.empty:
    st.warning(
        "Banco vazio ou não encontrado. Rode o script de coleta para popular 'banco_test.db'.")
    st.stop()

# opções de agregação de odds
agg_method = st.sidebar.selectbox(
    "Como agregar múltiplas odds por jogo/mercado?", ["max (melhor odd)", "mean (média)"])
agg_method_key = "max" if agg_method.startswith("max") else "mean"
df_odds_agg = aggregate_odds(df_odds, method=agg_method_key)

# montar DF combinado: apenas combina id_jogo -> traz liga/nome/temporada
df_combined = df_odds_agg.merge(
    df_jogos, how="left", left_on="id_jogo", right_on="id_jogo")

# normalizar colunas mínimas
if "data" in df_combined.columns:
    df_combined["data"] = pd.to_datetime(
        df_combined["data"], utc=True, errors="coerce")

# --------------------------
# Filtros com defaults seguros  # >>> CORREÇÃO
# --------------------------
ligas_available = sorted(df_combined["liga_nome"].dropna(
).unique()) if "liga_nome" in df_combined.columns else []
anos_available = sorted(df_combined["temporada"].dropna(
).unique()) if "temporada" in df_combined.columns else []
mercados_available = sorted(df_combined["mercado"].dropna(
).unique()) if "mercado" in df_combined.columns else []

ligas_sel = st.sidebar.multiselect(
    "Ligas", options=ligas_available, default=ligas_available[:5] if ligas_available else [])
anos_sel = st.sidebar.multiselect(
    "Temporadas", options=anos_available, default=anos_available[-2:] if anos_available else [])
mercados_sel = st.sidebar.multiselect(
    "Mercados", options=mercados_available, default=mercados_available[:3] if mercados_available else [])

stake = st.sidebar.number_input(
    "Stake por aposta (fixo)", min_value=1, max_value=100000, value=100, step=10)
odd_min = st.sidebar.number_input(
    "Odd mínima", value=1.01, min_value=1.01, max_value=100.0, step=0.01)
odd_max = st.sidebar.number_input(
    "Odd máxima", value=10.0, min_value=1.01, max_value=100.0, step=0.01)
initial_bank = st.sidebar.number_input(
    "Banca inicial (apenas para visual)", min_value=0.0, value=0.0, step=10.0)

# validações simples
if not mercados_sel or not ligas_sel or not anos_sel:
    st.info(
        "Escolha pelo menos uma liga, uma temporada e um mercado para rodar o backtest.")
    st.stop()

df_filtered = df_combined[
    df_combined["liga_nome"].isin(ligas_sel) &
    df_combined["temporada"].isin(anos_sel) &
    df_combined["mercado"].isin(mercados_sel)
].copy()

if df_filtered.empty:
    st.warning("Não há dados com os filtros selecionados.")
    st.stop()

# --------------------------
# Rodar simulações (por mercado)
# --------------------------
with st.spinner("Rodando backtests..."):
    results_metrics = []
    market_curves = {}
    market_drawdowns = {}
    market_dfs = {}

    # Chaves esperadas para garantir consistência (>>> CORREÇÃO)
    expected_metric_keys = ["mercado", "registros", "apostas_realizadas",
                            "lucro_final", "total_staked", "roi", "taxa_acerto",
                            "max_drawdown", "avg_odd"]

    for market in mercados_sel:
        df_market = df_filtered[df_filtered["mercado"] == market].copy()
        df_res, metrics = simulate_strategy(df_market, market, stake=stake,
                                            odd_min=odd_min, odd_max=odd_max,
                                            initial_bank=initial_bank)
        if df_res is not None and not df_res.empty:
            # padronizar keys caso falte alguma
            for k in expected_metric_keys:
                if k not in metrics:
                    metrics[k] = 0.0 if k not in (
                        "mercado", "taxa_acerto") else None
            results_metrics.append(metrics)
            market_curves[market] = df_res[["data", "banca"]].copy()
            market_drawdowns[market] = calc_drawdown_series(
                df_res["banca"]).reset_index(drop=True)
            market_dfs[market] = df_res
        else:
            # mesmo que vazia, incluir métrica informativa (mesmas chaves)  >>> CORREÇÃO
            results_metrics.append({
                "mercado": market,
                "registros": 0,
                "apostas_realizadas": 0,
                "lucro_final": 0.0,
                "total_staked": 0.0,
                "roi": 0.0,
                "taxa_acerto": None,
                "max_drawdown": 0.0,
                "avg_odd": None
            })

# --------------------------
# Tabela de métricas (blindada)  >>> CORREÇÃO
# --------------------------
st.subheader("📊 Métricas por mercado")
df_metrics = pd.DataFrame(results_metrics)

# garantir que todas as colunas esperadas existam (proteção contra KeyError)
for k in expected_metric_keys:
    if k not in df_metrics.columns:
        if k == "taxa_acerto" or k == "avg_odd":
            df_metrics[k] = None
        else:
            df_metrics[k] = 0.0

# agora é seguro ordenar
if not df_metrics.empty:
    df_metrics = df_metrics.sort_values(
        "lucro_final", ascending=False).reset_index(drop=True)
    st.dataframe(df_metrics)
else:
    st.info("Nenhuma métrica disponível para exibir.")

# --------------------------
# Curva de Banca
# --------------------------
st.subheader("📈 Curva de Banca (comparativa)")
fig = go.Figure()
for market, curve in market_curves.items():
    if curve is None or curve.empty:
        continue
    x = curve["data"]
    y = curve["banca"]
    fig.add_trace(go.Scatter(x=x, y=y, mode="lines+markers",
                  name=market, hovertemplate="%{x}<br>Banca: %{y:.2f}"))
fig.update_layout(yaxis_title="Banca", xaxis_title="Data",
                  legend_title="Mercado", height=450)
st.plotly_chart(fig, use_container_width=True)

# --------------------------
# Drawdown
# --------------------------
st.subheader("📉 Drawdown (comparado)")
fig_dd = go.Figure()
for market, df_market in market_dfs.items():
    if df_market is None or df_market.empty:
        continue
    dd = calc_drawdown_series(df_market["banca"])
    fig_dd.add_trace(go.Scatter(
        x=df_market["data"], y=dd, fill='tozeroy', name=market, hovertemplate="%{x}<br>Drawdown: %{y:.2f}"))
fig_dd.update_layout(yaxis_title="Drawdown", xaxis_title="Data", height=350)
st.plotly_chart(fig_dd, use_container_width=True)

# --------------------------
# Novos comparativos
# --------------------------
# Boxplot de odds (proteção caso não exista coluna)
st.subheader("📊 Distribuição de odds por mercado")
if "odd" in df_filtered.columns:
    fig_box = px.box(df_filtered, x="mercado", y="odd", points="all",
                     title="Distribuição das odds por mercado")
    st.plotly_chart(fig_box, use_container_width=True)
else:
    st.info("Coluna 'odd' não encontrada para plotar a distribuição.")

# ROI por mercado (garantido que df_metrics tem 'roi' e 'mercado')
st.subheader("💹 ROI por mercado")
if not df_metrics.empty and "roi" in df_metrics.columns and "mercado" in df_metrics.columns:
    # transformar NaNs para 0 para visual
    df_metrics["roi_display"] = df_metrics["roi"].fillna(0.0).astype(float)
    fig_roi = px.bar(df_metrics, x="mercado", y="roi_display",
                     text="roi_display", title="ROI (%) por mercado")
    fig_roi.update_traces(texttemplate="%{text:.2%}", textposition="outside")
    fig_roi.update_layout(yaxis_tickformat=".0%")
    st.plotly_chart(fig_roi, use_container_width=True)
else:
    st.info("Não há dados suficientes para calcular ROI.")

# Heatmap de lucro por liga e mercado (>>> CORREÇÃO: usar market_dfs que contém 'lucro')
st.subheader("🔥 Heatmap de lucro por liga e mercado")
# construir pivot a partir de market_dfs
heatmap_frames = []
for market, df_m in market_dfs.items():
    if df_m is None or df_m.empty:
        continue
    if "liga_nome" not in df_m.columns or "lucro" not in df_m.columns:
        continue
    s = df_m.groupby("liga_nome")["lucro"].sum().rename(market)
    heatmap_frames.append(s)

if heatmap_frames:
    pivot = pd.concat(heatmap_frames, axis=1).fillna(0)
    # garantir index/columns ordenados
    pivot = pivot.sort_index()
    fig_heat = px.imshow(pivot, text_auto=".2f",
                         labels=dict(x="Mercado", y="Liga", color="Lucro"),
                         aspect="auto")
    st.plotly_chart(fig_heat, use_container_width=True)
else:
    st.info("Não há dados suficientes (ou coluna 'lucro' ausente) para gerar o heatmap.")

# --------------------------
# Tabela detalhada e export
# --------------------------
st.subheader("📋 Detalhes das apostas")
# >>> CORREÇÃO: proteção
first_market = mercados_sel[0] if mercados_sel else None

if first_market and market_dfs.get(first_market) is not None and not market_dfs[first_market].empty:
    st.write(f"Mostrando detalhes para: **{first_market}**")
    cols_show = ["data", "liga_nome", "temporada", "mandante",
                 "visitante", "mercado", "odd", "lucro", "banca"]
    available_cols = [
        c for c in cols_show if c in market_dfs[first_market].columns]
    st.dataframe(market_dfs[first_market].loc[:, available_cols].sort_values(
        "data").reset_index(drop=True))
else:
    st.info("Nenhum detalhe disponível para o mercado selecionado.")


def to_excel_bytes(dict_of_dfs):
    """Recebe dict {sheet_name: df} e retorna bytes do arquivo xlsx."""
    output = io.BytesIO()
    # tentar engines comuns
    try:
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            for name, df in dict_of_dfs.items():
                df.to_excel(writer, sheet_name=str(name)[:31], index=False)
    except Exception:
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            for name, df in dict_of_dfs.items():
                df.to_excel(writer, sheet_name=str(name)[:31], index=False)
    output.seek(0)
    return output.getvalue()


# preparar pacotes para download
csv_all = pd.DataFrame(results_metrics).to_csv(index=False).encode("utf-8")
st.download_button("Baixar métricas (CSV)", data=csv_all,
                   file_name="metrics_backtest.csv", mime="text/csv")

# Excel com uma aba por mercado (detalhes) — só se houver dados
market_dfs_nonempty = {m: market_dfs[m].fillna(
    "") for m in market_dfs if market_dfs[m] is not None and not market_dfs[m].empty}
if market_dfs_nonempty:
    excel_bytes = to_excel_bytes(market_dfs_nonempty)
    st.download_button("Baixar detalhes por mercado (Excel)", data=excel_bytes, file_name="detalhes_backtest.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
else:
    st.info("Nenhum detalhe de apostas para exportar (nenhum mercado com dados).")

st.info("Dica: use a opção 'max (melhor odd)' se quiser simular com a melhor odd disponível entre bookmakers (backtest mais otimista).")

