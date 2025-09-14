import sqlite3
import random
from datetime import datetime, timedelta

DB_PATH = "banco_test.db"

# Configuração das ligas e times
LIGAS = {
    "Premier League": ["Arsenal", "Chelsea", "Liverpool", "Manchester City", "Manchester United", "Tottenham", "Leicester City", "West Ham", "Everton", "Aston Villa",
                       "Newcastle", "Wolves", "Crystal Palace", "Southampton", "Brighton", "Brentford", "Fulham", "Bournemouth", "Nottingham Forest", "Sheffield United"],
    "Bundesliga": ["Bayern Munich", "Borussia Dortmund", "RB Leipzig", "Leverkusen", "Eintracht Frankfurt", "Stuttgart", "Wolfsburg", "Freiburg", "Hoffenheim", "Union Berlin",
                   "Mainz", "Augsburg", "Hertha Berlin", "Bochum", "Darmstadt", "Nürnberg", "Kaiserslautern", "Paderborn"],
    "Serie A": ["Juventus", "Inter", "Milan", "Napoli", "Roma", "Lazio", "Atalanta", "Fiorentina", "Torino", "Sassuolo",
                "Udinese", "Sampdoria", "Verona", "Empoli", "Monza", "Salernitana", "Cremonese", "Lecce", "Bologna", "Como", "Spezia"],
    "La Liga": ["Real Madrid", "Barcelona", "Atletico Madrid", "Sevilla", "Valencia", "Real Sociedad", "Villarreal", "Athletic Bilbao", "Celta Vigo", "Granada",
                "Getafe", "Mallorca", "Osasuna", "Rayo Vallecano", "Real Betis", "Espanyol", "Almeria", "Cadiz", "Elche", "Girona"],
    "Brasileirão Série A": ["Palmeiras", "Flamengo", "Corinthians", "São Paulo", "Grêmio", "Internacional", "Fluminense", "Cruzeiro", "Vasco", "Bahia",
                            "Sport", "Ceará", "Vitória", "Atlético Mineiro", "Santos", "Botafogo", "RB Bragantino", "Mirassol", "Juventude", "Fortaleza"],
    "Brasileirão Série B": ["Cuiaba", "Chapecoense", "CRB", "Guarani", "Vila Nova", "Ponte Preta", "Náutico", "Avaí", "CSA", "Ituano",
                            "Londrina", "Operário-PR", "Sampaio Corrêa", "Tombense", "Brusque", "Novorizontino", "Figueirense", "Botafogo-SP", "Athletic MG", "Coritiba"],
}

# Número de jogos por temporada de cada liga
JOGOS_POR_TEMPORADA = {
    "Premier League": 380,
    "Bundesliga": 306,
    "Serie A": 380,
    "La Liga": 380,
    "Brasileirão Série A": 380,
    "Brasileirão Série B": 380,
}

TEMPORADAS = [2022, 2023, 2024, 2025]

MERCADOS = {
    "Match Winner": ["Home", "Draw", "Away"],
    "Double Chance": ["1X", "12", "X2"],
    "Both Teams To Score": ["Yes", "No"],
    "Over/Under 1.5": ["Over", "Under"],
    "Over/Under 2.5": ["Over", "Under"],
    "Over/Under 3.5": ["Over", "Under"],
    "Asian Handicap 0.5": ["Home", "Away"],
    "Asian Handicap 1.0": ["Home", "Away"],
    "Correct Score": ["1-0", "2-0", "2-1", "0-0", "1-1", "3-1", "3-2"],
    "Total Goals": ["0-1", "2-3", "4+"],
}

BOOKMAKERS = ["Bet365", "Betfair", "Pinnacle", "1xBet"]

# --------------------------
# Setup banco
# --------------------------
conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Resetar tabelas
c.execute("DROP TABLE IF EXISTS odds")
c.execute("DROP TABLE IF EXISTS jogos")

c.execute("""
CREATE TABLE IF NOT EXISTS jogos (
    id_jogo INTEGER PRIMARY KEY,
    liga_nome TEXT,
    temporada INTEGER,
    data TEXT,
    mandante TEXT,
    visitante TEXT,
    gols_mandante INTEGER,
    gols_visitante INTEGER
)
""")

c.execute("""
CREATE TABLE IF NOT EXISTS odds (
    id_odd INTEGER PRIMARY KEY AUTOINCREMENT,
    id_jogo INTEGER,
    mercado TEXT,
    odd REAL,
    bookmaker TEXT
)
""")

# --------------------------
# Função para odds
# --------------------------


def gerar_odds_realistas(mercado):
    if mercado == "Match Winner":
        return [("Home", round(random.uniform(1.3, 3.5), 2)),
                ("Draw", round(random.uniform(2.5, 4.0), 2)),
                ("Away", round(random.uniform(1.5, 4.0), 2))]
    elif "Over/Under" in mercado:
        return [("Over", round(random.uniform(1.6, 2.5), 2)),
                ("Under", round(random.uniform(1.5, 2.4), 2))]
    elif mercado == "Both Teams To Score":
        return [("Yes", round(random.uniform(1.5, 2.2), 2)),
                ("No", round(random.uniform(1.6, 2.4), 2))]
    elif "Handicap" in mercado:
        return [("Home", round(random.uniform(1.7, 2.3), 2)),
                ("Away", round(random.uniform(1.7, 2.3), 2))]
    return []


# --------------------------
# Gerar dados
# --------------------------
for liga_idx, (liga, times) in enumerate(LIGAS.items()):
    for temporada in TEMPORADAS:
        print(f"Gerando {liga} - {temporada}")

        base_id = (liga_idx + 1) * 1_000_000 + temporada * 10_000
        num_jogos = JOGOS_POR_TEMPORADA[liga]

        # começar em julho (realista para Europa e Brasil)
        start_date = datetime(temporada, 7, 1)

        for idx in range(num_jogos):
            id_jogo = base_id + idx
            # espaçar mais realista
            match_date = start_date + timedelta(days=idx * 2)
            home, away = random.sample(times, 2)
            gols_home, gols_away = random.randint(0, 4), random.randint(0, 4)

            # Inserir jogo
            c.execute("""
                INSERT INTO jogos (id_jogo, liga_nome, temporada, data, mandante, visitante, gols_mandante, gols_visitante)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (id_jogo, liga, temporada, match_date.isoformat(), home, away, gols_home, gols_away))

            # Odds
            for mercado in MERCADOS:
                odds_vals = gerar_odds_realistas(mercado)
                for outcome, odd in odds_vals:
                    for bookmaker in BOOKMAKERS:
                        c.execute("""
                            INSERT INTO odds (id_jogo, mercado, odd, bookmaker)
                            VALUES (?, ?, ?, ?)
                        """, (id_jogo, f"{mercado} - {outcome}", odd, bookmaker))

conn.commit()
conn.close()
print("Mock database criada com sucesso!")
