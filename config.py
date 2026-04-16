# =============================================================================
# config.py
# Parâmetros globais do pipeline de proposições da Câmara dos Deputados.
# Todos os scripts importam daqui — nunca use números mágicos nos outros arquivos.
# =============================================================================

from pathlib import Path

# -----------------------------------------------------------------------------
# JANELA TEMPORAL
# -----------------------------------------------------------------------------
DATA_INICIO = "2019-01-01"
DATA_FIM    = None          # None = até hoje (resolvido em tempo de execução)

# -----------------------------------------------------------------------------
# TIPOS DE PROPOSIÇÃO
# PDC e PDL são a mesma figura em legislaturas diferentes; incluímos os dois.
# -----------------------------------------------------------------------------
TIPOS_RELEVANTES = {"PL", "PEC", "PLP", "PDL", "PDC"}

# -----------------------------------------------------------------------------
# CLASSIFICAÇÃO DE SITUAÇÃO (3 classes)
#
# Lógica de precedência (aplicada nessa ordem):
#   1. Transformada em norma jurídica
#   2. Em tramitação
#   3. Arquivada  ← tudo que não se encaixa nas anteriores vai aqui,
#                   incluindo NaN, "Indefinido" e "Devolvida ao autor" (1120)
# -----------------------------------------------------------------------------
SITUACAO_NORMA      = "Transformada em norma jurídica"
SITUACAO_TRAMITACAO = "Em tramitação"
SITUACAO_ARQUIVADA  = "Arquivada"

C_NORMA = {1140}

C_TRAMITACAO = {
    901,   # Aguardando Constituição de Comissão Temporária
    902,   # Aguardando Criação de Comissão Temporária
    903,   # Aguardando Deliberação
    904,   # Aguardando Deliberação de Recurso
    905,   # Aguardando Designação de Relator
    907,   # Aguardando Designação de Relator(a)
    910,   # Aguardando Parecer
    915,   # Aguardando Parecer
    918,   # Aguardando Recurso
    922,   # Aguardando Redação Final
    924,   # Pronta para Pauta
    925,   # Tramitando em Conjunto
    926,   # Aguardando Apreciação pelo Senado Federal
    927,   # Aguardando Votação
    928,   # Em Votação
    932,   # Aguardando Definição Encaminhamento
    1170,  # Aguardando Designação - Relator deixou de ser membro
    1200,  # Aguardando Autorização do Despacho
    1220,  # Aguardando Despacho do Presidente (Análise)
    1221,  # Aguardando Despacho do Presidente (Autorização)
    1270,  # Aguardando Despacho
    1293,  # Aguardando Envio ao Senado Federal
    1303,  # Enviada ao Senado Federal
}

C_ARQUIVADA = {
    923,   # Arquivada
    930,   # Enviada ao Arquivo
    931,   # Aguardando Remessa ao Arquivo
    937,   # Vetada Totalmente
    940,   # Aguardando Despacho de Arquivamento
    941,   # Recusada
    950,   # Retirada pelo(a) Autor(a)
    1285,  # Tramitação Finalizada
    1292,  # Perdeu a Eficácia
}

# Códigos que explicitamente vão para Arquivada mesmo não estando em C_ARQUIVADA
# (ex: Devolvida ao autor)
C_ARQUIVADA_EXTRA = {1120}

# -----------------------------------------------------------------------------
# TEMAS
#
# A Câmara retorna temas como [["Nome do Tema", relevancia], ...]
# O GPT retorna códigos numéricos [codTema, ...] que precisam ser convertidos.
# Prioridade: temas da Câmara prevalecem; GPT é fallback para proposições sem tema.
# -----------------------------------------------------------------------------
CODTEMA_PARA_NOME = {
    34: "Administração Pública",
    35: "Arte, Cultura e Religião",
    37: "Comunicações",
    39: "Esporte e Lazer",
    40: "Economia",
    41: "Cidades e Desenvolvimento Urbano",
    42: "Direito Civil e Processual Civil",
    43: "Direito Penal e Processual Penal",
    44: "Direitos Humanos e Minorias",
    46: "Educação",
    48: "Meio Ambiente e Desenvolvimento Sustentável",
    51: "Estrutura Fundiária",
    52: "Previdência e Assistência Social",
    53: "Processo Legislativo e Atuação Parlamentar",
    54: "Energia, Recursos Hídricos e Minerais",
    55: "Relações Internacionais e Comércio Exterior",
    56: "Saúde",
    57: "Defesa e Segurança",
    58: "Trabalho e Emprego",
    60: "Turismo",
    61: "Viação, Transporte e Mobilidade",
    62: "Ciência, Tecnologia e Inovação",
    64: "Agricultura, Pecuária, Pesca e Extrativismo",
    66: "Indústria, Comércio e Serviços",
    67: "Direito e Defesa do Consumidor",
    68: "Direito Constitucional",
    70: "Finanças Públicas e Orçamento",
    72: "Homenagens e Datas Comemorativas",
    74: "Política, Partidos e Eleições",
    76: "Direito e Justiça",
    85: "Ciências Exatas e da Terra",
    86: "Ciências Sociais e Humanas",
}

NOME_PARA_CODTEMA = {v: k for k, v in CODTEMA_PARA_NOME.items()}

# -----------------------------------------------------------------------------
# URLs E ENDPOINTS
# -----------------------------------------------------------------------------
BASE_API = "https://dadosabertos.camara.leg.br/api/v2"
BASE_ARQ = "https://dadosabertos.camara.leg.br/arquivos/proposicoes/csv/proposicoes-{ano}.csv"

# -----------------------------------------------------------------------------
# CAMINHOS
# -----------------------------------------------------------------------------
ROOT_DIR    = Path(__file__).parent
DATA_DIR    = ROOT_DIR / "data"
RAW_DIR     = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
FINAL_DIR   = DATA_DIR / "final"
STATE_DIR   = ROOT_DIR / "state"

for _dir in (RAW_DIR, INTERIM_DIR, FINAL_DIR, STATE_DIR):
    _dir.mkdir(parents=True, exist_ok=True)

STATE_FILE = STATE_DIR / "ultima_atualizacao.json"

# -----------------------------------------------------------------------------
# PARÂMETROS DE COLETA
# -----------------------------------------------------------------------------
TIMEOUT          = 60
SLEEP_ENTRE_REQS = 0.05
MAX_RETRIES      = 5

# -----------------------------------------------------------------------------
# PARÂMETROS DO CLASSIFICADOR GPT
# Modelo e prompt ficam em 3_classificar_temas.py;
# aqui ficam só os parâmetros numéricos.
# -----------------------------------------------------------------------------
GPT_MODEL       = "gpt-4o-mini"
GPT_MAX_TEMAS   = 4
GPT_MIN_VOTES   = 2   # votos mínimos para um tema ser aceito no sistema de votação
GPT_N_RUNS      = 3   # número de chamadas por proposição para o sistema de votação
