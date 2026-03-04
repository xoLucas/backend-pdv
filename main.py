from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from geopy.geocoders import Nominatim
import googlemaps
import time
from supabase import create_client, Client
import io

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------
# CONFIGURAÇÕES DE API
# ---------------------------------------------------------
URL_SUPABASE = "https://amdsexfmtwqgdihlyqti.supabase.co"
CHAVE_SUPABASE = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFtZHNleGZtdHdxZ2RpaGx5cXRpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzI2MzY2OTksImV4cCI6MjA4ODIxMjY5OX0.EAybwVPJzFFHzd6LQe_zUAJ96ZzdVh5tjjVLbnYgHgA"
GOOGLE_MAPS_API_KEY = "AIzaSyBhybZVLHkLFfysI3bTa76IbBtxvA3EheE"

supabase: Client = create_client(URL_SUPABASE, CHAVE_SUPABASE)
geolocator = Nominatim(user_agent="app_pdv_tracker_v3")
gmaps = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)

# =========================================================
# FUNÇÕES DE AUXÍLIO (GEOLOCALIZAÇÃO)
# =========================================================

def limpar_endereco(endereco):
    """ Remove ruídos como quebras de linha e fixa a região de busca """
    if not endereco: return ""
    # Remove o '\n' que aparece no final de alguns endereços na sua planilha
    endereco_limpo = str(endereco).replace('\n', ' ').strip()
    # Adiciona contexto para o buscador não se perder fora de Linhares/ES
    return f"{endereco_limpo}, Linhares, ES, Brasil"

def buscar_coordenadas(endereco_original):
    """ Tenta Nominatim (Grátis) -> Se falhar -> Google (Pago/Cota) """
    endereco_para_busca = limpar_endereco(endereco_original)
    
    # 1. TENTATIVA COM NOMINATIM
    try:
        location = geolocator.geocode(endereco_para_busca, timeout=10)
        if location:
            print(f"✅ Nominatim achou: {endereco_original}")
            return location.latitude, location.longitude
    except Exception as e:
        print(f"⚠️ Erro no Nominatim: {e}")

    # 2. TENTATIVA COM GOOGLE MAPS (Caso a primeira falhe)
    try:
        print(f"🔍 Nominatim falhou. Chamando Google para: {endereco_original}")
        result = gmaps.geocode(endereco_para_busca)
        if result:
            loc = result[0]['geometry']['location']
            return loc['lat'], loc['lng']
    except Exception as e:
        print(f"❌ Erro no Google Maps: {e}")

    return None, None

# =========================================================
# O MOTOR QUE RODA NOS BASTIDORES
# =========================================================
def processar_em_segundo_plano(df, grupo_id, coluna_endereco):
    # Remove linhas onde o endereço está vazio
    df = df.dropna(subset=[coluna_endereco])

    for index, row in df.iterrows():
        endereco_raw = str(row[coluna_endereco]).strip()

        if endereco_raw == "" or endereco_raw.lower() == "nan":
            continue

        # Busca inteligente (Cascata)
        lat, lon = buscar_coordenadas(endereco_raw)

        # Função segura para pegar as outras colunas
        def pegar_dado(nome_coluna, padrao):
            valor = row.get(nome_coluna, padrao)
            return padrao if pd.isna(valor) else str(valor).strip()

        pdv_data = {
            "numero_pdv": pegar_dado('PDV', 'N/A'),
            "nome": pegar_dado('Nome', 'Desconhecido'),
            "endereco": endereco_raw.replace('\n', ' '), # Salva no banco sem o \n
            "cnpj": pegar_dado('CNPJ', 'Sem CNPJ'),
            "lat": lat,
            "lon": lon,
            "status": "pendente",
            "grupo_id": grupo_id
        }

        try:
            supabase.table('pdvs').insert(pdv_data).execute()
        except Exception as e:
            print(f"Erro ao salvar no banco: {e}")

        # Pausa para respeitar os limites do Nominatim
        time.sleep(1.2)

# =========================================================
# A ROTA PRINCIPAL DO SITE
# =========================================================
@app.post("/processar-planilha/")
async def processar_planilha(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...), 
    grupo_id: str = Form(...)
):
    try:
        conteudo_arquivo = await file.read()
        
        if file.filename.lower().endswith('.csv'):
            try:
                df = pd.read_csv(io.BytesIO(conteudo_arquivo), sep=None, engine='python', encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(io.BytesIO(conteudo_arquivo), sep=None, engine='python', encoding='latin1')
        else:
            df = pd.read_excel(io.BytesIO(conteudo_arquivo))
            
        # Acha a coluna endereço (ignora maiúsculas/minúsculas)
        coluna_endereco = next((col for col in df.columns if str(col).lower().strip() == 'endereço'), None)
        
        if not coluna_endereco:
            return {"erro": "A planilha precisa ter uma coluna chamada 'Endereço'."}
            
        background_tasks.add_task(processar_em_segundo_plano, df, grupo_id, coluna_endereco)
        
        return {"mensagem": "Processamento iniciado! Os PDVs aparecerão no mapa conforme forem localizados."}
        
    except Exception as e:
        return {"erro": f"Erro ao ler arquivo: {str(e)}"}