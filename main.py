from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from geopy.geocoders import Nominatim
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
# ATENÇÃO: COLOQUE AS SUAS CHAVES DO SUPABASE AQUI
# ---------------------------------------------------------
URL_SUPABASE = "https://amdsexfmtwqgdihlyqti.supabase.co"
CHAVE_SUPABASE = "sb_publishable_xisIAK-FwYMFXirHON0GqA_XCQrp6St"

supabase: Client = create_client(URL_SUPABASE, CHAVE_SUPABASE)
geolocator = Nominatim(user_agent="app_pdv_tracker_v3")

# =========================================================
# O MOTOR QUE RODA NOS BASTIDORES (SEGUNDO PLANO)
# =========================================================
def processar_em_segundo_plano(df, grupo_id, coluna_endereco):
    # 1. Filtro Mágico: Remove todas as linhas em branco/fantasmas do CSV
    df = df.dropna(subset=[coluna_endereco])

    for index, row in df.iterrows():
        endereco_completo = str(row[coluna_endereco]).strip()

        # Pula se o endereço for inválido mesmo após o filtro
        if endereco_completo == "" or endereco_completo.lower() == "nan":
            continue

        try:
            # Busca as coordenadas
            location = geolocator.geocode(endereco_completo)
            lat = location.latitude if location else None
            lon = location.longitude if location else None
        except Exception:
            lat, lon = None, None

        # Função segura para pegar as outras colunas (evitando erros de NaN)
        def pegar_dado(nome_coluna, padrao):
            valor = row.get(nome_coluna, padrao)
            return padrao if pd.isna(valor) else str(valor).strip()

        pdv_data = {
            "numero_pdv": pegar_dado('PDV', 'N/A'),
            "nome": pegar_dado('Nome', 'Desconhecido'),
            "endereco": endereco_completo,
            "cnpj": pegar_dado('CNPJ', 'Sem CNPJ'),
            "lat": lat,
            "lon": lon,
            "status": "pendente",
            "grupo_id": grupo_id
        }

        try:
            # Insere UM por UM no banco para atualizar o mapa em tempo real!
            supabase.table('pdvs').insert(pdv_data).execute()
        except Exception as e:
            print(f"Erro ao salvar no banco: {e}")

        # A pausa obrigatória
        time.sleep(1)

# =========================================================
# A ROTA PRINCIPAL DO SITE
# =========================================================
@app.post("/processar-planilha/")
async def processar_planilha(
    background_tasks: BackgroundTasks, # Injeta a função de segundo plano
    file: UploadFile = File(...), 
    grupo_id: str = Form(...)
):
    try:
        conteudo_arquivo = await file.read()
        
        # Lê CSV ou Excel
        if file.filename.lower().endswith('.csv'):
            try:
                df = pd.read_csv(io.BytesIO(conteudo_arquivo), sep=None, engine='python', encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(io.BytesIO(conteudo_arquivo), sep=None, engine='python', encoding='latin1')
        else:
            df = pd.read_excel(io.BytesIO(conteudo_arquivo))
            
        # Acha a coluna endereço
        coluna_endereco = next((col for col in df.columns if str(col).lower().strip() == 'endereço'), None)
        
        if not coluna_endereco:
            return {"erro": "A planilha precisa ter uma coluna chamada exata 'Endereço'."}
            
        # MANDA O TRABALHO PARA O SEGUNDO PLANO E RESPONDE NA HORA!
        background_tasks.add_task(processar_em_segundo_plano, df, grupo_id, coluna_endereco)
        
        return {"mensagem": "Processamento iniciado! Os PDVs vão aparecer no mapa em tempo real."}
        
    except Exception as e:
        return {"erro": f"Erro ao ler arquivo: {str(e)}"}