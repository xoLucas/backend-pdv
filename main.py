from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from geopy.geocoders import Nominatim
import time
from supabase import create_client, Client

app = FastAPI()

# Configuração do CORS para permitir que o seu site (Frontend) converse com esta API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Em um cenário profissional, colocaríamos a URL do Netlify aqui
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------
# ATENÇÃO: COLOQUE AS SUAS CHAVES DO SUPABASE AQUI
# Substitua os textos entre aspas pelas chaves que você copiou
# ---------------------------------------------------------
URL_SUPABASE = "https://amdsexfmtwqgdihlyqti.supabase.co"
CHAVE_SUPABASE = "sb_publishable_xisIAK-FwYMFXirHON0GqA_XCQrp6St"

# Inicializa a conexão com o banco de dados
supabase: Client = create_client(URL_SUPABASE, CHAVE_SUPABASE)

# Inicializa o buscador de coordenadas do OpenStreetMap
geolocator = Nominatim(user_agent="app_pdv_tracker_v1")

@app.post("/processar-planilha/")
async def processar_planilha(
    file: UploadFile = File(...), 
    grupo_id: str = Form(...)
):
    try:
        # Lê a planilha Excel que foi enviada
        df = pd.read_excel(file.file)
        lista_pdvs = []
        
        # Passa por cada linha da planilha
        for index, row in df.iterrows():
            endereco_completo = row['Endereço']
            
            # Tenta buscar a latitude e longitude
            try:
                location = geolocator.geocode(endereco_completo)
                lat = location.latitude if location else None
                lon = location.longitude if location else None
            except Exception:
                lat, lon = None, None
                
            # Monta os dados do PDV para salvar no banco
            lista_pdvs.append({
                "numero_pdv": str(row.get('PDV', 'N/A')),
                "nome": str(row.get('Nome', 'Desconhecido')),
                "endereco": endereco_completo,
                "cnpj": str(row.get('CNPJ', 'Sem CNPJ')),
                "lat": lat,
                "lon": lon,
                "status": "pendente",
                "grupo_id": grupo_id
            })
            
            # Pausa de 1 segundo obrigatória para não ser bloqueado pelo OpenStreetMap
            time.sleep(1) 

        # Salva toda a lista de uma vez no banco de dados Supabase
        resposta = supabase.table('pdvs').insert(lista_pdvs).execute()
        
        return {
            "mensagem": "Planilha processada e salva no banco com sucesso!", 
            "qtd_processada": len(lista_pdvs)
        }
        
    except Exception as e:
        # Se der algum erro (ex: planilha no formato errado), avisa o frontend
        return {"erro": str(e)}