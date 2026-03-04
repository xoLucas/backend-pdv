from fastapi import FastAPI, UploadFile, File, Form
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
geolocator = Nominatim(user_agent="app_pdv_tracker_v1")

@app.post("/processar-planilha/")
async def processar_planilha(
    file: UploadFile = File(...), 
    grupo_id: str = Form(...)
):
    try:
        # NOVO: Lê o conteúdo do arquivo que foi enviado
        conteudo_arquivo = await file.read()
        
        # NOVO: Verifica de forma inteligente se é CSV ou Excel
        if file.filename.lower().endswith('.csv'):
            # Tenta ler como CSV (Auto-detecta vírgula ou ponto-e-vírgula)
            try:
                df = pd.read_csv(io.BytesIO(conteudo_arquivo), sep=None, engine='python', encoding='utf-8')
            except UnicodeDecodeError:
                # Se der erro de acentuação, tenta o padrão brasileiro (latin1)
                df = pd.read_csv(io.BytesIO(conteudo_arquivo), sep=None, engine='python', encoding='latin1')
        else:
            # Se não for CSV, lê como Excel normal
            df = pd.read_excel(io.BytesIO(conteudo_arquivo))
            
        lista_pdvs = []
        
        for index, row in df.iterrows():
            # Tenta encontrar a coluna 'Endereço', independente se tiver letra maiúscula ou minúscula
            coluna_endereco = next((col for col in df.columns if str(col).lower().strip() == 'endereço'), None)
            
            if not coluna_endereco:
                return {"erro": "A planilha precisa ter uma coluna chamada exata 'Endereço'."}
                
            endereco_completo = row[coluna_endereco]
            
            try:
                location = geolocator.geocode(endereco_completo)
                lat = location.latitude if location else None
                lon = location.longitude if location else None
            except Exception:
                lat, lon = None, None
                
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
            
            time.sleep(1) 

        resposta = supabase.table('pdvs').insert(lista_pdvs).execute()
        
        return {
            "mensagem": "Planilha processada e salva no banco com sucesso!", 
            "qtd_processada": len(lista_pdvs)
        }
        
    except Exception as e:
        return {"erro": f"Erro ao processar o arquivo: {str(e)}"}