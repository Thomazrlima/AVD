import csv
import requests
import json
from datetime import datetime

THINGSBOARD_HOST = "https://demo.thingsboard.io"  # Servidor demo - ajuste se necessário
ACCESS_TOKEN = "H71I1IimzBVHeHcxSakR"
TELEMETRY_URL = f"{THINGSBOARD_HOST}/api/v1/{ACCESS_TOKEN}/telemetry"

def converter_para_numero(valor):
    """Converte string com vírgula para float, retorna None se vazio"""
    if not valor or valor.strip() == "":
        return None
    try:
        return float(valor.replace(",", "."))
    except ValueError:
        return None

def ler_csv_e_enviar(caminho_csv):
    """Lê o arquivo CSV e envia os dados para o ThingsBoard"""
    with open(caminho_csv, 'r', encoding='utf-8-sig') as arquivo:
        primeira_linha = arquivo.readline()
        if primeira_linha.startswith('//'):
            pass
        else:
            arquivo.seek(0)
        
        leitor = csv.DictReader(arquivo, delimiter=';', quotechar='"')
        
        primeira_iteracao = True
        for linha_original in leitor:
            linha = {k.strip('"'): v for k, v in linha_original.items()}

            if primeira_iteracao:
                print(f"Colunas disponíveis: {list(linha.keys())}")
                primeira_iteracao = False
            
            if not linha.get('Temp. Ins. (C)') or linha['Temp. Ins. (C)'].strip() == "":
                continue
            
            data = linha['Data']
            hora = linha['Hora (UTC)']
            timestamp_str = f"{data} {hora[:2]}:{hora[2:]}:00"
            timestamp = datetime.strptime(timestamp_str, "%d/%m/%Y %H:%M:%S")
            timestamp_ms = int(timestamp.timestamp() * 1000)
            
            telemetria = {}
            
            campos = {
                'temperatura_instantanea': 'Temp. Ins. (C)',
                'temperatura_maxima': 'Temp. Max. (C)',
                'temperatura_minima': 'Temp. Min. (C)',
                'umidade_instantanea': 'Umi. Ins. (%)',
                'umidade_maxima': 'Umi. Max. (%)',
                'umidade_minima': 'Umi. Min. (%)',
                'ponto_orvalho_instantaneo': 'Pto Orvalho Ins. (C)',
                'ponto_orvalho_maximo': 'Pto Orvalho Max. (C)',
                'ponto_orvalho_minimo': 'Pto Orvalho Min. (C)',
                'pressao_instantanea': 'Pressao Ins. (hPa)',
                'pressao_maxima': 'Pressao Max. (hPa)',
                'pressao_minima': 'Pressao Min. (hPa)',
                'velocidade_vento': 'Vel. Vento (m/s)',
                'direcao_vento': 'Dir. Vento (m/s)',
                'rajada_vento': 'Raj. Vento (m/s)',
                'radiacao': 'Radiacao (KJ/m²)',
                'chuva': 'Chuva (mm)'
            }
            
            for chave_telemetria, chave_csv in campos.items():
                valor = converter_para_numero(linha.get(chave_csv, ''))
                if valor is not None:
                    telemetria[chave_telemetria] = valor
            
            payload = {
                "ts": timestamp_ms,
                "values": telemetria
            }
            
            try:
                headers = {'Content-Type': 'application/json'}
                resposta = requests.post(TELEMETRY_URL, 
                                       data=json.dumps(payload), 
                                       headers=headers,
                                       timeout=10)
                
                if resposta.status_code == 200:
                    print(f"✓ Dados enviados: {data} {hora} - {len(telemetria)} campos")
                elif resposta.status_code == 401:
                    print(f"✗ Erro 401 - Token inválido ou dispositivo não autorizado")
                    print(f"   Verifique se o token '{ACCESS_TOKEN}' está correto no ThingsBoard")
                    print(f"   URL: {TELEMETRY_URL}")
                    break
                else:
                    print(f"✗ Erro ao enviar {data} {hora}: {resposta.status_code}")
                    if resposta.text:
                        print(f"   Resposta: {resposta.text}")
                    
            except requests.exceptions.RequestException as e:
                print(f"✗ Erro de conexão ao enviar {data} {hora}: {e}")

if __name__ == "__main__":
    caminho_csv = r"c:\PENTES\AVD\dados\SALGUEIRO.csv"
    print(f"Iniciando envio de dados de {caminho_csv}")
    print(f"Servidor: {THINGSBOARD_HOST}")
    print("-" * 60)
    
    ler_csv_e_enviar(caminho_csv)
    
    print("-" * 60)
    print("Processo concluído!")
