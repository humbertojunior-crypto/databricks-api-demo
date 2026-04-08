from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import json
import os
import logging
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurações do Databricks via REST API
DATABRICKS_CONFIG = {
    "host": os.getenv("DATABRICKS_HOST"),
    "token": os.getenv("DATABRICKS_TOKEN"),
    "warehouse_id": os.getenv("DATABRICKS_WAREHOUSE_ID")
}

def validate_config():
    """Valida configurações"""
    missing = [k for k, v in DATABRICKS_CONFIG.items() if not v]
    if missing:
        logger.error(f"Configurações faltando: {missing}")
        return False
    return True

def execute_databricks_query(sql_query):
    """Executa query via REST API do Databricks"""
    try:
        if not validate_config():
            return None, "Configurações Databricks incompletas"
        
        # URL da API REST do Databricks
        url = f"https://{DATABRICKS_CONFIG['host']}/api/2.0/sql/statements"
        
        headers = {
            "Authorization": f"Bearer {DATABRICKS_CONFIG['token']}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "statement": sql_query,
            "warehouse_id": DATABRICKS_CONFIG["warehouse_id"],
            "wait_timeout": "30s"
        }
        
        logger.info(f"Executando query: {sql_query}")
        
        # Fazer requisição
        response = requests.post(url, headers=headers, json=payload, timeout=60)
        
        if response.status_code != 200:
            logger.error(f"Erro HTTP {response.status_code}: {response.text}")
            return None, f"Erro HTTP {response.status_code}: {response.text}"
        
        result = response.json()
        
        # Verificar se a execução foi bem-sucedida
        if result.get("status", {}).get("state") != "SUCCEEDED":
            error_msg = result.get("status", {}).get("error", {}).get("message", "Erro desconhecido")
            return None, f"Query falhou: {error_msg}"
        
        # Extrair dados dos resultados
        manifest = result.get("manifest", {})
        chunks = result.get("result", {}).get("data_array", [])
        
        # Schema das colunas
        schema = manifest.get("schema", {}).get("columns", [])
        column_names = [col.get("name") for col in schema]
        
        # Processar dados
        data = []
        for chunk in chunks:
            for row in chunk:
                row_dict = {column_names[i]: row[i] for i in range(len(row))}
                data.append(row_dict)
        
        return data, None
        
    except requests.Timeout:
        return None, "Timeout na execução da query"
    except Exception as e:
        logger.error(f"Erro na execução: {str(e)}")
        return None, str(e)

@app.route('/')
def home():
    """Página inicial"""
    return {
        "message": "🚀 API Databricks REST → Toqan",
        "status": "production",
        "version": "rest-api",
        "databricks_host": DATABRICKS_CONFIG.get("host", "não configurado"),
        "endpoints": {
            "/health": "Status da conexão",
            "/query": "Executa queries SQL",
            "/tables": "Lista tabelas disponíveis"
        },
        "exemplo": "/query?sql=SELECT * FROM sua_tabela LIMIT 10",
        "timestamp": datetime.now().isoformat()
    }

@app.route('/health')
def health():
    """Testa conexão com Databricks"""
    try:
        if not validate_config():
            missing = [k for k, v in DATABRICKS_CONFIG.items() if not v]
            return {
                "status": "error",
                "message": "Configurações faltando",
                "missing": missing
            }, 500
        
        # Teste simples
        data, error = execute_databricks_query("SELECT 1 as test, CURRENT_TIMESTAMP() as timestamp")
        
        if error:
            return {
                "status": "error",
                "message": error,
                "databricks_connection": "failed"
            }, 500
        
        return {
            "status": "healthy",
            "databricks_connection": "ok",
            "test_query": "SELECT 1",
            "test_result": data[0] if data else None,
            "host": DATABRICKS_CONFIG["host"],
            "warehouse_id": DATABRICKS_CONFIG["warehouse_id"]
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }, 500

@app.route('/tables')
def list_tables():
    """Lista tabelas"""
    try:
        data, error = execute_databricks_query("SHOW TABLES")
        
        if error:
            return {"status": "error", "message": error}, 500
        
        return {
            "status": "success",
            "tables_count": len(data),
            "tables": data,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500

@app.route('/query')
def execute_query():
    """Executa query SQL"""
    try:
        sql_query = request.args.get('sql')
        limit = request.args.get('limit', 1000)
        
        if not sql_query:
            return {
                "status": "error", 
                "message": "Parâmetro 'sql' é obrigatório"
            }, 400
        
        # Segurança básica
        dangerous = ['drop', 'delete', 'truncate', 'alter', 'create']
        if any(word in sql_query.lower() for word in dangerous):
            return {
                "status": "error",
                "message": "Query contém comandos não permitidos"
            }, 400
        
        # Adicionar LIMIT se necessário
        if "limit" not in sql_query.lower():
            sql_query += f" LIMIT {limit}"
        
        data, error = execute_databricks_query(sql_query)
        
        if error:
            return {
                "status": "error",
                "query": sql_query,
                "message": error
            }, 500
        
        return {
            "status": "success",
            "query": sql_query,
            "row_count": len(data),
            "columns": list(data[0].keys()) if data else [],
            "data": data,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "query": sql_query if 'sql_query' in locals() else None,
            "message": str(e)
        }, 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
