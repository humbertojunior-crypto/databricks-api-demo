
from flask import Flask, request, jsonify, render_template_string
from databricks import sql
import pandas as pd
import os
import logging
from flask_cors import CORS
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurações do Databricks (via variáveis de ambiente)
DATABRICKS_CONFIG = {
    "server_hostname": os.getenv("DATABRICKS_HOST"),
    "http_path": os.getenv("DATABRICKS_PATH"), 
    "access_token": os.getenv("DATABRICKS_TOKEN")
}

def validate_config():
    """Valida se todas as configurações necessárias estão presentes"""
    missing = []
    for key, value in DATABRICKS_CONFIG.items():
        if not value:
            missing.append(key)
    
    if missing:
        logger.error(f"Configurações faltando: {missing}")
        return False
    return True

def get_databricks_connection():
    """Cria conexão com Databricks"""
    try:
        if not validate_config():
            return None
            
        connection = sql.connect(**DATABRICKS_CONFIG)
        logger.info("✅ Conexão Databricks estabelecida")
        return connection
    except Exception as e:
        logger.error(f"❌ Erro ao conectar Databricks: {str(e)}")
        return None

@app.route('/')
def home():
    """Página inicial da API Real"""
    return {
        "message": "🚀 API Databricks Real → Toqan",
        "status": "production",
        "databricks_host": DATABRICKS_CONFIG.get("server_hostname", "não configurado"),
        "endpoints": {
            "/health": "Status da conexão Databricks",
            "/query": "Executa queries SQL no Databricks",
            "/tables": "Lista tabelas disponíveis",
            "/describe/<table>": "Estrutura da tabela"
        },
        "exemplo_uso": "/query?sql=SELECT * FROM sua_tabela LIMIT 10",
        "configuracao": "Configurado via variáveis de ambiente",
        "timestamp": datetime.now().isoformat()
    }

@app.route('/health')
def health():
    """Testa conexão com Databricks"""
    try:
        if not validate_config():
            return {
                "status": "error",
                "message": "Configurações Databricks não encontradas",
                "missing": [k for k, v in DATABRICKS_CONFIG.items() if not v]
            }, 500
            
        connection = get_databricks_connection()
        if not connection:
            return {
                "status": "error", 
                "message": "Falha na conexão com Databricks"
            }, 500
            
        # Teste rápido
        cursor = connection.cursor()
        cursor.execute("SELECT 1 as test, CURRENT_TIMESTAMP() as timestamp")
        result = cursor.fetchone()
        cursor.close()
        connection.close()
        
        return {
            "status": "healthy",
            "databricks_connection": "ok",
            "test_query": "SELECT 1",
            "test_result": result[0] if result else None,
            "databricks_time": str(result[1]) if result and len(result) > 1 else None,
            "host": DATABRICKS_CONFIG["server_hostname"]
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return {
            "status": "error",
            "message": str(e),
            "databricks_connection": "failed"
        }, 500

@app.route('/tables')
def list_tables():
    """Lista todas as tabelas disponíveis"""
    try:
        connection = get_databricks_connection()
        if not connection:
            return {"status": "error", "message": "Conexão Databricks falhou"}, 500
            
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        cursor.close()
        connection.close()
        
        # Converte para lista de dicionários
        tables = []
        for row in results:
            table_dict = {columns[i]: row[i] for i in range(len(columns))}
            tables.append(table_dict)
            
        return {
            "status": "success",
            "tables_count": len(tables),
            "tables": tables,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"List tables error: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }, 500

@app.route('/query')
def execute_query():
    """Executa query SQL no Databricks"""
    try:
        # Validação da query
        sql_query = request.args.get('sql')
        limit = request.args.get('limit', 1000)
        
        if not sql_query:
            return {
                "status": "error",
                "message": "Parâmetro 'sql' é obrigatório",
                "exemplo": "/query?sql=SELECT * FROM sua_tabela LIMIT 10"
            }, 400
        
        # Segurança básica - evitar queries perigosas
        dangerous_keywords = ['drop', 'delete', 'truncate', 'alter', 'create']
        if any(keyword in sql_query.lower() for keyword in dangerous_keywords):
            return {
                "status": "error", 
                "message": "Query contém comandos não permitidos",
                "blocked_keywords": dangerous_keywords
            }, 400
        
        # Adicionar LIMIT se não tiver
        if "limit" not in sql_query.lower() and "count" not in sql_query.lower():
            sql_query += f" LIMIT {limit}"
        
        # Executar query
        connection = get_databricks_connection()
        if not connection:
            return {"status": "error", "message": "Conexão Databricks falhou"}, 500
            
        cursor = connection.cursor()
        logger.info(f"Executando query: {sql_query}")
        
        cursor.execute(sql_query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        cursor.close()
        connection.close()
        
        # Converte para lista de dicionários
        data = []
        for row in results:
            row_dict = {columns[i]: row[i] for i in range(len(columns))}
            data.append(row_dict)
        
        return {
            "status": "success",
            "query": sql_query,
            "row_count": len(data),
            "columns": columns,
            "data": data,
            "timestamp": datetime.now().isoformat(),
            "execution_time": "< 1s"
        }
        
    except Exception as e:
        logger.error(f"Query execution error: {str(e)}")
        return {
            "status": "error",
            "query": sql_query,
            "message": str(e)
        }, 500

@app.route('/describe/<table_name>')
def describe_table(table_name):
    """Retorna estrutura da tabela"""
    try:
        connection = get_databricks_connection()
        if not connection:
            return {"status": "error", "message": "Conexão Databricks falhou"}, 500
            
        cursor = connection.cursor()
        cursor.execute(f"DESCRIBE TABLE {table_name}")
        
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        cursor.close()
        connection.close()
        
        # Converte para lista de dicionários
        schema = []
        for row in results:
            col_dict = {columns[i]: row[i] for i in range(len(columns))}
            schema.append(col_dict)
            
        return {
            "status": "success",
            "table": table_name,
            "schema": schema,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Describe table error: {str(e)}")
        return {
            "status": "error",
            "table": table_name,
            "message": str(e)
        }, 500

@app.route('/quick/<table_name>')
def quick_sample(table_name):
    """Amostra rápida de uma tabela"""
    try:
        limit = request.args.get('limit', 10)
        sql_query = f"SELECT * FROM {table_name} LIMIT {limit}"
        
        # Redireciona para o endpoint de query
        return execute_query_direct(sql_query)
        
    except Exception as e:
        return {
            "status": "error",
            "table": table_name,
            "message": str(e)
        }, 500

def execute_query_direct(sql_query):
    """Executa query diretamente (função auxiliar)"""
    try:
        connection = get_databricks_connection()
        if not connection:
            return {"status": "error", "message": "Conexão Databricks falhou"}, 500
            
        cursor = connection.cursor()
        cursor.execute(sql_query)
        
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        cursor.close()
        connection.close()
        
        data = []
        for row in results:
            row_dict = {columns[i]: row[i] for i in range(len(columns))}
            data.append(row_dict)
        
        return {
            "status": "success",
            "query": sql_query,
            "row_count": len(data),
            "columns": columns,
            "data": data
        }
        
    except Exception as e:
        return {
            "status": "error",
            "query": sql_query,
            "message": str(e)
        }, 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('DEBUG', 'False').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug)
