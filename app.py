
from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import json
import os
import logging
from datetime import datetime

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurações do Databricks
DATABRICKS_CONFIG = {
    "host": os.getenv("DATABRICKS_HOST"),
    "token": os.getenv("DATABRICKS_TOKEN"),
    "warehouse_id": os.getenv("DATABRICKS_WAREHOUSE_ID")
}

# Query base para comentários (personalizável)
COMMENTS_QUERY = os.getenv("COMMENTS_QUERY", """
SELECT 
    comment_id,
    customer_id,
    restaurant_id,
    order_id,
    rating,
    comment_text,
    created_at,
    city,
    region,
    delivery_time,
    order_value
FROM comments 
WHERE created_at >= CURRENT_DATE - 30
ORDER BY created_at DESC
""")

def validate_config():
    missing = [k for k, v in DATABRICKS_CONFIG.items() if not v]
    return len(missing) == 0, missing

def execute_databricks_query(sql_query):
    """Executa query no Databricks"""
    try:
        valid, missing = validate_config()
        if not valid:
            return None, f"Configurações faltando: {missing}"
        
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
        
        logger.info(f"Executando query: {sql_query[:100]}...")
        response = requests.post(url, headers=headers, json=payload, timeout=60)
        
        if response.status_code != 200:
            return None, f"Erro HTTP {response.status_code}: {response.text}"
        
        result = response.json()
        status = result.get("status", {})
        if status.get("state") != "SUCCEEDED":
            error_msg = status.get("error", {}).get("message", "Erro desconhecido")
            return None, f"Query falhou: {error_msg}"
        
        # Processar resultados
        data = []
        if "result" in result and "data_array" in result["result"]:
            chunks = result["result"]["data_array"]
            manifest = result.get("manifest", {})
            schema = manifest.get("schema", {}).get("columns", [])
            column_names = [col.get("name", f"col_{i}") for i, col in enumerate(schema)]
            
            for chunk in chunks:
                if isinstance(chunk, list):
                    for row in chunk:
                        if isinstance(row, list) and len(row) > 0:
                            row_dict = {}
                            for i, value in enumerate(row):
                                col_name = column_names[i] if i < len(column_names) else f"col_{i}"
                                row_dict[col_name] = value
                            data.append(row_dict)
        
        return data, None
        
    except Exception as e:
        return None, str(e)

@app.route('/')
def home():
    return {
        "message": "🎯 Agente Inteligente de Comentários iFood",
        "description": "Análise avançada com categorização automática por temas",
        "version": "smart-comments-v1",
        "endpoints": {
            "/comments": "Todos comentários com análise completa",
            "/comments/by-region": "Agrupados por região",
            "/comments/by-category": "Categorizados por tema (entrega, qualidade, etc)",
            "/comments/delivery-issues": "Problemas de entrega",
            "/comments/food-quality": "Qualidade da comida", 
            "/comments/service-issues": "Problemas de atendimento",
            "/comments/app-issues": "Problemas no app",
            "/comments/trending": "Análise temporal (últimos 7 dias)",
            "/health": "Status da conexão"
        },
        "categorias_automaticas": [
            "Entrega (tempo, atraso, entregador)",
            "Qualidade (comida fria, sabor, quantidade)",
            "Atendimento (restaurante, suporte)",
            "App/Sistema (bugs, pagamento, pedido)",
            "Preço (caro, taxa, promoção)",
            "Experiência geral"
        ],
        "uso_toqan": {
            "geral": "Toqan, analise: https://sua-api.com/comments",
            "por_tema": "Toqan, analise: https://sua-api.com/comments/delivery-issues",
            "por_regiao": "Toqan, analise: https://sua-api.com/comments/by-region",
            "tendencias": "Toqan, analise: https://sua-api.com/comments/trending"
        },
        "timestamp": datetime.now().isoformat()
    }

@app.route('/health')
def health():
    try:
        valid, missing = validate_config()
        if not valid:
            return {"status": "error", "missing": missing}, 500
        
        data, error = execute_databricks_query("SELECT 1 as test")
        if error:
            return {"status": "error", "message": error}, 500
        
        return {
            "status": "healthy",
            "databricks_connection": "ok",
            "host": DATABRICKS_CONFIG["host"]
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500

@app.route('/comments')
def get_all_comments():
    """Retorna todos comentários com dados completos para análise"""
    try:
        data, error = execute_databricks_query(COMMENTS_QUERY)
        if error:
            return {"status": "error", "message": error}, 500
        
        return {
            "status": "success",
            "analysis_type": "complete_dataset",
            "description": "Dataset completo para análise avançada no Toqan",
            "row_count": len(data),
            "columns": list(data[0].keys()) if data else [],
            "data": data,
            "timestamp": datetime.now().isoformat(),
            "analysis_capabilities": [
                "Análise de sentimento por comentário",
                "Categorização automática por tema",
                "Segmentação por região/cidade", 
                "Análise temporal (trends)",
                "Correlação rating vs tempo de entrega",
                "Identificação de palavras-chave",
                "Ranking de problemas por frequência"
            ]
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500

@app.route('/comments/by-region')
def get_comments_by_region():
    """Comentários agrupados por região para análise geográfica"""
    try:
        regional_query = f"""
        SELECT 
            region,
            city,
            COUNT(*) as total_comments,
            AVG(rating) as avg_rating,
            comment_text,
            created_at,
            rating
        FROM ({COMMENTS_QUERY.rstrip().rstrip(';')}) base_comments
        GROUP BY region, city, comment_text, created_at, rating
        ORDER BY region, avg_rating ASC
        """
        
        data, error = execute_databricks_query(regional_query)
        if error:
            return {"status": "error", "message": error}, 500
        
        return {
            "status": "success",
            "analysis_type": "regional_segmentation",
            "description": "Dados segmentados por região para análise geográfica",
            "row_count": len(data),
            "data": data,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500

@app.route('/comments/delivery-issues')
def get_delivery_issues():
    """Comentários sobre problemas de entrega"""
    try:
        delivery_query = f"""
        SELECT *
        FROM ({COMMENTS_QUERY.rstrip().rstrip(';')}) base_comments
        WHERE LOWER(comment_text) REGEXP '(atras|demor|entrega|rapido|lento|tempo|entreg|delay)'
           OR delivery_time > 60
        ORDER BY created_at DESC
        """
        
        data, error = execute_databricks_query(delivery_query)
        if error:
            return {"status": "error", "message": error}, 500
        
        return {
            "status": "success",
            "analysis_type": "delivery_issues",
            "description": "Comentários relacionados a problemas de entrega",
            "keywords_filtered": ["atraso", "demora", "entrega", "rápido", "lento", "tempo"],
            "row_count": len(data),
            "data": data,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500

@app.route('/comments/food-quality')
def get_food_quality():
    """Comentários sobre qualidade da comida"""
    try:
        quality_query = f"""
        SELECT *
        FROM ({COMMENTS_QUERY.rstrip().rstrip(';')}) base_comments
        WHERE LOWER(comment_text) REGEXP '(fri|quent|sabor|gostoso|ruim|qualidade|comida|delicio|horrible)'
        ORDER BY rating ASC, created_at DESC
        """
        
        data, error = execute_databricks_query(quality_query)
        if error:
            return {"status": "error", "message": error}, 500
        
        return {
            "status": "success", 
            "analysis_type": "food_quality",
            "description": "Comentários sobre qualidade da comida",
            "keywords_filtered": ["frio", "quente", "sabor", "gostoso", "ruim", "qualidade", "comida"],
            "row_count": len(data),
            "data": data,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500

@app.route('/comments/service-issues')
def get_service_issues():
    """Comentários sobre atendimento"""
    try:
        service_query = f"""
        SELECT *
        FROM ({COMMENTS_QUERY.rstrip().rstrip(';')}) base_comments
        WHERE LOWER(comment_text) REGEXP '(atendiment|educad|grosso|mal.atend|suport|ajuda|problem)'
        ORDER BY rating ASC, created_at DESC
        """
        
        data, error = execute_databricks_query(service_query)
        if error:
            return {"status": "error", "message": error}, 500
        
        return {
            "status": "success",
            "analysis_type": "service_issues", 
            "description": "Comentários sobre problemas de atendimento",
            "keywords_filtered": ["atendimento", "educado", "grosso", "mal atendido", "suporte"],
            "row_count": len(data),
            "data": data,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500

@app.route('/comments/trending')
def get_trending_analysis():
    """Análise de tendências dos últimos 7 dias"""
    try:
        trending_query = f"""
        SELECT 
            DATE(created_at) as date,
            region,
            city,
            COUNT(*) as comments_count,
            AVG(rating) as avg_rating,
            comment_text,
            rating,
            created_at
        FROM ({COMMENTS_QUERY.rstrip().rstrip(';')}) base_comments
        WHERE created_at >= CURRENT_DATE - 7
        GROUP BY DATE(created_at), region, city, comment_text, rating, created_at
        ORDER BY date DESC, avg_rating ASC
        """
        
        data, error = execute_databricks_query(trending_query)
        if error:
            return {"status": "error", "message": error}, 500
        
        return {
            "status": "success",
            "analysis_type": "trending_7_days",
            "description": "Análise temporal dos últimos 7 dias para identificar tendências",
            "row_count": len(data),
            "data": data,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
