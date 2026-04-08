
from flask import Flask, request, jsonify
from flask_cors import CORS
import json
from datetime import datetime, timedelta
import random

app = Flask(__name__)
CORS(app)

# Dados demo simulando Databricks
DEMO_TABLES = [
    {"database": "analytics", "tableName": "vendas"},
    {"database": "analytics", "tableName": "clientes"}, 
    {"database": "sales", "tableName": "pedidos"},
    {"database": "marketing", "tableName": "campanhas"}
]

DEMO_VENDAS = [
    {
        "data": "2024-12-01",
        "cidade": "São Paulo",
        "categoria": "Pizza",
        "valor": 45.50,
        "pedidos": 120
    },
    {
        "data": "2024-12-01", 
        "cidade": "Rio de Janeiro",
        "categoria": "Hambúrguer",
        "valor": 32.80,
        "pedidos": 95
    },
    {
        "data": "2024-12-02",
        "cidade": "São Paulo", 
        "categoria": "Sushi",
        "valor": 78.90,
        "pedidos": 67
    },
    {
        "data": "2024-12-02",
        "cidade": "Belo Horizonte",
        "categoria": "Pizza", 
        "valor": 41.20,
        "pedidos": 84
    }
]

def generate_more_data():
    """Gera mais dados demo para simular um dataset maior"""
    cidades = ["São Paulo", "Rio de Janeiro", "Belo Horizonte", "Salvador", "Brasília"]
    categorias = ["Pizza", "Hambúrguer", "Sushi", "Comida Italiana", "Comida Mexicana"]
    
    data = []
    base_date = datetime(2024, 11, 1)
    
    for i in range(100):
        date_offset = timedelta(days=random.randint(0, 30))
        current_date = (base_date + date_offset).strftime("%Y-%m-%d")
        
        data.append({
            "data": current_date,
            "cidade": random.choice(cidades),
            "categoria": random.choice(categorias), 
            "valor": round(random.uniform(25.0, 85.0), 2),
            "pedidos": random.randint(50, 200)
        })
    
    return data

@app.route('/')
def home():
    """Página inicial da API Demo"""
    return {
        "message": "🚀 API Demo Databricks → Toqan",
        "status": "demo_mode",
        "note": "Esta é uma versão demo com dados simulados",
        "endpoints": {
            "/query": "Execute 'queries' com dados demo",
            "/tables": "Liste tabelas demo disponíveis",
            "/health": "Status da API"
        },
        "exemplo_uso": "?sql=SELECT cidade, SUM(valor) FROM vendas GROUP BY cidade"
    }

@app.route('/health')
def health():
    """Status da API demo"""
    return {
        "status": "healthy",
        "mode": "demo",
        "databricks_connection": "simulated",
        "message": "API demo funcionando perfeitamente!"
    }

@app.route('/tables')  
def list_tables():
    """Lista tabelas demo"""
    return {
        "status": "success",
        "mode": "demo",
        "tables_count": len(DEMO_TABLES),
        "tables": DEMO_TABLES,
        "note": "Dados simulados para demonstração"
    }

@app.route('/query')
def execute_query():
    """Simula execução de queries"""
    sql_query = request.args.get('sql', '').lower()
    
    if not sql_query:
        return {
            "status": "error",
            "message": "Parâmetro 'sql' é obrigatório"
        }, 400
    
    # Simula diferentes tipos de query
    if 'vendas' in sql_query or 'pedidos' in sql_query:
        # Gera dataset maior para análise
        if 'limit 5' in sql_query:
            data = DEMO_VENDAS[:5]
        elif 'group by' in sql_query:
            # Simula agregação
            if 'cidade' in sql_query:
                data = [
                    {"cidade": "São Paulo", "total_valor": 2450.30, "total_pedidos": 156},
                    {"cidade": "Rio de Janeiro", "total_valor": 1890.75, "total_pedidos": 134},
                    {"cidade": "Belo Horizonte", "total_valor": 1234.50, "total_pedidos": 89}
                ]
            elif 'categoria' in sql_query:
                data = [
                    {"categoria": "Pizza", "total_valor": 1876.20, "total_pedidos": 145},
                    {"categoria": "Hambúrguer", "total_valor": 1543.80, "total_pedidos": 122},
                    {"categoria": "Sushi", "total_valor": 2156.30, "total_pedidos": 87}
                ]
        else:
            # Dataset completo
            data = generate_more_data()
    else:
        # Query genérica
        data = [
            {"resultado": "Query executada com sucesso", "timestamp": datetime.now().isoformat()},
            {"note": "Esta é uma simulação de dados", "query": sql_query}
        ]
    
    return {
        "status": "success", 
        "mode": "demo",
        "query": sql_query,
        "row_count": len(data),
        "columns": list(data[0].keys()) if data else [],
        "data": data,
        "note": "Dados simulados - versão demo"
    }

@app.route('/describe/<table_name>')
def describe_table(table_name):
    """Simula estrutura de tabela"""
    schemas = {
        "vendas": [
            {"col_name": "data", "data_type": "date"},
            {"col_name": "cidade", "data_type": "string"}, 
            {"col_name": "categoria", "data_type": "string"},
            {"col_name": "valor", "data_type": "decimal"},
            {"col_name": "pedidos", "data_type": "int"}
        ],
        "clientes": [
            {"col_name": "id", "data_type": "bigint"},
            {"col_name": "nome", "data_type": "string"},
            {"col_name": "email", "data_type": "string"},
            {"col_name": "cidade", "data_type": "string"}
        ]
    }
    
    schema = schemas.get(table_name, [{"col_name": "id", "data_type": "string"}])
    
    return {
        "status": "success",
        "mode": "demo", 
        "table": table_name,
        "schema": schema
    }

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
