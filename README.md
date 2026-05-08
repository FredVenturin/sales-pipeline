# sales-pipeline

Pipeline de dados de vendas com arquitetura Medallion, construído com Python, Apache Airflow, dbt e PostgreSQL — tudo containerizado com Docker.

Gera dados simulados de vendas, processa em três camadas de qualidade (Bronze → Silver → Gold) e orquestra a execução diária via Airflow.

---

## Arquitetura Medallion

```
Airflow DAG (@daily)
        ↓
[Bronze] → CSV bruto gerado em data/bronze/
        ↓
[Silver] → dbt staging: limpeza e tipagem no PostgreSQL
        ↓
[Gold]   → dbt marts: agregações analíticas
            ├── revenue_by_region
            ├── top_products
            ├── customer_summary
            └── monthly_trends
```

---

## Stack

| Ferramenta | Uso |
|---|---|
| Python 3.11 | geração de dados e ingestão Bronze |
| Apache Airflow 2.9 | orquestração do pipeline |
| dbt Core 1.7 | transformações Silver e Gold |
| PostgreSQL 15 | data warehouse |
| Docker + Compose | containerização do ambiente |
| pytest | testes de qualidade |

---

## Como rodar

**Pré-requisitos:** Docker Desktop instalado e rodando.

**1. Clonar o repositório**
```bash
git clone https://github.com/FredVenturin/sales-pipeline.git
cd sales-pipeline
```

**2. Subir o ambiente**
```bash
docker-compose up -d
```

Aguarde ~30 segundos para os serviços inicializarem.

**3. Acessar o Airflow**

Acesse `http://localhost:8080` com:
- **Usuário:** admin
- **Senha:** admin

**4. Executar o pipeline**

Na interface do Airflow, encontre a DAG `sales_pipeline` e clique em **Trigger DAG**.

O pipeline executa automaticamente em 4 etapas:
1. `generate_data` — gera 500 pedidos simulados com erros intencionais
2. `load_bronze` — carrega os dados no PostgreSQL
3. `dbt_staging` — limpa e tipifica os dados (Silver)
4. `dbt_marts` — agrega os dados para análise (Gold)

---

## Estrutura do projeto

```
sales-pipeline/
├── dags/
│   └── sales_pipeline.py     # DAG do Airflow
├── dbt/
│   ├── models/
│   │   ├── staging/          # Silver: limpeza e tipagem
│   │   └── marts/            # Gold: agregações analíticas
│   └── dbt_project.yml
├── scripts/
│   ├── generate_data.py      # gerador de dados simulados
│   └── load_bronze.py        # ingestão Bronze → PostgreSQL
├── tests/                    # testes pytest
├── Dockerfile                # imagem Airflow com dbt
└── docker-compose.yml        # infraestrutura completa
```

---

## Modelos Gold

| Modelo | Pergunta respondida |
|---|---|
| `revenue_by_region` | Qual a receita por região e mês? |
| `top_products` | Quais produtos vendem mais? |
| `customer_summary` | Qual o comportamento de compra por cliente? |
| `monthly_trends` | Como a receita evolui mês a mês? |

---

## Decisões técnicas

**Por que Docker?**
Garante que o ambiente é idêntico em qualquer máquina. Airflow, PostgreSQL e dbt sobem com um único comando.

**Por que dbt para transformações?**
dbt versiona as transformações como SQL puro, gera documentação e lineage automáticos, e tem testes de qualidade declarativos — padrão de mercado para camadas Silver e Gold.

**Por que Airflow?**
Orquestração com retry automático, agendamento e interface visual de monitoramento — cada task mapeada para uma camada Medallion.

**Nota sobre encoding no Windows:**
O `load_bronze.py` usa psycopg2 conectando diretamente ao host `postgres` do Docker. Isso funciona corretamente dentro do container Linux — sem dependência do encoding do sistema operacional local.

