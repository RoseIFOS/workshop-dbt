# 📊 Workshop dbt + Airflow

Pipeline de dados **ponta a ponta** que **gera dados sintéticos** de e-commerce, **modela** com
**dbt** (camadas staging → intermediate → mart, em *star schema*) e **orquestra** com **Apache
Airflow** (Astronomer + Cosmos), rodando em **PostgreSQL** local (dev) ou na nuvem via **Railway**
(prod).

```
gerar dados → carregar no Postgres → transformar com dbt → orquestrar com Airflow → deploy na nuvem
```

---

## 📚 Documentação

| Documento | Para que serve |
|-----------|----------------|
| 🎓 **[Guia do Aluno](docs/GUIA_DO_ALUNO.md)** | Tutorial **passo a passo**, do zero, para quem está **aprendendo** Engenharia de Dados. Começa por aqui. |
| 📘 **[Documentação Completa](docs/DOCUMENTACAO.md)** | **Referência** técnica do projeto (arquitetura, modelos, deploy, troubleshooting). Para consulta rápida. |
| 🖼️ **[Diagramas](docs/diagrams/)** | Arquitetura, fluxo de dados, deploy e modelo dimensional (Mermaid + PNG/SVG). |

> 👉 **É iniciante?** Comece pelo **[Guia do Aluno](docs/GUIA_DO_ALUNO.md)**.
> 👉 **Já conhece o projeto e quer consultar algo específico?** Vá direto à **[Documentação Completa](docs/DOCUMENTACAO.md)**.

---

## 🔗 Repositórios dependentes

O projeto é composto por **dois repositórios Git independentes** (cada etapa tem seu próprio ciclo
de vida e deploy). O `3_airflow` consome o `2_data_warehouse` como **submódulo Git**.

| Repositório | Etapa / Pasta | Conteúdo | Link |
|-------------|---------------|----------|------|
| **workshop-dbt-dw** | Etapa 2 · `2_data_warehouse` | Projeto **dbt** (models staging → intermediate → mart) | [github.com/RoseIFOS/workshop-dbt-dw](https://github.com/RoseIFOS/workshop-dbt-dw) |
| **workshop-dbt-airflow** | Etapa 3 · `3_airflow` | Projeto **Astro/Airflow** (DAG via Cosmos) | [github.com/RoseIFOS/workshop-dbt-airflow](https://github.com/RoseIFOS/workshop-dbt-airflow) |

> 🔁 **Dependência:** `workshop-dbt-airflow` referencia `workshop-dbt-dw` via submódulo (em
> `3_airflow/dbt/workshop-dbt-dw`), usando URL **HTTPS** para que o build na nuvem funcione sem
> chave SSH.

---

## 🧰 Stack de tecnologias

| Camada | Tecnologia |
|--------|-----------|
| Geração de dados | Python, Faker (pt_BR), Pandas, NumPy, DuckDB |
| Banco (dev) | PostgreSQL 16 (Docker local, porta 5433) |
| Banco (prod) | PostgreSQL gerenciado na **Railway** |
| Transformação | **dbt** (dbt-core + dbt-postgres) |
| Pacotes dbt | dbt_utils, dbt_expectations, dbt_date |
| Orquestração | **Apache Airflow** via **Astronomer Runtime** + **Cosmos** |
| Deploy | Astro CLI (`astro deploy` + `astro dbt deploy`) |
| CI/CD | GitHub Actions |
| Gerenciador de pacotes Python | `uv` (no setup local) |

---

## 🚀 Início rápido

> Pré-requisitos: Docker Desktop, `uv`, Astro CLI e Git instalados. Passo a passo completo de
> instalação no **[Guia do Aluno](docs/GUIA_DO_ALUNO.md)**.

```powershell
# 1) Etapa 1 — sobe o Postgres local e gera os dados (dentro de 1_local_setup)
docker compose up -d
uv sync
uv run python generate_fake_data.py

# 2) Etapa 2 — constrói e testa o data warehouse (dentro de 2_data_warehouse/wks_dbt)
dbt deps
dbt build        # = seed + run + test

# 3) Etapa 3 — sobe o Airflow local (dentro de 3_airflow)
astro dev start  # UI em http://localhost:8080 (admin / admin)
```

---

## 🗂️ Estrutura do projeto

```
wks_dbt_airflow/
├── 1_local_setup/                 # Etapa 1: geração de dados + Postgres local + documentação
│   ├── docker-compose.yml         # Postgres 16 (container dbt_postgres, porta 5433)
│   ├── generate_fake_data.py      # Gera cadastros/pedidos sintéticos → CSV
│   ├── pyproject.toml             # Deps (uv): faker, pandas, duckdb, dbt...
│   ├── README.md                  # Este arquivo (ponto de entrada do projeto)
│   └── docs/                      # Documentação
│       ├── DOCUMENTACAO.md        #   referência técnica
│       ├── GUIA_DO_ALUNO.md       #   tutorial passo a passo
│       └── diagrams/              #   diagramas (.mmd, .png, .svg)
│
├── 2_data_warehouse/              # Etapa 2: projeto dbt (repo Git: workshop-dbt-dw)
│   └── wks_dbt/                   #   models/ staging → intermediate → mart
│
└── 3_airflow/                     # Etapa 3: projeto Astro/Airflow (repo Git: workshop-dbt-airflow)
    ├── dags/dag.py                #   DbtDag (Cosmos), switch dev/prod
    └── dbt/workshop-dbt-dw/       #   submódulo → projeto dbt
```

---

## 🔄 O que o pipeline produz

A partir de **clientes** (`cadastros`) e **compras** (`pedidos`), o dbt entrega dois *marts*
prontos para consumo em BI:

- **`mart_metricas_clientes`** — segmentação **RFM** (Campeão, Cliente Fiel, Potencial, Em Risco
  de Churn, etc.), ticket médio, recência, frequência e estação preferida.
- **`mart_vendas_por_periodo`** — vendas agregadas por dia, com média móvel de 7 dias, variação e
  taxa de crescimento.

Detalhes de cada modelo, testes e regras de negócio no **[Guia do Aluno](docs/GUIA_DO_ALUNO.md)**.
