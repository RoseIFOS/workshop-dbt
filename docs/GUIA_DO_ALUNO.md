# 🎓 Guia do Aluno — Construindo um Pipeline de Dados com dbt + Airflow do Zero

> **Para quem é este guia:** você está **aprendendo Engenharia de Dados** e quer construir,
> com as próprias mãos, um pipeline completo: gerar dados → carregar num banco → transformar
> com **dbt** → orquestrar com **Airflow** → colocar na **nuvem**.
>
> **Como usar:** siga **na ordem**, de cima para baixo. Cada passo diz exatamente **o que clicar**,
> **qual comando rodar** e **em qual pasta**. Sempre que aparecer um 💡 **Por quê?**, pare e leia —
> é ali que você aprende de verdade, não só copia.
>
> **Premissa deste guia:** o óbvio será dito. Se você já sabe algo, pule. Se é iniciante, nada
> ficará "subentendido".

---

## 📑 Índice

- [Parte 0 — Conceitos que você PRECISA entender antes de começar](#parte-0--conceitos-que-você-precisa-entender-antes-de-começar)
- [Parte 1 — Instalando as ferramentas (uma única vez)](#parte-1--instalando-as-ferramentas-uma-única-vez)
- [Parte 2 — Etapa 1: Setup local e geração de dados](#parte-2--etapa-1-setup-local-e-geração-de-dados)
- [Parte 3 — Etapa 2: O Data Warehouse com dbt](#parte-3--etapa-2-o-data-warehouse-com-dbt)
- [Parte 4 — Etapa 3: Orquestração com Airflow + Cosmos](#parte-4--etapa-3-orquestração-com-airflow--cosmos)
- [Parte 5 — CI/CD com GitHub Actions](#parte-5--cicd-com-github-actions)
- [Parte 6 — Deploy na nuvem (Astronomer)](#parte-6--deploy-na-nuvem-astronomer)
- [Parte 7 — Troubleshooting (erros comuns e como sair deles)](#parte-7--troubleshooting-erros-comuns-e-como-sair-deles)
- [Glossário](#glossário)

---

## Parte 0 — Conceitos que você PRECISA entender antes de começar

Não pule esta parte. Cinco minutos aqui economizam horas de confusão depois.

### O que vamos construir?

Imagine uma loja online (e-commerce). Ela tem **clientes** (cadastros) e **compras** (pedidos).
Esses dados nascem "crus" e bagunçados. Nosso trabalho como Engenheiro de Dados é **transformá-los**
em tabelas limpas e prontas para análise — por exemplo, uma tabela que diz *"este cliente é um
Campeão, gastou R$ 8.000 e comprou pela última vez há 5 dias"*.

O fluxo completo é:

```
gerar dados → carregar no Postgres → transformar com dbt → orquestrar com Airflow → deploy na nuvem
```

### ELT vs ETL (entenda a sigla)

- **ETL** = Extract, Transform, Load → transforma os dados **antes** de carregar no banco.
- **ELT** = Extract, Load, **Transform** → carrega os dados crus no banco **primeiro** e transforma
  **dentro** do banco, usando SQL.

Este projeto é **ELT**. 💡 **Por quê?** Bancos modernos são muito rápidos em SQL. É mais simples,
versionável e barato deixar o banco fazer o trabalho pesado de transformação. O **dbt** é a
ferramenta que organiza esse "T" do ELT.

### O que é o dbt? (em uma frase)

**dbt** (data build tool) é uma ferramenta que deixa você escrever transformações de dados como
**arquivos SQL versionados no Git**, com **testes automáticos** e **documentação**. Cada arquivo
`.sql` vira uma **tabela ou view** no banco. Você escreve um `SELECT`, o dbt cuida do `CREATE TABLE`.

### O que é o Airflow? (em uma frase)

**Apache Airflow** é um **orquestrador**: ele agenda e executa tarefas na ordem certa, no horário
certo, e te avisa se algo falha. No nosso caso, ele vai rodar o dbt **todo dia automaticamente**.

### O que é o Cosmos?

**Cosmos** (da Astronomer) é uma "ponte" que transforma seu projeto dbt em uma **DAG do Airflow
automaticamente** — cada modelo dbt vira uma tarefa no Airflow. Você não precisa escrever a
orquestração na mão.

### As 3 camadas do dbt (a regra de ouro deste projeto)

Os dados fluem por **3 camadas**, sempre nesta direção:

```
seeds (CSV cru) → staging → intermediate → mart
```

| Camada | O que faz | Vira o quê no banco? |
|--------|-----------|----------------------|
| **staging** | Limpa e **renomeia** colunas. 1 staging para cada fonte. Sem regra de negócio. | `view` (leve, sempre atualizada) |
| **intermediate** | Cria **dimensões** e **fatos** (o modelo dimensional / star schema). | `table` (materializada) |
| **mart** | Tabelas **prontas para o consumo** (dashboards, BI), com regras de negócio aplicadas. | `table` |

💡 **Por quê separar em camadas?** Para **organização e manutenção**. Se a fonte muda um nome de
coluna, você arruma **só na staging** e o resto continua funcionando. É o mesmo princípio de
"separar responsabilidades" da programação.

### Star schema (modelo dimensional) em 30 segundos

Um **star schema** organiza os dados em:
- **Tabelas-fato** (`fact`): o que **aconteceu** e é mensurável → cada **pedido**, com valor.
- **Tabelas-dimensão** (`dim`): o **contexto** → quem é o cliente (`dim_clientes`), em que dia
  (`dim_date`).

As dimensões se conectam à fato por **chaves**. Desenhado, parece uma estrela (a fato no centro,
as dimensões em volta). 💡 **Por quê?** É o formato que ferramentas de BI (Power BI, Tableau)
entendem melhor e que torna as consultas analíticas rápidas e intuitivas.

### A estrutura de pastas do projeto

```
wks_dbt_airflow/
├── 1_local_setup/        # Etapa 1: gera dados + sobe o Postgres local + documentação
│   ├── README.md         #   ← README geral do projeto (ponto de entrada)
│   └── docs/             #   ← toda a documentação vive aqui
│       ├── DOCUMENTACAO.md   # Documentação de REFERÊNCIA (consulta rápida)
│       ├── GUIA_DO_ALUNO.md  # Este guia (tutorial passo a passo)
│       └── diagrams/         # Diagramas de arquitetura
├── 2_data_warehouse/     # Etapa 2: o projeto dbt (repositório Git próprio)
│   └── wks_dbt/          #   ← o projeto dbt de fato fica AQUI dentro
└── 3_airflow/            # Etapa 3: o projeto Airflow/Astro (outro repositório Git)
```

💡 **Por quê duas pastas viram dois repositórios Git separados** (`2_data_warehouse` e `3_airflow`)?
Porque eles têm **ciclos de vida diferentes**: o dbt muda quando a lógica de dados muda; o Airflow
muda quando a orquestração muda. Separar permite deployar um sem mexer no outro. Você vai ver isso
em detalhe na Parte 6.

---

## Parte 1 — Instalando as ferramentas (uma única vez)

Antes de qualquer código, instale o que está faltando na sua máquina. Este guia assume **Windows
11 com PowerShell** (mas os comandos dbt/git são iguais em qualquer SO).

> ✅ **Como saber se já tenho algo instalado?** Abra o terminal (PowerShell) e rode o comando de
> versão. Se aparecer um número de versão, está instalado. Se der "não reconhecido", precisa instalar.

### 1.1 — Abra o PowerShell

Pressione a tecla **Windows**, digite `PowerShell`, clique em **Windows PowerShell**. Uma janela
preta/azul abre. É nela que você roda os comandos deste guia.

### 1.2 — Git (controle de versão)

Verifique:
```powershell
git --version
```
Se não tiver, baixe em https://git-scm.com/download/win, execute o instalador e clique
**Next** em tudo (as opções padrão servem).

### 1.3 — Docker Desktop (para subir o banco de dados)

Verifique:
```powershell
docker --version
```
💡 **Por quê Docker?** Em vez de instalar o PostgreSQL direto no Windows (chato e propenso a
conflitos), o Docker sobe um banco "dentro de um contêiner" isolado, igual em qualquer máquina.
É descartável: se bagunçar, apaga e sobe de novo.

Se não tiver: baixe **Docker Desktop** em https://www.docker.com/products/docker-desktop/,
instale, **reinicie o PC**, e **abra o Docker Desktop** (o ícone da baleia tem que ficar verde
na bandeja do sistema). **Mantenha-o aberto** sempre que for usar o banco.

### 1.4 — `uv` (gerenciador de pacotes Python, rápido)

💡 **Por quê `uv` e não `pip`?** `uv` é um gerenciador moderno, muito mais rápido, que cria o
ambiente virtual e instala dependências a partir do `pyproject.toml` num comando só. Este projeto
já usa `uv`.

Instale (no PowerShell):
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```
Feche e reabra o PowerShell. Verifique:
```powershell
uv --version
```

### 1.5 — Astro CLI (para rodar e deployar o Airflow)

💡 **Por quê Astro CLI?** É a ferramenta da Astronomer que sobe um Airflow local completo em
contêineres e faz deploy para a nuvem com um comando. Evita a configuração manual e dolorosa do
Airflow puro.

Instale (no PowerShell, como administrador — clique direito no PowerShell → **Executar como
administrador**):
```powershell
winget install -e --id Astronomer.Astro
```
Feche e reabra o PowerShell. Verifique:
```powershell
astro version
```

### 1.6 — Conta no GitHub

Se ainda não tem, crie em https://github.com/signup. Você vai precisar para versionar o código
e para o CI/CD (Parte 5).

> ✅ **Checklist da Parte 1:** `git --version`, `docker --version`, `uv --version`,
> `astro version` — todos retornando uma versão. Docker Desktop **aberto e verde**.

---

## Parte 2 — Etapa 1: Setup local e geração de dados

**Objetivo desta etapa:** subir um PostgreSQL local e gerar dados sintéticos (fakes) de clientes
e pedidos, que serão a matéria-prima do nosso pipeline.

📁 **Tudo nesta parte acontece dentro da pasta `1_local_setup`.**

### 2.1 — Entre na pasta do projeto

No PowerShell:
```powershell
cd "C:\Users\seu_usuario\caminho\ate\wks_dbt_airflow\1_local_setup"
```
> Troque o caminho pelo local onde você clonou/baixou o projeto. Dica: arraste a pasta para o
> PowerShell que ele cola o caminho.

### 2.2 — Crie o arquivo `.env` (senhas do banco)

💡 **Por quê um `.env`?** Nunca se escreve usuário/senha direto no código (vaza no Git). Coloca-se
num arquivo `.env` que fica **fora do Git** (já está no `.gitignore`). O Docker lê esse arquivo.

Crie um arquivo chamado **`.env`** dentro de `1_local_setup` com este conteúdo:

```env
DBT_USER=dbt_user
DBT_PASSWORD=dbt_password
```

> Em produção você usaria uma senha forte. Aqui, por ser local e didático, tudo bem usar uma simples.

### 2.3 — Suba o PostgreSQL com Docker

O arquivo `docker-compose.yml` (já existe no projeto) descreve o banco. Veja como ele é:

```yaml
services:
  postgres:
    image: postgres:16                 # versão 16 do Postgres
    container_name: dbt_postgres       # nome do contêiner
    environment:
      POSTGRES_USER: ${DBT_USER}       # vem do .env
      POSTGRES_PASSWORD: ${DBT_PASSWORD}
      POSTGRES_DB: dbt_db              # nome do banco que será criado
    ports:
      - "5433:5432"                    # porta 5433 no SEU PC → 5432 dentro do contêiner
    volumes:
      - postgres_data:/var/lib/postgresql/data   # guarda os dados mesmo se reiniciar
    healthcheck:                       # checa se o banco está pronto antes de liberar
      test: ["CMD-SHELL", "pg_isready -U ${DBT_USER} -d dbt_db"]
      interval: 5s
      timeout: 5s
      retries: 5
      start_period: 10s

volumes:
  postgres_data:
```

💡 **Por quê a porta 5433 e não 5432?** A 5432 é a porta padrão do Postgres. Se você já tiver
um Postgres na máquina, daria conflito. Usar **5433 no host** evita isso. **Guarde esse número:
você vai precisar dele para conectar.**

Com o **Docker Desktop aberto**, rode (dentro de `1_local_setup`):
```powershell
docker compose up -d
```
- `up` = sobe os serviços; `-d` = "detached" (roda em segundo plano, libera o terminal).

Confirme que está de pé:
```powershell
docker ps
```
Você deve ver uma linha com `dbt_postgres` e status `healthy`. 🎉 Seu banco está rodando.

### 2.4 — Instale as dependências Python com `uv`

O `pyproject.toml` lista o que precisamos:

```toml
[project]
name = "1-local-setup"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = [
    "dbt-core>=1.11.11",
    "dbt-postgres>=1.10.0",
    "duckdb>=1.5.3",
    "faker>=40.21.0",
    "numpy>=2.4.6",
    "pandas>=3.0.3",
]
```

Instale tudo (dentro de `1_local_setup`):
```powershell
uv sync
```
💡 **O que aconteceu?** O `uv` criou uma pasta `.venv` (ambiente virtual isolado) e instalou
todos os pacotes ali. Isso evita "sujar" o Python global do seu sistema.

### 2.5 — Entenda e rode o gerador de dados

O script `generate_fake_data.py` faz, em resumo:

1. Usa o **Faker** (em `pt_BR`) para inventar nomes, CPFs, endereços brasileiros realistas.
2. Gera **10.000 clientes** e **50.000 pedidos** (cada pedido ligado a um CPF de cliente).
3. Usa o **DuckDB** como um "banco de rascunho" rápido em memória/arquivo só para montar e
   validar os dados.
4. **Exporta tudo para CSV** e apaga o rascunho do DuckDB.

Trecho-chave (a geração de um cliente):
```python
fake = Faker('pt_BR')
random.seed(42)        # 💡 semente fixa = dados REPRODUZÍVEIS (todo mundo gera os mesmos)
np.random.seed(42)

# ... para cada cliente:
cpf = fake.bothify(text='###.###.###-##')
chunk_data.append({
    'id': str(uuid.uuid4()),
    'nome': fake.name(),
    'data_nascimento': fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
    'cpf': cpf,
    'cidade': fake.city(),
    'estado': fake.state_abbr(),
    'email': f"{cpf.replace('.', '').replace('-', '')}@exemplo.com.br",
    # ...
})
```

💡 **Por quê `random.seed(42)`?** Fixar a "semente" faz o gerador produzir **sempre os mesmos
dados**. Isso é ouro para aprender: seus resultados vão bater com os do guia e com os de colegas.

Rode o script (dentro de `1_local_setup`):
```powershell
uv run python generate_fake_data.py
```
> `uv run` executa o script **dentro** do ambiente virtual, sem precisar "ativar" nada.

Você verá logs como `Gerados 10000/10000 registros...` e, no fim, estatísticas e o tempo total.

### 2.6 — Confira os CSVs gerados

Os arquivos saem na subpasta `seeds/`. Confira:
```powershell
ls .\seeds\
```
Você deve ver **`cadastros.csv`** e **`pedidos.csv`**. 💡 Esses dois arquivos são os **seeds**
do dbt — a matéria-prima crua que o dbt vai carregar e transformar na próxima etapa.

> ✅ **Checklist da Parte 2:** Docker rodando `dbt_postgres` (healthy); `cadastros.csv` e
> `pedidos.csv` existindo em `1_local_setup/seeds/`.

---

## Parte 3 — Etapa 2: O Data Warehouse com dbt

**Objetivo:** transformar os CSVs crus em um modelo dimensional limpo e em tabelas de consumo,
usando dbt.

📁 **Tudo nesta parte acontece dentro de `2_data_warehouse/wks_dbt`** (o projeto dbt).

### 3.1 — Entenda a anatomia de um projeto dbt

Dentro de `2_data_warehouse/wks_dbt` você tem:

```
wks_dbt/
├── dbt_project.yml      # o "coração": nome, perfil, configs das camadas
├── packages.yml         # pacotes externos (tipo "bibliotecas" do dbt)
├── seeds/               # CSVs crus (cadastros.csv, pedidos.csv)
└── models/              # as transformações SQL (staging, intermediate, mart)
    ├── staging/
    ├── intermediate/
    └── mart/
```

### 3.2 — Copie os seeds gerados para o dbt

O dbt procura os CSVs na pasta `seeds/` **do projeto dbt**. Copie os arquivos gerados na Etapa 1:

```powershell
# A partir da raiz do repositório:
Copy-Item .\1_local_setup\seeds\cadastros.csv .\2_data_warehouse\wks_dbt\seeds\
Copy-Item .\1_local_setup\seeds\pedidos.csv   .\2_data_warehouse\wks_dbt\seeds\
```

💡 **Por quê copiar?** Os seeds são parte do projeto dbt e ficam versionados junto com ele. Num
projeto real, os dados viriam de uma fonte externa (uma extração), mas aqui usamos seeds para o
exemplo ser autossuficiente.

### 3.3 — Configure o `profiles.yml` (como o dbt conecta no banco)

💡 **Conceito central:** o `dbt_project.yml` diz **o que** transformar; o `profiles.yml` diz
**onde** (em qual banco) — usuário, senha, host, porta. O `profiles.yml` fica **fora do projeto**,
na sua pasta de usuário, em `~/.dbt/profiles.yml`, justamente para **não versionar senhas**.

Crie/edite o arquivo `C:\Users\seu_usuario\.dbt\profiles.yml` com este conteúdo:

```yaml
wks_dbt:                      # ← TEM que bater com 'profile:' do dbt_project.yml
  target: dev                 # ambiente padrão quando você roda dbt sem --target
  outputs:
    dev:                      # ambiente de desenvolvimento → Postgres local (Docker)
      type: postgres
      host: localhost
      port: 5433              # ← a porta que definimos no docker-compose!
      user: dbt_user          # ← do seu .env
      pass: dbt_password      # ← do seu .env
      dbname: dbt_db
      schema: public
      threads: 4              # quantos modelos rodam em paralelo
```

> Se a pasta `.dbt` não existir, crie: `New-Item -ItemType Directory -Force "$HOME\.dbt"`.

### 3.4 — Entenda o `dbt_project.yml` (e as 3 camadas)

Este arquivo já existe. As partes que importam:

```yaml
name: 'wks_dbt'
profile: 'wks_dbt'           # ← liga este projeto ao bloco 'wks_dbt' do profiles.yml

vars:
  "dbt_date:time_zone": "America/Sao_Paulo"   # fuso usado pela dimensão de tempo

models:
  wks_dbt:
    staging:
      +materialized: view     # 💡 staging = view (leve, sempre reflete a fonte)
    intermediate:
      +materialized: table    # 💡 dim/fact = table (materializa, fica rápido de consultar)
    mart:
      +materialized: table    # 💡 mart = table (pronto para o BI consumir)
```

💡 **`view` vs `table` — quando usar cada um?**
- **view** = uma "consulta salva". Não ocupa espaço, sempre atualizada, mas recalcula a cada
  leitura. Boa para **staging** (camada fina, muda pouco).
- **table** = materializa o resultado fisicamente. Ocupa espaço, precisa rebuildar, mas é
  **rápida de consultar**. Boa para **intermediate e mart** (lógica pesada, consultada muito).

### 3.5 — Declare e instale os pacotes (`packages.yml`)

💡 **Por quê pacotes?** Em vez de reinventar a roda, usamos funções prontas da comunidade dbt.
Aqui usamos três:
- **`dbt_utils`**: utilidades, ex. `generate_surrogate_key` (gera chaves substitutas).
- **`dbt_expectations`**: testes de qualidade avançados (ex.: "este valor está entre 0 e 1 milhão?").
- **`dbt_date`** (vem junto): gera uma **dimensão de tempo** completa automaticamente.

O arquivo `packages.yml`:
```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: "1.3.0"

  - package: metaplane/dbt_expectations
    version: "0.10.8"
```

Instale os pacotes (dentro de `2_data_warehouse/wks_dbt`):
```powershell
cd 2_data_warehouse\wks_dbt
dbt deps
```
💡 Isso baixa os pacotes para a pasta `dbt_packages/` (que **não** vai para o Git — está no
`.gitignore`).

### 3.6 — Camada STAGING: limpar e padronizar

A staging faz **só** uma coisa: pega o dado cru e o deixa apresentável (renomeia colunas, ajusta
tipos). **Sem regra de negócio aqui.**

Exemplo — `models/staging/stg_cadastros.sql`:
```sql
with source as (
   select * from {{ ref('cadastros') }}    -- {{ ref(...) }} aponta para o seed cadastros.csv
),

transformed as (
   select
      -- Chaves
      id as id_cliente,                     -- renomeia 'id' → 'id_cliente' (mais claro)
      cpf,

      -- Dados Pessoais
      nome,
      data_nascimento as dt_nascimento,     -- padroniza prefixo 'dt_' para datas
      genero,

      -- Contato
      email,
      telefone,

      -- Localização
      pais, cidade, estado, cep,

      -- Datas + metadado de auditoria
      data_cadastro as dt_cadastro,
      current_timestamp as etl_inserted_at  -- 💡 quando este registro entrou no modelo
   from source
)

select * from transformed
```

💡 **Entenda o `{{ ref('cadastros') }}`:** essa é a **função mais importante do dbt**. Em vez de
escrever o nome físico da tabela, você "referencia" outro modelo/seed pelo nome. O dbt:
1. descobre **a ordem certa** de execução (monta o grafo de dependências sozinho);
2. troca o `ref` pelo nome real da tabela no banco, no schema certo.
**Nunca** escreva nomes de tabela "na mão" — sempre use `ref()` (para modelos/seeds) ou
`source()` (para fontes externas).

Exemplo com **regra leve de padronização** — `models/staging/stg_pedidos.sql` (trecho):
```sql
select
    id_pedido,
    cpf,
    valor_pedido,
    valor_frete,
    valor_desconto,
    -- cálculo simples e determinístico (ok na staging):
    (valor_pedido + valor_frete - coalesce(valor_desconto, 0)) as valor_total_pedido,
    cupom,
    case when cupom is not null then true else false end as tem_cupom,
    status_pedido,
    data_pedido as dt_pedido,
    current_timestamp as etl_inserted_at
from source
```
💡 `coalesce(valor_desconto, 0)` troca `NULL` por `0` antes de somar — senão `valor + frete - NULL`
daria `NULL` (em SQL, qualquer conta com NULL vira NULL).

### 3.7 — Camada INTERMEDIATE: dimensões e fato (o star schema)

**Dimensão de clientes** — `models/intermediate/dim/int_dim_clientes.sql`:
```sql
{{
    config(
        materialized = 'table',
        unique_key = 'sk_cliente',
        tags = ['intermediate', 'dimension']
    )
}}

with clientes as (
    select * from {{ ref('stg_cadastros') }}   -- 💡 lê da STAGING, não do seed
)

select
    -- Chave SUBSTITUTA (surrogate key): um hash gerado a partir do CPF
    {{ dbt_utils.generate_surrogate_key(['cpf']) }} as sk_cliente,

    cpf,                          -- chave de NEGÓCIO (natural)
    nome, email, estado, cidade, dt_nascimento, dt_cadastro,

    current_timestamp as dbt_updated_at,
    '{{ run_started_at }}' as dbt_loaded_at
from clientes
```

💡 **O que é uma surrogate key (`sk_cliente`) e por que usar?** É uma chave "artificial" gerada
pelo dbt (um hash). 💡 **Por quê não usar só o CPF?** Porque chaves de negócio podem mudar de
formato, ter máscara/sem máscara, ou ser sensíveis. Uma surrogate key é estável, uniforme e
desacopla a fato da dimensão. `dbt_utils.generate_surrogate_key(['cpf'])` gera esse hash de forma
consistente.

**Dimensão de tempo** — `models/intermediate/dim/int_dim_date.sql` (uma linha só!):
```sql
{{ dbt_date.get_date_dimension("2015-01-01", modules.datetime.datetime.now().strftime('%Y-%m-%d')) }}
```
💡 **Mágica do `dbt_date`:** essa macro gera uma tabela de calendário completa (de 2015 até hoje)
com dezenas de colunas úteis — nome do dia da semana, número do mês, trimestre, ano, etc.
💡 **Por quê uma dimensão de tempo?** Para responder "vendas por trimestre", "por dia da semana"
sem ter que calcular isso em toda consulta. É um padrão clássico de Data Warehouse.

**Fato de pedidos** — `models/intermediate/fact/int_fact_pedidos.sql`:
```sql
{{ config(materialized='table', unique_key='sk_pedido', tags=['intermediate','fact']) }}

with pedidos as (
    select * from {{ ref('stg_pedidos') }}
),
dim_clientes as (
    select sk_cliente, cpf from {{ ref('int_dim_clientes') }}
),
dim_date as (
    select date_day from {{ ref('int_dim_date') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['p.id_pedido']) }} as sk_pedido,
    dc.sk_cliente as fk_cliente,         -- 💡 FK que liga a fato à dimensão de clientes
    p.id_pedido,
    p.dt_pedido,
    date_trunc('day', p.dt_pedido) as data_pedido,
    p.valor_total_pedido,                -- 💡 a MÉTRICA (o que se mede na fato)
    current_timestamp as dbt_updated_at,
    '{{ run_started_at }}' as dbt_loaded_at
from pedidos p
left join dim_clientes dc on p.cpf = dc.cpf                      -- liga pelo CPF
left join dim_date dd on date_trunc('day', p.dt_pedido) = dd.date_day
```

💡 **Anatomia de uma fato:** ela tem **chaves estrangeiras** (`fk_cliente`) que apontam para as
dimensões, e **métricas** (`valor_total_pedido`) que são os números que você vai somar/médiar.
Os `left join` ligam cada pedido ao seu cliente e à sua data.

### 3.8 — Camada MART: tabelas prontas para o negócio

Aqui mora a **regra de negócio**. Exemplo: **segmentação RFM** de clientes
(`models/mart/mart_metricas_clientes.sql`). RFM = **R**ecência (quando comprou por último),
**F**requência (com que frequência), **M**onetário (quanto gastou).

Trecho da classificação (o "produto final" para o time de marketing):
```sql
select
    *,
    case
        when valor_total_gasto is null or valor_total_gasto = 0 then 'Inativo'
        when valor_total_gasto > 5000 and dias_desde_ultimo_pedido <= 30
             and frequencia_media_mensal >= 2 then 'Campeão'
        when valor_total_gasto > 3000 and dias_desde_ultimo_pedido <= 60 then 'Cliente Fiel'
        when valor_total_gasto > 1000 and dias_desde_ultimo_pedido <= 90 then 'Potencial'
        when valor_total_gasto > 0 and dias_desde_ultimo_pedido > 180 then 'Em Risco de Churn'
        when valor_total_gasto > 0 then 'Em Observação'
        else 'Inativo'
    end as segmento_rfm
from pedidos_por_cliente
```

💡 **Por quê isso é um "mart" e não uma "intermediate"?** Porque carrega **regra de negócio**
(o que define um "Campeão") e é o que um analista vai abrir direto no Power BI. A intermediate
era genérica; o mart é específico para uma pergunta de negócio.

O outro mart, `mart_vendas_por_periodo.sql`, agrega vendas por dia e calcula coisas como
**média móvel de 7 dias** com funções de janela (window functions):
```sql
-- Média móvel de 7 dias: média da receita do dia atual + 6 dias anteriores
avg(receita_bruta) over (
    order by date_day
    rows between 6 preceding and current row
) as receita_media_7d
```
💡 **Window function** (`... over (...)`) calcula um valor "olhando para as linhas vizinhas" sem
colapsar o resultado num `group by`. É essencial para análises temporais (tendências, crescimento).

### 3.9 — Documentação e testes (`_*.yml`)

💡 **Por quê isso importa?** Documentação e testes são o que separam um "script que roda" de um
**pipeline confiável**. O dbt deixa você declarar ambos em arquivos YAML ao lado dos modelos.

Exemplo — `models/staging/_stg__models.yml` (trecho):
```yaml
version: 2

models:
  - name: stg_cadastros
    description: "Staging dos cadastros de clientes: dados limpos e padronizados."
    columns:
      - name: id_cliente
        description: "ID único do cliente"
        tests:
          - unique        # 💡 garante que não há IDs repetidos
          - not_null      # 💡 garante que o ID nunca é nulo
      - name: cpf
        description: "CPF do cliente"
        tests:
          - not_null
```

Teste de qualidade avançado (com `dbt_expectations`) — em `_int__models.yml`:
```yaml
      - name: valor_total_pedido
        description: "Valor total do pedido"
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 1000000      # 💡 nenhum pedido deve passar de R$ 1.000.000
              strictly: false
```
💡 **Por quê testar faixas de valor?** Para pegar erro de dado (ex.: um pedido de R$ 9 bilhões por
bug na origem). O teste **quebra o pipeline** se aparecer algo absurdo — melhor falhar cedo do que
mandar lixo para o dashboard.

### 3.10 — Rode o dbt (o momento da verdade)

Sempre dentro de `2_data_warehouse/wks_dbt`. Rode na ordem:

```powershell
dbt deps     # 1. baixa pacotes (já fizemos, mas é idempotente)
dbt seed     # 2. carrega os CSVs como tabelas no Postgres
dbt run      # 3. constrói os modelos: staging → intermediate → mart
dbt test     # 4. roda todos os testes de qualidade
```

Ou, atalho que faz tudo de uma vez (a forma recomendada):
```powershell
dbt build    # = seed + run + test, na ordem correta de dependências
```

💡 **`dbt run` vs `dbt build`:** `run` só constrói os modelos. `build` constrói **e** testa
**e** carrega seeds, respeitando o grafo de dependências (se um modelo falha no teste, ele não
deixa os modelos que dependem dele rodarem). Use `build` por padrão.

Se tudo der certo, você verá algo como `Completed successfully` e `PASS=NN`. 🎉

### 3.11 — Veja a documentação visual gerada pelo dbt

```powershell
dbt docs generate    # gera o site de documentação
dbt docs serve       # abre no navegador (http://localhost:8080)
```
💡 Isso abre um site interativo com o **lineage graph** (o grafo que mostra seed → staging →
intermediate → mart). Ótimo para entender o fluxo e para mostrar em entrevista.

### 3.12 — Versione o projeto dbt no Git (repositório próprio)

💡 **Por quê agora?** Porque o Airflow (Parte 4) vai puxar este projeto dbt do GitHub como um
**submódulo**. Então ele precisa estar num repositório próprio.

Crie um repositório no GitHub chamado, por exemplo, **`workshop-dbt-dw`**:
1. Acesse https://github.com/new
2. Em **Repository name**, digite `workshop-dbt-dw`
3. Deixe **Public**, **não** marque "Add a README"
4. Clique **Create repository**

Antes de subir, garanta que o `.gitignore` do projeto dbt ignora o que não deve ir:
```gitignore
target/
dbt_packages/
logs/
package-lock.yml
```
💡 **Por quê ignorar `package-lock.yml`?** Ele é gerado pelo `dbt deps` e pode ter um formato que
varia entre versões de dbt. Se versionado, causa o erro `packages.yml is malformed` no CI/nuvem.
Deixe-o fora do Git.

Suba (a partir de `2_data_warehouse`, que é a raiz desse repositório):
```powershell
cd 2_data_warehouse
git init
git add .
git commit -m "Projeto dbt: staging, intermediate (star schema) e marts"
git branch -M main
git remote add origin https://github.com/SEU_USUARIO/workshop-dbt-dw.git
git push -u origin main
```

> ✅ **Checklist da Parte 3:** `dbt build` verde; `dbt docs serve` mostrando o lineage; projeto
> dbt no GitHub em `workshop-dbt-dw`.

---

## Parte 4 — Etapa 3: Orquestração com Airflow + Cosmos

**Objetivo:** fazer o Airflow rodar o projeto dbt automaticamente (agendado), localmente, via
Astro CLI e Cosmos.

📁 **Tudo nesta parte acontece dentro da pasta `3_airflow`.**

### 4.1 — Inicialize o projeto Astro

💡 **Por quê `astro dev init`?** Ele cria a estrutura padrão de um projeto Airflow gerenciado pela
Astronomer (Dockerfile, pastas `dags/`, `requirements.txt`, etc.).

```powershell
cd 3_airflow
astro dev init
```
> Se a pasta já tiver os arquivos do projeto (como neste repositório), pode pular — eles já estão
> prontos. O passo é mostrado para você entender de onde vêm.

### 4.2 — Configure o `Dockerfile` (instalar o dbt no Airflow)

O Airflow precisa do executável do `dbt` para o Cosmos chamá-lo. Mas instalar dbt no mesmo
ambiente do Airflow causa conflito de dependências. 💡 **Solução:** instalar o dbt num
**ambiente virtual isolado** dentro da imagem.

`3_airflow/Dockerfile`:
```dockerfile
FROM astrocrpublic.azurecr.io/runtime:3.2-5

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-postgres==1.9.0 && deactivate
```
💡 Isso cria um venv chamado `dbt_venv` e instala o `dbt-postgres` ali. A DAG vai apontar para
`.../dbt_venv/bin/dbt` (você verá no `dag.py`).

### 4.3 — Configure o `requirements.txt`

`3_airflow/requirements.txt`:
```text
astronomer-cosmos==1.14.2
apache-airflow-providers-postgres
```
💡 `astronomer-cosmos` é a biblioteca que converte o dbt em DAG. O `providers-postgres` ensina o
Airflow a conectar no PostgreSQL.

### 4.4 — Adicione o projeto dbt como SUBMÓDULO do Git

💡 **O que é um submódulo?** É um repositório Git **dentro** de outro. Aqui, o projeto dbt
(`workshop-dbt-dw`) vira um submódulo dentro do projeto Airflow. Assim, o Airflow sempre usa uma
versão específica e rastreável do dbt, sem duplicar código.

```powershell
cd 3_airflow
git submodule add https://github.com/SEU_USUARIO/workshop-dbt-dw.git dbt/workshop-dbt-dw
```

Isso cria o arquivo `.gitmodules`:
```ini
[submodule "dbt/workshop-dbt-dw"]
	path = dbt/workshop-dbt-dw
	url = https://github.com/RoseIFOS/workshop-dbt-dw.git
```

⚠️ **MUITO IMPORTANTE — use URL em HTTPS, não SSH.** 💡 **Por quê?** Quando a nuvem (Astro) for
construir a imagem, ela **não tem sua chave SSH**. Se a URL for SSH (`git@github.com:...`), o
build falha com `Host key verification failed`. Com HTTPS funciona em qualquer ambiente.

### 4.5 — Monte o dbt dentro do contêiner (dev local)

💡 **O problema:** a DAG espera o projeto dbt em `/usr/local/airflow/dbt/wks_dbt` **dentro do
contêiner**. Mas no seu PC ele está em `dbt/workshop-dbt-dw/wks_dbt`. **A solução:** um
`docker-compose.override.yml` que "monta" (espelha) a pasta local no caminho esperado.

`3_airflow/docker-compose.override.yml`:
```yaml
services:
  scheduler:
    volumes:
      - ./dbt/workshop-dbt-dw/wks_dbt:/usr/local/airflow/dbt/wks_dbt
  dag-processor:
    volumes:
      - ./dbt/workshop-dbt-dw/wks_dbt:/usr/local/airflow/dbt/wks_dbt
```
💡 **A regra de ouro dos caminhos:** três lugares devem apontar para **o mesmo** caminho
`/usr/local/airflow/dbt/wks_dbt` — (1) este mount, (2) o `dbt_project_path` na DAG, e (3) o deploy
na nuvem (Parte 6). Se um divergir, você verá `Could not find dbt_project.yml`.

### 4.6 — Escreva a DAG (`dags/dag.py`)

Esta é a peça central. Ela usa o **Cosmos** (`DbtDag`) e tem um **switch de ambiente** (dev/prod).

`3_airflow/dags/dag.py`:
```python
from airflow.models import Variable
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
import os
from pendulum import datetime

# Perfil DEV → Postgres local (Docker)
profile_config_dev = ProfileConfig(
    profile_name="wks_dbt",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="docker_postgres_db",      # conexão do Airflow (criada na UI, passo 4.8)
        profile_args={"schema": "public"},
    ),
)

# Perfil PROD → Postgres na nuvem (Railway)
profile_config_prod = ProfileConfig(
    profile_name="wks_dbt",
    target_name="prod",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="railway_postgres_db",
        profile_args={"schema": "public"},
    ),
)

# Lê a Variable "dbt_env"; se não existir, assume "dev"
dbt_env = Variable.get("dbt_env", default_var="dev").lower()
if dbt_env not in ("dev", "prod"):
    raise ValueError(f"dbt_env inválido: {dbt_env!r}, use 'dev' ou 'prod'")

# Escolhe o perfil conforme o ambiente
profile_config = profile_config_dev if dbt_env == "dev" else profile_config_prod

my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path="/usr/local/airflow/dbt/wks_dbt",   # 💡 o caminho da regra de ouro
        project_name="wks_dbt",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",  # 💡 o venv do Dockerfile
    ),
    operator_args={
        "install_deps": True,                 # roda `dbt deps` antes (baixa pacotes)
        "target": profile_config.target_name,
    },
    schedule="@daily",                        # roda 1x por dia
    start_date=datetime(2026, 6, 3),
    catchup=False,                            # 💡 NÃO reexecuta dias passados
    dag_id=f"dag_wks_dbt_{dbt_env}",          # nome muda conforme o ambiente
    default_args={"retries": 2},              # tenta de novo 2x se falhar
)
```

💡 **Por quê o switch dev/prod numa Variable?** Para usar **a mesma DAG** apontando para bancos
diferentes sem mudar código. Você troca a Variable `dbt_env` de `dev` para `prod` e a DAG passa a
escrever no banco de produção. Isso é uma prática profissional de promoção entre ambientes.

💡 **`catchup=False` — por quê importa?** Se você define `start_date` no passado e deixa
`catchup=True`, o Airflow tentaria rodar **todos os dias desde aquela data** de uma vez (pode ser
centenas de execuções!). `False` evita esse susto.

### 4.7 — Suba o Airflow local

Com o **Docker Desktop aberto** e o **Postgres da Etapa 1 rodando**:
```powershell
cd 3_airflow
astro dev start
```
💡 Isso sobe vários contêineres (webserver, scheduler, etc.). Quando terminar, ele abre (ou te dá
o link) **http://localhost:8080**. Login padrão: usuário `admin`, senha `admin`.

Para reiniciar após mudanças no código:
```powershell
astro dev restart
```

### 4.8 — Crie a Variable e as Connections na UI do Airflow

💡 **O que é cada uma?**
- **Variable** = uma configuração simples (chave/valor). Usamos `dbt_env`.
- **Connection** = credenciais para o Airflow acessar um sistema externo (aqui, os bancos).

**Criar a Variable `dbt_env`:**
1. Na UI (http://localhost:8080), menu **Admin → Variables**.
2. Clique no **+** (Add a new record).
3. **Key:** `dbt_env` · **Val:** `dev`
4. Clique **Save**.

**Criar a Connection `docker_postgres_db`** (banco local):
1. Menu **Admin → Connections** → clique no **+**.
2. Preencha:
   - **Connection Id:** `docker_postgres_db`
   - **Connection Type:** `Postgres`
   - **Host:** `host.docker.internal` 💡 (de dentro do contêiner, é assim que se acessa o
     "localhost" do seu PC, onde está o Postgres da Etapa 1)
   - **Schema:** `dbt_db` (este campo, no provider do Postgres, é o **nome do banco**)
   - **Login:** `dbt_user` · **Password:** `dbt_password`
   - **Port:** `5433`
3. Clique **Save**.

> A connection `railway_postgres_db` só será necessária quando você criar o banco de produção
> (Parte 6). Por ora, deixe a Variable em `dev`.

### 4.9 — Rode a DAG na interface

1. Na home do Airflow, ache a DAG **`dag_wks_dbt_dev`**.
2. Clique no **toggle** à esquerda do nome para **ativá-la** (sai de pausada).
3. Clique no botão **▶ (Trigger DAG)** à direita para rodar agora.
4. Clique no nome da DAG → aba **Graph** para ver cada modelo dbt virar uma tarefa rodando
   (verde = sucesso).

🎉 Se tudo ficou verde, o Airflow acabou de executar seu pipeline dbt de ponta a ponta!

> ✅ **Checklist da Parte 4:** `astro dev start` rodando; Variable `dbt_env=dev`; Connection
> `docker_postgres_db` criada; DAG `dag_wks_dbt_dev` verde no Graph.

---

## Parte 5 — CI/CD com GitHub Actions

**Objetivo:** toda vez que você der `git push` no projeto dbt, o GitHub valida automaticamente que
o dbt **builda e passa nos testes** — antes de qualquer deploy.

💡 **O que é CI?** Continuous Integration = a cada mudança, uma máquina neutra roda seus testes
para garantir que nada quebrou. Pega erro **antes** dele chegar na produção.

📁 **Acontece no repositório do dbt (`2_data_warehouse`), na pasta `.github/workflows/`.**

### 5.1 — Crie o banco de produção (Railway)

💡 **Por quê?** O CI precisa de um banco real para rodar o `dbt build` contra ele. Usaremos a
**Railway** (tem plano grátis e sobe um Postgres em segundos).

1. Acesse https://railway.app e faça login com o GitHub.
2. Clique **New Project → Provision PostgreSQL**.
3. Quando criar, clique no serviço **Postgres → aba Variables/Connect** e anote:
   `host (PGHOST)`, `port (PGPORT)`, `user (PGUSER)`, `password (PGPASSWORD)`, `database (PGDATABASE)`.

### 5.2 — Cadastre os Secrets no GitHub

💡 **Por quê Secrets?** O CI não pode ter a senha do banco escrita no código. Os "Secrets" são
variáveis criptografadas que o GitHub injeta na hora de rodar.

1. No GitHub, abra o repositório **`workshop-dbt-dw`**.
2. Clique em **Settings** (aba no topo do repositório).
3. No menu esquerdo: **Secrets and variables → Actions**.
4. Clique **New repository secret** e crie um por um:
   - `RAILWAY_DB_HOST` → o host da Railway
   - `RAILWAY_DB_PORT` → a porta
   - `RAILWAY_DB_USER` → o usuário
   - `RAILWAY_DB_PASS` → a senha
   - `RAILWAY_DB_NAME` → o nome do banco

### 5.3 — Crie o workflow de CI

Arquivo `2_data_warehouse/.github/workflows/dbt-ci.yml`:
```yaml
name: CI

on:
  push:
    branches:
      - main            # 💡 dispara a cada push na main

jobs:
  dbt-build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.13'

      - name: Install dbt
        run: pip install dbt-core==1.9.4 dbt-postgres==1.9.0

      - name: Configure dbt profile
        run: |
          mkdir -p ~/.dbt
          cat > ~/.dbt/profiles.yml <<EOF
          wks_dbt:
            target: prod
            outputs:
              prod:
                type: postgres
                threads: 4
                host: ${{ secrets.RAILWAY_DB_HOST }}
                user: ${{ secrets.RAILWAY_DB_USER }}
                pass: ${{ secrets.RAILWAY_DB_PASS }}
                port: ${{ secrets.RAILWAY_DB_PORT }}
                dbname: ${{ secrets.RAILWAY_DB_NAME }}
                schema: public
          EOF

      - name: dbt deps
        working-directory: wks_dbt
        run: dbt deps --profiles-dir ~/.dbt

      - name: dbt build
        working-directory: wks_dbt
        run: dbt build --profiles-dir ~/.dbt
```

💡 **Lendo o arquivo:** o CI faz checkout do código → instala Python e dbt → **gera um
`profiles.yml` na hora** com os secrets → roda `dbt deps` e `dbt build`. Se o build falhar (um
modelo quebrado ou um teste reprovado), o CI fica **vermelho** e o push é sinalizado como problema.

### 5.4 — Veja o CI rodando

Depois de dar `git push` (de uma mudança qualquer no `2_data_warehouse`):
1. No GitHub, abra o repositório → aba **Actions**.
2. Clique na execução mais recente.
3. Veja os passos um a um. ✅ Verde = passou. ❌ Vermelho = clique para ler o log e corrigir.

> ✅ **Checklist da Parte 5:** Secrets cadastrados; `dbt-ci.yml` na pasta certa; aba Actions
> mostrando o CI **verde**.

---

## Parte 6 — Deploy na nuvem (Astronomer)

**Objetivo:** colocar o Airflow rodando na nuvem (Astro), executando o dbt contra o banco de
produção (Railway), sem depender do seu PC.

💡 **O ponto-chave deste projeto: são DOIS deploys independentes.** Um envia o **código dbt**,
outro envia as **DAGs/imagem do Airflow**. Entender isso é o que separa "fiz funcionar local" de
"sei colocar em produção".

```
Passo 1: astro dbt deploy   (da pasta 2_data_warehouse/wks_dbt)  → envia o projeto dbt
Passo 2: astro deploy       (da pasta 3_airflow)                 → envia DAGs + imagem Docker
```

💡 **Por quê separar?** Mudou só a lógica de dados (SQL)? Faz só o `astro dbt deploy` — rápido,
sem rebuildar a imagem do Airflow. Mudou a orquestração? Faz o `astro deploy`. Deploys menores e
mais seguros.

### 6.1 — Crie um Deployment no Astro

1. Acesse https://cloud.astronomer.io e faça login (tem trial grátis).
2. Crie uma **Organization/Workspace** se for a primeira vez (siga o assistente).
3. Clique **Deployment → Create Deployment**, dê um nome (ex.: `wks-prod`), confirme.
4. Anote o **Deployment ID** (aparece nos detalhes do deployment ou via `astro deployment list`).

### 6.2 — Autentique a CLI

```powershell
astro login
```
💡 Abre o navegador para você confirmar o login. Sem isso, qualquer `astro deploy` falha com
`no context set, have you authenticated to Astro?`.

### 6.3 — Passo 1: deploy do dbt

💡 **Onde rodar importa muito.** O `astro dbt deploy` precisa enxergar o `dbt_project.yml`, que
está **dentro de `wks_dbt`**. Rode a partir de lá:
```powershell
cd 2_data_warehouse\wks_dbt
astro dbt deploy <DEPLOYMENT_ID>
```
💡 Por padrão, o `astro dbt deploy` coloca o projeto em
`/usr/local/airflow/dbt/<nome-do-projeto-dbt>` → ou seja, `/usr/local/airflow/dbt/wks_dbt`.
**É exatamente o caminho da regra de ouro** que está no `dbt_project_path` da DAG. Por isso tudo
se encaixa.

> Erro comum: rodar de `2_data_warehouse` (a pasta acima) → `dbt project file not found at
> .../2_data_warehouse/dbt_project.yml`. Solução: `cd` para dentro de `wks_dbt` primeiro.

### 6.4 — Passo 2: deploy do Airflow

```powershell
cd ..\..\3_airflow
astro deploy
```
💡 Isso builda a imagem Docker (com o `dbt_venv`), envia as DAGs e sobe tudo no Deployment.

### 6.5 — Configure Variable e Connections NO DEPLOYMENT da nuvem

⚠️ As Variables/Connections que você criou na UI local **não vão** para a nuvem. Recadastre no
Astro:
1. No Astro Cloud, abra seu Deployment → menu **Environment** (ou **Variables/Connections**).
2. Crie a Variable `dbt_env` = `prod` (agora queremos produção).
3. Crie a Connection `railway_postgres_db` (tipo Postgres) com **host/porta/usuário/senha/db da
   Railway** (os mesmos dados dos Secrets da Parte 5).

💡 Como `dbt_env=prod`, a DAG vai usar `profile_config_prod` → conexão `railway_postgres_db` →
escreve no banco de produção. É o switch dev/prod funcionando na nuvem.

> ⚠️ Se a senha estiver errada: `FATAL: password authentication failed for user "railway"`.
> Corrija a Connection no Deployment.

### 6.6 — (Opcional) Deploy do dbt automático via GitHub Actions

Em vez de rodar `astro dbt deploy` na mão, você pode automatizar com este workflow no repositório
do dbt (`2_data_warehouse/.github/workflows/astronomer.yml`):
```yaml
name: Astronomer CI - Deploy dbt code

on:
  push:
    branches:
      - main

env:
  ASTRO_API_TOKEN: ${{ secrets.ASTRO_API_TOKEN }}

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
    - name: Deploy to Astro
      uses: astronomer/deploy-action@v0.10.1
      with:
        deployment-id: ${{ secrets.ASTRO_DEPLOYMENT_ID }}
        deploy-type: dbt
        root-folder: wks_dbt      # pasta do projeto dbt (onde está o dbt_project.yml)
```
💡 Precisa cadastrar dois novos Secrets: `ASTRO_API_TOKEN` (gerado em Astro → Workspace Settings →
API Tokens) e `ASTRO_DEPLOYMENT_ID`. Aí, todo push na main faz o deploy do dbt sozinho.

### 6.7 — Rode a DAG na nuvem e valide ponta a ponta

1. No Astro Cloud, abra o Deployment → **Open Airflow** (abre a UI do Airflow na nuvem).
2. Ative e dispare a DAG `dag_wks_dbt_prod`.
3. Confira o **Graph** tudo verde.
4. Conecte no banco da Railway (com DBeaver, ou o painel da Railway) e confira que as tabelas
   `mart_metricas_clientes` e `mart_vendas_por_periodo` foram criadas e populadas. 🎉

> ✅ **Checklist da Parte 6:** `astro login` ok; `astro dbt deploy` e `astro deploy` concluídos;
> Variable `dbt_env=prod` e Connection `railway_postgres_db` no Deployment; DAG `dag_wks_dbt_prod`
> verde; tabelas mart populadas na Railway.

---

## Parte 7 — Troubleshooting (erros comuns e como sair deles)

| Erro que aparece | Por que acontece | Como resolver |
|------------------|------------------|---------------|
| `packages.yml is malformed` / `not valid under any of the given schemas` | `package-lock.yml` foi versionado e foi gerado por outra versão de dbt | Remova-o do Git e adicione `package-lock.yml` ao `.gitignore` |
| `Host key verification failed` (no submódulo) | `.gitmodules` está com URL **SSH** e a nuvem não tem sua chave | Troque a URL para **HTTPS** no `.gitmodules` |
| `Could not find dbt_project.yml at /usr/local/airflow/dbt/wks_dbt` | O `dbt_project_path` da DAG não bate com o caminho montado/deployado | Alinhe **tudo** em `/usr/local/airflow/dbt/wks_dbt` (mount, DAG e deploy) |
| `dbt project file not found at .../2_data_warehouse/dbt_project.yml` | `astro dbt deploy` rodado na pasta errada | Rode de **dentro** de `2_data_warehouse/wks_dbt` |
| `no context set, have you authenticated to Astro?` | CLI não logada | Rode `astro login` |
| `password authentication failed for user "railway"` | Senha errada na Connection de produção | Corrija a Connection `railway_postgres_db` no Deployment |
| `this is not an Astro project directory` | Comando `astro` rodado fora de `3_airflow` | `cd 3_airflow` antes |
| `Did not find matching node for patch` | O `_stg__models.yml` referencia `stg_cadastros_2`, mas o modelo se chama `stg_cadastros2` | Ajuste o nome no `.yml` para casar com o arquivo `.sql` |
| Docker: `Cannot connect to the Docker daemon` | Docker Desktop fechado | Abra o Docker Desktop e espere ficar verde |
| `connection refused` ao conectar no Postgres local pela DAG | Host errado na Connection | Use `host.docker.internal` (não `localhost`) na Connection do Airflow |

💡 **Dica de ouro do mentor:** ao ver um erro, **leia a última linha** primeiro (a mensagem real
costuma estar no fim do log) e procure por **caminho de arquivo** e **nome de conexão** na
mensagem — 80% dos problemas deste projeto são (a) caminho do dbt divergente ou (b) credencial de
banco errada.

---

## Glossário

| Termo | Significado curto |
|-------|-------------------|
| **ELT** | Extract-Load-Transform: carrega cru e transforma dentro do banco (com SQL/dbt) |
| **dbt** | Ferramenta que organiza transformações SQL em modelos versionados, testados e documentados |
| **Modelo (dbt)** | Um arquivo `.sql` que vira uma tabela ou view no banco |
| **Seed** | Um CSV carregado pelo dbt como tabela (`dbt seed`) |
| **Materialização** | Como o modelo vira objeto no banco: `view` (consulta salva) ou `table` (materializa) |
| **`ref()`** | Função dbt que referencia outro modelo/seed e monta o grafo de dependências |
| **Staging** | Camada que limpa/renomeia dados crus; 1 por fonte; sem regra de negócio |
| **Intermediate** | Camada com dimensões e fatos (o star schema) |
| **Mart** | Camada final, com regra de negócio, pronta para BI |
| **Star schema** | Modelo com tabelas-fato (métricas) cercadas por tabelas-dimensão (contexto) |
| **Surrogate key** | Chave artificial (hash) gerada para identificar uma linha de forma estável |
| **RFM** | Recência, Frequência, Monetário — método de segmentar clientes |
| **Window function** | Função SQL que calcula olhando linhas vizinhas (`... over (...)`) sem agrupar |
| **Airflow** | Orquestrador: agenda e executa tarefas na ordem e horário certos |
| **DAG** | Directed Acyclic Graph — o "fluxo de tarefas" do Airflow |
| **Cosmos** | Biblioteca que converte um projeto dbt numa DAG do Airflow automaticamente |
| **Connection (Airflow)** | Credenciais para o Airflow acessar um sistema externo (ex.: um banco) |
| **Variable (Airflow)** | Configuração simples chave/valor lida pelas DAGs (ex.: `dbt_env`) |
| **Submódulo (Git)** | Um repositório Git aninhado dentro de outro |
| **CI** | Continuous Integration: valida o código automaticamente a cada push |
| **Astro CLI** | Ferramenta da Astronomer para rodar Airflow local e deployar na nuvem |

---

> 📚 **Próximos passos sugeridos para fixar o aprendizado:**
> 1. Adicione uma nova métrica ao `mart_metricas_clientes` (ex.: "valor médio por estação").
> 2. Crie um teste `accepted_values` no `segmento_rfm` (lista os segmentos válidos).
> 3. Mude `dbt_env` para `prod` localmente e veja a DAG apontar para outro banco.
> 4. Quebre algo de propósito (ex.: um teste `not_null`) e veja o CI ficar vermelho — entender
>    a falha ensina tanto quanto o sucesso.
>
> Para consulta rápida de referência (sem o passo a passo didático), use a [DOCUMENTACAO.md](DOCUMENTACAO.md).
</content>
</invoke>
