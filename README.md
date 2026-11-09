# Análise de Emendas Parlamentares

Pipeline analítico em notebooks Databricks/Spark para ler o CSV de emendas, preservar os snapshots de origem, validar e tipar os dados e publicar um modelo dimensional Delta, dados podem ser carregados e consumidos por BI.

<img width="1342" height="749" alt="Power BI Desktop 08_11_2026 15_56_05" src="https://github.com/user-attachments/assets/c94e8e17-13c1-44e1-9f95-3f189f80c3de" />

Você pode encontrar os dados de emendas parlamentares no seguinte Link: https://portaldatransparencia.gov.br/download-de-dados/emendas-parlamentares (acesso em 08/11/2026)
## Arquitetura

```text
CSV
  └── src/extract/extract_emendas.ipynb
        └── emendas_parlamentares_bronze (histórico append-only)
              └── src/transform/transform_data.ipynb
                    ├── emendas_parlamentares_prata (snapshot mais recente)
                    ├── emendas_parlamentares_rejeicoes
                    └── emendas_parlamentares_quality_metrics
                          └── src/gold/gold_modeling.ipynb
                                ├── emendas_parlamentares_dim_tempo
                                ├── emendas_parlamentares_dim_autor
                                ├── emendas_parlamentares_dim_localidade
                                ├── emendas_parlamentares_dim_programa_orcamentario
                                └── emendas_parlamentares_fato
                                      └── diagnostics/analysis.ipynb (execução manual)
```

Configure o catálogo e o schema ativos no compute antes de executar os notebooks. Os nomes de tabela são não qualificados e serão criados nesse schema.

## Pré-requisitos e execução

- Compute Databricks com Spark e Delta Lake habilitados e permissões de leitura do arquivo e escrita no catálogo/schema.
- O arquivo deve estar em um local acessível pelo Spark (por exemplo, um Volume Unity Catalog ou Workspace Files).
- Execute na ordem: extração, transformação, Gold e diagnósticos.
- Na extração, informe os parâmetros `source_path` (caminho completo do CSV) e `batch_id` (identificador único da versão do arquivo). Use um novo `batch_id` para cada snapshot. A reexecução do mesmo lote é ignorada, evitando duplicação; mantenha execuções serializadas para evitar concorrência no mesmo lote.
- A leitura valida o cabeçalho contra o esquema esperado e falha para linhas CSV estruturalmente malformadas. Os campos de origem são lidos como texto; as conversões acontecem na Prata.

## Pipeline e orquestração Databricks

O arquivo `databricks.yml` define o bundle e o job `etl_emendas_job` (“[Pipeline] Emendas Parlamentares”). O job orquestra os notebooks em sequência, com dependências explícitas:

1. `extract_bronze` executa `src/extract/extract_emendas.ipynb`.
2. `transform_prata` executa `src/transform/transform_data.ipynb` após a extração.
3. `golding_ouro` executa `src/gold/gold_modeling.ipynb` após a transformação.
   
<img width="1165" height="508" alt="Pipeline  Emendas Parlamentares - Databricks - Google Chrome 09_11_2026 20_37_13" src="https://github.com/user-attachments/assets/32ae2abb-a373-4fc0-a6c0-7d31c785236f" />

Se uma etapa falhar, as etapas dependentes não são executadas. O notebook `diagnostics/analysis.ipynb` não faz parte do job e deve ser executado separadamente para análise e validações adicionais.

Para validar e publicar o job, use a Databricks CLI autenticada e execute os comandos a partir da raiz deste projeto:

```bash
databricks bundle validate
databricks bundle deploy
```

Depois da publicação, execute o job com:

```bash
databricks bundle run etl_emendas_job
```

A configuração atual define a sequência das tarefas, mas não define agendamento nem fornece os parâmetros `source_path` e `batch_id` à tarefa de extração. Configure esses parâmetros no job antes de executá-lo, além do acesso do compute ao catálogo/schema e ao arquivo CSV. Para processar outro snapshot, use um novo `batch_id`; mantenha a concorrência do job em um para evitar lotes simultâneos.

O notebook de extração cria a tabela Bronze na primeira execução. Se encontrar o esquema Bronze antigo do projeto, faz uma migração de esquema única, preserva as linhas existentes como lote `legacy` e inclui o novo lote; a migração para com erro se houver colunas desconhecidas, em vez de descartá-las. As execuções seguintes acrescentam lotes e metadados (`source_file`, `source_batch_id`, `Data_Carga` e `source_row_id`). A chave de linha é derivada de um hash do conteúdo, arquivo e ocorrência; linhas idênticas continuam sendo ocorrências distintas.
<img width="1159" height="806" alt="Pipeline  Emendas Parlamentares - Databricks - Google Chrome 09_11_2026 20_37_02" src="https://github.com/user-attachments/assets/ac2fd80a-cab9-4789-baf2-f29ba8cb198b" />

## Grão e atualização das tabelas

- **Bronze:** uma linha por ocorrência em cada lote carregado; mantém o histórico de snapshots.
- **Prata:** uma linha por ocorrência no lote mais recente; a tabela é reconstruída em cada execução. `source_row_id` identifica tecnicamente cada ocorrência; `codigo_emenda` pode repetir ou ser nulo.
- **Gold fato:** uma linha por `source_row_id` da Prata atual. As medidas são valores empenhados, liquidados, pagos e de restos a pagar.
- **Gold dimensões:** tempo, autor, localidade e classificação/programa orçamentário. As SKs são SHA-256 determinísticos dos atributos naturais, e cada dimensão contém o membro `UNKNOWN`.
- **Qualidade:** rejeições e métricas representam a transformação mais recente. Conversões inválidas são persistidas e interrompem o pipeline antes da promoção para Gold. Códigos IBGE negativos, já tratados como sentinelas na transformação original, são normalizados para nulo e contabilizados separadamente.

Prata, rejeições, métricas e Gold são snapshots reconstruídos; Gold usa `CREATE OR REPLACE TABLE`. Bronze é append-only para os lotes, mas não é um mecanismo de transação entre múltiplas execuções. Não execute lotes concorrentes; para produção, orquestre com concorrência máxima de um e use permissões/controles de publicação.

As SKs determinísticas dependem dos atributos naturais e permanecem estáveis quando novos membros são acrescentados. Mudanças nos atributos naturais formam outro membro; o modelo não implementa histórico SCD Tipo 2.

<img width="504" height="414" alt="emendas_parlamentares_fato - Databricks - Google Chrome 08_11_2026 16_01_44" src="https://github.com/user-attachments/assets/a7223f05-2278-458e-afbc-ac3cb831e4b8" />

## Validações

`transform/transform_data.ipynb` verifica conversões e reconcilia a contagem entre lote e Prata. `gold/gold_modeling.ipynb` valida o grão da fato e a unicidade da chave de origem. `diagnostics/analysis.ipynb` verifica lote atual, contagens, chaves, integridade referencial, membros desconhecidos e conversões, além de disponibilizar análises de negócio. As validações críticas falham com mensagem explícita.

## Dados e publicação

O CSV em `data/` é um arquivo de exemplo do projeto. Antes de redistribuí-lo, confirme a origem, licença e termos de uso. Para uso recorrente ou produção, prefira armazenar os dados num Volume ou storage controlado, fora do repositório, e configure `source_path` por job.

### Como Conectar ao Dashboard
1. Abra o **Power BI Desktop**.
2. Vá em **Obter Dados (Get Data)** > **Databricks**.
3. Insira o **Server Hostname** e **HTTP Path** do seu SQL Warehouse no Databricks.
4. Escolha o modo de conectividade (**DirectQuery** para dados em tempo real ou **Import** para melhor performance visual).
5. Autentique-se utilizando seu token/credenciais configuradas.
6. Carregue todas as tabelas fato e dimensão **e faça a seguinte modelagem de dados**:

<img width="936" height="739" alt="Power BI Desktop 08_11_2026 16_05_37" src="https://github.com/user-attachments/assets/2f41d190-af31-4e45-8661-8f7d6c47c8ff" />

7. Após isso, poderá criar dashboards a partir dos dados disponíveis.

## Próximos passos

Melhorias planejadas para aproximar o projeto de um cenário de produção:

- [ ] **Orquestração:** encadear extração, transformação, Gold e diagnósticos em um Databricks Workflow (ou Asset Bundle), com `source_path` e `batch_id` como parâmetros e concorrência máxima de uma execução.
- [ ] **Testes automatizados:** extrair a lógica de conversão e validação para módulos Python com testes `pytest` e executá-los em um pipeline de CI (GitHub Actions).
- [ ] **Otimização das tabelas:** avaliar particionamento, `OPTIMIZE` e Z-Order conforme o volume de dados cresce.


