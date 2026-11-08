# Análise de Emendas Parlamentares

Pipeline analítico em notebooks Databricks/Spark para ler o CSV de emendas, preservar os snapshots de origem, validar e tipar os dados e publicar um modelo dimensional Delta.

## Arquitetura

```text
CSV
  └── extract/extract_emendas.ipynb
        └── emendas_parlamentares_bronze (histórico append-only)
              └── transform/transform_data.ipynb
                    ├── emendas_parlamentares_prata (snapshot mais recente)
                    ├── emendas_parlamentares_rejeicoes
                    └── emendas_parlamentares_quality_metrics
                          └── gold/gold_modeling.ipynb
                                ├── emendas_parlamentares_dim_tempo
                                ├── emendas_parlamentares_dim_autor
                                ├── emendas_parlamentares_dim_localidade
                                ├── emendas_parlamentares_dim_programa_orcamentario
                                └── emendas_parlamentares_fato
                                      └── diagnostics/analysis.ipynb
```

Configure o catálogo e o schema ativos no compute antes de executar os notebooks. Os nomes de tabela são não qualificados e serão criados nesse schema.

## Pré-requisitos e execução

- Compute Databricks com Spark e Delta Lake habilitados e permissões de leitura do arquivo e escrita no catálogo/schema.
- O arquivo deve estar em um local acessível pelo Spark (por exemplo, um Volume Unity Catalog ou Workspace Files).
- Execute na ordem: extração, transformação, Gold e diagnósticos.
- Na extração, informe os parâmetros `source_path` (caminho completo do CSV) e `batch_id` (identificador único da versão do arquivo). Use um novo `batch_id` para cada snapshot. A reexecução do mesmo lote é ignorada, evitando duplicação; mantenha execuções serializadas para evitar concorrência no mesmo lote.
- A leitura valida o cabeçalho contra o esquema esperado e falha para linhas CSV estruturalmente malformadas. Os campos de origem são lidos como texto; as conversões acontecem na Prata.

O notebook de extração cria a tabela Bronze na primeira execução. Se encontrar o esquema Bronze antigo do projeto, faz uma migração de esquema única, preserva as linhas existentes como lote `legacy` e inclui o novo lote; a migração para com erro se houver colunas desconhecidas, em vez de descartá-las. As execuções seguintes acrescentam lotes e metadados (`source_file`, `source_batch_id`, `Data_Carga` e `source_row_id`). A chave de linha é derivada de um hash do conteúdo, arquivo e ocorrência; linhas idênticas continuam sendo ocorrências distintas.

## Grão e atualização das tabelas

- **Bronze:** uma linha por ocorrência em cada lote carregado; mantém o histórico de snapshots.
- **Prata:** uma linha por ocorrência no lote mais recente; a tabela é reconstruída em cada execução. `source_row_id` identifica tecnicamente cada ocorrência; `codigo_emenda` pode repetir ou ser nulo.
- **Gold fato:** uma linha por `source_row_id` da Prata atual. As medidas são valores empenhados, liquidados, pagos e de restos a pagar.
- **Gold dimensões:** tempo, autor, localidade e classificação/programa orçamentário. As SKs são SHA-256 determinísticos dos atributos naturais, e cada dimensão contém o membro `UNKNOWN`.
- **Qualidade:** rejeições e métricas representam a transformação mais recente. Conversões inválidas são persistidas e interrompem o pipeline antes da promoção para Gold. Códigos IBGE negativos, já tratados como sentinelas na transformação original, são normalizados para nulo e contabilizados separadamente.

Prata, rejeições, métricas e Gold são snapshots reconstruídos; Gold usa `CREATE OR REPLACE TABLE`. Bronze é append-only para os lotes, mas não é um mecanismo de transação entre múltiplas execuções. Não execute lotes concorrentes; para produção, orquestre com concorrência máxima de um e use permissões/controles de publicação.

As SKs determinísticas dependem dos atributos naturais e permanecem estáveis quando novos membros são acrescentados. Mudanças nos atributos naturais formam outro membro; o modelo não implementa histórico SCD Tipo 2.

## Validações

`transform/transform_data.ipynb` verifica conversões e reconcilia a contagem entre lote e Prata. `gold/gold_modeling.ipynb` valida o grão da fato e a unicidade da chave de origem. `diagnostics/analysis.ipynb` verifica lote atual, contagens, chaves, integridade referencial, membros desconhecidos e conversões, além de disponibilizar análises de negócio. As validações críticas falham com mensagem explícita.

## Dados e publicação

O CSV em `data/` é um arquivo de exemplo do projeto. Antes de redistribuí-lo, confirme a origem, licença e termos de uso. Para uso recorrente ou produção, prefira armazenar os dados num Volume ou storage controlado, fora do repositório, e configure `source_path` por job.
