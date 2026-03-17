⚓ Porto da Figueira da Foz - Port Management & Analytics
📌 Sobre o Projeto
Este projeto é uma solução end-to-end de Engenharia de Dados e Business Intelligence desenvolvida para analisar e monitorizar as operações logísticas e financeiras do Porto da Figueira da Foz.

O objetivo principal foi criar um pipeline de dados robusto que extrai informações fragmentadas de diferentes fontes, modela esses dados num Data Mart otimizado para análise e culmina num Dashboard interativo de suporte à decisão.

🏗️ Arquitetura e Tecnologias
O projeto foi construído seguindo as melhores práticas de ETL (Extract, Transform, Load) e modelação multidimensional:

Extração (Origem): Bases de dados MySQL e ficheiros CSV.

Transformação (ETL): Scripts em Python (limpeza de dados, tratamento de nulos e formatação).

Carga (Data Mart): SQL Server (armazenamento centralizado no formato Star Schema).

Visualização (BI): Power BI (Dashboards, relatórios e modelação DAX).

📊 Modelo de Dados (Star Schema)
A base de dados analítica (TP_DataMart) foi estruturada num esquema em estrela para maximizar a performance das consultas no Power BI:

Tabela de Factos:

FactViagem: Regista os eventos de transporte (Valor da Taxa, Duração em Dias, Qtd. de Contentores, etc.).

Tabelas de Dimensão:

DimTempo: Hierarquia temporal (Ano, Mês, Data).

DimBarco: Detalhes da frota (Nome, Tipo de Barco).

DimLocalizacao: Dados geográficos (Cidade e País de Origem).

DimCondutor: Perfil dos responsáveis pela carga (Nome, Sexo, Certificação).

📈 Principais KPIs e Insights
O dashboard final em Power BI responde a questões críticas de negócio através de medidas DAX personalizadas:

Receita Operacional: Total de taxas portuárias faturadas.

Volume de Tráfego: Contagem de viagens e total de contentores movimentados.

Eficiência Logística: Tempo médio de trânsito (em dias).

Análise Geográfica: Mapeamento das principais rotas e origens de mercadoria.

🚀 Como Executar o Projeto
Clonar este repositório: git clone https://github.com/teu-user/porto-figueira-analytics.git

Configurar as credenciais da base de dados no ficheiro config.py (ou .env).

Executar o script de ETL: python etl_pipeline.py.

Abrir o ficheiro Dashboard_Porto.pbix no Power BI Desktop e atualizar a fonte de dados para o SQL Server local.
