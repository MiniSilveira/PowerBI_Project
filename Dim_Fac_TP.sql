------------------------------------------------------------
-- CRIAÇÃO OU RECRIAÇÃO DO DATA MART TP_DataMart
------------------------------------------------------------

IF DB_ID('TP_DataMart') IS NULL
    CREATE DATABASE TP_DataMart;
GO

USE TP_DataMart;
GO

------------------------------------------------------------
-- DROP das tabelas 
------------------------------------------------------------

IF OBJECT_ID('dbo.FactViagem', 'U') IS NOT NULL
    DROP TABLE dbo.FactViagem;
IF OBJECT_ID('dbo.DimLocalizacao', 'U') IS NOT NULL
    DROP TABLE dbo.DimLocalizacao;
IF OBJECT_ID('dbo.DimCondutor', 'U') IS NOT NULL
    DROP TABLE dbo.DimCondutor;
IF OBJECT_ID('dbo.DimBarco', 'U') IS NOT NULL
    DROP TABLE dbo.DimBarco;
IF OBJECT_ID('dbo.DimTempo', 'U') IS NOT NULL
    DROP TABLE dbo.DimTempo;
GO

------------------------------------------------------------
-- DIMENSÃO TEMPO
------------------------------------------------------------

CREATE TABLE dbo.DimTempo (
    IdTempo      INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    Data         DATE        NOT NULL,
    Dia          TINYINT     NOT NULL,
    Mes          TINYINT     NOT NULL,
    Ano          SMALLINT    NOT NULL,
    Trimestre    TINYINT     NOT NULL,
    Semestre     TINYINT     NOT NULL,
    NomeMes      NVARCHAR(20) NOT NULL
);
GO

------------------------------------------------------------
-- DIMENSÃO BARCO
------------------------------------------------------------

CREATE TABLE dbo.DimBarco (
    IdBarcoSK      INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    IdBarcoOrigem  INT           NULL,           
    NomeBarco      NVARCHAR(100) NOT NULL,
    TipoBarco      NVARCHAR(50)  NULL,
    TamanhoBarco   INT           NOT NULL DEFAULT 0,  
    CapacidadeTEU  INT           NOT NULL DEFAULT 0
);
GO

------------------------------------------------------------
-- DIMENSÃO CONDUTOR
------------------------------------------------------------

CREATE TABLE dbo.DimCondutor (
    IdCondutorSK  INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    NomeCondutor  NVARCHAR(100) NOT NULL,
    Idade         TINYINT       NULL,
    Certificacao  NVARCHAR(50)  NULL,
    Sexo          CHAR(1)       NOT NULL DEFAULT 'U'  
);
GO

------------------------------------------------------------
-- DIMENSÃO LOCALIZAÇÃO
------------------------------------------------------------

CREATE TABLE dbo.DimLocalizacao (
    IdLocalizacaoSK INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    CidadeOrigem    NVARCHAR(100) NOT NULL,
    PaisOrigem      NVARCHAR(100) NOT NULL,
    CidadeDestino   NVARCHAR(100) NOT NULL,
    PaisDestino     NVARCHAR(100) NOT NULL
);
GO

------------------------------------------------------------
-- TABELA DE FACTOS: FACTVIAGEM
------------------------------------------------------------

CREATE TABLE dbo.FactViagem (
    IdFactViagem   INT IDENTITY(1,1) NOT NULL PRIMARY KEY,

    
    IdTempo        INT NOT NULL,
    IdBarco        INT NOT NULL,
    IdCondutor     INT NOT NULL,
    IdLocalizacao  INT NOT NULL,
    IdViagemOrigem INT NOT NULL,
    ValorTaxa      DECIMAL(18,2) NOT NULL DEFAULT 0,  
    DuracaoDias    INT           NOT NULL DEFAULT 0,  
    QtdContentores INT           NOT NULL DEFAULT 0,  
    PesoTotalKg    DECIMAL(18,2) NULL               
);
GO

------------------------------------------------------------
-- FOREIGN KEYS
------------------------------------------------------------

ALTER TABLE dbo.FactViagem
ADD CONSTRAINT FK_FactViagem_DimTempo
    FOREIGN KEY (IdTempo) REFERENCES dbo.DimTempo (IdTempo);

ALTER TABLE dbo.FactViagem
ADD CONSTRAINT FK_FactViagem_DimBarco
    FOREIGN KEY (IdBarco) REFERENCES dbo.DimBarco (IdBarcoSK);

ALTER TABLE dbo.FactViagem
ADD CONSTRAINT FK_FactViagem_DimCondutor
    FOREIGN KEY (IdCondutor) REFERENCES dbo.DimCondutor (IdCondutorSK);

ALTER TABLE dbo.FactViagem
ADD CONSTRAINT FK_FactViagem_DimLocalizacao
    FOREIGN KEY (IdLocalizacao) REFERENCES dbo.DimLocalizacao (IdLocalizacaoSK);
GO
