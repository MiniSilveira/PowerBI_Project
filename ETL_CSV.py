import os
import csv
from datetime import datetime
import pyodbc

# ------------------------------------------------------------------------------
# 1. CONFIGURAÇÕES
# ------------------------------------------------------------------------------

CSV_PATH = os.getenv("CSV_DADOS_PATH", "dados_atualizado.csv")

# Definições do SQL Server
MSSQL_HOST   = os.getenv("MSSQL_HOST", "LOCALHOST")
MSSQL_PORT   = int(os.getenv("MSSQL_PORT", "1433"))
MSSQL_USER   = os.getenv("MSSQL_USER", "sa")
MSSQL_PWD    = os.getenv("MSSQL_PWD",  "1234")
MSSQL_DB     = os.getenv("MSSQL_DB",   "TP_DataMart")  
MSSQL_DRIVER = os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")

conn = None
cursor = None


def get_mssql_conn():
    conn_str = (
        f"DRIVER={{{MSSQL_DRIVER}}};"
        f"SERVER={MSSQL_HOST};"
        f"DATABASE={MSSQL_DB};"
        f"Trusted_Connection=yes;"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

# ------------------------------------------------------------------------------
# 2. FUNÇÕES DE DIMENSÃO
# ------------------------------------------------------------------------------


def get_or_create_dim_tempo(cursor, data_obj):
    # 1) Ver se já existe essa data na DimTempo
    cursor.execute("SELECT IdTempo FROM DimTempo WHERE Data = ?", data_obj)
    row = cursor.fetchone()
    if row and row[0] is not None:
        return row[0]

    # 2) Inserir se não existir
    ano, mes, dia = data_obj.year, data_obj.month, data_obj.day
    trimestre = (mes - 1) // 3 + 1
    semestre = 1 if mes <= 6 else 2

    meses_pt = {
        1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril', 5: 'Maio', 6: 'Junho',
        7: 'Julho', 8: 'Agosto', 9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
    }

    query = """
        INSERT INTO DimTempo (Data, Dia, Mes, Ano, Trimestre, Semestre, NomeMes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    cursor.execute(query, (data_obj, dia, mes, ano, trimestre, semestre, meses_pt[mes]))

    # 3) Voltar a ir buscar o IdTempo pela data
    cursor.execute("SELECT IdTempo FROM DimTempo WHERE Data = ?", data_obj)
    row = cursor.fetchone()
    if not row or row[0] is None:
        raise Exception(f"Falha ao obter IdTempo para a data {data_obj}")

    return row[0]


def get_or_create_dim_barco(cursor, nome, tipo, capacidade_teu):
    # 1) Procurar pelo nome do barco
    cursor.execute("SELECT IdBarcoSK FROM DimBarco WHERE NomeBarco = ?", nome)
    row = cursor.fetchone()
    if row and row[0] is not None:
        return row[0]

    # 2)Inserir se não existir
    query = """
        INSERT INTO DimBarco (NomeBarco, TipoBarco, TamanhoBarco, CapacidadeTEU)
        VALUES (?, ?, ?, ?)
    """
    cursor.execute(query, (nome, tipo, 0, capacidade_teu))

    # 3) Voltar a procurar pelo nome
    cursor.execute("SELECT IdBarcoSK FROM DimBarco WHERE NomeBarco = ?", nome)
    row = cursor.fetchone()
    if not row or row[0] is None:
        raise Exception(f"Falha ao obter IdBarcoSK para barco {nome}")

    return row[0]


def get_or_create_dim_condutor(cursor, nome, idade, certificacao, sexo):
    # 1) Procurar pelo nome do condutor
    cursor.execute("SELECT IdCondutorSK FROM DimCondutor WHERE NomeCondutor = ?", nome)
    row = cursor.fetchone()
    if row and row[0] is not None:
        return row[0]

    # 2)Inserir se não existir
    query = """
        INSERT INTO DimCondutor (NomeCondutor, Idade, Certificacao, Sexo)
        VALUES (?, ?, ?, ?)
    """
    cursor.execute(query, (nome, idade, certificacao, sexo))

    # 3) Voltar a procurar pelo nome
    cursor.execute("SELECT IdCondutorSK FROM DimCondutor WHERE NomeCondutor = ?", nome)
    row = cursor.fetchone()
    if not row or row[0] is None:
        raise Exception(f"Falha ao obter IdCondutorSK para condutor {nome}")

    return row[0]


def get_or_create_dim_localizacao(cursor, cidade_orig, pais_orig):
    cidade_dest = 'Figueira da Foz'
    pais_dest = 'Portugal'

    # 1) Procurar pela combinação origem+destino
    cursor.execute("""
        SELECT IdLocalizacaoSK 
        FROM DimLocalizacao 
        WHERE CidadeOrigem = ? AND PaisOrigem = ? AND CidadeDestino = ? AND PaisDestino = ?
    """, (cidade_orig, pais_orig, cidade_dest, pais_dest))
    row = cursor.fetchone()
    if row and row[0] is not None:
        return row[0]

    # 2) Inserir se não existir
    query = """
        INSERT INTO DimLocalizacao (CidadeOrigem, PaisOrigem, CidadeDestino, PaisDestino)
        VALUES (?, ?, ?, ?)
    """
    cursor.execute(query, (cidade_orig, pais_orig, cidade_dest, pais_dest))

    # 3) Voltar a procurar
    cursor.execute("""
        SELECT IdLocalizacaoSK 
        FROM DimLocalizacao 
        WHERE CidadeOrigem = ? AND PaisOrigem = ? AND CidadeDestino = ? AND PaisDestino = ?
    """, (cidade_orig, pais_orig, cidade_dest, pais_dest))
    row = cursor.fetchone()
    if not row or row[0] is not None:
        return row[0]
    raise Exception(f"Falha ao obter IdLocalizacaoSK para {cidade_orig}, {pais_orig}")


# ------------------------------------------------------------------------------
# 3. MAIN (ETL)
# ------------------------------------------------------------------------------


def main():
    print("--- INÍCIO ETL CSV -> SQL SERVER ---")
    global conn, cursor
    linhas_proc = 0

    try:
        conn = get_mssql_conn()
        cursor = conn.cursor()
        print("Ligação estabelecida.")

        if not os.path.exists(CSV_PATH):
            print(f"ERRO: Ficheiro {CSV_PATH} não encontrado.")
            return

        with open(CSV_PATH, mode='r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f, delimiter=';')

            print("A processar linhas...")

            for row in reader:
                try:
                    # 1) Transformação de dados
                    dt_chegada = datetime.strptime(row['datachegada'], '%d/%m/%Y')
                    dt_partida = datetime.strptime(row['datapartida'], '%d/%m/%Y')

                    val_taxa = float(row['taxa'].replace('.', '').replace(',', '.'))

                    cap_str = row['capacidadeteu']
                    capacidade_teu = int(cap_str) if cap_str and cap_str.isdigit() else 0

                    idade = int(row['idadecondutor']) if row['idadecondutor'] else None

                    qtd_contentores = int(row['numerocontentares']) if row['numerocontentares'] else 0
                    peso_total = float(row['peso']) if row['peso'] else 0.0

                    # 2) Dimensões
                    id_tempo = get_or_create_dim_tempo(cursor, dt_chegada)

                    id_barco = get_or_create_dim_barco(
                        cursor,
                        row['nomebarco'],
                        row['tipobarco'],
                        capacidade_teu
                    )

                    id_condutor = get_or_create_dim_condutor(
                        cursor,
                        row['nomecondutor'],
                        idade,
                        row['certificacao'],
                        row['sexo']
                    )

                    id_localizacao = get_or_create_dim_localizacao(
                        cursor,
                        row['cidade_origem'],
                        row['pais_origem']
                    )

                    # 3) Factos
                    duracao = (dt_chegada - dt_partida).days

                    cursor.execute("""
                        INSERT INTO FactViagem 
                            (IdTempo, IdBarco, IdCondutor, IdLocalizacao, IdViagemOrigem,
                             ValorTaxa, DuracaoDias, QtdContentores, PesoTotalKg)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        id_tempo,
                        id_barco,
                        id_condutor,
                        id_localizacao,
                        row['idviagem'],
                        val_taxa,
                        duracao,
                        qtd_contentores,
                        peso_total
                    ))

                    linhas_proc += 1
                    if linhas_proc % 100 == 0:
                        conn.commit()
                        print(f"Processadas {linhas_proc} linhas...")

                except Exception as e:
                    print(f"Erro na linha {linhas_proc + 1}: {e}")
                    continue

            conn.commit()
            print(f"SUCESSO! Total carregado: {linhas_proc}")

    except Exception as e:
        print(f"Erro Crítico Geral: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("Ligação fechada.")


if __name__ == "__main__":
    main()
