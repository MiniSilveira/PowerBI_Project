import os
import sys
import mysql.connector
import pyodbc
from datetime import datetime

# ==============================================================================
# 1. CONFIGURAÇÕES
# ==============================================================================
print("A carregar configurações...")

# MySQL (Fonte)
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1") 
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PWD  = os.getenv("MYSQL_PWD",  "1234")      
MYSQL_DB   = os.getenv("MYSQL_DB",   "TP_G2")

# SQL Server (Destino)
MSSQL_HOST   = os.getenv("MSSQL_HOST", "LOCALHOST")
MSSQL_PORT   = int(os.getenv("MSSQL_PORT", "1433"))
MSSQL_USER   = os.getenv("MSSQL_USER", "sa")
MSSQL_PWD    = os.getenv("MSSQL_PWD",  "1234")
MSSQL_DB     = os.getenv("MSSQL_DB",   "TP_DataMart") 
MSSQL_DRIVER = os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")

conn_mysql = None
conn_sql = None

# ==============================================================================
# 2. FUNÇÕES AUXILIARES
# ==============================================================================

def get_mysql_conn():
    return mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PWD, database=MYSQL_DB
    )

def get_mssql_conn():
    conn_str = (
        f"DRIVER={{{MSSQL_DRIVER}}};SERVER={MSSQL_HOST};DATABASE={MSSQL_DB};"
        f"Trusted_Connection=yes;TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

# --- DIMENSÕES ---

def get_or_create_dim_tempo(cursor, data_iso):
    # Garantir que temos um datetime
    if isinstance(data_iso, str):
        data_obj = datetime.strptime(data_iso, '%Y-%m-%d')
    else:
        data_obj = data_iso

    # 1) Ver se já existe essa data
    cursor.execute("SELECT IdTempo FROM DimTempo WHERE Data = ?", data_obj)
    row = cursor.fetchone()
    if row and row[0] is not None:
        return row[0]

    # 2) Inserir se não existir
    ano, mes, dia = data_obj.year, data_obj.month, data_obj.day
    trimestre = (mes - 1) // 3 + 1
    semestre = 1 if mes <= 6 else 2
    meses_pt = {
        1:'Janeiro', 2:'Fevereiro', 3:'Março', 4:'Abril', 5:'Maio', 6:'Junho',
        7:'Julho', 8:'Agosto', 9:'Setembro', 10:'Outubro', 11:'Novembro', 12:'Dezembro'
    }

    query = """
        INSERT INTO DimTempo (Data, Dia, Mes, Ano, Trimestre, Semestre, NomeMes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    cursor.execute(query, (data_obj, dia, mes, ano, trimestre, semestre, meses_pt[mes]))

    # 3) Voltar a ir buscar
    cursor.execute("SELECT IdTempo FROM DimTempo WHERE Data = ?", data_obj)
    row = cursor.fetchone()
    if not row or row[0] is None:
        raise Exception(f"Falha ao obter IdTempo para data {data_obj}")
    return row[0]



def get_or_create_dim_barco(cursor, dados_barco):
    # 1) tentar encontrar barco pelo Id de origem
    cursor.execute(
        "SELECT IdBarcoSK FROM DimBarco WHERE IdBarcoOrigem = ?",
        dados_barco['id_origem']
    )
    row = cursor.fetchone()

    # 2) se já existir, atualizar e devolver
    if row and row[0] is not None:
        cursor.execute("""
            UPDATE DimBarco 
            SET TamanhoBarco = ?, CapacidadeTEU = ?
            WHERE IdBarcoSK = ?
        """, (int(dados_barco['tamanho']), dados_barco['capacidade'], row[0]))
        return row[0]

    # 3) Inserir se não existir
    query = """
        INSERT INTO DimBarco (IdBarcoOrigem, NomeBarco, TipoBarco, TamanhoBarco, CapacidadeTEU)
        VALUES (?, ?, ?, ?, ?)
    """
    cursor.execute(query, (
        dados_barco['id_origem'],
        dados_barco['nome'],
        dados_barco['tipo'],
        int(dados_barco['tamanho']),
        dados_barco['capacidade']
    ))

    # 4) voltar a procurar pelo IdBarcoOrigem
    cursor.execute(
        "SELECT IdBarcoSK FROM DimBarco WHERE IdBarcoOrigem = ?",
        dados_barco['id_origem']
    )
    row = cursor.fetchone()
    if not row or row[0] is None:
        raise Exception(f"Falha ao obter IdBarcoSK para barco origem {dados_barco['id_origem']}")
    return row[0]


def get_or_create_dim_condutor(cursor, dados_condutor):
    # 1) procurar pelo nome do condutor
    cursor.execute(
        "SELECT IdCondutorSK FROM DimCondutor WHERE NomeCondutor = ?",
        dados_condutor['nome']
    )
    row = cursor.fetchone()
    if row and row[0] is not None:
        return row[0]

    # 2) inserir se não existir
    query = """
        INSERT INTO DimCondutor (NomeCondutor, Idade, Certificacao, Sexo)
        VALUES (?, ?, ?, ?)
    """
    cursor.execute(query, (
        dados_condutor['nome'],
        dados_condutor['idade'],
        dados_condutor['certificacao'],
        'U'  
    ))

    # 3) voltar a procurar
    cursor.execute(
        "SELECT IdCondutorSK FROM DimCondutor WHERE NomeCondutor = ?",
        dados_condutor['nome']
    )
    row = cursor.fetchone()
    if not row or row[0] is None:
        raise Exception(f"Falha ao obter IdCondutorSK para condutor {dados_condutor['nome']}")
    return row[0]



def get_or_create_dim_localizacao(cursor, dados_loc):
    cidade_orig = dados_loc['cidade']
    pais_orig = dados_loc['pais']
    cidade_dest = 'Figueira da Foz'
    pais_dest = 'Portugal'

    # 1) procurar localização já existente
    cursor.execute("""
        SELECT IdLocalizacaoSK 
        FROM DimLocalizacao 
        WHERE CidadeOrigem = ? AND PaisOrigem = ? 
          AND CidadeDestino = ? AND PaisDestino = ?
    """, (cidade_orig, pais_orig, cidade_dest, pais_dest))
    row = cursor.fetchone()
    if row and row[0] is not None:
        return row[0]

    # 2) inserir se não existir
    query = """
        INSERT INTO DimLocalizacao (CidadeOrigem, PaisOrigem, CidadeDestino, PaisDestino)
        VALUES (?, ?, ?, ?)
    """
    cursor.execute(query, (cidade_orig, pais_orig, cidade_dest, pais_dest))

    # 3) voltar a procurar
    cursor.execute("""
        SELECT IdLocalizacaoSK 
        FROM DimLocalizacao 
        WHERE CidadeOrigem = ? AND PaisOrigem = ? 
          AND CidadeDestino = ? AND PaisDestino = ?
    """, (cidade_orig, pais_orig, cidade_dest, pais_dest))
    row = cursor.fetchone()
    if not row or row[0] is None:
        raise Exception(f"Falha ao obter IdLocalizacaoSK para {cidade_orig}, {pais_orig}")
    return row[0]


# ==============================================================================
# 3. MAIN
# ==============================================================================

def main():
    print("--- INÍCIO ETL MYSQL -> SQL SERVER ---")
    global conn_mysql, conn_sql

    try:
        # 1) Ligar às bases de dados
        conn_mysql = get_mysql_conn()
        cur_mysql = conn_mysql.cursor(dictionary=True)
        
        conn_sql = get_mssql_conn()
        cur_sql = conn_sql.cursor()
        print("Conexões feitas.")

        # 2) Limpeza inicial das tabelas de destino
        print("A limpar tabelas...")
        cur_sql.execute("DELETE FROM FactViagem")
        cur_sql.execute("DELETE FROM DimBarco")
        cur_sql.execute("DELETE FROM DimCondutor")
        cur_sql.execute("DELETE FROM DimLocalizacao") 
        cur_sql.execute("DELETE FROM DimTempo")
        conn_sql.commit()

        # 3) EXTRAÇÃO (SELECT) do MySQL
        sql_extract = """
            SELECT 
                v.idviagem, v.datachegada, 
                b.idbarco, b.nomebarco, b.tipobarco, b.tamanhobarco, b.capacidadeteu,
                c.nomecondutor, c.idadecondutor, c.certificacao,
                l.cidade, l.pais,
                (SELECT SUM(valor) FROM taxas WHERE viagem_idviagem = v.idviagem) as total_taxas,
                DATEDIFF(v.datachegada, v.datapartida) as duracao_dias
            FROM viagem v
            JOIN barco b ON v.barco_idbarco = b.idbarco
            JOIN condutor c ON v.condutor_idcondutor = c.idcondutor
            JOIN localizacao l ON v.localizacao_idlocalizacao = l.idlocalizacao
            WHERE v.status = 'Concluida'
        """
        cur_mysql.execute(sql_extract)
        rows = cur_mysql.fetchall()
        print(f"{len(rows)} viagens encontradas.")

        # 4) LOOP pelas viagens
        count = 0
        for row in rows:
            try:
               
                if not row['datachegada']:
                    print(f"[AVISO] Viagem {row['idviagem']} sem data de chegada – ignorada.")
                    continue

                id_tempo = get_or_create_dim_tempo(cur_sql, row['datachegada'])

            
                if id_tempo is None:
                    print(f"[ERRO] get_or_create_dim_tempo devolveu None para a viagem {row['idviagem']} com data {row['datachegada']}")
                    continue

                # 4.2) Dimensão Barco
                tamanho_val = row['tamanhobarco'] if row['tamanhobarco'] is not None else 0

                dict_barco = {
                    'id_origem': row['idbarco'],
                    'nome': row['nomebarco'],
                    'tipo': row['tipobarco'],
                    'tamanho': tamanho_val,
                    'capacidade': row['capacidadeteu'] if row['capacidadeteu'] else 0
                }
                id_barco = get_or_create_dim_barco(cur_sql, dict_barco)

                # 4.3) Dimensão Condutor
                dict_condutor = {
                    'nome': row['nomecondutor'],
                    'idade': row['idadecondutor'],
                    'certificacao': row['certificacao']
                }
                id_condutor = get_or_create_dim_condutor(cur_sql, dict_condutor)

                # 4.4) Dimensão Localização
                dict_loc = {'cidade': row['cidade'], 'pais': row['pais']}
                id_localizacao = get_or_create_dim_localizacao(cur_sql, dict_loc)

                # 4.5) Medidas / factos
                val_taxa = float(row['total_taxas']) if row['total_taxas'] else 0.0
                duracao = int(row['duracao_dias']) if row['duracao_dias'] else 0

                # 4.6) Inserir na Tabela de factos FactViagem
                insert_fact = """
                    INSERT INTO FactViagem 
                    (IdTempo, IdBarco, IdCondutor, IdLocalizacao, IdViagemOrigem,
                     ValorTaxa, DuracaoDias, QtdContentores)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """
                cur_sql.execute(
                    insert_fact,
                    (id_tempo, id_barco, id_condutor, id_localizacao,
                     row['idviagem'], val_taxa, duracao, 0)
                )

                count += 1
                if count % 100 == 0:
                    conn_sql.commit()
                    print(f"... {count} processados.")

            except Exception as e_row:
                print(f"Erro na viagem {row['idviagem']}: {e_row}")
                continue

        
        conn_sql.commit()
        print("SUCESSO! ETL Terminado.")

    except Exception as e:
        print(f"ERRO GERAL: {e}")
    finally:
        if conn_mysql:
            conn_mysql.close()
        if conn_sql:
            conn_sql.close()
        print("Conexões fechadas.")


if __name__ == "__main__":
    main()
