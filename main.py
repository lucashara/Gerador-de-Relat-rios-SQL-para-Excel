import os
import logging
import pandas as pd
from config_bd import gerenciar_sessao
from sqlalchemy.sql import text
from datetime import datetime

# Configurações iniciais de log
logging.basicConfig(
    handlers=[
        logging.FileHandler("process.log", "a", "utf-8"),
        logging.StreamHandler(),
    ],
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
)

# Lista de códigos para substituição
codusur_list = [5268, 5239, 5248, 5235, 5285, 5243, 5254, 5264]


def process_sql_file(file_path):
    with open(file_path, "r") as file:
        sql_content = file.read()  # Mantém o conteúdo original
    return sql_content


def execute_query(sql_query, codusur):
    with gerenciar_sessao() as sessao:
        try:
            sql_query = sql_query.replace(
                ":CODUSUR", str(codusur)
            )  # Substitui o parâmetro pelo valor
            result = sessao.execute(text(sql_query))
            rows = result.fetchall()
            columns = result.keys()
            df = pd.DataFrame(rows, columns=columns)
        except Exception as e:
            logging.error(f"Erro ao executar a consulta para codusur {codusur}: {e}")
            df = pd.DataFrame()  # Retornar DataFrame vazio em caso de erro
    return df


def export_to_excel(dataframes, output_file, sheet_names):
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        for df, sheet_name in zip(dataframes, sheet_names):
            if not df.empty:
                df.to_excel(writer, sheet_name=str(sheet_name), index=False)


def main():
    logging.info("Iniciando o script de geração de relatórios.")
    sql_dir = "sql"
    resultado_dir = "resultado"

    if not os.path.exists(sql_dir):
        logging.error(f"Diretório '{sql_dir}' não encontrado.")
        return

    if not os.path.exists(resultado_dir):
        os.makedirs(resultado_dir)
        logging.info(f"Diretório '{resultado_dir}' criado.")

    sql_files = [f for f in os.listdir(sql_dir) if f.endswith(".sql")]
    if not sql_files:
        logging.info("Nenhum arquivo .sql encontrado no diretório.")
        return

    for sql_file in sql_files:
        logging.info(f"Processando o arquivo '{sql_file}'.")
        try:
            file_path = os.path.join(sql_dir, sql_file)
            sql_query = process_sql_file(file_path)
            dataframes = []
            sheet_names = []

            for codusur in codusur_list:
                df = execute_query(sql_query, codusur)

                if not df.empty:
                    sheet_name = str(codusur)  # Nome da aba com o valor de codusur
                    dataframes.append(df)
                    sheet_names.append(sheet_name)
                    logging.info(
                        f"Consulta com codusur = {codusur} retornou {len(df)} resultados."
                    )
                else:
                    logging.info(
                        f"Consulta com codusur = {codusur} não retornou resultados. Pulando para o próximo."
                    )

            if dataframes:
                timestamp = datetime.now().strftime("%d%m%Y-%H%M%S")
                output_file = os.path.join(
                    resultado_dir, f"{os.path.splitext(sql_file)[0]}_{timestamp}.xlsx"
                )
                export_to_excel(dataframes, output_file, sheet_names)
                logging.info(
                    f"Arquivo '{sql_file}' processado e exportado como '{output_file}'."
                )
            else:
                logging.info(
                    f"Arquivo '{sql_file}' não gerou resultados em nenhuma consulta."
                )

        except Exception as e:
            logging.error(f"Erro ao processar o arquivo '{sql_file}': {e}")

    logging.info("Script de geração de relatórios finalizado.")


if __name__ == "__main__":
    main()
