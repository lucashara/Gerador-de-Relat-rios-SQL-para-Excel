from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import DisconnectionError, SQLAlchemyError
from sqlalchemy.sql import text
from contextlib import contextmanager
import logging
import os
import time

# Carrega as variáveis de ambiente para configuração de conexões
from dotenv import load_dotenv

load_dotenv()

# String de conexão com detalhes para conectar ao banco Oracle utilizando cx_Oracle
oracle_connection_string = "oracle+cx_oracle://{username}:{password}@{hostname}:{port}/?service_name={service_name}"


def reconectar_com_backoff_exponencial(sessao, tentativas=10):
    """Tenta reconectar com backoff exponencial."""
    tentativa = 0
    while tentativa < tentativas:
        try:
            sessao.commit()
            return
        except DisconnectionError as e:
            tempo_espera = 2**tentativa
            logging.error(
                f"Erro de conexão na tentativa {tentativa + 1}: {e}. Tentando novamente em {tempo_espera} segundos."
            )
            time.sleep(tempo_espera)
            tentativa += 1
        except SQLAlchemyError as e:
            logging.error(f"Erro de SQLAlchemy na tentativa {tentativa + 1}: {e}")
            sessao.rollback()
            raise e
    logging.error("Todas as tentativas de reconexão falharam.")
    raise DisconnectionError("Todas as tentativas de reconexão falharam.")


@contextmanager
def gerenciar_sessao():
    """Fornece um escopo transacional ao redor de uma série de operações.
    Garante que cada sessão seja encerrada corretamente após as operações,
    tratando erros e desconexões de forma adequada."""
    engine = create_engine(
        oracle_connection_string.format(
            username=os.getenv("DB_USERNAME"),  # Usuário do banco de dados
            password=os.getenv("DB_PASSWORD"),  # Senha do banco de dados
            hostname=os.getenv("DB_HOSTNAME"),  # Host do servidor do banco de dados
            port=os.getenv("DB_PORT"),  # Porta do servidor do banco de dados
            service_name=os.getenv(
                "DB_SERVICE_NAME"
            ),  # Nome do serviço do banco Oracle
        ),
        pool_pre_ping=True,  # Verifica a conexão antes de cada operação para evitar usar uma conexão inválida
        echo=False,  # Desativado para produção; ativar (True) para debug se necessário
    )
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    sessao = SessionLocal()
    try:
        yield sessao  # Prover a sessão para as operações
        reconectar_com_backoff_exponencial(
            sessao
        )  # Tenta reconectar com backoff exponencial
    except SQLAlchemyError as e:
        logging.error(f"Erro de SQLAlchemy: {e}")
        sessao.rollback()
        raise e  # Relança a exceção para ser tratada em um nível superior
    finally:
        sessao.close()  # Assegura que a sessão seja sempre fechada após o uso
