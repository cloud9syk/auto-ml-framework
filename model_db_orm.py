from datetime import datetime

from sqlalchemy import create_engine, Column, String, Integer, ForeignKey, JSON, TIMESTAMP, UniqueConstraint, \
    BigInteger, \
    Text, Boolean, REAL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

model_db_info = "postgres://alab:alab@localhost:5432/airflow_data_test"

db = create_engine(model_db_info)
base = declarative_base()


def get_session():
    Session = sessionmaker(db)
    return Session()


class InferredTagMeta(base):
    """
    todo: 스키마 설명 추가
    """
    __tablename__ = 'inferred_tag_meta'
    tag_id = Column(String(30), primary_key=True)
    tag_name = Column(String(50))
    problem_type = Column(String(100))
    selection_criterion = Column(String(20))


class InferredTagDagInfo(base):
    """
    todo: 스키마 설명 추가
    """
    __tablename__ = 'inferred_tag_dag_info'

    dag_id = Column(String(250), primary_key=True)
    tag_id = Column(String(30), ForeignKey('inferred_tag_meta.tag_id'), nullable=False)
    dag_args = Column(JSON)
    dag_purpose = Column(String(10))


class InferredTagDagRunState(base):
    """
    todo: 스키마 설명 추가
    """
    __tablename__ = 'inferred_tag_dag_run_state'

    target_yrmn = Column(String(8), primary_key=True)
    dag_id = Column(String(250), primary_key=True)
    run_id = Column(String(250), primary_key=True)
    execution_date = Column(TIMESTAMP, primary_key=True)
    state = Column(String(20), primary_key=True)
    created_date = Column(TIMESTAMP)
    message = Column(String(500))
    channel = Column(String(50))
    recipients = Column(String(250))


class SourceDataset(base):
    """
    todo: 스키마 설명 추가
    """
    __tablename__ = 'source_dataset'

    source_dataset_id = Column(BigInteger, primary_key=True, autoincrement=True)
    target_yrmn = Column(String(8), nullable=False)
    dag_id = Column(String(250), ForeignKey('inferred_tag_dag_info.dag_id'), nullable=False)
    run_id = Column(String(250), nullable=False)
    execution_date = Column(TIMESTAMP, nullable=False)
    task_id = Column(String(250), nullable=False)
    file_path = Column(Text)
    ip_address = Column(String(15))
    created_date = Column(TIMESTAMP)
    is_valid = Column(Boolean)

    UniqueConstraint('target_yrmn', 'dag_id', 'run_id', 'execution_date', 'task_id')


class WrangledDataset(base):
    """
    todo: 스키마 설명 추가
    """
    __tablename__ = 'wrangled_dataset'

    wrangled_dataset_id = Column(BigInteger, primary_key=True, autoincrement=True)
    target_yrmn = Column(String(8), nullable=False)
    dag_id = Column(String(250), nullable=False)
    run_id = Column(String(250), nullable=False)
    execution_date = Column(TIMESTAMP, nullable=False)
    task_id = Column(String(250), nullable=False)
    source_dataset_id = Column(BigInteger, ForeignKey('source_dataset.source_dataset_id'), nullable=False)
    file_path = Column(Text)
    ip_address = Column(String(15))
    created_date = Column(TIMESTAMP)
    is_valid = Column(Boolean)

    UniqueConstraint('target_yrmn', 'dag_id', 'run_id', 'execution_date', 'task_id')


class PredictiveModel(base):
    """
    todo: 스키마 설명 추가
    """
    __tablename__ = 'predictive_model'

    predictive_model_id = Column(BigInteger, primary_key=True, autoincrement=True)
    target_yrmn = Column(String(8), nullable=False)
    dag_id = Column(String(250), nullable=False)
    run_id = Column(String(250), nullable=False)
    execution_date = Column(TIMESTAMP, nullable=False)
    task_id = Column(String(250), nullable=False)
    wrangled_dataset_id = Column(BigInteger, ForeignKey('wrangled_dataset.wrangled_dataset_id'))
    model_info = Column(Text)
    hyperparameters = Column(JSON)
    file_path = Column(Text)
    ip_address = Column(String(15))
    created_date = Column(TIMESTAMP)
    is_valid = Column(Boolean)

    UniqueConstraint('target_yrmn', 'dag_id', 'run_id', 'execution_date', 'task_id')


class ModelFeatureImportance(base):
    """
    todo: 스키마 설명 추가
    """
    __tablename__ = 'model_feature_importance'

    predictive_model_id = Column(BigInteger, ForeignKey('predictive_model.predictive_model_id'), primary_key=True)
    feature_importance = Column(JSON)


class ModelEvaluation(base):
    """
    todo: 스키마 설명 추가
    """
    __tablename__ = 'model_evaluation'

    predictive_model_id = Column(BigInteger, ForeignKey('predictive_model.predictive_model_id'), primary_key=True)
    metric_name = Column(String(20), primary_key=True)
    metric_value = Column(REAL)


class Scoring(base):
    """
    todo: 스키마 설명 추가
    """
    __tablename__ = 'scoring'

    score_id = Column(BigInteger, primary_key=True, autoincrement=True)
    target_yrmn = Column(String(8), nullable=False)
    dag_id = Column(String(250), nullable=False)
    run_id = Column(String(250), nullable=False)
    execution_date = Column(TIMESTAMP, nullable=False)
    task_id = Column(String(250), nullable=False)
    wrangled_dataset_id = Column(BigInteger, ForeignKey('wrangled_dataset.wrangled_dataset_id'))
    created_date = Column(TIMESTAMP)
    is_valid = Column(Boolean)

    UniqueConstraint('target_yrmn', 'dag_id', 'run_id', 'execution_date', 'task_id')


class SelectedModel(base):
    """
    todo: 스키마 설명 추가
    """
    __tablename__ = 'selected_model'

    predictive_model_id = Column(BigInteger, primary_key=True, autoincrement=True)
    target_yrmn = Column(String(8), nullable=False)
    dag_id = Column(String(250), nullable=False)
    run_id = Column(String(250), nullable=False)
    execution_date = Column(TIMESTAMP, nullable=False)
    task_id = Column(String(250), nullable=False)
    model_info = Column(Text)
    hyperparameters = Column(JSON)

    UniqueConstraint('target_yrmn', 'dag_id', 'run_id', 'execution_date', 'task_id')


class ErrorLog(base):
    """
    todo: 스키마 설명 추가
    """
    __tablename__ = 'error_log'

    error_log_id = Column(BigInteger, primary_key=True, autoincrement=True)
    dag_id = Column(String(250), nullable=False)
    run_id = Column(String(250), nullable=False)
    execution_date = Column(TIMESTAMP, nullable=False)
    task_id = Column(String(250), nullable=False)
    message = Column(String(1000), nullable=False)
    created_date = Column(TIMESTAMP, default=datetime.utcnow())


base.metadata.create_all(db)
