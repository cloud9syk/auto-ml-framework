from mlf.model_db_orm import *

# sqlalchemy session
session = get_session()


def tag_id_exists(tag_id):
    tag_id_is_exist = session.query(InferredTagMeta).filter_by(
        tag_id=tag_id).scalar() is not None
    return tag_id_is_exist


def dag_id_exists(dag_id):
    dag_id_is_exist = session.query(InferredTagDagInfo).filter_by(dag_id=dag_id).scalar() is not None
    return dag_id_is_exist


def error_log_exists(dag_run):
    error_log_is_exist = session.query(ErrorLog).filter(ErrorLog.dag_id == dag_run.dag_id,
                                                        ErrorLog.run_id == dag_run.run_id).scalar() is not None
    return error_log_is_exist


def get_tag_id(task_instance):
    tag_id = session.query(InferredTagDagInfo.tag_id).filter(InferredTagDagInfo.dag_id == task_instance.dag_id)
    selection_criterion = session.query(InferredTagMeta.selection_criterion). \
        filter(InferredTagMeta.tag_id == tag_id)
    return selection_criterion


def get_error_logs(dag_run):
    error_logs = session.query(ErrorLog).filter(ErrorLog.dag_id == dag_run.dag_id,
                                                ErrorLog.run_id == dag_run.run_id).all()
    return error_logs


def commit_query(orm_object):
    session.add(orm_object)
    session.commit()