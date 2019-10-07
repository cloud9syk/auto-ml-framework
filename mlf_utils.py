from airflow.models import Variable

from mlf.mlf_db_utils import *


global target_yrmn
target_yrmn = str(Variable.get('target_yrmn'))


def insert_inferred_tag_meta(input_user_config):
    # inferred_tag_meta 테이블에 입력
    # 현재는 전역변수로 Meta정보를 받고 있으며 향후 config.ini로 대체하는 기능을 추가 개발할 수 있다.
    config_tag_meta = input_user_config['tag_meta']
    if not tag_id_exists(config_tag_meta['tag_id']):
        tag_meta = InferredTagMeta(tag_id=config_tag_meta['tag_id'],
                                   tag_name=config_tag_meta['tag_name'],
                                   problem_type=config_tag_meta['problem_type'],
                                   selection_criterion=config_tag_meta['selection_criterion'])
        commit_query(tag_meta)


def insert_inferred_tag_dag_info(input_user_config):
    # inferred_tag_dag_info 테이블에 입력
    # DAG를 수행하기 위한 정보 또한 전역변수로 받아 ModelDB에 입력한다.
    dag_info = input_user_config['dag_info']
    dag_info['dag_args']['start_date'] = str(dag_info['dag_args']['start_date'])
    config_tag_meta = input_user_config['tag_meta']

    if not dag_id_exists(dag_info['dag_args']['dag_id']) and tag_id_exists(config_tag_meta['tag_id']):
        dag_info = InferredTagDagInfo(dag_id=dag_info['dag_args']['dag_id'],
                                      tag_id=config_tag_meta['tag_id'],
                                      dag_args=dag_info['dag_args'],
                                      dag_purpose=dag_info['dag_purpose'])
        commit_query(dag_info)


def insert_tag_dag_run_state(dag_run, input_user_config):
    # inferred_tag_dag_run_state 테이블에 입력
    # DAG가 실행하는 시점에서 생성되는 데이터를 ModelDB에 저장한다.
    notification_info = input_user_config['notification_info']
    dag_start_state = InferredTagDagRunState(target_yrmn=target_yrmn,
                                             dag_id=dag_run.dag_id,
                                             run_id=dag_run.run_id,
                                             execution_date=dag_run.execution_date,
                                             state='start',
                                             message="{} has been started".format(dag_run.dag_id),
                                             channel=notification_info['channel'],
                                             recipients=notification_info['recipients'],
                                             created_date=datetime.utcnow()
                                             )
    commit_query(dag_start_state)


def insert_source_dataset(dag_run, task_instance, path_string):
    source_dataset = SourceDataset(target_yrmn=target_yrmn,
                                   dag_id=task_instance.dag_id,
                                   run_id=dag_run.run_id,
                                   execution_date=task_instance.execution_date,
                                   task_id=task_instance.task_id,
                                   created_date=datetime.utcnow(),
                                   file_path=path_string
                                   # ToDo : ip_address, is_valid 입력 로직 추가
                                   )
    commit_query(source_dataset)


def insert_wrangled_dataset(dag_run, task_instance, prepare_dataset_path, source_dataset_id):
    wrangled_dataset = WrangledDataset(target_yrmn=target_yrmn,
                                       dag_id=task_instance.dag_id,
                                       run_id=dag_run.run_id,
                                       execution_date=task_instance.execution_date,
                                       task_id=task_instance.task_id,
                                       source_dataset_id=source_dataset_id,
                                       created_date=datetime.utcnow(),
                                       file_path=str(prepare_dataset_path)
                                       # ToDo : file_path, ip_address, is_valid 입력 로직 추가
                                       )
    commit_query(wrangled_dataset)


def insert_predictive_model(dag_run, task_instance, hyperparameters, model_info_string, pipeline_pkl_file_path, wrangled_dataset_id):
    predictive_model = PredictiveModel(target_yrmn=target_yrmn,
                                       dag_id=task_instance.dag_id,
                                       run_id=dag_run.run_id,
                                       execution_date=task_instance.execution_date,
                                       task_id=task_instance.task_id,
                                       wrangled_dataset_id=wrangled_dataset_id,
                                       model_info=model_info_string,
                                       hyperparameters=hyperparameters,
                                       created_date=datetime.utcnow(),
                                       file_path=pipeline_pkl_file_path,
                                       is_valid=True
                                       # ToDo : file_path, ip_address, is_valid 입력 로직 추가
                                       )
    commit_query(predictive_model)


def insert_model_evaluation(metrics, predictive_model_id):
    for key, value in metrics.items():
        model_evaluation = ModelEvalutaion(predictive_model_id=predictive_model_id,
                                           metric_name=key,
                                           metric_value=value
                                           )
        commit_query(model_evaluation)


def insert_model_feature_importance(feature_importance, predictive_model_id):
    model_feature_importance = ModelFeatureImportance(predictive_model_id=predictive_model_id,
                                                      feature_importance=feature_importance
                                                      )
    commit_query(model_feature_importance)


def insert_selected_model(dag_run, task_instance, selected_model_id):
    selected_model = SelectedModel(predictive_model_id=selected_model_id,
                                   target_yrmn=target_yrmn,
                                   dag_id=task_instance.dag_id,
                                   run_id=dag_run.run_id,
                                   execution_date=task_instance.execution_date,
                                   task_id=task_instance.task_id,
                                   model_info=session.query(PredictiveModel.model_info).filter(
                                       PredictiveModel.predictive_model_id == selected_model_id),
                                   hyperparameters=session.query(PredictiveModel.hyperparameters).filter(
                                       PredictiveModel.predictive_model_id == selected_model_id))
    commit_query(selected_model)


def insert_success_state(dag_run, notification_info):
    dag_success_state = InferredTagDagRunState(target_yrmn=target_yrmn,
                                               dag_id=dag_run.dag_id,
                                               run_id=dag_run.run_id,
                                               execution_date=dag_run.execution_date,
                                               state='success',
                                               message="{} has succeed finish".format(dag_run.dag_id),
                                               channel=notification_info['channel'],
                                               recipients=notification_info['recipients'],
                                               created_date=datetime.utcnow()
                                               )
    commit_query(dag_success_state)


def insert_failed_state(dag_run, error_list, notification_info):
    dag_fail_state = InferredTagDagRunState(target_yrmn=target_yrmn,
                                            dag_id=dag_run.dag_id,
                                            run_id=dag_run.run_id,
                                            execution_date=dag_run.execution_date,
                                            state='failed',
                                            message=str(error_list),
                                            channel=notification_info['channel'],
                                            recipients=notification_info['recipients'],
                                            created_date=datetime.utcnow()
                                            )
    commit_query(dag_fail_state)


def insert_scoring(dag_run, task_instance, wrangled_dataset_id):
    scoring = Scoring(target_yrmn=target_yrmn,
                      dag_id=task_instance.dag_id,
                      run_id=dag_run.run_id,
                      execution_date=task_instance.execution_date,
                      task_id=task_instance.task_id,
                      wrangled_dataset_id=wrangled_dataset_id,
                      created_date=datetime.utcnow(),
                      # ToDo : file_path, ip_address, is_valid 입력 로직 추가
                      )
    commit_query(scoring)


def insert_error_log(dag_run, task_instance, e):
    error_message = 'MLF Exception!! {}'.format(e)
    error_log = ErrorLog(dag_id=task_instance.dag_id,
                         run_id=dag_run.run_id,
                         execution_date=task_instance.execution_date,
                         task_id=task_instance.task_id,
                         message=error_message,
                         created_date=datetime.utcnow())
    commit_query(error_log)


def get_source_dataset_id(dag_run, task_instance):
    source_dataset_id = session.query(SourceDataset.source_dataset_id).filter(
        SourceDataset.target_yrmn == target_yrmn,
        SourceDataset.dag_id == task_instance.dag_id,
        SourceDataset.run_id == dag_run.run_id,
        SourceDataset.execution_date == task_instance.execution_date,
    )
    return source_dataset_id


def get_wrangled_dataset_id(dag_run, task_instance):
    wrangled_dataset_id = session.query(WrangledDataset.wrangled_dataset_id).filter(
        WrangledDataset.target_yrmn == target_yrmn,
        WrangledDataset.dag_id == task_instance.dag_id,
        WrangledDataset.run_id == dag_run.run_id,
        WrangledDataset.execution_date == task_instance.execution_date,
    )
    return wrangled_dataset_id


def get_predictive_model_id(dag_run, task_instance):
    predictive_model_id = session.query(PredictiveModel.predictive_model_id).filter(
        PredictiveModel.target_yrmn == target_yrmn,
        PredictiveModel.dag_id == task_instance.dag_id,
        PredictiveModel.run_id == dag_run.run_id,
        PredictiveModel.execution_date == task_instance.execution_date,
        PredictiveModel.task_id == task_instance.task_id
    )
    return predictive_model_id


def get_selected_model_id(dag_run, task_instance, selection_criterion):
    selected_model_id = session.query(PredictiveModel.predictive_model_id).filter(
        PredictiveModel.target_yrmn == target_yrmn,
        PredictiveModel.dag_id == task_instance.dag_id,
        PredictiveModel.run_id == dag_run.run_id,
        PredictiveModel.execution_date == task_instance.execution_date,
        PredictiveModel.predictive_model_id == ModelEvaluation.predictive_model_id,
        ModelEvalutaion.metric_name == selection_criterion
    ).order_by(ModelEvalutaion.metric_value.desc()).first()
    return selected_model_id


def get_tag_id(task_instance):
    tag_id = session.query(InferredTagDagInfo.tag_id).filter(InferredTagDagInfo.dag_id == task_instance.dag_id)
    selection_criterion = session.query(InferredTagMeta.selection_criterion). \
        filter(InferredTagMeta.tag_id == tag_id)
    return selection_criterion


def get_error_logs(dag_run):
    error_logs = session.query(ErrorLog).filter(ErrorLog.dag_id == dag_run.dag_id,
                                                ErrorLog.run_id == dag_run.run_id).all()
    return error_logs


def get_selected_model_file_path(dag_run, tag_id ):
    dag_id = get_dag_id(dag_run, tag_id)
    predictive_model_id = get_selected_predictive_model_id(dag_id)
    for selected_model in session.query(PredictiveModel).filter(
                    PredictiveModel.predictive_model_id == predictive_model_id):
        selected_model_file_path = str(selected_model.file_path)
    return selected_model_file_path


def get_selected_predictive_model_id(dag_id):
    predictive_model_id = session.query(SelectedModel.predictive_model_id).filter(SelectedModel.dag_id == dag_id,
                                                                                  SelectedModel.target_yrmn == target_yrmn)
    return predictive_model_id


def get_dag_id(dag_run, tag_id):
    dag_id = session.query(InferredTagDagInfo.dag_id).filter(InferredTagDagInfo.tag_id == tag_id,
                                                             InferredTagDagInfo.dag_id != dag_run.dag_id)
    return dag_id

