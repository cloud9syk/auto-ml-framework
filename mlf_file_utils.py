import os
import pickle


def make_pkl_file_name(prefix, dag_id, run_id):
    pkl_file_name = "_".join([prefix,dag_id,run_id])
    return pkl_file_name


def make_pkl_file_path(path, pkl_file_name):
    pkl_file_path = os.path.join(path, pkl_file_name)
    return pkl_file_path


def make_error_message(error_list, error_logs):
    for error_log in error_logs:
        message = "{}'s {} has {}".format(error_log.dag_id, error_log.task_id, error_log.message)
        error_list.append(message)


def dump_pickle(df, path_string):
    with open(path_string, 'wb') as f:
        pickle.dump(df, f)


def load_pickle(path_string):
    with open(path_string, 'rb') as f:
        df = pickle.load(f)
    return df