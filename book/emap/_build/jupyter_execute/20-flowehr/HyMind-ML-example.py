from typing import List
import os
import tempfile
from pathlib import Path
import pickle
from uuid import uuid4

import cloudpickle
import pandas as pd
import mlflow
from mlflow.tracking import MlflowClient
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import ParameterGrid, train_test_split
from sklearn.metrics import confusion_matrix
from sklearn.compose import ColumnTransformer
from sklearn.exceptions import NotFittedError
from sklearn.utils.validation import check_is_fitted
from sklearn.compose import ColumnTransformer, make_column_selector
from sklearn.impute import MissingIndicator, SimpleImputer
from sklearn.pipeline import Pipeline as SKPipeline
from sklearn.preprocessing import (
    FunctionTransformer,
    OneHotEncoder,
    OrdinalEncoder,
    StandardScaler,
)

from hycastle.lens.base import BaseLens
from hycastle.lens.transformers import DateTimeExploder, timedelta_as_hours

%matplotlib inline

secret_path = 'secret'
os.environ['EMAP_DB_USER'], os.environ['EMAP_DB_PASSWORD'] = Path(secret_path).read_text().strip().split('\n')

from hylib.dt import LONDON_TZ
from hycastle.lens.icu import BournvilleICUSitRepLens
from hycastle.icu_store.live import live_dataset
from hycastle.icu_store.retro import retro_dataset
from hymind.lib.models.icu_aggregate import AggregateDemandModel

mlflow_var = os.getenv('HYMIND_REPO_TRACKING_URI')
mlflow.set_tracking_uri(mlflow_var)   

client = MlflowClient()

df = retro_dataset('T03')

df.shape

df.head()

# lens = BournvilleICUSitRepLens()

class DemoLens(BaseLens):
    numeric_output = True
    index_col = "episode_slice_id"

    @property
    def input_cols(self) -> List[str]:
        return [
            "episode_slice_id",
            "admission_age_years",
            "avg_heart_rate_1_24h",
            "max_temp_1_12h",
            "avg_resp_rate_1_24h",
            "elapsed_los_td",
            "admission_dt",
            "horizon_dt",
            "n_inotropes_1_4h",
            "wim_1",
            "bay_type",
            "sex",
            "vent_type_1_4h",
        ]

    def specify(self) -> ColumnTransformer:
        return ColumnTransformer(
            [
                (
                    "select",
                    "passthrough",
                    [
                        "episode_slice_id",
                        "admission_age_years",
                        "n_inotropes_1_4h",
                        "wim_1",
                    ],
                ),
                ("bay_type_enc", OneHotEncoder(), ["bay_type"]),
                (
                    "sex_enc",
                    OrdinalEncoder(
                        handle_unknown="use_encoded_value", unknown_value=-1
                    ),
                    ["sex"],
                ),
                (
                    "admission_dt_exp",
                    DateTimeExploder(),
                    ["admission_dt", "horizon_dt"],
                ),
                (
                    "vent_type_1_4h_enc",
                    OrdinalEncoder(
                        handle_unknown="use_encoded_value", unknown_value=-1
                    ),
                    ["vent_type_1_4h"],
                ),
                (
                    "vitals_impute",
                    SimpleImputer(strategy="mean", add_indicator=False),
                    [
                        "avg_heart_rate_1_24h",
                        "max_temp_1_12h",
                        "avg_resp_rate_1_24h",
                    ],
                ),
                (
                    "elapsed_los_td_hrs",
                    FunctionTransformer(timedelta_as_hours),
                    ["elapsed_los_td"],
                ),
            ]
        )


lens = DemoLens()

X = lens.fit_transform(df)

y = df['discharged_in_48hr'].astype(int)

X_train, X_valid, y_train, y_valid = train_test_split(X, y, test_size=0.2)

m = RandomForestClassifier(n_jobs=-1, n_estimators=50, max_depth=2)
%time m.fit(X_train.values, y_train.values.ravel())

tmp_path = Path('tmp')
tmp_path.mkdir(parents=True, exist_ok=True)

def mlflow_log_string(text, filename):
    full_path = tmp_path / filename
    with open(full_path, 'w') as f:
        f.write(str(text))
    mlflow.log_artifact(full_path)

def mlflow_log_tag_dict(tag_dict, filename):
    """Logs tag dict to MLflow (while preserving order unlike mlflow.log_dict)"""
    full_path = tmp_path / filename
    with open(full_path, 'w') as f:
        yaml.dump(tag_dict, f, sort_keys=False)
    mlflow.log_artifact(full_path)
    
def mlflow_log_lens(l):
    full_path = l.pickle(tmp_path)
    mlflow.log_artifact(full_path, 'lens')

# Owner|Type|Name|Date
exp_name = 'NS|models|jendemo|2021-10-05'


os.environ["MLFLOW_EXPERIMENT_NAME"] = exp_name
experiment_id = mlflow.create_experiment(exp_name)

experiment_id

def artifact_path():
    pth = Path(mlflow.get_artifact_uri())
    pth.mkdir(parents=True, exist_ok=True)
    return pth

grid = {
    'n_estimators':[5, 10],
    'max_depth':[2, 10]
}

runs_per_param_set = 2

for i in range(runs_per_param_set):
    
    for g in ParameterGrid(grid):
        m = RandomForestClassifier(n_jobs=-1)

        with mlflow.start_run():
            #mlflow_logs()
            
            m.set_params(**g)
            mlflow.log_params(g)

            m.fit(X_train.values, y_train.values.ravel())
            
            eval_df = pd.DataFrame({
                        'predict_proba':m.predict_proba(X_valid.values)[:,1], 
                        'label':y_valid.to_numpy().ravel()
                       }, 
                columns=['predict_proba','label'])
            
            train_accuracy = m.score(X_train, y_train.to_numpy())
            mlflow.log_metric('train_accuracy', train_accuracy)
            valid_accuracy = m.score(X_valid, y_valid.to_numpy())       
            mlflow.log_metric('valid_accuracy', valid_accuracy)
            
            train_confusion = confusion_matrix(m.predict(X_train.values), y_train.to_numpy())
            mlflow_log_string(train_confusion, 'train_confusion.txt')
            valid_confusion = confusion_matrix(m.predict(X_valid.values), y_valid.to_numpy())
            mlflow_log_string(valid_confusion, 'valid_confusion.txt')

            mlflow.sklearn.log_model(m, 'model')

runs = mlflow.search_runs()
runs.head()

params = [col for col in runs if col.startswith('params')]
best_params = runs.groupby(params)['metrics.valid_accuracy'].mean().idxmax()
best_row = runs.set_index(keys=params).loc[best_params]

best_run_id = list(best_row['run_id'])[0]
best_run_id

with mlflow.start_run(run_id=best_run_id):
    # tag the run as best_row
    mlflow.set_tag('best_run', 1)   

with mlflow.start_run(run_id=best_run_id):
     mlflow_log_lens(lens)

model_name = 'demo-model-jen'
version = 1

mlflow.register_model(f'runs:/{best_run_id}/model', model_name)

model_info = client.get_model_version(model_name, version)
model_info

run_info = client.get_run(model_info.run_id)
run_info

model = mlflow.sklearn.load_model(f'models:/{model_name}/{version}')

model

with tempfile.TemporaryDirectory() as tmp:
    tmp_dir = Path(tmp)
    
    client.download_artifacts(model_info.run_id, 'lens', tmp_dir)
    
    lens_path = next((tmp_dir / 'lens').rglob('*.pkl'))
    with open(lens_path, 'rb') as f:
        loaded_lens = pickle.load(f)

loaded_lens

live_df = live_dataset('T03')

live_df.loc[:, ['episode_slice_id', 'admission_dt', 'bed_code', 'avg_heart_rate_1_24h']].sort_values('admission_dt', ascending=False).head()

X_df = loaded_lens.transform(live_df)

predictions = model.predict_proba(X_df)

live_df['prediction'] = predictions[:, 1]

live_df.loc[:, ['episode_slice_id', 'prediction']]










