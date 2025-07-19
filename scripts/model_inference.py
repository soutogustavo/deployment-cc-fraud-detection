import xgboost as xgb
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import IntegerType


# Load pre-trained model
model = xgb.Booster()
model.load_model("models/fraud_detection_xgboost.model")

@pandas_udf(IntegerType())
def predict_fraud(*cols: pd.Series) -> pd.Series:
    X = pd.concat(cols, axis=1)
    dmatrix = xgb.DMatrix(X)
    preds = model.predict(dmatrix)
    
    return pd.Series((preds > 0.5).astype(int))
