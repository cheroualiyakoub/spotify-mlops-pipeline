import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import OneHotEncoder

class SafeOneHotEncoder(BaseEstimator, TransformerMixin):
    """
    Safely one-hot encodes specified categorical columns.
    - Uses `OneHotEncoder` internally (recommended) or `pd.get_dummies`.
    - Handles unseen categories in the test set.
    - Integrates with scikit-learn pipelines.
    
    Parameters:
    -----------
    columns : list
        List of column names to one-hot encode.
    use_pandas : bool (default=False)
        If True, uses `pd.get_dummies` (not recommended for pipelines).
    handle_unknown : str (default='ignore')
        Options: 'ignore' (encode as zeros) or 'error' (raise exception).
    """
    
    def __init__(self, columns=['genre', 'key', 'time_signature'], use_pandas=False, handle_unknown='ignore'):
        self.columns = columns
        self.use_pandas = use_pandas
        self.handle_unknown = handle_unknown
        self.encoder = None
        self.dummy_columns = []  # Stores columns to ensure consistency
        
    def fit(self, X, y=None):
        if self.use_pandas:
            # Learn dummy columns from training data
            self.dummy_columns = pd.get_dummies(X[self.columns]).columns.tolist()
        else:
            # Use OneHotEncoder to learn categories
            self.encoder = OneHotEncoder(
                handle_unknown=self.handle_unknown, 
                sparse_output=False
            )
            self.encoder.fit(X[self.columns])
        return self
    
    def transform(self, X):
        if self.use_pandas:
            # Encode with pd.get_dummies and align columns
            X_encoded = pd.get_dummies(X[self.columns])
            X_encoded = X_encoded.reindex(columns=self.dummy_columns, fill_value=0)
        else:
            # Encode with OneHotEncoder
            X_encoded = self.encoder.transform(X[self.columns])
            encoded_columns = self.encoder.get_feature_names_out(self.columns)
            X_encoded = pd.DataFrame(X_encoded, columns=encoded_columns, index=X.index)
        
        # Drop original columns and concatenate encoded data
        X_remaining = X.drop(columns=self.columns)
        return pd.concat([X_remaining, X_encoded], axis=1)
    
    def __reduce__(self):
        return (self.__class__, ())  # Add this for pickling