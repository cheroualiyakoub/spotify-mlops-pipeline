from sklearn.base import BaseEstimator, TransformerMixin

class DataCleaner(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.cols_to_drop = ['track_id', 'track_name', 'artist_name']
        
    def fit(self, X, y=None):
      
        return self
        
    def transform(self, X):
        return self.initial_clean_data(X)
    
    def initial_clean_data(self, df):
        df_clean = df.copy()
        return df_clean.drop(columns=self.cols_to_drop, errors='ignore')
    
    def __reduce__(self):
        return (self.__class__, ())  # Add this for pickling