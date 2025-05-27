import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

class BalancedResampler(BaseEstimator, TransformerMixin):
    def __init__(self, target_minority_percentage=0.4, random_state=42):
        self.target_minority_percentage = target_minority_percentage
        self.random_state = random_state
        
    def fit_resample(self, X, y):
        # Convert to DataFrame if needed
        if not isinstance(X, pd.DataFrame):
            X = pd.DataFrame(X)
            
        # Combine features and target
        df = X.copy()
        df['target'] = y

        # Split by class
        df_min = df[df['target'] == 1]  # Minority class
        df_maj = df[df['target'] == 0]  # Majority class

        # Calculate sample sizes
        n_min = len(df_min)
        required_maj = int(n_min / self.target_minority_percentage - n_min)
        
        # Sample majority class
        df_maj_sampled = df_maj.sample(
            n=min(required_maj, len(df_maj)),
            random_state=self.random_state
        )
        
        # Combine and shuffle
        balanced_df = pd.concat([df_min, df_maj_sampled]).sample(frac=1, random_state=self.random_state)
        
        # Return separated X and y
        return balanced_df.drop('target', axis=1), balanced_df['target']