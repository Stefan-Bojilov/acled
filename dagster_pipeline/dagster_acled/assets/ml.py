from datetime import datetime, timedelta
import os
import tempfile
from typing import Any, Dict

import dagster as dg
from dagster_acled.assets.report import ReportConfig
from dagster_acled.partitions import daily_partition
from dagster_acled.resources.resources import PostgreSQLResource
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from PyPDF2 import PdfMerger
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import RandomizedSearchCV, cross_val_score, train_test_split
from sklearn.preprocessing import LabelEncoder
import xgboost as xgb


class ModelConfig(dg.Config):
    """Configuration for ACLED fatality prediction model."""
    
    training_days_back: int = 365 
    test_size: float = 0.2
    random_state: int = 42
    
    n_trials_tuning: int = 50
    cv_folds: int = 5
    
    forecast_days_ahead: int = 7 
    
    model_version: str = "v1"
    retrain_threshold_days: int = 30  

# ============================================================================
# PREPROCESSING FUNCTIONS 
# ============================================================================

def safe_numeric(value):
    """Convert numpy types to native Python types"""
    if hasattr(value, 'item'):  
        return value.item()
    return float(value) if isinstance(value, (int, float)) else value

def target_encode_cv(df: pd.DataFrame, categorical_col: str, target_col: str, cv_folds: int = 5, smoothing: int = 10) -> np.ndarray:
    """Target encoding with cross-validation and smoothing."""
    from sklearn.model_selection import KFold
    
    kf = KFold(n_splits=cv_folds, shuffle=True, random_state=42)
    target_encoded = np.zeros(len(df))
    global_mean = df[target_col].mean()
    
    for train_idx, val_idx in kf.split(df):
        train_df = df.iloc[train_idx]
        target_stats = train_df.groupby(categorical_col)[target_col].agg(['mean', 'count'])
        target_stats['smoothed_mean'] = (
            (target_stats['count'] * target_stats['mean'] + smoothing * global_mean) /
            (target_stats['count'] + smoothing)
        )
        
        target_encoded[val_idx] = df.iloc[val_idx][categorical_col].map(target_stats['smoothed_mean'])
        target_encoded[val_idx] = np.where(
            pd.isna(target_encoded[val_idx]), 
            global_mean, 
            target_encoded[val_idx]
        )
    
    return target_encoded

def prepare_features_for_model(df: pd.DataFrame, target_col: str = 'fatalities', fit_encoders: bool = True, encoders: dict = None):
    """Complete preprocessing pipeline for XGBoost."""
        
    df_processed = df.copy()
    
    if encoders is None:
        encoders = {}
    
    # One-hot encoding for low cardinality
    onehot_cols = ['disorder_type', 'event_type', 'sub_event_type', 'inter1', 'inter2']
    df_encoded = pd.get_dummies(df_processed, columns=onehot_cols, prefix=onehot_cols)
    
    # Target encoding for high cardinality
    target_encode_cols = ['admin2', 'admin3']
    
    if fit_encoders:
        for col in target_encode_cols:
            encoded_col_name = f'{col}_target_encoded'
            df_encoded[encoded_col_name] = target_encode_cv(df_encoded, col, target_col)
            
            encoders[f'{col}_global_mean'] = df_encoded[target_col].mean()
            encoders[f'{col}_target_stats'] = df_encoded.groupby(col)[target_col].agg(['mean', 'count'])
    else:
        for col in target_encode_cols:
            encoded_col_name = f'{col}_target_encoded'
            target_stats = encoders[f'{col}_target_stats']
            global_mean = encoders[f'{col}_global_mean']
            
            smoothing = 10
            target_stats_smoothed = (
                (target_stats['count'] * target_stats['mean'] + smoothing * global_mean) /
                (target_stats['count'] + smoothing)
            )
            
            df_encoded[encoded_col_name] = df_encoded[col].map(target_stats_smoothed)
            df_encoded[encoded_col_name] = df_encoded[encoded_col_name].fillna(global_mean)
    
    high_variance_cols = ['actor1', 'actor2']
    for col in high_variance_cols:
        if fit_encoders:
            encoded_col_name = f'{col}_target_encoded'
            df_encoded[encoded_col_name] = target_encode_cv(df_encoded, col, target_col)
            encoders[f'{col}_global_mean'] = df_encoded[target_col].mean()
            encoders[f'{col}_target_stats'] = df_encoded.groupby(col)[target_col].agg(['mean', 'count'])
        else:
            encoded_col_name = f'{col}_target_encoded'
            target_stats = encoders[f'{col}_target_stats']
            global_mean = encoders[f'{col}_global_mean']
            smoothing = 10
            target_stats_smoothed = (
                (target_stats['count'] * target_stats['mean'] + smoothing * global_mean) /
                (target_stats['count'] + smoothing)
            )
            df_encoded[encoded_col_name] = df_encoded[col].map(target_stats_smoothed)
            df_encoded[encoded_col_name] = df_encoded[encoded_col_name].fillna(global_mean)
    
    lower_variance_cols = ['interaction', 'admin1']
    for col in lower_variance_cols:
        if fit_encoders:
            le = LabelEncoder()
            encoded_col_name = f'{col}_label_encoded'
            df_encoded[encoded_col_name] = le.fit_transform(df_encoded[col].astype(str))
            encoders[f'{col}_label_encoder'] = le
        else:
            le = encoders[f'{col}_label_encoder']
            encoded_col_name = f'{col}_label_encoded'
            df_encoded[encoded_col_name] = df_encoded[col].astype(str).apply(
                lambda x: le.transform([x])[0] if x in le.classes_ else -1
            )
    
    if 'latitude' in df_encoded.columns and 'longitude' in df_encoded.columns:
        center_lat = df_encoded['latitude'].median()
        center_lon = df_encoded['longitude'].median()
        
        df_encoded['distance_from_center'] = np.sqrt(
            (df_encoded['latitude'] - center_lat)**2 + 
            (df_encoded['longitude'] - center_lon)**2
        )
        
        if fit_encoders:
            encoders['center_lat'] = center_lat
            encoders['center_lon'] = center_lon
    
    original_categorical = [
        'disorder_type', 'event_type', 'sub_event_type', 'inter1', 'inter2',
        'admin2', 'admin3', 'actor1', 'actor2', 'interaction', 'admin1'
    ]
    
    final_features = [col for col in df_encoded.columns if col not in original_categorical]
    df_final = df_encoded[final_features].copy()
    
    return df_final, encoders

# ============================================================================
# DAGSTER ASSETS
# ============================================================================

@dg.asset(
    name="acled_model_training_data",
    description="Prepare training data for fatality prediction model from PostgreSQL",
    group_name="machine_learning",
    deps=['acled_daily_to_postgres']
)
def acled_model_training_data(
    context: dg.AssetExecutionContext,
    config: ModelConfig,
    postgres: PostgreSQLResource,
) -> pd.DataFrame:
    """Extract and prepare training data from PostgreSQL for the ML model."""
    
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=config.training_days_back)
    
    context.log.info(f"Extracting training data from {start_date} to {end_date}")
    
    query = f"""
    SELECT 
        disorder_type,
        event_type,
        sub_event_type,
        actor1,
        actor2,
        inter1,
        inter2,
        interaction,
        admin1,
        admin2, 
        admin3,
        latitude,
        longitude,
        fatalities,
        event_date
    FROM {postgres.table_name} 
    WHERE event_date >= %s 
        AND event_date <= %s
        AND fatalities IS NOT NULL
        AND fatalities > 0
        AND latitude IS NOT NULL 
        AND longitude IS NOT NULL
    ORDER BY event_date
    """
    
    conn = postgres.get_connection()
    try:
        df = pd.read_sql_query(query, conn, params=[start_date, end_date])
        context.log.info(f"Extracted {len(df)} records for training")
        
        fatality_stats = {
            'min_fatalities': int(df['fatalities'].min()),
            'max_fatalities': int(df['fatalities'].max()),
            'median_fatalities': float(df['fatalities'].median()),
            'mean_fatalities': float(df['fatalities'].mean()),
            'std_fatalities': float(df['fatalities'].std()),
        }
        
        event_type_dist = df['event_type'].value_counts().head(10)
        event_type_table = [
            {"event_type": event_type, "count": int(count), "percentage": round(count/len(df)*100, 1)}
            for event_type, count in event_type_dist.items()
        ]
        
        regional_dist = df['admin1'].value_counts().head(15)
        
        fatality_buckets = pd.cut(df['fatalities'], bins=[0, 1, 5, 10, 25, 50, 100, float('inf')], 
                                 labels=['1', '2-5', '6-10', '11-25', '26-50', '51-100', '100+'])
        bucket_counts = fatality_buckets.value_counts().sort_index()
        
        df['month'] = pd.to_datetime(df['event_date']).dt.to_period('M')
        monthly_stats = df.groupby('month').agg({
            'fatalities': ['count', 'sum', 'mean'],
            'admin1': 'nunique'
        }).round(2)
        monthly_stats.columns = ['event_count', 'total_fatalities', 'avg_fatalities', 'unique_regions']
        monthly_stats = monthly_stats.reset_index()
        monthly_stats['month_str'] = monthly_stats['month'].astype(str)
        
        monthly_records = []
        for _, row in monthly_stats.iterrows():
            monthly_records.append({
                "month_str": str(row['month']),  # Convert Period to string
                "event_count": int(row['event_count']),
                "total_fatalities": float(row['total_fatalities']),
                "avg_fatalities": float(row['avg_fatalities']),
                "unique_regions": int(row['unique_regions'])
            })

        missing_analysis = {}
        key_columns = ['disorder_type', 'event_type', 'actor1', 'actor2', 'admin1', 'admin2']
        for col in key_columns:
            if col in df.columns:
                null_count = int(df[col].isnull().sum())
                empty_count = int((df[col].astype(str) == '').sum())
                missing_analysis[f'{col}_missing'] = null_count + empty_count
                missing_analysis[f'{col}_completeness_pct'] = float(round((1 - (null_count + empty_count)/len(df)) * 100, 1))
        
        context.add_output_metadata({
            "training_records": len(df),
            "unique_regions": int(df['admin1'].nunique()),
            "unique_event_types": int(df['event_type'].nunique()),
            "unique_countries": int(df.get('country', pd.Series()).nunique()) if 'country' in df.columns else 0,
            
            "high_fatality_events": int((df['fatalities'] >= 10).sum()),
            "moderate_fatality_events": int(((df['fatalities'] >= 5) & (df['fatalities'] < 10)).sum()),
            "low_fatality_events": int((df['fatalities'] < 5).sum()),
            
            "days_of_data": int((end_date - start_date).days),
            
            **missing_analysis,
            "overall_data_quality": float(round(sum([v for k, v in missing_analysis.items() if k.endswith('_completeness_pct')]) / max(len([k for k in missing_analysis.keys() if k.endswith('_completeness_pct')]), 1), 1)),
            
            "event_type_distribution": dg.TableMetadataValue(
                records=[dg.TableRecord(r) for r in event_type_table],
                schema=dg.TableSchema([
                    dg.TableColumn(name="event_type", type="text"),
                    dg.TableColumn(name="count", type="int"),
                    dg.TableColumn(name="percentage", type="float"),
                ])
            ),
            
            "fatality_distribution": dg.TableMetadataValue(
                records=[dg.TableRecord({"range": str(bucket), "count": int(count)}) 
                        for bucket, count in bucket_counts.items()],
                schema=dg.TableSchema([
                    dg.TableColumn(name="range", type="text"),
                    dg.TableColumn(name="count", type="int"),
                ])
            ),
            
            "monthly_trends": dg.TableMetadataValue(
                records=[dg.TableRecord(record) for record in monthly_records],
                schema=dg.TableSchema([
                    dg.TableColumn(name="month_str", type="text"),
                    dg.TableColumn(name="event_count", type="int"),
                    dg.TableColumn(name="total_fatalities", type="float"),
                    dg.TableColumn(name="avg_fatalities", type="float"),
                    dg.TableColumn(name="unique_regions", type="int"),
                ])
            ),
            
            "date_range_start": start_date.isoformat(),
            "date_range_end": end_date.isoformat(),
        })
        
        return df
        
    finally:
        conn.close()


@dg.asset(
    name="acled_trained_model",
    description="Train XGBoost fatality prediction model with hyperparameter tuning",
    group_name="machine_learning",
    io_manager_key="s3_pickle_io_manager",
)
def acled_trained_model(
    context: dg.AssetExecutionContext,
    config: ModelConfig,
    acled_model_training_data: pd.DataFrame,
) -> Dict[str, Any]:
    """Train and tune XGBoost model for fatality prediction."""
    
    context.log.info("Starting model training and hyperparameter tuning")
    
    df_processed, encoders = prepare_features_for_model(
        acled_model_training_data, 
        target_col='fatalities',
        fit_encoders=True
    )
    
    columns_to_drop = ['event_date', 'month']  
    columns_to_drop = [col for col in columns_to_drop if col in df_processed.columns]
    
    if columns_to_drop:
        context.log.info(f"Dropping non-feature columns: {columns_to_drop}")
        df_processed = df_processed.drop(columns=columns_to_drop)

    non_numeric_cols = []
    for col in df_processed.columns:
        if col != 'fatalities' and not pd.api.types.is_numeric_dtype(df_processed[col]):
            non_numeric_cols.append(col)
    
    if non_numeric_cols:
        context.log.warning(f"Found non-numeric columns that will be dropped: {non_numeric_cols}")
        context.log.warning(f"Column dtypes: {df_processed[non_numeric_cols].dtypes.to_dict()}")
        df_processed = df_processed.drop(columns=non_numeric_cols)
    
    columns_to_drop = ['event_date', 'month']  
    columns_to_drop = [col for col in columns_to_drop if col in df_processed.columns]
    
    if columns_to_drop:
        context.log.info(f"Dropping non-feature columns: {columns_to_drop}")
        df_processed = df_processed.drop(columns=columns_to_drop)

    non_numeric_cols = []
    for col in df_processed.columns:
        if col != 'fatalities' and not pd.api.types.is_numeric_dtype(df_processed[col]):
            non_numeric_cols.append(col)
    
    if non_numeric_cols:
        context.log.warning(f"Found non-numeric columns that will be dropped: {non_numeric_cols}")
        df_processed = df_processed.drop(columns=non_numeric_cols)
    
    X = df_processed.drop(columns=['fatalities'])
    y = df_processed['fatalities']
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, 
        test_size=config.test_size, 
        random_state=config.random_state,
        stratify=None
    )
    
    context.log.info(f"Training set: {len(X_train)} samples, Test set: {len(X_test)} samples")
    context.log.info(f"Features: {X_train.shape[1]}")
    
    training_start = datetime.now()
    
    param_dist = {
        'n_estimators': [100, 200, 300, 500, 800],
        'max_depth': [3, 4, 5, 6, 7, 8],
        'learning_rate': [0.01, 0.03, 0.05, 0.1, 0.15, 0.2],
        'subsample': [0.6, 0.7, 0.8, 0.9, 1.0],
        'colsample_bytree': [0.6, 0.7, 0.8, 0.9, 1.0],
        'min_child_weight': [1, 3, 5, 7, 10],
        'gamma': [0, 0.1, 0.2, 0.3, 0.5],
        'reg_alpha': [0, 0.001, 0.01, 0.1, 1],
        'reg_lambda': [0, 0.001, 0.01, 0.1, 1]
    }
    
    xgb_model = xgb.XGBRegressor(
        objective='reg:squarederror',
        random_state=config.random_state,
        n_jobs=-1
    )
    
    random_search = RandomizedSearchCV(
        xgb_model,
        param_distributions=param_dist,
        n_iter=config.n_trials_tuning,
        scoring='neg_root_mean_squared_error',
        cv=config.cv_folds,
        random_state=config.random_state,
        n_jobs=-1,
        verbose=1
    )
    
    random_search.fit(X_train, y_train)
    best_model = random_search.best_estimator_
    
    training_time = (datetime.now() - training_start).total_seconds()
    
    # Evaluate model
    y_train_pred = best_model.predict(X_train)
    y_test_pred = best_model.predict(X_test)
    
    train_rmse = np.sqrt(mean_squared_error(y_train, y_train_pred))
    test_rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))
    test_mae = mean_absolute_error(y_test, y_test_pred)
    test_r2 = r2_score(y_test, y_test_pred)
    train_r2 = r2_score(y_train, y_train_pred)
    
    test_errors = np.abs(y_test - y_test_pred)
    accuracy_buckets = {
        'perfect_predictions': int((test_errors == 0).sum()),
        'within_1_fatality': int((test_errors <= 1).sum()),
        'within_5_fatalities': int((test_errors <= 5).sum()),
        'within_10_fatalities': int((test_errors <= 10).sum()),
        'over_10_error': int((test_errors > 10).sum()),
    }
    
    context.log.info(f"Model performance - Train RMSE: {train_rmse:.3f}, Test RMSE: {test_rmse:.3f}")
    context.log.info(f"Test MAE: {test_mae:.3f}, Test R²: {test_r2:.3f}")
    
    feature_importance = pd.DataFrame({
        'feature': X_train.columns,
        'importance': best_model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    cv_scores = cross_val_score(best_model, X_train, y_train, 
                               cv=config.cv_folds, 
                               scoring='neg_root_mean_squared_error')
    cv_rmse_scores = -cv_scores
    
    model_package = {
        'model': best_model,
        'encoders': encoders,
        'feature_columns': list(X_train.columns),
        'best_params': random_search.best_params_,
        'performance_metrics': {
            'train_rmse': float(train_rmse),
            'test_rmse': float(test_rmse),
            'test_mae': float(test_mae),
            'test_r2': float(test_r2),
            'train_r2': float(train_r2),
            'cv_score': float(-random_search.best_score_)
        },
        'feature_importance': feature_importance.to_dict('records'),
        'training_date': datetime.now().isoformat(),
        'model_version': config.model_version,
        'training_samples': len(X_train)
    }
    
    # Add extensive metadata for plotting and analysis
    context.add_output_metadata({
        # Basic model info for plotting
        "model_version": config.model_version,
        "training_samples": int(len(X_train)),
        "test_samples": int(len(X_test)),
        "num_features": int(X_train.shape[1]),
        "training_time_seconds": float(round(training_time, 1)),
        
        # Performance metrics for plotting
        "train_rmse": float(train_rmse),
        "test_rmse": float(test_rmse),
        "train_r2": float(train_r2),
        "test_r2": float(test_r2),
        "test_mae": float(test_mae),
        "cv_score": float(-random_search.best_score_),
        "cv_score_std": float(cv_rmse_scores.std()),
        
        # Model complexity metrics for plotting
        "best_n_estimators": int(random_search.best_params_.get('n_estimators', 0)),
        "best_max_depth": int(random_search.best_params_.get('max_depth', 0)),
        "best_learning_rate": float(random_search.best_params_.get('learning_rate', 0)),
        
        # Prediction accuracy for plotting - convert all to native Python types
        "perfect_predictions": int(accuracy_buckets['perfect_predictions']),
        "within_1_fatality": int(accuracy_buckets['within_1_fatality']),
        "within_5_fatalities": int(accuracy_buckets['within_5_fatalities']),
        "within_10_fatalities": int(accuracy_buckets['within_10_fatalities']),
        "over_10_error": int(accuracy_buckets['over_10_error']),
        "prediction_accuracy_pct": float(round((accuracy_buckets['within_5_fatalities'] / len(y_test)) * 100, 1)),
        
        # Overfitting detection for plotting
        "overfitting_indicator": float(round(abs(train_rmse - test_rmse), 3)),
        "generalization_score": float(round(min(test_r2 / max(train_r2, 0.001), 1), 3)),
        
        # Feature importance table
        "top_features": dg.TableMetadataValue(
            records=[dg.TableRecord({
                "feature": str(row['feature'][:30]),  # Truncate long names
                "importance": float(row['importance']),  # Convert numpy float to Python float
                "importance_pct": float(round(row['importance'] / feature_importance['importance'].sum() * 100, 1))
            }) for row in feature_importance.head(15).to_dict('records')],
            schema=dg.TableSchema([
                dg.TableColumn(name="feature", type="text"),
                dg.TableColumn(name="importance", type="float"),
                dg.TableColumn(name="importance_pct", type="float"),
            ])
        ),
        
        # Best hyperparameters table
        "best_hyperparameters": dg.TableMetadataValue(
            records=[dg.TableRecord({"parameter": str(k), "value": str(v)}) 
                    for k, v in random_search.best_params_.items()],
            schema=dg.TableSchema([
                dg.TableColumn(name="parameter", type="text"),
                dg.TableColumn(name="value", type="text"),
            ])
        ),
        
        # CV scores table
        "cv_fold_scores": dg.TableMetadataValue(
            records=[dg.TableRecord({"fold": f"Fold {i+1}", "rmse": float(round(score, 3))}) 
                    for i, score in enumerate(cv_rmse_scores)],
            schema=dg.TableSchema([
                dg.TableColumn(name="fold", type="text"),
                dg.TableColumn(name="rmse", type="float"),
            ])
        ),
    })
    
    return model_package


@dg.asset(
    name="acled_fatality_predictions",
    description="Generate fatality predictions for recent and upcoming periods",
    group_name="machine_learning",
    deps=['acled_daily_to_postgres']
)
def acled_fatality_predictions(
    context: dg.AssetExecutionContext,
    config: ModelConfig,
    acled_trained_model: Dict[str, Any],
    postgres: PostgreSQLResource,
) -> pd.DataFrame:
    """Generate predictions for recent events and forecasting scenarios."""
    
    context.log.info("Generating fatality predictions")
    
    # FIXED: First check what data is actually available
    conn = postgres.get_connection()
    # Check available date range in database
    date_check_query = """
    SELECT 
        MIN(event_date) as earliest_date,
        MAX(event_date) as latest_date,
        COUNT(*) as total_records
    FROM acled_events_no_delete 
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """
    
    date_info = pd.read_sql_query(date_check_query, conn)
    earliest_date = date_info.iloc[0]['earliest_date']
    latest_date = date_info.iloc[0]['latest_date']
    total_records = date_info.iloc[0]['total_records']
    
    context.log.info(f"Available data range: {earliest_date} to {latest_date} ({total_records:,} records)")
    
    if latest_date is None:
        context.log.error("No data found in database")
        return pd.DataFrame()
    
    end_date = min(datetime.now().date(), latest_date)
    start_date = max(
        end_date - timedelta(days=config.forecast_days_ahead),
        earliest_date
    )
    
    # If still no overlap, use the most recent available data
    if start_date > end_date:
        end_date = latest_date
        start_date = latest_date - timedelta(days=min(7, config.forecast_days_ahead))  # At least try last week
    
    context.log.info(f"Using adjusted date range: {start_date} to {end_date}")
    
    query = f"""
    SELECT 
        event_id_cnty,
        event_date,
        disorder_type,
        event_type,
        sub_event_type,
        actor1,
        actor2,
        inter1,
        inter2,
        interaction,
        admin1,
        admin2,
        admin3,
        latitude,
        longitude,
        fatalities
    FROM {postgres.table_name} 
    WHERE event_date >= %s 
        AND event_date <= %s
        AND latitude IS NOT NULL 
        AND longitude IS NOT NULL
    ORDER BY event_date
    """
    
    conn = postgres.get_connection()
    try:
        recent_df = pd.read_sql_query(query, conn, params=[start_date, end_date])
        
        if len(recent_df) == 0:
            context.log.warning("No recent data found for predictions")
            return pd.DataFrame()
        
        context.log.info(f"Making predictions for {len(recent_df)} recent events")
        
        df_processed, _ = prepare_features_for_model(
            recent_df,
            target_col='fatalities',
            fit_encoders=False,
            encoders=acled_trained_model['encoders']
        )
        
        # Handle feature column mismatch
        model = acled_trained_model['model']
        training_features = acled_trained_model['feature_columns']
        current_features = [col for col in df_processed.columns if col != 'fatalities']
        
        missing_features = set(training_features) - set(current_features)
        for feature in missing_features:
            df_processed[feature] = 0
        
        X_pred = df_processed[training_features].copy()
        
        for col in X_pred.columns:
            if X_pred[col].dtype == 'object':
                X_pred[col] = pd.to_numeric(X_pred[col], errors='coerce').fillna(0)
        
        predictions = model.predict(X_pred)
        
        results_df = recent_df[['event_id_cnty', 'event_date', 'admin1', 'admin2', 
                               'event_type', 'fatalities']].copy()
        results_df['predicted_fatalities'] = predictions
        results_df['prediction_error'] = results_df['fatalities'] - results_df['predicted_fatalities']
        results_df['absolute_error'] = np.abs(results_df['prediction_error'])
        results_df['percentage_error'] = np.where(results_df['fatalities'] > 0, 
                                                 results_df['absolute_error'] / results_df['fatalities'] * 100, 0)
        
        mae = results_df['absolute_error'].mean()
        rmse = np.sqrt((results_df['prediction_error'] ** 2).mean())
        mape = results_df['percentage_error'].mean()
        
        confidence_analysis = {
            'high_confidence_predictions': int((results_df['absolute_error'] <= 1).sum()),
            'medium_confidence_predictions': int(((results_df['absolute_error'] > 1) & (results_df['absolute_error'] <= 5)).sum()),
            'low_confidence_predictions': int((results_df['absolute_error'] > 5).sum()),
        }
        
        regional_performance = results_df.groupby('admin1').agg({
            'absolute_error': ['mean', 'count'],
            'predicted_fatalities': 'mean',
            'fatalities': 'mean'
        }).round(2)
        regional_performance.columns = ['avg_error', 'prediction_count', 'avg_predicted', 'avg_actual']
        regional_performance = regional_performance.reset_index().head(20)
        
        event_type_performance = results_df.groupby('event_type').agg({
            'absolute_error': 'mean',
            'predicted_fatalities': 'mean',
            'fatalities': 'mean'
        }).round(2).reset_index()
        
        def categorize_risk(fatalities):
            if fatalities >= 20: return 'Critical'
            elif fatalities >= 10: return 'High'
            elif fatalities >= 5: return 'Medium'
            else: return 'Low'
        
        results_df['predicted_risk_level'] = results_df['predicted_fatalities'].apply(categorize_risk)
        results_df['actual_risk_level'] = results_df['fatalities'].apply(categorize_risk)
        
        risk_accuracy = (results_df['predicted_risk_level'] == results_df['actual_risk_level']).mean()
        
        context.log.info(f"Prediction accuracy - MAE: {mae:.3f}, RMSE: {rmse:.3f}, MAPE: {mape:.1f}%")
        
        context.add_output_metadata({
            # Basic prediction metrics for plotting
            "prediction_records": len(results_df),
            "prediction_mae": float(mae),
            "prediction_rmse": float(rmse),
            "prediction_mape": float(mape),
            "risk_classification_accuracy": round(float(risk_accuracy) * 100, 1),
            
            # Prediction distribution for plotting
            "avg_predicted_fatalities": round(float(predictions.mean()), 2),
            "avg_actual_fatalities": round(float(results_df['fatalities'].mean()), 2),
            "max_predicted_fatalities": round(float(predictions.max()), 2),
            "max_actual_fatalities": int(results_df['fatalities'].max()),
            
            # Confidence metrics for plotting
            **confidence_analysis,
            "high_confidence_pct": round((confidence_analysis['high_confidence_predictions'] / len(results_df)) * 100, 1),
            
            # Error distribution for plotting
            "predictions_within_1": int((results_df['absolute_error'] <= 1).sum()),
            "predictions_within_5": int((results_df['absolute_error'] <= 5).sum()),
            "predictions_within_10": int((results_df['absolute_error'] <= 10).sum()),
            "large_errors": int((results_df['absolute_error'] > 10).sum()),
            
            # Bias detection for plotting
            "prediction_bias": round(float(results_df['prediction_error'].mean()), 3),
            "overestimation_rate": round(float((results_df['prediction_error'] < 0).mean() * 100), 1),
            "underestimation_rate": round(float((results_df['prediction_error'] > 0).mean() * 100), 1),
            
            # Risk level distribution for plotting
            "critical_risk_predicted": int((results_df['predicted_fatalities'] >= 20).sum()),
            "high_risk_predicted": int(((results_df['predicted_fatalities'] >= 10) & (results_df['predicted_fatalities'] < 20)).sum()),
            "medium_risk_predicted": int(((results_df['predicted_fatalities'] >= 5) & (results_df['predicted_fatalities'] < 10)).sum()),
            "low_risk_predicted": int((results_df['predicted_fatalities'] < 5).sum()),
            
            # Performance tables
            "regional_performance": dg.TableMetadataValue(
                records=[dg.TableRecord(row.to_dict()) for _, row in regional_performance.iterrows()],
                schema=dg.TableSchema([
                    dg.TableColumn(name="admin1", type="text"),
                    dg.TableColumn(name="avg_error", type="float"),
                    dg.TableColumn(name="prediction_count", type="int"),
                    dg.TableColumn(name="avg_predicted", type="float"),
                    dg.TableColumn(name="avg_actual", type="float"),
                ])
            ),
            
            "event_type_performance": dg.TableMetadataValue(
                records=[dg.TableRecord(row.to_dict()) for _, row in event_type_performance.iterrows()],
                schema=dg.TableSchema([
                    dg.TableColumn(name="event_type", type="text"),
                    dg.TableColumn(name="absolute_error", type="float"),
                    dg.TableColumn(name="predicted_fatalities", type="float"),
                    dg.TableColumn(name="fatalities", type="float"),
                ])
            ),
            
            # Date context
            "date_range_start": start_date.isoformat(),
            "date_range_end": end_date.isoformat(),
        })
        
        return results_df
        
    finally:
        conn.close()


@dg.asset(
    name="acled_monthly_report_with_ml",
    description="Generate enhanced monthly report with ML-based fatality forecasting - complete 3-page PDF",
    group_name="acled_reports",
    deps=['acled_daily_to_postgres', 'acled_trained_model', 'acled_fatality_predictions'],
    io_manager_key="reports_s3_io_manager",
)
def acled_monthly_report_with_ml(
    context: dg.AssetExecutionContext,
    config: ReportConfig,
    postgres: PostgreSQLResource,
    acled_trained_model: dict[str, Any],
    acled_fatality_predictions: pd.DataFrame,
) -> str:
    """Generate enhanced monthly ACLED report with ML forecasting insights as a 3-page PDF."""
    
    # Date range using config
    if config.end_date:
        end_date = datetime.strptime(config.end_date, '%Y-%m-%d').date()
    else:
        end_date = datetime.now().date()
    
    start_date = end_date - timedelta(days=config.days_back)
    
    context.log.info(f"Generating enhanced ML report for period: {start_date} to {end_date}")
    
    conn = postgres.get_connection()
    
    try:
        
        quality_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN event_type IS NULL OR event_type = '' THEN 1 END) as missing_event_type,
            COUNT(CASE WHEN country IS NULL OR country = '' THEN 1 END) as missing_country,
            COUNT(CASE WHEN event_date IS NULL THEN 1 END) as missing_date,
            COUNT(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 END) as missing_coordinates
        FROM acled_events_no_delete 
        WHERE event_date >= %s AND event_date <= %s
        """
        
        daily_counts_query = """
        SELECT event_date, COUNT(*) as event_count, 
               COALESCE(SUM(fatalities), 0) as total_fatalities
        FROM acled_events_no_delete 
        WHERE event_date >= %s AND event_date <= %s
        GROUP BY event_date 
        ORDER BY event_date
        """
        
        event_types_query = """
        SELECT event_type, COUNT(*) as count
        FROM acled_events_no_delete 
        WHERE event_date >= %s AND event_date <= %s
        GROUP BY event_type 
        ORDER BY count DESC
        LIMIT 10
        """
        
        region_query = """
        SELECT admin1, COUNT(*) as event_count
        FROM acled_events_no_delete 
        WHERE event_date >= %s AND event_date <= %s
          AND admin1 IS NOT NULL AND admin1 != ''
        GROUP BY admin1 
        ORDER BY event_count DESC
        """
        
        actor_query = """
        SELECT 
            COUNT(CASE WHEN actor1 IS NOT NULL AND actor1 != '' THEN 1 END) as actor1_present,
            COUNT(CASE WHEN actor2 IS NOT NULL AND actor2 != '' THEN 1 END) as actor2_present,
            COUNT(CASE WHEN disorder_type IS NOT NULL AND disorder_type != '' THEN 1 END) as disorder_type_present,
            COUNT(CASE WHEN civilian_targeting IS NOT NULL AND civilian_targeting != '' THEN 1 END) as civilian_targeting_present
        FROM acled_events_no_delete 
        WHERE event_date >= %s AND event_date <= %s
        """
        
        ukraine_region_query = """
        SELECT 
            admin1, 
            COUNT(*) as event_count,
            COALESCE(SUM(fatalities), 0) as total_fatalities,
            AVG(latitude) as avg_lat,
            AVG(longitude) as avg_lon
        FROM acled_events_no_delete 
        WHERE event_date >= %s AND event_date <= %s
          AND country = 'Ukraine'
          AND admin1 IS NOT NULL AND admin1 != ''
        GROUP BY admin1
        ORDER BY event_count DESC
        """
        
        ml_performance_query = """
        SELECT 
            DATE_TRUNC('week', event_date) as week,
            COUNT(*) as actual_events,
            AVG(fatalities) as avg_actual_fatalities,
            SUM(fatalities) as total_actual_fatalities
        FROM acled_events_no_delete 
        WHERE event_date >= %s AND event_date <= %s
        GROUP BY DATE_TRUNC('week', event_date)
        ORDER BY week
        """
        
        high_fatality_events_query = """
        SELECT 
            event_date,
            admin1,
            admin2,
            event_type,
            actor1,
            actor2,
            fatalities,
            location
        FROM acled_events_no_delete 
        WHERE event_date >= %s AND event_date <= %s
          AND fatalities >= 10
        ORDER BY fatalities DESC
        LIMIT 20
        """
        
        quality_df = pd.read_sql_query(quality_query, conn, params=[start_date, end_date])
        daily_df = pd.read_sql_query(daily_counts_query, conn, params=[start_date, end_date])
        events_df = pd.read_sql_query(event_types_query, conn, params=[start_date, end_date])
        region_df = pd.read_sql_query(region_query, conn, params=[start_date, end_date])
        actor_df = pd.read_sql_query(actor_query, conn, params=[start_date, end_date])
        ukraine_df = pd.read_sql_query(ukraine_region_query, conn, params=[start_date, end_date])
        
        weekly_actuals = pd.read_sql_query(ml_performance_query, conn, params=[start_date, end_date])
        high_fatality_df = pd.read_sql_query(high_fatality_events_query, conn, params=[start_date, end_date])
        
        
        total_records = quality_df.iloc[0]['total_records']
        critical_missing = (quality_df.iloc[0]['missing_event_type'] + 
                           quality_df.iloc[0]['missing_country'] + 
                           quality_df.iloc[0]['missing_date'] + 
                           quality_df.iloc[0]['missing_coordinates'])
        
        if total_records > 0:
            max_possible_missing = total_records * 4
            data_quality_score = ((max_possible_missing - critical_missing) / max_possible_missing) * 100
        else:
            data_quality_score = 0
        
        total_events = daily_df['event_count'].sum()
        total_fatalities = daily_df['total_fatalities'].sum()
        avg_daily_events = daily_df['event_count'].mean()
        

        ukraine_total_events = ukraine_df['event_count'].sum() if len(ukraine_df) > 0 else 0
        ukraine_total_fatalities = ukraine_df['total_fatalities'].sum() if len(ukraine_df) > 0 else 0
        ukraine_regions_with_events = len(ukraine_df) if len(ukraine_df) > 0 else 0
        
        if len(ukraine_df) > 0:
            most_active_region = ukraine_df.iloc[0]['admin1']
            most_active_count = ukraine_df.iloc[0]['event_count']
        else:
            most_active_region = "N/A"
            most_active_count = 0

        model_metrics = acled_trained_model['performance_metrics']
        prediction_mae = acled_fatality_predictions['absolute_error'].mean() if len(acled_fatality_predictions) > 0 else 0
        prediction_rmse = np.sqrt((acled_fatality_predictions['prediction_error'] ** 2).mean()) if len(acled_fatality_predictions) > 0 else 0
        
        
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
            temp_filepath = temp_file.name
        
        plt.style.use('seaborn-v0_8-whitegrid')
        plt.rcParams.update({
            'font.family': 'sans-serif',
            'font.sans-serif': ['Helvetica', 'Arial', 'DejaVu Sans'],
            'axes.spines.top': False,
            'axes.spines.right': False,
            'axes.grid': True,
            'axes.axisbelow': True,
            'grid.alpha': 0.2,
            'grid.linewidth': 0.5,
            'figure.facecolor': 'white',
            'axes.facecolor': 'white',
            'axes.edgecolor': '#CCCCCC',
            'axes.linewidth': 1,
            'xtick.color': '#333333',
            'ytick.color': '#333333',
        })
        
        # Color palette
        primary_color = '#2C3E50'
        secondary_color = '#3498DB'
        accent_color = '#E74C3C'
        success_color = '#27AE60'
        warning_color = '#F39C12'
        ml_color = '#9B59B6'
        text_color = '#34495E'
        darker_gray = '#BDC3C7'

        with tempfile.TemporaryDirectory() as temp_dir:
            page1_path = os.path.join(temp_dir, 'page1.pdf')
            page2_path = os.path.join(temp_dir, 'page2.pdf')
            page3_path = os.path.join(temp_dir, 'page3_ml.pdf')
            
            
            fig = plt.figure(figsize=(11, 8.5))
            fig.patch.set_facecolor('white')

            plt.subplots_adjust(hspace=0.3, wspace=0.3)

            header_ax = fig.add_axes([0, 0.92, 1, 0.08])
            header_ax.axis('off')
            
            gradient = np.linspace(0, 1, 256).reshape(1, -1)
            header_ax.imshow(gradient, extent=[0, 1, 0, 1], aspect='auto', 
                             cmap=plt.cm.Blues_r, alpha=0.15)
            
            header_ax.text(0.02, 0.5, 'ACLED INTELLIGENCE REPORT', fontsize=20, fontweight='bold', 
                          va='center', color=primary_color, fontfamily='sans-serif')
            header_ax.text(0.98, 0.5, f'{start_date.strftime("%B %d")} – {end_date.strftime("%B %d, %Y")}', 
                          fontsize=11, va='center', ha='right', color=text_color)
            
            header_ax.axhline(y=0.05, color=secondary_color, linewidth=3, alpha=0.8)
            
            metrics_y = 0.78
            card_height = 0.10
            
            metrics = [
                ('Total Events', f'{total_events:,}', secondary_color),
                ('Fatalities', f'{total_fatalities:,}', accent_color),
                ('Daily Average', f'{avg_daily_events:.0f}', warning_color),
                ('Data Quality', f'{data_quality_score:.1f}%', 
                 success_color if data_quality_score >= 90 else warning_color if data_quality_score >= 70 else accent_color)
            ]
            
            for i, (label, value, color) in enumerate(metrics):
                x_pos = 0.05 + i * 0.235
                card_ax = fig.add_axes([x_pos, metrics_y, 0.21, card_height])
                card_ax.axis('off')
                
                shadow = mpatches.FancyBboxPatch((0.02, 0.02), 0.96, 0.96,
                                                boxstyle="round,pad=0.02",
                                                facecolor='gray', alpha=0.1,
                                                transform=card_ax.transAxes)
                card_ax.add_patch(shadow)
                
                card = mpatches.FancyBboxPatch((0, 0), 1, 1,
                                              boxstyle="round,pad=0.02",
                                              facecolor='white',
                                              edgecolor=color, linewidth=2,
                                              transform=card_ax.transAxes)
                card_ax.add_patch(card)
                
                icon_bar = mpatches.Rectangle((0, 0), 0.03, 1,
                                             facecolor=color, alpha=0.8,
                                             transform=card_ax.transAxes)
                card_ax.add_patch(icon_bar)
                
                card_ax.text(0.5, 0.62, value, fontsize=18, fontweight='bold',
                            ha='center', va='center', color=color)
                card_ax.text(0.5, 0.28, label.upper(), fontsize=9, 
                            ha='center', va='center', color=text_color, alpha=0.7)
            
            ax1 = fig.add_axes([0.05, 0.45, 0.42, 0.28])
            
            actor_completeness = {
                'Primary Actor': (actor_df.iloc[0]['actor1_present'] / total_records * 100) if total_records > 0 else 0,
                'Secondary Actor': (actor_df.iloc[0]['actor2_present'] / total_records * 100) if total_records > 0 else 0,
                'Disorder Type': (actor_df.iloc[0]['disorder_type_present'] / total_records * 100) if total_records > 0 else 0,
                'Civilian Target': (actor_df.iloc[0]['civilian_targeting_present'] / total_records * 100) if total_records > 0 else 0,
            }
            
            y_pos = np.arange(len(actor_completeness))
            values = list(actor_completeness.values())
            colors_comp = [success_color if v >= 90 else warning_color if v >= 70 else accent_color for v in values]
            
            bars = ax1.barh(y_pos, values, color=colors_comp, alpha=0.7, height=0.6)
            ax1.set_yticks(y_pos)
            ax1.set_yticklabels(list(actor_completeness.keys()), fontsize=9, color=text_color)
            ax1.set_xlabel('Data Completeness (%)', fontsize=9, color=text_color)
            ax1.set_xlim(0, 105)
            
            for i, (bar, v) in enumerate(zip(bars, values)):
                ax1.text(v + 2, bar.get_y() + bar.get_height()/2, f'{v:.1f}%', 
                        va='center', fontsize=8, color=text_color, fontweight='bold')
            
            ax1.set_title('DATA COMPLETENESS ANALYSIS', fontweight='bold', fontsize=10, 
                         color=primary_color, pad=12, loc='left')
            ax1.grid(axis='x', alpha=0.2)
            ax1.spines['left'].set_linewidth(0.5)
            ax1.spines['bottom'].set_linewidth(0.5)
            
            ax2 = fig.add_axes([0.52, 0.45, 0.43, 0.28])
            daily_df['event_date'] = pd.to_datetime(daily_df['event_date'])
            daily_df['rolling_avg'] = daily_df['event_count'].rolling(window=7, min_periods=1).mean()
            
            ax2.fill_between(daily_df['event_date'], 0, daily_df['event_count'], 
                            color=secondary_color, alpha=0.15, label='Daily Events')
            ax2.plot(daily_df['event_date'], daily_df['event_count'], 
                    color=secondary_color, linewidth=1, alpha=0.5)
            ax2.plot(daily_df['event_date'], daily_df['rolling_avg'], 
                    color=primary_color, linewidth=2.5, label='7-Day Average')
            
            ax2.set_ylabel('Event Count', fontsize=9, color=text_color)
            ax2.set_title('DAILY ACTIVITY TRENDS', fontweight='bold', fontsize=10, 
                         color=primary_color, pad=12, loc='left')
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            ax2.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))  
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right', fontsize=7)
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
            ax2.legend(loc='upper right', fontsize=8, framealpha=0.9)
            ax2.grid(True, alpha=0.2)
            ax2.spines['left'].set_linewidth(0.5)
            ax2.spines['bottom'].set_linewidth(0.5)
            
            ax3 = fig.add_axes([0.05, 0.12, 0.42, 0.28])
            if len(events_df) > 0:
                event_labels = []
                for et in events_df['event_type']:
                    if len(et) > 25:
                        et = et[:22] + '...'
                    et = et.replace('Violence against civilians', 'Civilian Violence')
                    et = et.replace('Explosions/Remote violence', 'Explosions/Remote')
                    et = et.replace('Strategic developments', 'Strategic Dev.')
                    event_labels.append(et)
                
                colors_events = plt.cm.Blues(np.linspace(0.4, 0.8, len(events_df)))
                
                bars = ax3.barh(range(len(events_df)), events_df['count'], 
                               color=colors_events, alpha=0.8, height=0.7)
                ax3.set_yticks(range(len(events_df)))
                ax3.set_yticklabels(event_labels, fontsize=8, color=text_color)
                ax3.set_xlabel('Number of Events', fontsize=9, color=text_color)
                
                for i, v in enumerate(events_df['count']):
                    ax3.text(v + max(events_df['count']) * 0.01, i, f'{v:,}', 
                            va='center', fontsize=8, color=text_color, fontweight='bold')
            
            ax3.set_title('EVENT CLASSIFICATION', fontweight='bold', fontsize=10, 
                         color=primary_color, pad=1, loc='left')
            ax3.grid(axis='x', alpha=0.2)
            ax3.spines['left'].set_linewidth(0.5)
            ax3.spines['bottom'].set_linewidth(0.5)
            
            ax4 = fig.add_axes([0.52, 0.12, 0.43, 0.28])
            region_df_top10 = region_df.head(10)
            if len(region_df_top10) > 0:
                region_labels = []
                for region in region_df_top10['admin1']:
                    if len(region) > 20:
                        region = region[:17] + '...'
                    region_labels.append(region)
                
                colors_regions = plt.cm.Oranges(np.linspace(0.4, 0.8, len(region_df_top10)))
                
                bars = ax4.barh(range(len(region_df_top10)), region_df_top10['event_count'], 
                               color=colors_regions, alpha=0.8, height=0.7)
                ax4.set_yticks(range(len(region_df_top10)))
                ax4.set_yticklabels(region_labels, fontsize=8, color=text_color)
                ax4.set_xlabel('Number of Events', fontsize=9, color=text_color)
                
                for i, v in enumerate(region_df_top10['event_count']):
                    ax4.text(v + max(region_df_top10['event_count']) * 0.01, i, f'{v:,}', 
                            va='center', fontsize=8, color=text_color, fontweight='bold')
            
            ax4.set_title('GEOGRAPHIC DISTRIBUTION (TOP 10)', fontweight='bold', fontsize=10, 
                         color=primary_color, pad=1, loc='left')
            ax4.grid(axis='x', alpha=0.2)
            ax4.spines['left'].set_linewidth(0.5)
            ax4.spines['bottom'].set_linewidth(0.5)
            
            footer_ax = fig.add_axes([0, 0.02, 1, 0.04])
            footer_ax.axis('off')
            footer_ax.text(0.02, 0.5, f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")} UTC', 
                          fontsize=8, va='center', color=darker_gray, style='italic')
            footer_ax.text(0.98, 0.5, 'ACLED Data Intelligence Platform - Page 1 of 3', 
                          fontsize=8, va='center', ha='right', color=darker_gray, style='italic')
            footer_ax.axhline(y=0.9, color=darker_gray, linewidth=0.5, alpha=0.3)
            
            plt.savefig(page1_path, format='pdf', dpi=300, bbox_inches='tight', 
                       facecolor='white', edgecolor='none')
            plt.close()
            
            
            fig2 = plt.figure(figsize=(11, 8.5))
            fig2.patch.set_facecolor('white')
            
            header_ax2 = fig2.add_axes([0, 0.92, 1, 0.08])
            header_ax2.axis('off')
            gradient2 = np.linspace(0, 1, 256).reshape(1, -1)
            header_ax2.imshow(gradient2, extent=[0, 1, 0, 1], aspect='auto', 
                             cmap=plt.cm.Blues_r, alpha=0.15)
            header_ax2.text(0.02, 0.5, 'UKRAINE REGIONAL ANALYSIS', fontsize=20, fontweight='bold', 
                           va='center', color=primary_color)
            header_ax2.text(0.98, 0.5, f'{start_date.strftime("%B %d")} – {end_date.strftime("%B %d, %Y")}', 
                           fontsize=11, va='center', ha='right', color=text_color)
            header_ax2.axhline(y=0.05, color=secondary_color, linewidth=3, alpha=0.8)
            
            stats_y = 0.78
            stats_height = 0.10
            
            ukraine_stats = [
                ('Ukraine Events', f'{ukraine_total_events:,}', secondary_color),
                ('Ukraine Fatalities', f'{ukraine_total_fatalities:,}', accent_color),
                ('Active Regions', f'{ukraine_regions_with_events}', warning_color),
                ('Hottest Region', f'{most_active_region[:15]}', primary_color)
            ]
            
            for i, (label, value, color) in enumerate(ukraine_stats):
                x_pos = 0.05 + i * 0.235
                card_ax = fig2.add_axes([x_pos, stats_y, 0.21, stats_height])
                card_ax.axis('off')
                
                shadow = mpatches.FancyBboxPatch((0.02, 0.02), 0.96, 0.96,
                                                boxstyle="round,pad=0.02",
                                                facecolor='gray', alpha=0.1,
                                                transform=card_ax.transAxes)
                card_ax.add_patch(shadow)
                
                card = mpatches.FancyBboxPatch((0, 0), 1, 1,
                                              boxstyle="round,pad=0.02",
                                              facecolor='white',
                                              edgecolor=color, linewidth=2,
                                              transform=card_ax.transAxes)
                card_ax.add_patch(card)
                
                icon_bar = mpatches.Rectangle((0, 0), 0.03, 1,
                                             facecolor=color, alpha=0.8,
                                             transform=card_ax.transAxes)
                card_ax.add_patch(icon_bar)
                
                card_ax.text(0.5, 0.62, value, fontsize=16, fontweight='bold',
                            ha='center', va='center', color=color)
                card_ax.text(0.5, 0.28, label.upper(), fontsize=8, 
                            ha='center', va='center', color=text_color, alpha=0.7)
            
            if len(ukraine_df) > 0:
                ax_left = fig2.add_axes([0.05, 0.15, 0.44, 0.55])
                ukraine_df_top = ukraine_df.head(15)
                norm = plt.Normalize(vmin=ukraine_df_top['event_count'].min(), 
                                   vmax=ukraine_df_top['event_count'].max())
                colors_ukraine = plt.cm.YlOrRd(norm(ukraine_df_top['event_count']))
                ukraine_df_display = ukraine_df_top.sort_values('event_count', ascending=True)
                
                bars = ax_left.barh(range(len(ukraine_df_display)), 
                                   ukraine_df_display['event_count'],
                                   color=colors_ukraine[::-1], alpha=0.8, height=0.7)
                
                ax_left.set_yticks(range(len(ukraine_df_display)))
                ax_left.set_yticklabels(ukraine_df_display['admin1'], fontsize=9, color=text_color)
                ax_left.set_xlabel('Number of Events', fontsize=10, color=text_color)
                ax_left.set_title('TOP 15 REGIONS BY EVENT COUNT', fontweight='bold', 
                                fontsize=11, color=primary_color, pad=15)
                
                for i, (idx, row) in enumerate(ukraine_df_display.iterrows()):
                    ax_left.text(row['event_count'] + max(ukraine_df_display['event_count']) * 0.01, 
                               i, f"{row['event_count']:,}", 
                               va='center', fontsize=8, color=text_color, fontweight='bold')
                
                ax_left.grid(axis='x', alpha=0.2)
                ax_left.spines['top'].set_visible(False)
                ax_left.spines['right'].set_visible(False)
                
                ax_right = fig2.add_axes([0.54, 0.15, 0.41, 0.55])
                bubble_data = ukraine_df.head(20).copy()
                
                max_fatalities = bubble_data['total_fatalities'].max()
                if max_fatalities > 0:
                    bubble_sizes = 50 + (bubble_data['total_fatalities'] / max_fatalities) * 500
                else:
                    bubble_sizes = [100] * len(bubble_data)
                
                scatter = ax_right.scatter(bubble_data['event_count'], 
                                          range(len(bubble_data)),
                                          s=bubble_sizes,
                                          c=bubble_data['event_count'],
                                          cmap='YlOrRd',
                                          alpha=0.6,
                                          edgecolors='black',
                                          linewidth=0.5)
                
                for i, (idx, row) in enumerate(bubble_data.iterrows()):
                    ax_right.text(row['event_count'], i, 
                                row['admin1'][:12], 
                                ha='center', va='center',
                                fontsize=7, color='black',
                                fontweight='bold')
                
                ax_right.set_xlabel('Event Count', fontsize=10, color=text_color)
                ax_right.set_ylabel('Region Rank', fontsize=10, color=text_color)
                ax_right.set_title('EVENT INTENSITY & FATALITIES', fontweight='bold', 
                                 fontsize=11, color=primary_color, pad=15)
                ax_right.set_ylim(-1, len(bubble_data))
                ax_right.grid(True, alpha=0.2)
                ax_right.spines['top'].set_visible(False)
                ax_right.spines['right'].set_visible(False)
                
                legend_elements = [
                    plt.scatter([], [], s=100, c='gray', alpha=0.6, label='Low Fatalities'),
                    plt.scatter([], [], s=300, c='gray', alpha=0.6, label='Medium Fatalities'),
                    plt.scatter([], [], s=500, c='gray', alpha=0.6, label='High Fatalities')
                ]
                ax_right.legend(handles=legend_elements, loc='upper right', fontsize=8)
                
            else:
                no_data_ax = fig2.add_axes([0.1, 0.3, 0.8, 0.4])
                no_data_ax.text(0.5, 0.5, 'No Ukraine data available for this period', 
                              ha='center', va='center', fontsize=16, color=text_color)
                no_data_ax.axis('off')
            
            footer_ax2 = fig2.add_axes([0, 0.02, 1, 0.04])
            footer_ax2.axis('off')
            footer_ax2.text(0.02, 0.5, f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")} UTC', 
                           fontsize=8, va='center', color=darker_gray, style='italic')
            footer_ax2.text(0.98, 0.5, 'ACLED Data Intelligence Platform - Page 2 of 3', 
                           fontsize=8, va='center', ha='right', color=darker_gray, style='italic')
            footer_ax2.axhline(y=0.9, color=darker_gray, linewidth=0.5, alpha=0.3)
            
            plt.savefig(page2_path, format='pdf', dpi=300, bbox_inches='tight',
                       facecolor='white', edgecolor='none')
            plt.close()
            
            
            fig3 = plt.figure(figsize=(11, 8.5))
            fig3.patch.set_facecolor('white')
            
            header_ax3 = fig3.add_axes([0, 0.92, 1, 0.08])
            header_ax3.axis('off')
            gradient3 = np.linspace(0, 1, 256).reshape(1, -1)
            header_ax3.imshow(gradient3, extent=[0, 1, 0, 1], aspect='auto', 
                             cmap=plt.cm.Purples_r, alpha=0.15)
            header_ax3.text(0.02, 0.5, 'ML FORECASTING & MODEL INSIGHTS', fontsize=20, fontweight='bold', 
                           va='center', color=primary_color)
            
            training_date_str = acled_trained_model["training_date"][:10] if "training_date" in acled_trained_model else "Unknown"
            header_ax3.text(0.98, 0.5, f'Model: {acled_trained_model["model_version"]} | Trained: {training_date_str}', 
                           fontsize=10, va='center', ha='right', color=primary_color)
            header_ax3.axhline(y=0.05, color=ml_color, linewidth=3, alpha=0.8)
            
            # ML Model Performance Cards
            ml_stats_y = 0.78
            ml_stats_height = 0.10
            
            ml_cards = [
                ('Model RMSE', f"{model_metrics['test_rmse']:.2f}", ml_color),
                ('R² Score', f"{model_metrics['test_r2']:.3f}", success_color if model_metrics['test_r2'] > 0.5 else warning_color),
                ('Training Samples', f"{acled_trained_model['training_samples']:,}", warning_color),
                ('Features Used', f"{len(acled_trained_model['feature_columns'])}", accent_color)
            ]
            
            for i, (label, value, color) in enumerate(ml_cards):
                x_pos = 0.05 + i * 0.235
                card_ax = fig3.add_axes([x_pos, ml_stats_y, 0.21, ml_stats_height])
                card_ax.axis('off')
                
                # Card with shadow effect
                shadow = mpatches.FancyBboxPatch((0.02, 0.02), 0.96, 0.96,
                                                boxstyle="round,pad=0.02",
                                                facecolor='gray', alpha=0.1,
                                                transform=card_ax.transAxes)
                card_ax.add_patch(shadow)
                
                card = mpatches.FancyBboxPatch((0, 0), 1, 1,
                                              boxstyle="round,pad=0.02",
                                              facecolor='white',
                                              edgecolor=color, linewidth=2,
                                              transform=card_ax.transAxes)
                card_ax.add_patch(card)
                
                icon_bar = mpatches.Rectangle((0, 0), 0.03, 1,
                                             facecolor=color, alpha=0.8,
                                             transform=card_ax.transAxes)
                card_ax.add_patch(icon_bar)
                
                card_ax.text(0.5, 0.62, value, fontsize=16, fontweight='bold',
                            ha='center', va='center', color=color)
                card_ax.text(0.5, 0.28, label.upper(), fontsize=8, 
                            ha='center', va='center', color=text_color, alpha=0.7)
            
            # Model Feature Importance (left upper)
            ax_features = fig3.add_axes([0.05, 0.45, 0.42, 0.28])
            
            top_features = pd.DataFrame(acled_trained_model['feature_importance']).head(10)
            
            if len(top_features) > 0:
                # Create feature labels (clean up names)
                feature_labels = []
                for feature in top_features['feature']:
                    clean_name = feature.replace('_target_encoded', '').replace('_label_encoded', '')
                    clean_name = clean_name.replace('_', ' ').title()
                    if len(clean_name) > 20:
                        clean_name = clean_name[:17] + '...'
                    feature_labels.append(clean_name)
                
                colors_features = plt.cm.Purples(np.linspace(0.4, 0.8, len(top_features)))
                
                bars = ax_features.barh(range(len(top_features)), top_features['importance'], 
                                       color=colors_features, alpha=0.8, height=0.7)
                ax_features.set_yticks(range(len(top_features)))
                ax_features.set_yticklabels(feature_labels, fontsize=8, color=text_color)
                ax_features.set_xlabel('Feature Importance', fontsize=9, color=text_color)
                ax_features.set_title('TOP 10 PREDICTIVE FEATURES', fontweight='bold', fontsize=10, 
                                    color=primary_color, pad=12, loc='left')
                
                # Add importance values on bars
                for i, v in enumerate(top_features['importance']):
                    ax_features.text(v + max(top_features['importance']) * 0.01, i, f'{v:.3f}', 
                                   va='center', fontsize=7, color=text_color, fontweight='bold')
            else:
                ax_features.text(0.5, 0.5, 'Feature importance data not available', 
                               ha='center', va='center', fontsize=12, color=text_color)
                ax_features.set_title('TOP 10 PREDICTIVE FEATURES', fontweight='bold', fontsize=10, 
                                    color=primary_color, pad=12, loc='left')
            
            ax_features.grid(axis='x', alpha=0.2)
            ax_features.spines['left'].set_linewidth(0.5)
            ax_features.spines['bottom'].set_linewidth(0.5)
            
            # Risk Level Distribution (right upper) - Fixed version
            ax_risk_levels = fig3.add_axes([0.52, 0.45, 0.43, 0.28])

            if len(acled_fatality_predictions) > 0:
                # Categorize predictions into business-friendly risk levels
                def get_risk_level(fatalities):
                    if fatalities >= 20: return "Critical"
                    elif fatalities >= 10: return "High" 
                    elif fatalities >= 5: return "Moderate"
                    elif fatalities >= 1: return "Low"
                    else: return "Minimal"
                
                acled_fatality_predictions['risk_level'] = acled_fatality_predictions['predicted_fatalities'].apply(get_risk_level)
                risk_counts = acled_fatality_predictions['risk_level'].value_counts()
                
                risk_colors = {
                    'Critical': '#c62828', 'High': '#ef5350', 'Moderate': '#ff9800',
                    'Low': '#ffc107', 'Minimal': '#4caf50'
                }
                
                colors = [risk_colors.get(level, '#666') for level in risk_counts.index]
                
                # Improved donut chart with better label positioning
                wedges, texts, autotexts = ax_risk_levels.pie(
                    risk_counts.values, 
                    labels=None,  # Remove default labels to prevent overlap
                    colors=colors,
                    autopct='%1.0f%%',
                    startangle=90,
                    pctdistance=0.85,
                    textprops={'fontsize': 8, 'color': text_color, 'weight': 'bold'}
                )
                
                # Add custom legend instead of overlapping labels
                ax_risk_levels.legend(wedges, [f"{level}: {count}" for level, count in zip(risk_counts.index, risk_counts.values)],
                                    title="Risk Levels", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1),
                                    fontsize=8)
                
                # Center information
                centre_circle = plt.Circle((0,0), 0.70, fc='white')
                ax_risk_levels.add_artist(centre_circle)
                
                total_events = len(acled_fatality_predictions)
                high_risk_events = len(acled_fatality_predictions[acled_fatality_predictions['predicted_fatalities'] >= 10])
                
                ax_risk_levels.text(0, 0.1, f'{total_events}', ha='center', va='center', 
                                fontsize=16, fontweight='bold', color=primary_color)
                ax_risk_levels.text(0, -0.15, 'Events\nAnalyzed', ha='center', va='center', 
                                fontsize=9, color=text_color)
                
                ax_risk_levels.set_title('PREDICTED RISK DISTRIBUTION', fontweight='bold', fontsize=10, 
                                    color=primary_color, pad=12)

                # Regional Performance (bottom left) - Fixed version  
                if len(acled_fatality_predictions) > 0:
                    ax_regional = fig3.add_axes([0.05, 0.12, 0.42, 0.25])
                    
                    regional_performance = acled_fatality_predictions.groupby('admin1').agg({
                        'predicted_fatalities': 'sum',
                        'fatalities': 'count'
                    }).reset_index().head(10)  # Reduce to 10 to avoid crowding
                    regional_performance = regional_performance.sort_values('predicted_fatalities', ascending=True)
                    
                    if len(regional_performance) > 0:
                        colors_regional = plt.cm.Reds(np.linspace(0.3, 0.9, len(regional_performance)))
                        
                        bars = ax_regional.barh(range(len(regional_performance)), 
                                            regional_performance['predicted_fatalities'],
                                            color=colors_regional, alpha=0.8, height=0.6)  # Reduced height
                        
                        ax_regional.set_yticks(range(len(regional_performance)))
                        # Truncate region names more aggressively
                        region_labels = [r[:12] + '...' if len(r) > 12 else r for r in regional_performance['admin1']]
                        ax_regional.set_yticklabels(region_labels, fontsize=7, color=text_color)  # Smaller font
                        ax_regional.set_xlabel('Total Predicted Fatalities', fontsize=9, color=text_color)
                        ax_regional.set_title('TOP RISK REGIONS', fontweight='bold', 
                                            fontsize=10, color=primary_color, pad=12, loc='left')
                        
                        # Better positioned value labels
                        max_val = max(regional_performance['predicted_fatalities'])
                        for i, (fatalities, events) in enumerate(zip(regional_performance['predicted_fatalities'], 
                                                                regional_performance['fatalities'])):
                            # Position text better to avoid overlap
                            text_x = fatalities + max_val * 0.05
                            ax_regional.text(text_x, i, f'{fatalities:.0f}', va='center', fontsize=7, 
                                        color=text_color, fontweight='bold')
                        
                        ax_regional.grid(axis='x', alpha=0.2)
                        ax_regional.spines['left'].set_linewidth(0.5)
                        ax_regional.spines['bottom'].set_linewidth(0.5)
                    
                    # Monthly Trend Forecast (bottom right) - Fixed version
                    ax_trends = fig3.add_axes([0.52, 0.12, 0.43, 0.25])
                    
                    # Group by month for trend analysis
                    acled_fatality_predictions['event_month'] = pd.to_datetime(acled_fatality_predictions['event_date']).dt.to_period('M')
                    
                    monthly_trend = acled_fatality_predictions.groupby('event_month').agg({
                        'predicted_fatalities': 'sum',
                        'fatalities': 'sum'
                    }).round(1)
                    
                    monthly_trend = monthly_trend.reset_index()
                    monthly_trend['month_str'] = monthly_trend['event_month'].astype(str)
                    
                    if len(monthly_trend) >= 2:
                        months = range(len(monthly_trend))
                        
                        ax_trends.plot(months, monthly_trend['predicted_fatalities'], 
                                    marker='o', linewidth=3, markersize=5, color=ml_color, 
                                    label='Predicted')
                        
                        ax_trends.plot(months, monthly_trend['fatalities'], 
                                    marker='s', linewidth=2, markersize=4, color=accent_color, 
                                    linestyle='--', alpha=0.8, label='Actual')
                        
                        ax_trends.fill_between(months, monthly_trend['predicted_fatalities'], 
                                            alpha=0.2, color=ml_color)
                        
                        ax_trends.set_xticks(months[::max(1, len(months)//4)])  # Show fewer x-axis labels
                        selected_labels = [monthly_trend['month_str'].iloc[i][-5:] for i in range(0, len(months), max(1, len(months)//4))]
                        ax_trends.set_xticklabels(selected_labels, rotation=0, ha='center', fontsize=8)  # No rotation, smaller font
                        
                        ax_trends.set_ylabel('Total Fatalities', fontsize=9, color=text_color)
                        ax_trends.set_title('MONTHLY FATALITY TRENDS', fontweight='bold', fontsize=10, 
                                        color=primary_color, pad=12, loc='left')
                        
                        # Trend indicator with better positioning
                        if len(monthly_trend) >= 2:
                            last_two = monthly_trend['predicted_fatalities'].iloc[-2:]
                            if len(last_two) == 2:
                                trend = "↗" if last_two.iloc[1] > last_two.iloc[0] else "↘" if last_two.iloc[1] < last_two.iloc[0] else "→"
                                trend_color = '#c62828' if trend == "↗" else '#4caf50' if trend == "↘" else '#666'
                                
                                ax_trends.text(0.02, 0.95, f'Trend: {trend}', transform=ax_trends.transAxes,
                                            fontsize=10, ha='left', va='top', color=trend_color, fontweight='bold')
                        
                        ax_trends.legend(loc='upper right', fontsize=8)
                        ax_trends.grid(True, alpha=0.3)
                        ax_trends.spines['left'].set_linewidth(0.5)
                        ax_trends.spines['bottom'].set_linewidth(0.5)
                                
                    else:
                        ax_trends.text(0.5, 0.5, 'Insufficient data for trend analysis\n(Need multiple months)', 
                                    ha='center', va='center', fontsize=10, color=text_color)
                        ax_trends.set_title('FATALITY TRENDS BY MONTH', fontweight='bold', fontsize=10, 
                                        color=primary_color, pad=12, loc='left')
                    
                else:
                    # No predictions available - show placeholder
                    ax_regional = fig3.add_axes([0.05, 0.12, 0.9, 0.25])
                    ax_regional.text(0.5, 0.5, 'No recent prediction data available for analysis\n\nTo enable ML insights:\n1. Ensure model training completed successfully\n2. Run prediction asset on recent data\n3. Check data availability for the selected time period', 
                                ha='center', va='center', fontsize=12, color=text_color,
                                bbox=dict(boxstyle="round,pad=0.5", facecolor='#f8f9fa', alpha=0.8))
                    ax_regional.set_title('PREDICTION ANALYSIS PLACEHOLDER', fontweight='bold', 
                                        fontsize=10, color=primary_color, pad=12, loc='left')
                    ax_regional.axis('off')
                
                # Footer for page 3
                footer_ax3 = fig3.add_axes([0, 0.02, 1, 0.04])
                footer_ax3.axis('off')
                footer_ax3.text(0.02, 0.5, f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")} UTC | Model: {acled_trained_model["model_version"]}', 
                            fontsize=8, va='center', color=darker_gray, style='italic')
                footer_ax3.text(0.98, 0.5, 'ACLED Intelligence Platform - ML Insights - Page 3 of 3', 
                            fontsize=8, va='center', ha='right', color=darker_gray, style='italic')
                footer_ax3.axhline(y=0.9, color=darker_gray, linewidth=0.5, alpha=0.3)
                
                plt.savefig(page3_path, format='pdf', dpi=300, bbox_inches='tight',
                        facecolor='white', edgecolor='none')
                plt.close()
            
            # =========================================================================
            # MERGE ALL THREE PDFs INTO FINAL REPORT
            # =========================================================================
            
                merger = PdfMerger()
                merger.append(page1_path)
                merger.append(page2_path)
                merger.append(page3_path)
                merger.write(temp_filepath)
                merger.close()
        
        context.log.info(f"Enhanced 3-page ML report generated at: {temp_filepath}")
        
        # =============================================================================
        # COMPREHENSIVE METADATA
        # =============================================================================
        
        context.add_output_metadata({
            # Report info
            "report_type": "enhanced_ml_report",
            "pages": 3,
            "period_start": start_date.isoformat(),
            "period_end": end_date.isoformat(),
            
            # Original metrics
            "total_records": int(total_records),
            "total_events": int(total_events),
            "total_fatalities": int(total_fatalities),
            "data_quality_score": float(data_quality_score),
            "regions_covered": len(region_df),
            "event_types": len(events_df),
            "ukraine_total_events": int(ukraine_total_events),
            "ukraine_total_fatalities": int(ukraine_total_fatalities),
            "ukraine_regions_with_activity": int(ukraine_regions_with_events),
            
            # ML-specific metadata
            "model_version": acled_trained_model["model_version"],
            "model_training_date": acled_trained_model.get("training_date", "Unknown"),
            "model_performance": {
                "train_rmse": float(model_metrics['train_rmse']),
                "test_rmse": float(model_metrics['test_rmse']),
                "test_mae": float(model_metrics['test_mae']),
                "test_r2": float(model_metrics['test_r2']),
                "cv_score": float(model_metrics['cv_score'])
            },
            "predictions_analyzed": len(acled_fatality_predictions),
            "prediction_accuracy": {
                "mae": float(prediction_mae),
                "rmse": float(prediction_rmse)
            } if len(acled_fatality_predictions) > 0 else None,
            "high_fatality_events_detected": len(high_fatality_df),
            "training_features": len(acled_trained_model['feature_columns']),
            "training_samples": acled_trained_model['training_samples'],
            
            # Top feature importance for metadata
            "top_5_features": acled_trained_model['feature_importance'][:5] if len(acled_trained_model['feature_importance']) >= 5 else acled_trained_model['feature_importance']
        })
        
        return temp_filepath
        
    except Exception as e:
        context.log.error(f"Error generating enhanced ML report: {str(e)}")
        raise
    finally:
        conn.close()
