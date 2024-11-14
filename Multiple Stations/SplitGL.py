import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from xgboost import XGBRegressor
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Bidirectional, Dense, Dropout
from tensorflow.keras.optimizers import Adam
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt

# Load and preprocess the datasets
train_data = pd.read_excel('Train_Data.xlsx')  # Replace with your training data file name
test_data = pd.read_excel('Test_Data.xlsx')  # Replace with your testing data file name

# Drop unnecessary columns in both datasets
train_data = train_data.drop(columns=['Date'])
test_data = test_data.drop(columns=['Date'])

# Create lag features for 'S10' in the training data
def create_lag_features(data, column, n_lags=3):
    for lag in range(1, n_lags + 1):
        data[f'{column}_lag{lag}'] = data[column].shift(lag)
    return data

train_data = create_lag_features(train_data, 'S10', n_lags=3).dropna()
X_train = train_data.drop(columns=['S10']).values
y_train = train_data['S10'].values

# Create lag features for 'S10' in the testing data
test_data = create_lag_features(test_data, 'S10', n_lags=3).dropna()
X_test = test_data.drop(columns=['S10']).values
y_test = test_data['S10'].values

# Scale target variable (y)
scaler_y = MinMaxScaler()
y_train_scaled = scaler_y.fit_transform(y_train.reshape(-1, 1))
y_test_scaled = scaler_y.transform(y_test.reshape(-1, 1))

# Reshape for RNN input
X_train_rnn = X_train.reshape((X_train.shape[0], 1, X_train.shape[1]))
X_test_rnn = X_test.reshape((X_test.shape[0], 1, X_test.shape[1]))

# Custom function to train LSTM and BiLSTM
def build_and_train_rnn_model(model_type, X_train, y_train, X_test):
    model = Sequential()
    if model_type == "LSTM":
        model.add(LSTM(200, return_sequences=True, activation='tanh', input_shape=(X_train.shape[1], X_train.shape[2])))
    elif model_type == "BiLSTM":
        model.add(Bidirectional(LSTM(200, return_sequences=True, activation='tanh'), input_shape=(X_train.shape[1], X_train.shape[2])))
    model.add(Dropout(0.3))
    model.add(LSTM(100, activation='tanh'))
    model.add(Dropout(0.3))
    model.add(Dense(1))  # Output layer for regression
    model.compile(optimizer=Adam(learning_rate=0.0005), loss='mean_squared_error')
    model.fit(X_train, y_train, epochs=300, batch_size=16, validation_split=0.1, verbose=0)
    return scaler_y.inverse_transform(model.predict(X_test).flatten().reshape(-1, 1)).flatten()

from sklearn.model_selection import train_test_split

# Get predictions from both models for training and testing datasets
predictions_lstm_train = build_and_train_rnn_model("LSTM", X_train_rnn, y_train_scaled, X_train_rnn)
predictions_bilstm_train = build_and_train_rnn_model("BiLSTM", X_train_rnn, y_train_scaled, X_train_rnn)

predictions_lstm_test = build_and_train_rnn_model("LSTM", X_train_rnn, y_train_scaled, X_test_rnn)
predictions_bilstm_test = build_and_train_rnn_model("BiLSTM", X_train_rnn, y_train_scaled, X_test_rnn)

# Combine LSTM and BiLSTM predictions for training set for meta-model
stacked_predictions_train = np.column_stack([predictions_lstm_train, predictions_bilstm_train])
stacked_predictions_test = np.column_stack([predictions_lstm_test, predictions_bilstm_test])

# Split the stacked predictions and y_train into training and validation sets for the meta-model
stacked_X_train, stacked_X_val, stacked_y_train, stacked_y_val = train_test_split(stacked_predictions_train, y_train.flatten(), test_size=0.2, random_state=42)

# Train the meta-model on the training portion of the stacked predictions
meta_model = XGBRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
meta_model.fit(stacked_X_train, stacked_y_train)

# Make final predictions using the meta-model on the test set stacked predictions
final_predictions = meta_model.predict(stacked_predictions_test)

# Evaluate on the actual test set labels
mse = mean_squared_error(y_test.flatten(), final_predictions)
rmse = np.sqrt(mse)
r2 = r2_score(y_test.flatten(), final_predictions)

print("Stacked Model Results (LSTM + BiLSTM with XGBoost Meta-Model):")
print(f"Mean Squared Error (MSE): {mse}")
print(f"Root Mean Squared Error (RMSE): {rmse}")
print(f"R-Squared (R²): {r2}")

# Scatter plot of actual vs predicted values
plt.figure(figsize=(10, 6))
plt.scatter(y_test.flatten(), final_predictions, alpha=0.5, color='blue', label='Predicted vs. Actual')
plt.plot([min(y_test.flatten()), max(y_test.flatten())], [min(y_test.flatten()), max(y_test.flatten())], color='red', lw=2, label='Perfect Prediction Line')
plt.xlabel('Actual Values')
plt.ylabel('Predicted Values')
plt.title('Actual vs Predicted Values for Stacked Model')
plt.legend()
plt.grid(True)
plt.show()