# Budget RL Model for Dynamic Budget Prediction

This directory contains the Reinforcement Learning model for predicting optimal budget allocation in the transaction processing system.

## Overview

The RL model learns to predict the optimal budget based on:
- **State**: Current load, throughput, backlog, write concern costs, utilization
- **Action**: Budget allocation (discretized values)
- **Reward**: Based on throughput gain, backlog reduction, and budget efficiency

## Architecture

### Model Type
Deep Q-Network (DQN) with experience replay

### Neural Network
- Input: 5-dimensional state vector
- Hidden layers: 128 -> 128 -> 64 neurons
- Output: Q-values for 7 budget actions
- Activation: ReLU
- Regularization: Batch normalization + Dropout

## Files

- `budget_rl_model.py`: Core RL model implementation (DQN agent, neural network)
- `train_budget_model.py`: Training pipeline (offline and online training)
- `model_server.py`: Flask API server for serving predictions
- `config.yaml`: Configuration parameters
- `requirements.txt`: Python dependencies

## Java Integration

- `MetricsCollector.java`: Collects training data from Java application
- `RLModelClient.java`: HTTP client to call Python model from Java

## Setup

### 1. Install Dependencies

```bash
cd analysis/models
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Collect Training Data

Run your Java application with `MetricsCollector` enabled to generate training data:

```java
// In your BatchProcessor or Server initialization
MetricsCollector metricsCollector = new MetricsCollector("rl_training_data.csv");

// After processing each batch
metricsCollector.recordBatchMetrics(
    currentLoad,
    currentBacklog,
    budgetUsed,
    result,
    writeConcernCosts,
    capacity
);
```

### 3. Train the Model

```bash
python train_budget_model.py
```

This will:
- Load data from CSV
- Train the DQN agent
- Save checkpoints every 50 episodes
- Generate training metrics plot

### 4. Start Model Server

```bash
python model_server.py
```

The server runs on `http://localhost:5000` and provides:
- `GET /health`: Health check
- `POST /predict`: Get budget prediction for single state
- `POST /predict_batch`: Get predictions for multiple states
- `POST /update_model`: Hot-reload model after retraining

### 5. Integrate with Java

```java
// Initialize RL model client
RLModelClient rlClient = new RLModelClient("http://localhost:5000");

// Check if model is available
if (rlClient.isHealthy()) {
    // Get budget prediction
    double predictedBudget = rlClient.predictBudget(
        currentLoad,
        currentThroughput,
        currentBacklog,
        writeConcernCosts,
        capacity
    );
    
    // Use predicted budget
    return processTransactionUnderutilised(batch, predictedBudget, allowUpgrades, writeConcernCosts);
} else {
    // Fallback to default budget
    return processTransactionUnderutilised(batch, 10000, allowUpgrades, writeConcernCosts);
}
```

## Workflow

### Phase 1: Data Collection
1. Run system with `MetricsCollector` enabled
2. Process transactions with various budgets
3. Collect state, action, reward tuples
4. Export to CSV

### Phase 2: Training
1. Load collected data
2. Train DQN agent with experience replay
3. Validate on test set
4. Save best model

### Phase 3: Deployment
1. Start model server
2. Configure Java client to call server
3. Replace hardcoded budget with model predictions
4. Monitor performance

### Phase 4: Continuous Improvement
1. Continue collecting data in production
2. Periodically retrain model
3. Hot-reload updated model
4. Compare performance metrics

## Hyperparameters

### State Normalization
- `current_load`: / 1000
- `current_throughput`: / 1000
- `current_backlog`: / 5000
- `avg_wc_cost`: / 100
- `utilization`: (already 0-1)

### Budget Actions (Discrete)
- 0, 2500, 5000, 7500, 10000, 15000, 20000

### Reward Function
```
reward = α × (throughput_gain) - β × (backlog_increase) - γ × (budget_cost) + profit_bonus

Where:
  α = 1.0 (throughput weight)
  β = 0.5 (backlog penalty)
  γ = 0.1 (budget penalty)
```

### Training Parameters
- Learning rate: 0.001
- Discount factor (γ): 0.99
- Epsilon decay: 0.995
- Batch size: 64
- Memory size: 10,000
- Target network update: every 100 steps

## API Examples

### Health Check
```bash
curl http://localhost:5000/health
```

Response:
```json
{
  "status": "healthy",
  "model_loaded": true
}
```

### Predict Budget
```bash
curl -X POST http://localhost:5000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "current_load": 850,
    "current_throughput": 750.5,
    "current_backlog": 200,
    "avg_wc_cost": 45.2,
    "utilization": 0.85
  }'
```

Response:
```json
{
  "predicted_budget": 10000.0,
  "action_index": 4,
  "confidence": 2.453,
  "all_q_values": [0.12, 0.45, 1.23, 1.89, 2.453, 2.01, 1.67],
  "budget_actions": [0, 2500, 5000, 7500, 10000, 15000, 20000]
}
```

## Monitoring

Key metrics to track:
1. **Throughput**: Transactions processed per second
2. **Backlog**: Number of pending transactions
3. **Budget utilization**: Actual budget used vs predicted
4. **Profit**: Total profit from transactions
5. **Upgrade rate**: Percentage of transactions upgraded

## Troubleshooting

### Model Server Not Starting
- Check if port 5000 is available
- Verify all dependencies are installed
- Check model checkpoint exists

### Poor Predictions
- Collect more training data (at least 10,000 samples)
- Adjust reward function weights
- Increase training episodes
- Check state normalization factors

### Java Integration Issues
- Verify server URL is correct
- Check network connectivity
- Enable fallback budget for safety
- Monitor timeout settings

## Future Enhancements

1. **Online Learning**: Update model in real-time
2. **Multi-objective RL**: Balance multiple goals
3. **Advanced Architectures**: Try PPO or A3C
4. **Feature Engineering**: Add more state features
5. **Adaptive Actions**: Dynamic budget ranges
6. **Ensemble Models**: Combine multiple models
