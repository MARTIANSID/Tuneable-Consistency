"""
Reinforcement Learning Model for Dynamic Budget Prediction
Uses Deep Q-Network (DQN) to learn optimal budget allocation
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
from collections import deque
import random


class BudgetDQN(nn.Module):
    """
    Deep Q-Network for budget prediction
    
    Input Features (State):
    - current_load: number of transactions in current batch
    - current_throughput: transactions processed per second
    - current_backlog: number of transactions waiting
    - avg_write_concern_cost: average cost across all write concern levels
    - system_utilization: ratio of current_load to capacity
    
    Output:
    - Q-values for each budget action (discretized budget values)
    """
    
    def __init__(self, state_dim=5, action_dim=7, hidden_dim=128):
        super(BudgetDQN, self).__init__()
        
        # Neural network architecture
        self.fc1 = nn.Linear(state_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, hidden_dim)
        self.fc3 = nn.Linear(hidden_dim, hidden_dim // 2)
        self.fc4 = nn.Linear(hidden_dim // 2, action_dim)
        
        # Batch normalization for stable training
        self.bn1 = nn.BatchNorm1d(hidden_dim)
        self.bn2 = nn.BatchNorm1d(hidden_dim)
        self.dropout = nn.Dropout(0.2)
        
    def forward(self, x):
        """Forward pass through the network"""
        x = F.relu(self.bn1(self.fc1(x)))
        x = self.dropout(x)
        x = F.relu(self.bn2(self.fc2(x)))
        x = self.dropout(x)
        x = F.relu(self.fc3(x))
        x = self.fc4(x)
        return x


class BudgetRLAgent:
    """
    DQN Agent for learning optimal budget allocation
    """
    
    # Discretized budget actions (in cost units)
    BUDGET_ACTIONS = [0, 50, 100, 150, 200, 250, 300]
    
    def __init__(
        self,
        state_dim=5,
        action_dim=len(BUDGET_ACTIONS),
        learning_rate=0.001,
        gamma=0.99,
        epsilon_start=1.0,
        epsilon_end=0.01,
        epsilon_decay=0.995,
        memory_size=10000,
        batch_size=64,
        target_update_freq=100
    ):
        self.state_dim = state_dim
        self.action_dim = action_dim
        self.gamma = gamma  # Discount factor
        self.epsilon = epsilon_start  # Exploration rate
        self.epsilon_end = epsilon_end
        self.epsilon_decay = epsilon_decay
        self.batch_size = batch_size
        self.target_update_freq = target_update_freq
        self.steps = 0
        
        # Replay memory
        self.memory = deque(maxlen=memory_size)
        
        # Q-network and target network
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.q_network = BudgetDQN(state_dim, action_dim).to(self.device)
        self.target_network = BudgetDQN(state_dim, action_dim).to(self.device)
        self.target_network.load_state_dict(self.q_network.state_dict())
        
        # Optimizer
        self.optimizer = torch.optim.Adam(self.q_network.parameters(), lr=learning_rate)
        self.loss_fn = nn.MSELoss()
        
    def normalize_state(self, state):
        """Normalize state features for better training stability"""
        # Expected state: [current_load (txns/sec), throughput, backlog, avg_wc_cost, incoming_load]
        state = np.array(state, dtype=np.float32)
        
        # Apply normalization (you should calculate these from your data)
        # For now, using reasonable ranges
        # Adjust normalization for last feature now representing incoming load per second
        normalization_factors = np.array([10000.0, 10000.0, 5000.0, 100.0, 10000.0])
        normalized = state / normalization_factors
        
        return normalized
    
    def get_action(self, state, training=True):
        """
        Select action using epsilon-greedy policy
        
        Args:
            state: current system state
            training: if True, use epsilon-greedy; if False, use greedy
        
        Returns:
            action_idx: index of selected action
            budget: actual budget value
        """
        if training and random.random() < self.epsilon:
            # Exploration: random action
            action_idx = random.randrange(self.action_dim)
        else:
            # Exploitation: best action according to Q-network
            state_tensor = torch.FloatTensor(self.normalize_state(state)).unsqueeze(0).to(self.device)
            self.q_network.eval()  # Set to evaluation mode for inference
            with torch.no_grad():
                q_values = self.q_network(state_tensor)
                action_idx = q_values.argmax().item()
            self.q_network.train()  # Set back to training mode
        
        budget = self.BUDGET_ACTIONS[action_idx]
        return action_idx, budget
    
    def remember(self, state, action, reward, next_state, done):
        """Store experience in replay memory"""
        self.memory.append((state, action, reward, next_state, done))
    
    def replay(self):
        """
        Train the Q-network using experience replay
        """
        if len(self.memory) < self.batch_size:
            return None
        
        # Sample random batch from memory
        batch = random.sample(self.memory, self.batch_size)
        
        states, actions, rewards, next_states, dones = zip(*batch)
        
        # Convert to tensors
        states = torch.FloatTensor(np.array([self.normalize_state(s) for s in states])).to(self.device)
        actions = torch.LongTensor(actions).to(self.device)
        rewards = torch.FloatTensor(rewards).to(self.device)
        next_states = torch.FloatTensor(np.array([self.normalize_state(s) for s in next_states])).to(self.device)
        dones = torch.FloatTensor(dones).to(self.device)
        
        # Current Q-values
        current_q = self.q_network(states).gather(1, actions.unsqueeze(1)).squeeze(1)
        
        # Target Q-values (using target network)
        with torch.no_grad():
            next_q = self.target_network(next_states).max(1)[0]
            target_q = rewards + (1 - dones) * self.gamma * next_q
        
        # Compute loss and update
        loss = self.loss_fn(current_q, target_q)
        
        self.optimizer.zero_grad()
        loss.backward()
        torch.nn.utils.clip_grad_norm_(self.q_network.parameters(), 1.0)
        self.optimizer.step()
        
        # Update target network periodically
        self.steps += 1
        if self.steps % self.target_update_freq == 0:
            self.target_network.load_state_dict(self.q_network.state_dict())
        
        # Decay epsilon
        self.epsilon = max(self.epsilon_end, self.epsilon * self.epsilon_decay)
        
        return loss.item()
    
    def save(self, filepath):
        """Save model checkpoint"""
        torch.save({
            'q_network_state_dict': self.q_network.state_dict(),
            'target_network_state_dict': self.target_network.state_dict(),
            'optimizer_state_dict': self.optimizer.state_dict(),
            'epsilon': self.epsilon,
            'steps': self.steps
        }, filepath)
        print(f"Model saved to {filepath}")
    
    def load(self, filepath):
        """Load model checkpoint"""
        checkpoint = torch.load(filepath, map_location=self.device)
        self.q_network.load_state_dict(checkpoint['q_network_state_dict'])
        self.target_network.load_state_dict(checkpoint['target_network_state_dict'])
        self.optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
        self.epsilon = checkpoint['epsilon']
        self.steps = checkpoint['steps']
        print(f"Model loaded from {filepath}")


class RewardCalculator:
    """
    Calculate reward for the RL agent based on system performance
    Emphasizes maintaining MIN_TPS = 3000 as critical requirement
    """
    
    def __init__(self, min_tps=3000, alpha=5.0, beta=2.0, gamma=0.3, delta=10.0):
        """
        Args:
            min_tps: minimum required throughput (3000 TPS)
            alpha: weight for throughput gain (increased for importance)
            beta: weight for backlog penalty
            gamma: weight for budget cost penalty (reduced to allow necessary spending)
            delta: weight for MIN_TPS violation penalty (very high!)
        """
        self.min_tps = min_tps
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma
        self.delta = delta  # Heavy penalty for failing MIN_TPS
        
    def calculate_reward(
        self,
        throughput_current,
        throughput_previous,
        backlog_current,
        backlog_previous,
        budget_used,
        profit_gained
    ):
        """
        Reward with strong emphasis on maintaining MIN_TPS = 3000
        
        Priority order:
        1. MUST maintain throughput >= MIN_TPS (heavy penalty if violated)
        2. Maximize throughput above MIN_TPS
        3. Minimize backlog
        4. Maximize profit
        5. Minimize budget (but allow spending to meet MIN_TPS)
        
        Positive rewards for:
        - High throughput (especially above MIN_TPS)
        - Decreased backlog
        - Higher profit
        
        Negative rewards for:
        - Throughput below MIN_TPS (SEVERE PENALTY)
        - Increased backlog
        - Excessive budget usage
        """
        
        # 1. MIN_TPS Violation Penalty (CRITICAL - highest weight)
        min_tps_penalty = 0.0
        if throughput_current < self.min_tps:
            # Severe penalty for falling below MIN_TPS
            # Penalty scales with how far below threshold we are
            deficit_ratio = (self.min_tps - throughput_current) / self.min_tps
            min_tps_penalty = self.delta * deficit_ratio
            # Extra penalty if throughput is decreasing while below MIN_TPS
            if throughput_current < throughput_previous:
                min_tps_penalty *= 1.5
        
        # 2. Throughput Reward (high weight)
        # Reward for being above MIN_TPS and improving
        if throughput_current >= self.min_tps:
            # Bonus for exceeding MIN_TPS
            excess_ratio = (throughput_current - self.min_tps) / self.min_tps
            throughput_reward = self.alpha * (1.0 + excess_ratio)
            
            # Additional reward for improvement
            if throughput_current > throughput_previous:
                improvement = (throughput_current - throughput_previous) / max(throughput_previous, 1.0)
                throughput_reward += self.alpha * improvement
        else:
            # Below MIN_TPS: only reward if improving
            if throughput_current > throughput_previous:
                improvement = (throughput_current - throughput_previous) / max(throughput_previous, 1.0)
                throughput_reward = self.alpha * improvement * 0.5  # reduced reward
            else:
                throughput_reward = 0.0
        
        # 3. Backlog component - penalty for increase
        backlog_penalty = 0.0
        if backlog_current > backlog_previous:
            backlog_increase = (backlog_current - backlog_previous) / max(backlog_previous, 1.0)
            backlog_penalty = self.beta * backlog_increase
        elif backlog_current < backlog_previous:
            # Small reward for reducing backlog
            backlog_decrease = (backlog_previous - backlog_current) / max(backlog_previous, 1.0)
            backlog_penalty = -self.beta * backlog_decrease * 0.3  # negative penalty = reward
        
        # 4. Budget cost penalty (allow necessary spending for MIN_TPS)
        # Base cost penalty
        if throughput_current >= self.min_tps:
            budget_penalty = self.gamma * (budget_used / 20000.0)
        else:
            budget_penalty = self.gamma * (budget_used / 20000.0) * 0.3
        
        # 5. Profit bonus
        profit_bonus = profit_gained / 1000.0
        
        # Total reward with MIN_TPS as top priority
        reward = (
            throughput_reward 
            - min_tps_penalty      # SEVERE penalty for violating MIN_TPS
            - backlog_penalty 
            - budget_penalty 
            + profit_bonus
        )
        
        return reward

    def calculate_reward_with_load(
        self,
        avg_wc_cost,
        current_load_per_sec,
        throughput_current,
        throughput_previous,
        backlog_current,
        backlog_previous,
        budget_used,
        profit_gained
    ):
        """
        Enhanced reward that penalizes over-budgeting relative to incoming load
        and average write concern cost.

        Adds an efficiency penalty when budget_used is much larger than the
        estimated needed budget for current load.
        """
        # Reuse base components
        base_reward = self.calculate_reward(
            throughput_current,
            throughput_previous,
            backlog_current,
            backlog_previous,
            budget_used,
            profit_gained,
        )

        # Estimate needed budget per second based on incoming load and avg cost
        # Heuristic: assume we should roughly budget for current_load_per_sec at avg_wc_cost
        # Scale by 1.2 to allow some headroom
        estimated_needed = max(0.0, avg_wc_cost * current_load_per_sec * 1.2)

        if estimated_needed > 0.0:
            over_budget_ratio = max(0.0, (budget_used - estimated_needed) / estimated_needed)
            # Penalize over-budgeting more aggressively when throughput is already healthy
            efficiency_gamma = self.gamma * (1.5 if throughput_current >= self.min_tps else 0.7)
            efficiency_penalty = efficiency_gamma * over_budget_ratio
        else:
            efficiency_penalty = 0.0

        return base_reward - efficiency_penalty


def create_state(current_load, current_throughput, current_backlog, write_concern_costs, capacity):
    """
    Create state vector from system metrics
    
    Args:
        current_load: number of transactions in current batch
        current_throughput: recent throughput (transactions/sec)
        current_backlog: number of transactions waiting
        write_concern_costs: dict mapping write concern level to cost
        capacity: system capacity (e.g., number of tokens)
    
    Returns:
        state: numpy array of features
    """
    # Calculate average write concern cost
    avg_wc_cost = np.mean(list(write_concern_costs.values())) if write_concern_costs else 0
    
    # Calculate system utilization
    utilization = current_load / max(capacity, 1.0)
    
    state = [
        float(current_load),
        float(current_throughput),
        float(current_backlog),
        float(avg_wc_cost),
        float(utilization)
    ]
    
    return state
