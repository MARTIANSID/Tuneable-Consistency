"""
Online Training RL Model with gRPC Server
Trains continuously every 20 seconds with incoming experiences
"""

import grpc
from concurrent import futures
import time
import threading
import numpy as np
from collections import deque
from datetime import datetime
import sys
import os

# Add models directory to path
sys.path.append(os.path.dirname(__file__))

from budget_rl_model import BudgetRLAgent, RewardCalculator

# Import generated gRPC code (will be generated from proto)
try:
    import RLBudget_pb2
    import RLBudget_pb2_grpc
except ImportError:
    print("Warning: gRPC proto files not generated yet. Run: python -m grpc_tools.protoc ...")


class OnlineRLTrainer:
    """
    Manages online training of the RL model
    Trains every 20 seconds with accumulated experiences
    """
    
    def __init__(self, agent, reward_calculator, training_interval=20):
        self.agent = agent
        self.reward_calculator = reward_calculator
        self.training_interval = training_interval  # seconds
        
        # Experience buffer for online training
        self.experience_buffer = deque(maxlen=5000)
        self.buffer_lock = threading.Lock()
        
        # Training statistics
        self.total_training_samples = 0
        self.total_predictions = 0
        self.recent_losses = deque(maxlen=100)
        self.recent_rewards = deque(maxlen=100)
        self.last_training_time = time.time()
        
        # Start background training thread
        self.training_thread = threading.Thread(target=self._training_loop, daemon=True)
        self.training_thread.start()
        
        print(f"Online RL Trainer initialized. Training every {training_interval} seconds.")
    
    def add_experience(self, experience_dict):
        """Add new experience to the buffer"""
        with self.buffer_lock:
            self.experience_buffer.append(experience_dict)
    
    def _calculate_reward(self, experience):
        """Calculate reward from experience"""
        # Extract metrics
        throughput_current = experience['next_throughput']
        throughput_previous = experience['current_throughput']
        backlog_current = experience['next_backlog']
        backlog_previous = experience['current_backlog']
        budget_used = experience['budget_used']
        profit = experience['profit']
        avg_wc_cost = experience.get('avg_wc_cost', 0.0)
        current_load_per_sec = experience.get('current_load', 0.0)
        
        # Use enhanced reward with load-awareness
        reward = self.reward_calculator.calculate_reward_with_load(
            avg_wc_cost,
            current_load_per_sec,
            throughput_current,
            throughput_previous,
            backlog_current,
            backlog_previous,
            budget_used,
            profit,
        )
        
        return reward
    
    def _training_loop(self):
        """Background thread that trains model periodically"""
        while True:
            try:
                time.sleep(self.training_interval)
                self._train_on_buffer()
            except Exception as e:
                print(f"Error in training loop: {e}")
    
    def _train_on_buffer(self):
        """Train model on accumulated experiences"""
        with self.buffer_lock:
            if len(self.experience_buffer) < self.agent.batch_size:
                print(f"Not enough samples for training: {len(self.experience_buffer)}/{self.agent.batch_size}")
                return
            
            # Copy buffer for training
            experiences = list(self.experience_buffer)
        
        print(f"\n{'='*60}")
        print(f"Starting training at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Training on {len(experiences)} experiences")
        print(f"{'='*60}")
        
        training_losses = []
        training_rewards = []
        
        # Train on all experiences
        for exp in experiences:
            # Create state vectors
            state = [
                exp['current_load'],
                exp['current_throughput'],
                exp['current_backlog'],
                exp['avg_wc_cost'],
                exp['utilization']
            ]
            
            next_state = [
                exp['next_load'],
                exp['next_throughput'],
                exp['next_backlog'],
                exp['next_avg_wc_cost'],
                exp['next_utilization']
            ]
            
            # Calculate reward
            reward = self._calculate_reward(exp)
            training_rewards.append(reward)
            
            # Map budget to action
            budget_used = exp['budget_used']
            action = self._budget_to_action_idx(budget_used)
            
            # Store in replay memory
            done = False  # Continuous task
            self.agent.remember(state, action, reward, next_state, done)
            
            # Train
            loss = self.agent.replay()
            if loss is not None:
                training_losses.append(loss)
        
        # Update statistics
        avg_loss = np.mean(training_losses) if training_losses else 0
        avg_reward = np.mean(training_rewards) if training_rewards else 0
        
        with self.buffer_lock:
            self.recent_losses.extend(training_losses)
            self.recent_rewards.extend(training_rewards)
            self.total_training_samples += len(experiences)
            self.last_training_time = time.time()
        
        print(f"Training completed!")
        print(f"  Samples processed: {len(experiences)}")
        print(f"  Average loss: {avg_loss:.4f}")
        print(f"  Average reward: {avg_reward:.4f}")
        print(f"  Epsilon: {self.agent.epsilon:.4f}")
        print(f"  Total training samples: {self.total_training_samples}")
        print(f"{'='*60}\n")
        
        # Save checkpoint
        self._save_checkpoint()
    
    def _budget_to_action_idx(self, budget):
        """Map actual budget value to closest action index"""
        actions = self.agent.BUDGET_ACTIONS
        closest_idx = min(range(len(actions)), key=lambda i: abs(actions[i] - budget))
        return closest_idx
    
    def _save_checkpoint(self):
        """Save model checkpoint"""
        try:
            checkpoint_dir = "./checkpoints"
            checkpoint_path = f"{checkpoint_dir}/budget_model_online.pth"
            # Ensure directory exists
            os.makedirs(checkpoint_dir, exist_ok=True)
            self.agent.save(checkpoint_path)
            print(f"Checkpoint saved: {checkpoint_path}")
        except Exception as e:
            print(f"Error saving checkpoint: {e}")
    
    def get_stats(self):
        """Get training statistics"""
        with self.buffer_lock:
            return {
                'total_predictions': self.total_predictions,
                'total_training_samples': self.total_training_samples,
                'avg_loss': np.mean(list(self.recent_losses)) if self.recent_losses else 0,
                'avg_reward': np.mean(list(self.recent_rewards)) if self.recent_rewards else 0,
                'last_training_time': self.last_training_time,
                'buffer_size': len(self.experience_buffer),
                'epsilon': self.agent.epsilon
            }


class RLBudgetServicer:
    """gRPC Service Implementation"""
    
    def __init__(self, trainer):
        self.trainer = trainer
    
    def PredictBudget(self, request, context):
        """Handle budget prediction request"""
        try:
            # Create state from request
            state = [
                request.current_load,
                request.current_throughput,
                request.current_backlog,
                request.avg_wc_cost,
                request.utilization
            ]
            
            # Get prediction
            action_idx, predicted_budget = self.trainer.agent.get_action(state, training=False)
            
            # Get confidence (Q-value)
            import torch
            state_tensor = torch.FloatTensor(
                self.trainer.agent.normalize_state(state)
            ).unsqueeze(0).to(self.trainer.agent.device)
            
            # Ensure eval mode for single-sample inference with BatchNorm
            self.trainer.agent.q_network.eval()
            with torch.no_grad():
                q_values = self.trainer.agent.q_network(state_tensor)
                confidence = q_values[0][action_idx].item()
            # Restore training mode for subsequent training steps
            self.trainer.agent.q_network.train()
            
            # Update stats
            self.trainer.total_predictions += 1
            
            # Return response
            return RLBudget_pb2.BudgetResponse(
                predicted_budget=float(predicted_budget),
                action_index=int(action_idx),
                confidence=float(confidence),
                success=True,
                error_message=""
            )
        
        except Exception as e:
            print(f"Error in PredictBudget: {e}")
            return RLBudget_pb2.BudgetResponse(
                predicted_budget=10000.0,  # Fallback
                action_index=-1,
                confidence=0.0,
                success=False,
                error_message=str(e)
            )
    
    def RecordExperience(self, request, context):
        """Handle experience recording for online learning"""
        try:
            # Convert request to experience dictionary
            experience = {
                'current_load': request.current_load,
                'current_throughput': request.current_throughput,
                'current_backlog': request.current_backlog,
                'avg_wc_cost': request.avg_wc_cost,
                'utilization': request.utilization,
                'budget_used': request.budget_used,
                'profit': request.profit,
                'transactions_processed': request.transactions_processed,
                'transactions_upgraded': request.transactions_upgraded,
                'next_load': request.next_load,
                'next_throughput': request.next_throughput,
                'next_backlog': request.next_backlog,
                'next_avg_wc_cost': request.next_avg_wc_cost,
                'next_utilization': request.next_utilization,
                'timestamp': request.timestamp
            }
            
            # Add to training buffer
            self.trainer.add_experience(experience)
            
            return RLBudget_pb2.AckResponse(
                success=True,
                message=f"Experience recorded. Buffer size: {len(self.trainer.experience_buffer)}"
            )
        
        except Exception as e:
            print(f"Error in RecordExperience: {e}")
            return RLBudget_pb2.AckResponse(
                success=False,
                message=str(e)
            )
    
    def HealthCheck(self, request, context):
        """Handle health check request"""
        stats = self.trainer.get_stats()
        
        return RLBudget_pb2.HealthResponse(
            is_healthy=True,
            model_loaded=True,
            training_samples_count=stats['total_training_samples'],
            epsilon=float(stats['epsilon']),
            training_steps=self.trainer.agent.steps
        )
    
    def GetModelStats(self, request, context):
        """Handle stats request"""
        stats = self.trainer.get_stats()
        
        return RLBudget_pb2.StatsResponse(
            total_predictions=stats['total_predictions'],
            total_training_samples=stats['total_training_samples'],
            avg_loss=float(stats['avg_loss']),
            avg_reward=float(stats['avg_reward']),
            last_training_time=int(stats['last_training_time'])
        )


def serve(port=50051, training_interval=20):
    """Start gRPC server with online training"""
    
    print("=" * 70)
    print("  RL Budget Prediction Server (Online Training)")
    print("=" * 70)
    print(f"  Port: {port}")
    print(f"  Training Interval: {training_interval} seconds")
    print("=" * 70)
    
    # Initialize RL agent
    print("\nInitializing RL agent...")
    agent = BudgetRLAgent(
        state_dim=5,
        action_dim=7,
        learning_rate=0.001,
        gamma=0.99,
        epsilon_start=0.3,  # Lower initial exploration for online learning
        epsilon_end=0.01,
        epsilon_decay=0.999,
        memory_size=10000,
        batch_size=32  # Smaller batch for frequent updates
    )
    
    # Try to load existing model
    checkpoint_path = "./checkpoints/budget_model_online.pth"
    if os.path.exists(checkpoint_path):
        try:
            agent.load(checkpoint_path)
            print(f"✓ Loaded existing model from {checkpoint_path}")
        except Exception as e:
            print(f"⚠ Could not load model: {e}")
            print("  Starting with fresh model")
    else:
        print("  Starting with fresh model")
    
    # Initialize reward calculator
    reward_calculator = RewardCalculator(alpha=1.0, beta=0.5, gamma=0.1)
    
    # Initialize online trainer
    trainer = OnlineRLTrainer(agent, reward_calculator, training_interval)
    
    # Create gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Add servicer
    servicer = RLBudgetServicer(trainer)
    RLBudget_pb2_grpc.add_RLBudgetServiceServicer_to_server(servicer, server)
    
    # Start server
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    
    print(f"\n✓ Server started on port {port}")
    print(f"✓ Online training enabled (every {training_interval}s)")
    print("\nWaiting for requests...\n")
    
    try:
        while True:
            time.sleep(86400)  # Keep server alive
    except KeyboardInterrupt:
        print("\nShutting down server...")
        server.stop(0)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='RL Budget Prediction gRPC Server')
    parser.add_argument('--port', type=int, default=50051, help='Server port')
    parser.add_argument('--training-interval', type=int, default=120, 
                       help='Training interval in seconds')
    
    args = parser.parse_args()
    
    serve(port=args.port, training_interval=args.training_interval)
