import torch 
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
import numpy as np
import pandas as pd
import argparse
import logging
import os
from datetime import datetime
import sys
from typing import Union, List, Tuple
import json
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix, classification_report
import matplotlib.pyplot as plt
import seaborn as sns

# Set up logging
def setup_logging(log_dir: str = "logs") -> None:
    """
    Configure logging to save evaluation logs to a file and print to console.
    
    Args:
        log_dir (str): Directory to save log files.
    """
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"evaluation_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.info("Logging setup complete. Evaluation logs will be saved to: %s", log_file)

# Placeholder for the model class (replace with actual model definition or import)
class AgentModel(nn.Module):
    """
    Placeholder for the AI model architecture used during evaluation.
    Replace this with the actual model class or import it from a separate module.
    """
    def __init__(self, input_size: int = 10, hidden_size: int = 64, output_size: int = 2):
        super(AgentModel, self).__init__()
        self.layer1 = nn.Linear(input_size, hidden_size)
        self.layer2 = nn.Linear(hidden_size, hidden_size)
        self.layer3 = nn.Linear(hidden_size, output_size)
        self.relu = nn.ReLU()
        self.softmax = nn.Softmax(dim=1)
    
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = self.relu(self.layer1(x))
        x = self.relu(self.layer2(x))
        x = self.layer3(x)
        x = self.softmax(x)
        return x

def load_model(model_path: str, device: str = "cpu") -> nn.Module:
    """
    Load a pre-trained model from the specified path.
    
    Args:
        model_path (str): Path to the saved model weights.
        device (str): Device to load the model on (cpu or cuda).
    
    Returns:
        nn.Module: Loaded model ready for evaluation.
    """
    try:
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Model file not found at: {model_path}")
        
        model = AgentModel()  # Replace with actual model initialization if different
        model.load_state_dict(torch.load(model_path, map_location=device))
        model.to(device)
        model.eval()
        logging.info("Model successfully loaded from: %s", model_path)
        return model
    except Exception as e:
        logging.error("Error loading model: %s", str(e))
        raise

def preprocess_data(data: Union[np.ndarray, pd.DataFrame, List], 
                    labels: Union[np.ndarray, pd.Series, List] = None,
                    feature_columns: List[str] = None) -> Tuple[np.ndarray, np.ndarray]:
    """
    Preprocess input data and labels for evaluation (e.g., normalization, handling missing values).
    
    Args:
        data: Input data as numpy array, pandas DataFrame, or list.
        labels: Ground truth labels for evaluation.
        feature_columns (List[str]): List of feature columns if data is a DataFrame.
    
    Returns:
        Tuple[np.ndarray, np.ndarray]: Preprocessed data and labels.
    """
    try:
        if isinstance(data, pd.DataFrame):
            if feature_columns:
                data = data[feature_columns].values
            else:
                data = data.values
        elif isinstance(data, list):
            data = np.array(data)
        
        if labels is not None:
            if isinstance(labels, pd.Series):
                labels = labels.values
            elif isinstance(labels, list):
                labels = np.array(labels)
        
        # Handle missing values (simple imputation with mean)
        if np.any(np.isnan(data)):
            data = np.nan_to_num(data, nan=np.nanmean(data, axis=0))
            logging.warning("Missing values detected in input data. Imputed with mean.")
        
        # Ensure data is 2D
        if len(data.shape) == 1:
            data = data.reshape(1, -1)
        
        if labels is not None and len(labels.shape) > 1:
            labels = labels.flatten()
        
        logging.info("Input data preprocessed successfully. Data shape: %s, Labels shape: %s", 
                     data.shape, labels.shape if labels is not None else "N/A")
        return data, labels
    except Exception as e:
        logging.error("Error preprocessing data: %s", str(e))
        raise

def create_dataloader(data: np.ndarray, labels: np.ndarray = None, batch_size: int = 32) -> DataLoader:
    """
    Create a DataLoader for batch processing of input data and labels.
    
    Args:
        data (np.ndarray): Preprocessed input data.
        labels (np.ndarray): Ground truth labels (optional).
        batch_size (int): Size of each batch for evaluation.
    
    Returns:
        DataLoader: PyTorch DataLoader for batch processing.
    """
    try:
        data_tensor = torch.FloatTensor(data)
        if labels is not None:
            labels_tensor = torch.LongTensor(labels)
            dataset = TensorDataset(data_tensor, labels_tensor)
        else:
            dataset = TensorDataset(data_tensor)
        dataloader = DataLoader(dataset, batch_size=batch_size, shuffle=False, num_workers=2)
        logging.info("DataLoader created with batch size: %d", batch_size)
        return dataloader
    except Exception as e:
        logging.error("Error creating DataLoader: %s", str(e))
        raise

def evaluate_model(model: nn.Module, dataloader: DataLoader, device: str = "cpu") -> Tuple[np.ndarray, np.ndarray]:
    """
    Perform evaluation on the test data using the loaded model.
    
    Args:
        model (nn.Module): Pre-trained model for evaluation.
        dataloader (DataLoader): DataLoader with test data and labels.
        device (str): Device to perform evaluation on (cpu or cuda).
    
    Returns:
        Tuple[np.ndarray, np.ndarray]: Predictions and true labels from the model.
    """
    try:
        predictions = []
        true_labels = []
        with torch.no_grad():
            for batch in dataloader:
                inputs = batch[0].to(device)
                outputs = model(inputs)
                preds = torch.argmax(outputs, dim=1).cpu().numpy()
                predictions.append(preds)
                if len(batch) > 1:
                    true_labels.append(batch[1].cpu().numpy())
        
        predictions = np.concatenate(predictions, axis=0)
        true_labels = np.concatenate(true_labels, axis=0) if true_labels else np.array([])
        logging.info("Evaluation completed. Predictions shape: %s, True labels shape: %s", 
                     predictions.shape, true_labels.shape if true_labels.size > 0 else "N/A")
        return predictions, true_labels
    except Exception as e:
        logging.error("Error during evaluation: %s", str(e))
        raise

def compute_metrics(predictions: np.ndarray, true_labels: np.ndarray) -> dict:
    """
    Compute evaluation metrics like accuracy, precision, recall, and F1-score.
    
    Args:
        predictions (np.ndarray): Model predictions.
        true_labels (np.ndarray): Ground truth labels.
    
    Returns:
        dict: Dictionary of computed metrics.
    """
    try:
        metrics = {
            "accuracy": accuracy_score(true_labels, predictions),
            "precision": precision_score(true_labels, predictions, average="weighted", zero_division=0),
            "recall": recall_score(true_labels, predictions, average="weighted", zero_division=0),
            "f1_score": f1_score(true_labels, predictions, average="weighted", zero_division=0)
        }
        logging.info("Computed metrics: %s", metrics)
        return metrics
    except Exception as e:
        logging.error("Error computing metrics: %s", str(e))
        raise

def plot_confusion_matrix(predictions: np.ndarray, true_labels: np.ndarray, output_dir: str) -> None:
    """
    Plot and save confusion matrix as a heatmap.
    
    Args:
        predictions (np.ndarray): Model predictions.
        true_labels (np.ndarray): Ground truth labels.
        output_dir (str): Directory to save the plot.
    """
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        cm = confusion_matrix(true_labels, predictions)
        plt.figure(figsize=(10, 8))
        sns.heatmap(cm, annot=True, fmt="d", cmap="Blues")
        plt.title("Confusion Matrix")
        plt.xlabel("Predicted")
        plt.ylabel("True")
        output_path = os.path.join(output_dir, "confusion_matrix.png")
        plt.savefig(output_path)
        plt.close()
        logging.info("Confusion matrix saved to: %s", output_path)
    except Exception as e:
        logging.error("Error plotting confusion matrix: %s", str(e))
        raise

def save_evaluation_results(metrics: dict, classification_rep: str, output_path: str) -> None:
    """
    Save evaluation metrics and classification report to a file (JSON or TXT).
    
    Args:
        metrics (dict): Computed evaluation metrics.
        classification_rep (str): Classification report as a string.
        output_path (str): Path to save the evaluation results.
    """
    try:
        results = {
            "metrics": metrics,
            "classification_report": classification_rep
        }
        if output_path.endswith(".json"):
            with open(output_path, "w") as f:
                json.dump(results, f, indent=2)
        else:
            with open(output_path, "w") as f:
                f.write("Evaluation Metrics:\n")
                for key, value in metrics.items():
                    f.write(f"{key}: {value:.4f}\n")
                f.write("\nClassification Report:\n")
                f.write(classification_rep)
        logging.info("Evaluation results saved to: %s", output_path)
    except Exception as e:
        logging.error("Error saving evaluation results: %s", str(e))
        raise

def main():
    """
    Main function to run the evaluation pipeline.
    """
    parser = argparse.ArgumentParser(description="Evaluation script for AI model performance.")
    parser.add_argument("--model_path", type=str, required=True, 
                        help="Path to the pre-trained model file.")
    parser.add_argument("--test_data", type=str, required=True, 
                        help="Path to test data file (CSV).")
    parser.add_argument("--test_labels", type=str, required=True, 
                        help="Path to test labels file (CSV) or column name in test data.")
    parser.add_argument("--output_dir", type=str, default="evaluation_results", 
                        help="Directory to save evaluation results and plots.")
    parser.add_argument("--batch_size", type=int, default=32, 
                        help="Batch size for evaluation.")
    parser.add_argument("--device", type=str, default="cpu", 
                        choices=["cpu", "cuda"], help="Device to run evaluation on.")
    parser.add_argument("--log_dir", type=str, default="logs", 
                        help="Directory to save evaluation logs.")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_dir)
    
    try:
        # Check if CUDA is available if specified
        if args.device == "cuda" and not torch.cuda.is_available():
            logging.warning("CUDA not available. Falling back to CPU.")
            args.device = "cpu"
        
        logging.info("Starting evaluation pipeline with device: %s", args.device)
        
        # Load test data and labels
        if args.test_data.endswith(".csv"):
            test_data = pd.read_csv(args.test_data)
            if args.test_labels.endswith(".csv"):
                test_labels = pd.read_csv(args.test_labels).values.flatten()
            else:
                test_labels = test_data[args.test_labels].values
                test_data = test_data.drop(columns=[args.test_labels])
        else:
            raise ValueError("Only CSV test data is supported for now.")
        logging.info("Test data and labels loaded from: %s, %s", args.test_data, args.test_labels)
        
        # Preprocess data
        processed_data, processed_labels = preprocess_data(test_data, test_labels)
        
        # Create DataLoader for batch processing
        dataloader = create_dataloader(processed_data, processed_labels, batch_size=args.batch_size)
        
        # Load model
        model = load_model(args.model_path, device=args.device)
        
        # Perform evaluation
        predictions, true_labels = evaluate_model(model, dataloader, device=args.device)
        
        # Compute metrics
        metrics = compute_metrics(predictions, true_labels)
        
        # Generate classification report
        class_report = classification_report(true_labels, predictions, zero_division=0, output_dict=False)
        logging.info("Classification Report:\n%s", class_report)
        
        # Plot confusion matrix
        plot_confusion_matrix(predictions, true_labels, args.output_dir)
        
        # Save evaluation results
        if not os.path.exists(args.output_dir):
            os.makedirs(args.output_dir)
        results_path = os.path.join(args.output_dir, "evaluation_results.json")
        save_evaluation_results(metrics, class_report, results_path)
        
        logging.info("Evaluation pipeline completed successfully.")
    except Exception as e:
        logging.error("Evaluation pipeline failed: %s", str(e))
        sys.exit(1)

if __name__ == "__main__":
    main()
