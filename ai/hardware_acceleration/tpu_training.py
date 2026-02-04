""" 
tpu_training.py     
A script for training deep learning models on Google TPUs for faster computation.
This script uses TensorFlow's TPU distribution strategy to leverage the power of TPUs.
It includes data loading, model creation, training, and evaluation on a sample dataset (MNIST).
Ensure you run this in an environment with TPU access (e.g., Google Colab or Google Cloud).
"""

import tensorflow as tf
import tensorflow_datasets as tfds
import os
import logging

# Set up logging for debugging and information
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def initialize_tpu():
    """
    Initialize the TPU system and set up the TPU strategy for distributed training.
    Returns the TPU strategy object if successful, otherwise raises an exception.
    """
    try:
        # Detect and initialize the TPU
        tpu = tf.distribute.cluster_resolver.TPUClusterResolver()
        logger.info(f"Running on TPU: {tpu.master()}")
        
        # Connect to the TPU cluster
        tf.config.experimental_connect_to_cluster(tpu)
        tf.tpu.experimental.initialize_tpu_system(tpu)
        
        # Create a TPU strategy for distributed training
        strategy = tf.distribute.TPUStrategy(tpu)
        logger.info("TPU strategy initialized successfully.")
        return strategy
    except ValueError as e:
        logger.error("TPU initialization failed. Ensure TPU runtime is available.")
        logger.error(f"Error: {e}")
        raise Exception("TPU not found or initialization failed. Check environment setup.")

def load_and_preprocess_data(batch_size=128):
    """
    Load and preprocess the MNIST dataset for training and evaluation.
    Returns preprocessed training and test datasets compatible with TPU training.
    Args:
        batch_size (int): Batch size for training and evaluation.
    Returns:
        train_dataset: Preprocessed training dataset.
        test_dataset: Preprocessed test dataset.
        info: Dataset info.
    """
    try:
        # Load MNIST dataset from TensorFlow Datasets
        ds_train, info = tfds.load('mnist', split='train', as_supervised=True, with_info=True)
        ds_test = tfds.load('mnist', split='test', as_supervised=True)
        
        def preprocess_data(ds):
            # Convert images to float32 and normalize to [0, 1]
            ds = ds.map(lambda image, label: (tf.cast(image, tf.float32) / 255.0, label))
            # Cache the dataset for faster access
            ds = ds.cache()
            # Shuffle the dataset
            ds = ds.shuffle(10000)
            # Batch the dataset (batch size must be compatible with TPU)
            ds = ds.batch(batch_size, drop_remainder=True)
            # Prefetch for performance
            ds = ds.prefetch(tf.data.AUTOTUNE)
            return ds
        
        train_dataset = preprocess_data(ds_train)
        test_dataset = preprocess_data(ds_test)
        
        logger.info("Dataset loaded and preprocessed successfully.")
        return train_dataset, test_dataset, info
    except Exception as e:
        logger.error("Failed to load or preprocess dataset.")
        logger.error(f"Error: {e}")
        raise Exception("Dataset loading or preprocessing failed.")

def create_model(input_shape=(28, 28, 1), num_classes=10):
    """
    Create a simple convolutional neural network model for MNIST classification.
    Args:
        input_shape (tuple): Shape of input images.
        num_classes (int): Number of output classes.
    Returns:
        model: Compiled Keras model.
    """
    try:
        model = tf.keras.Sequential([
            tf.keras.layers.Conv2D(32, (3, 3), activation='relu', input_shape=input_shape),
            tf.keras.layers.MaxPooling2D((2, 2)),
            tf.keras.layers.Conv2D(64, (3, 3), activation='relu'),
            tf.keras.layers.MaxPooling2D((2, 2)),
            tf.keras.layers.Flatten(),
            tf.keras.layers.Dense(128, activation='relu'),
            tf.keras.layers.Dropout(0.5),
            tf.keras.layers.Dense(num_classes, activation='softmax')
        ])
        logger.info("Model created successfully.")
        return model
    except Exception as e:
        logger.error("Failed to create model.")
        logger.error(f"Error: {e}")
        raise Exception("Model creation failed.")

def compile_and_train_model(strategy, train_dataset, test_dataset, epochs=5):
    """
    Compile and train the model using the TPU strategy.
    Args:
        strategy: TPU distribution strategy.
        train_dataset: Preprocessed training dataset.
        test_dataset: Preprocessed test dataset.
        epochs (int): Number of training epochs.
    Returns:
        history: Training history object.
    """
    try:
        with strategy.scope():
            model = create_model()
            # Compile the model with appropriate optimizer and loss for TPU
            model.compile(
                optimizer=tf.keras.optimizers.Adam(learning_rate=1e-3),
                loss=tf.keras.losses.SparseCategoricalCrossentropy(),
                metrics=['accuracy']
            )
        
        logger.info("Starting model training on TPU...")
        history = model.fit(
            train_dataset,
            validation_data=test_dataset,
            epochs=epochs,
            verbose=1
        )
        logger.info("Model training completed successfully.")
        return history
    except Exception as e:
        logger.error("Failed to compile or train model on TPU.")
        logger.error(f"Error: {e}")
        raise Exception("Model training failed.")

def evaluate_model(strategy, model, test_dataset):
    """
    Evaluate the trained model on the test dataset.
    Args:
        strategy: TPU distribution strategy.
        model: Trained Keras model.
        test_dataset: Preprocessed test dataset.
    Returns:
        test_loss (float): Loss on test dataset.
        test_accuracy (float): Accuracy on test dataset.
    """
    try:
        with strategy.scope():
            test_loss, test_accuracy = model.evaluate(test_dataset, verbose=0)
        logger.info(f"Test Loss: {test_loss:.4f}, Test Accuracy: {test_accuracy:.4f}")
        return test_loss, test_accuracy
    except Exception as e:
        logger.error("Failed to evaluate model.")
        logger.error(f"Error: {e}")
        raise Exception("Model evaluation failed.")

def main():
    """
    Main function to orchestrate TPU training workflow.
    """
    try:
        # Initialize TPU strategy
        strategy = initialize_tpu()
        
        # Load and preprocess data
        batch_size_per_replica = 128
        num_replicas = strategy.num_replicas_in_sync
        global_batch_size = batch_size_per_replica * num_replicas
        logger.info(f"Global batch size: {global_batch_size} (Replicas: {num_replicas})")
        train_dataset, test_dataset, _ = load_and_preprocess_data(batch_size=global_batch_size)
        
        # Train the model
        history = compile_and_train_model(strategy, train_dataset, test_dataset, epochs=5)
        
        # Optionally, evaluate the model (already done in training with validation_data)
        logger.info("Training completed. Final results are logged above.")
        
    except Exception as e:
        logger.error("An error occurred in the main workflow.")
        logger.error(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()
